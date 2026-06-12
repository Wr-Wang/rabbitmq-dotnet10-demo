using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQDemo.Infrastructure.Configuration;
using RabbitMQDemo.Infrastructure.Publishing;
using RabbitMQDemo.Shared.Constants;
using RabbitMQDemo.Shared.Messages;

namespace RabbitMQDemo.Publisher;

/// <summary>
/// 发布者后台服务 —— 每 3 秒生成一条随机订单消息，
/// 分别通过 Fanout、Direct、Topic 三种交换机发送。
/// </summary>
public sealed class PublisherWorker : BackgroundService
{
    private readonly RabbitMQPublisher _publisher;
    private readonly IOptions<RabbitMQOptions> _options;
    private readonly ILogger<PublisherWorker> _logger;

    private static readonly string[] ProductNames =
        ["笔记本电脑", "机械键盘", "显示器", "无线鼠标", "USB-C 扩展坞"];

    private static readonly string[] EventTypes =
        ["Created", "Paid", "Shipped"];

    // 模拟订单生命周期推进 —— 记录活跃订单及其当前状态
    private static readonly List<(Guid OrderId, string ProductName, int Quantity, decimal Price, string State)> _activeOrders = [];
    private static readonly object _orderLock = new();

    public PublisherWorker(
        RabbitMQPublisher publisher,
        IOptions<RabbitMQOptions> options,
        ILogger<PublisherWorker> logger)
    {
        _publisher = publisher;
        _options = options;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("发布者启动，每 3 秒发送一条消息（模拟订单 Created→Paid→Shipped 生命周期）");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // 60% 概率创建新订单，40% 概率推进已有订单状态
                var shouldAdvance = _activeOrders.Count > 0 && Random.Shared.Next(100) < 40;
                if (shouldAdvance)
                {
                    await AdvanceExistingOrderAsync(stoppingToken).ConfigureAwait(false);
                }
                else
                {
                    await CreateNewOrderAsync(stoppingToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "发布消息时发生异常");
            }

            await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken).ConfigureAwait(false);
        }
    }

    private async Task CreateNewOrderAsync(CancellationToken cancellationToken)
    {
        var productName = ProductNames[Random.Shared.Next(ProductNames.Length)];
        var quantity = Random.Shared.Next(1, 10);
        var price = Math.Round((decimal)(Random.Shared.NextDouble() * 1000 + 100), 2);
        var totalPrice = Math.Round(quantity * price, 2);
        var orderId = Guid.NewGuid();

        var orderEvent = new OrderEvent(
            OrderId: orderId,
            ProductName: productName,
            Quantity: quantity,
            Price: price,
            CreatedAt: DateTime.UtcNow,
            EventType: "Created");

        var bodySummary = $"{productName} x{quantity} = ${totalPrice}";
        await PublishSingleEventAsync(orderEvent, orderId, "Created", bodySummary, totalPrice, cancellationToken)
            .ConfigureAwait(false);

        lock (_orderLock)
            _activeOrders.Add((orderId, productName, quantity, price, "Created"));

        _logger.LogInformation("🆕 新订单创建: OrderId={OrderId}, {Product} x{Quantity} = ${Total}",
            orderId, productName, quantity, totalPrice);
    }

    private async Task AdvanceExistingOrderAsync(CancellationToken cancellationToken)
    {
        (Guid OrderId, string ProductName, int Quantity, decimal Price, string State) order;
        lock (_orderLock)
        {
            // 只取可以推进的订单（非 Shipped）
            var candidates = _activeOrders.Where(o => o.State != "Shipped").ToList();
            if (candidates.Count == 0) return;
            order = candidates[Random.Shared.Next(candidates.Count)];
        }

        var nextState = order.State switch
        {
            "Created" => "Paid",
            "Paid" => "Shipped",
            _ => "Shipped"
        };

        var totalPrice = Math.Round(order.Quantity * order.Price, 2);
        var bodySummary = $"{order.ProductName} x{order.Quantity} = ${totalPrice}";

        var orderEvent = new OrderEvent(
            OrderId: order.OrderId,
            ProductName: order.ProductName,
            Quantity: order.Quantity,
            Price: order.Price,
            CreatedAt: DateTime.UtcNow,
            EventType: nextState);

        await PublishSingleEventAsync(orderEvent, order.OrderId, nextState, bodySummary, totalPrice, cancellationToken)
            .ConfigureAwait(false);

        lock (_orderLock)
        {
            var idx = _activeOrders.FindIndex(o => o.OrderId == order.OrderId);
            if (idx >= 0)
            {
                if (nextState == "Shipped")
                    _activeOrders.RemoveAt(idx); // Shipped 后移出活跃列表
                else
                    _activeOrders[idx] = (order.OrderId, order.ProductName, order.Quantity, order.Price, nextState);
            }
        }

        _logger.LogInformation(
            "🔄 订单状态推进: OrderId={OrderId}, {State}, {Product} x{Quantity} = ${Total}",
            order.OrderId, nextState, order.ProductName, order.Quantity, totalPrice);
    }

    private async Task PublishSingleEventAsync(
        OrderEvent orderEvent, Guid orderId, string eventType,
        string bodySummary, decimal totalPrice,
        CancellationToken cancellationToken)
    {
        var source = $"publisher-{Environment.MachineName}";
        var envelope = new MessageEnvelope<OrderEvent>(
            MessageId: Guid.NewGuid().ToString("N"),
            Version: "1.0",
            Timestamp: DateTime.UtcNow,
            Source: source,
            Body: orderEvent);

        var routingKey = eventType switch
        {
            "Created" => RoutingKeys.Created,
            "Paid" => RoutingKeys.Paid,
            "Shipped" => RoutingKeys.Shipped,
            _ => RoutingKeys.AllOrders
        };

        // 3 种交换机各发一次，共享同一 OrderId 用于 Dashboard 类型标签推进
        await _publisher.PublishAsync(
            ExchangeNames.Fanout, string.Empty, envelope,
            bodyType: eventType, bodySummary: bodySummary, amount: totalPrice, orderId: orderId.ToString(),
            cancellationToken: cancellationToken).ConfigureAwait(false);

        await _publisher.PublishAsync(
            ExchangeNames.Direct, routingKey, envelope,
            bodyType: eventType, bodySummary: bodySummary, amount: totalPrice, orderId: orderId.ToString(),
            cancellationToken: cancellationToken).ConfigureAwait(false);

        await _publisher.PublishAsync(
            ExchangeNames.Topic, routingKey, envelope,
            bodyType: eventType, bodySummary: bodySummary, amount: totalPrice, orderId: orderId.ToString(),
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }
}
