using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQDemo.Infrastructure.Configuration;
using RabbitMQDemo.Infrastructure.Connection;
using RabbitMQDemo.Infrastructure.Serialization;
using RabbitMQDemo.Shared.Constants;
using RabbitMQDemo.Shared.Interfaces;
using RabbitMQDemo.Shared.Messages;
using RabbitMQDemo.Infrastructure.Tracking;

namespace RabbitMQDemo.Infrastructure.Publishing;

/// <summary>
/// RabbitMQ 发布者封装 —— 支持三种交换机类型、Publisher Confirms、
/// mandatory + BasicReturn 回调、TraceContext 注入、消息持久化。
/// </summary>
public sealed class RabbitMQPublisher : IAsyncDisposable
{
    private readonly RabbitMQConnectionFactory _connectionFactory;
    private readonly IMessageSerializer _serializer;
    private readonly IOptions<RabbitMQOptions> _options;
    private readonly ILogger<RabbitMQPublisher> _logger;
    private readonly IMessageTracker _messageTracker;
    private readonly ActivitySource _activitySource = new("RabbitMQDemo.Publisher");

    private IChannel? _publishChannel;
    private bool _disposed;

    public RabbitMQPublisher(
        RabbitMQConnectionFactory connectionFactory,
        IMessageSerializer serializer,
        IOptions<RabbitMQOptions> options,
        ILogger<RabbitMQPublisher> logger,
        IMessageTracker? messageTracker = null)
    {
        _connectionFactory = connectionFactory;
        _serializer = serializer;
        _options = options;
        _logger = logger;
        _messageTracker = messageTracker ?? new NullMessageTracker();
    }

    /// <summary>
    /// 初始化发布者：声明三种业务交换机 + 备用交换机（AE），注册 BasicReturn 回调。
    /// </summary>
    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        // 创建 Channel — 若有 Publisher Confirms 需求则使用确认模式
        _publishChannel = _options.Value.PublisherConfirms
            ? await _connectionFactory.CreatePublisherConfirmChannelAsync(cancellationToken)
                .ConfigureAwait(false)
            : await _connectionFactory.CreateChannelAsync(cancellationToken)
                .ConfigureAwait(false);

        // 注册 BasicReturn 回调（mandatory 路由失败时触发）
        _publishChannel.BasicReturnAsync += async (_, args) =>
        {
            _logger.LogWarning(
                "消息路由失败 (mandatory): Exchange={Exchange}, RoutingKey={RoutingKey}, " +
                "ReplyCode={ReplyCode}, ReplyText={ReplyText}",
                args.Exchange, args.RoutingKey, args.ReplyCode, args.ReplyText);
            await Task.CompletedTask.ConfigureAwait(false);
        };

        // 声明备用交换机（AE）和备用队列
        await DeclareAlternateExchangeAsync(cancellationToken).ConfigureAwait(false);

        // 声明业务交换机（均绑定备用交换机）
        await _publishChannel.ExchangeDeclareAsync(
            exchange: ExchangeNames.Fanout,
            type: ExchangeType.Fanout,
            durable: true,
            arguments: new Dictionary<string, object?>
            {
                { "alternate-exchange", ExchangeNames.AlternateExchange }
            },
            cancellationToken: cancellationToken).ConfigureAwait(false);

        await _publishChannel.ExchangeDeclareAsync(
            exchange: ExchangeNames.Direct,
            type: ExchangeType.Direct,
            durable: true,
            arguments: new Dictionary<string, object?>
            {
                { "alternate-exchange", ExchangeNames.AlternateExchange }
            },
            cancellationToken: cancellationToken).ConfigureAwait(false);

        await _publishChannel.ExchangeDeclareAsync(
            exchange: ExchangeNames.Topic,
            type: ExchangeType.Topic,
            durable: true,
            arguments: new Dictionary<string, object?>
            {
                { "alternate-exchange", ExchangeNames.AlternateExchange }
            },
            cancellationToken: cancellationToken).ConfigureAwait(false);

        // 注册 Publisher Confirms 回调（Channel 已通过 CreateChannelOptions 启用确认模式）
        _publishChannel.BasicAcksAsync += (_, args) =>
        {
            _logger.LogDebug(
                "发布确认成功: DeliveryTag={DeliveryTag}, Multiple={Multiple}",
                args.DeliveryTag, args.Multiple);
            return Task.CompletedTask;
        };
        _publishChannel.BasicNacksAsync += (_, args) =>
        {
            _logger.LogWarning(
                "发布确认失败: DeliveryTag={DeliveryTag}, Multiple={Multiple}",
                args.DeliveryTag, args.Multiple);
            return Task.CompletedTask;
        };

        _logger.LogInformation("发布者初始化完成，Channel: {Channel}", _publishChannel.ChannelNumber);
    }

    /// <summary>
    /// 发布消息到指定交换机。
    /// </summary>
    /// <param name="exchange">交换机名称。</param>
    /// <param name="routingKey">路由键。</param>
    /// <param name="envelope">消息信封。</param>
    /// <param name="bodyType">消息体类型（如 "Created"/"Paid"/"Shipped"），用于追踪展示。</param>
    /// <param name="bodySummary">消息体摘要（如 "笔记本 x3 = $299.50"），用于追踪展示。</param>
    /// <param name="amount">订单金额，用于追踪统计。</param>
    /// <param name="orderId">订单 ID — 用于关联同一订单的事件推进类型标签。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    public async Task PublishAsync<T>(
        string exchange,
        string routingKey,
        MessageEnvelope<T> envelope,
        string? bodyType = null,
        string? bodySummary = null,
        decimal? amount = null,
        string? orderId = null,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ObjectDisposedException.ThrowIf(_publishChannel is null, this);

        using var activity = _activitySource.StartActivity(
            ActivityKind.Producer,
            name: "Publish");

        activity?.SetTag("message.id", envelope.MessageId);
        activity?.SetTag("messaging.system", "rabbitmq");
        activity?.SetTag("messaging.destination", exchange);
        activity?.SetTag("messaging.rabbitmq.routing_key", routingKey);

        var body = _serializer.Serialize(envelope);
        var props = new BasicProperties
        {
            MessageId = envelope.MessageId,
            Timestamp = new AmqpTimestamp(new DateTimeOffset(envelope.Timestamp).ToUnixTimeSeconds()),
            ContentType = "application/json",
            ContentEncoding = "utf-8",
            DeliveryMode = _options.Value.DeliveryMode == 1 ? DeliveryModes.Transient : DeliveryModes.Persistent,
            Headers = new Dictionary<string, object?>
            {
                { "source", envelope.Source }
            }
        };

        // 注入 W3C TraceContext
        var currentActivity = Activity.Current;
        if (currentActivity?.Id is not null)
        {
            props.Headers ??= new Dictionary<string, object?>();
            props.Headers["traceparent"] = currentActivity.Id;
            if (currentActivity.TraceStateString is not null)
                props.Headers["tracestate"] = currentActivity.TraceStateString;
        }

        await _publishChannel.BasicPublishAsync(
            exchange: exchange,
            routingKey: routingKey,
            mandatory: _options.Value.Mandatory,
            basicProperties: props,
            body: body,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        _logger.LogInformation(
            "消息已发布: Exchange={Exchange}, RoutingKey={RoutingKey}, " +
            "MessageId={MessageId}, BodySize={BodySize} bytes",
            exchange, routingKey, envelope.MessageId, body.Length);

        // 异步追踪（fire-and-forget，不阻塞主流程）
        _ = TrackPublishAsync(exchange, routingKey, envelope, bodyType, bodySummary, amount, orderId, body.Length);
    }

    private async Task DeclareAlternateExchangeAsync(CancellationToken cancellationToken)
    {
        if (_publishChannel is null) return;

        // 声明备用交换机（Fanout — 路由失败的消息广播给绑定的队列）
        await _publishChannel.ExchangeDeclareAsync(
            exchange: ExchangeNames.AlternateExchange,
            type: ExchangeType.Fanout,
            durable: true,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        // 声明备用队列
        await _publishChannel.QueueDeclareAsync(
            queue: QueueNames.Alternate,
            durable: true,
            exclusive: false,
            autoDelete: false,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        // 绑定备用队列到备用交换机
        await _publishChannel.QueueBindAsync(
            queue: QueueNames.Alternate,
            exchange: ExchangeNames.AlternateExchange,
            routingKey: string.Empty,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        _logger.LogInformation("备用交换机 AE 已声明: {Exchange}", ExchangeNames.AlternateExchange);
    }

    private async Task TrackPublishAsync<T>(
        string exchange,
        string routingKey,
        MessageEnvelope<T> envelope,
        string? bodyType,
        string? bodySummary,
        decimal? amount,
        string? orderId,
        int bodySize)
    {
        try
        {
            await _messageTracker.RecordPublishedAsync(new MessageRecord
            {
                MessageId = envelope.MessageId,
                OrderId = orderId,
                Exchange = exchange,
                RoutingKey = routingKey,
                BodyType = bodyType ?? typeof(T).Name,
                OriginalBodyType = bodyType,
                BodySummary = bodySummary ?? envelope.Body?.ToString() ?? "",
                Amount = amount,
                PublishedAt = envelope.Timestamp,
                BodySize = bodySize
            }).ConfigureAwait(false);
        }
        catch
        {
            // 追踪异常静默处理，不影响主流程
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        _activitySource.Dispose();

        if (_publishChannel is not null)
        {
            await _publishChannel.CloseAsync().ConfigureAwait(false);
            await _publishChannel.DisposeAsync().ConfigureAwait(false);
        }
    }
}
