using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQDemo.Infrastructure.Configuration;
using RabbitMQDemo.Infrastructure.Connection;
using RabbitMQDemo.Infrastructure.Consuming;
using RabbitMQDemo.Infrastructure.Serialization;
using RabbitMQDemo.Shared.Constants;
using RabbitMQDemo.Shared.Interfaces;
using RabbitMQDemo.Shared.Messages;

namespace RabbitMQDemo.Subscriber.Workers;

/// <summary>
/// Direct 消费者 —— 按 EventType 精确路由接收订单创建事件。
/// </summary>
public sealed class DirectConsumerWorker : RabbitMQConsumerService
{
    public DirectConsumerWorker(
        RabbitMQConnectionFactory connectionFactory,
        IMessageSerializer serializer,
        IOptions<RabbitMQOptions> options,
        IIdempotencyChecker idempotencyChecker,
        MessageProcessor messageProcessor,
        ILogger<DirectConsumerWorker> logger,
        IMessageTracker messageTracker)
        : base(connectionFactory, serializer, options, idempotencyChecker,
              messageProcessor, logger, "RabbitMQDemo.Subscriber.Direct",
              messageTracker)
    {
    }

    protected override string ExchangeName => ExchangeNames.Direct;
    protected override string ExchangeTypeName => ExchangeType.Direct;
    protected override string QueueName => QueueNames.DirectCreated;
    protected override string BindingRoutingKey => RoutingKeys.Created;

    protected override Task<bool> ProcessMessageCoreAsync(byte[] body, CancellationToken cancellationToken)
    {
        var envelope = _serializer.Deserialize<MessageEnvelope<OrderEvent>>(body);
        var o = envelope.Body;

        _logger.LogInformation(
            "📥 [Direct] 收到订单创建事件: " +
            "OrderId={OrderId}, {Product} x{Quantity} = ${Price}",
            o.OrderId, o.ProductName, o.Quantity, o.Price);

        return Task.FromResult(true);
    }
}
