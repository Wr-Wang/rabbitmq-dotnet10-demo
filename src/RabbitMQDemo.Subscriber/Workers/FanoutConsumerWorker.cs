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
/// Fanout 消费者 —— 接收所有订单事件（广播）。
/// </summary>
public sealed class FanoutConsumerWorker : RabbitMQConsumerService
{
    public FanoutConsumerWorker(
        RabbitMQConnectionFactory connectionFactory,
        IMessageSerializer serializer,
        IOptions<RabbitMQOptions> options,
        IIdempotencyChecker idempotencyChecker,
        MessageProcessor messageProcessor,
        ILogger<FanoutConsumerWorker> logger,
        IMessageTracker messageTracker)
        : base(connectionFactory, serializer, options, idempotencyChecker,
              messageProcessor, logger, "RabbitMQDemo.Subscriber.Fanout",
              messageTracker)
    {
    }

    protected override string ExchangeName => ExchangeNames.Fanout;
    protected override string ExchangeTypeName => ExchangeType.Fanout;
    protected override string QueueName => QueueNames.Fanout;
    protected override string BindingRoutingKey => string.Empty;

    protected override Task<bool> ProcessMessageCoreAsync(byte[] body, CancellationToken cancellationToken)
    {
        var envelope = _serializer.Deserialize<MessageEnvelope<OrderEvent>>(body);
        var o = envelope.Body;

        _logger.LogInformation(
            "📥 [Fanout] 收到所有订单事件: " +
            "OrderId={OrderId}, {Product} x{Quantity} = ${Price}, EventType={EventType}",
            o.OrderId, o.ProductName, o.Quantity, o.Price, o.EventType);

        return Task.FromResult(true);
    }
}
