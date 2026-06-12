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
/// Topic 消费者 —— 按通配符模式接收所有订单事件。
/// 绑定路由键 "order.*" 匹配所有订单事件。
/// </summary>
public sealed class TopicConsumerWorker : RabbitMQConsumerService
{
    public TopicConsumerWorker(
        RabbitMQConnectionFactory connectionFactory,
        IMessageSerializer serializer,
        IOptions<RabbitMQOptions> options,
        IIdempotencyChecker idempotencyChecker,
        MessageProcessor messageProcessor,
        ILogger<TopicConsumerWorker> logger,
        IMessageTracker messageTracker)
        : base(connectionFactory, serializer, options, idempotencyChecker,
              messageProcessor, logger, "RabbitMQDemo.Subscriber.Topic",
              messageTracker)
    {
    }

    protected override string ExchangeName => ExchangeNames.Topic;
    protected override string ExchangeTypeName => ExchangeType.Topic;
    protected override string QueueName => QueueNames.Topic;
    protected override string BindingRoutingKey => RoutingKeys.AllOrders;

    protected override Task<bool> ProcessMessageCoreAsync(byte[] body, CancellationToken cancellationToken)
    {
        var envelope = _serializer.Deserialize<MessageEnvelope<OrderEvent>>(body);
        var o = envelope.Body;

        _logger.LogInformation(
            "📥 [Topic] 收到订单事件 (模式匹配 order.*): " +
            "OrderId={OrderId}, {Product}, EventType={EventType}, Price=${Price}",
            o.OrderId, o.ProductName, o.EventType, o.Price);

        return Task.FromResult(true);
    }
}
