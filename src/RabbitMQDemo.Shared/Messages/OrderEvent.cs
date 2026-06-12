namespace RabbitMQDemo.Shared.Messages;

/// <summary>
/// 订单事件消息体 —— 演示用业务消息。
/// </summary>
public sealed record OrderEvent(
    Guid OrderId,
    string ProductName,
    int Quantity,
    decimal Price,
    DateTime CreatedAt,
    string EventType);
