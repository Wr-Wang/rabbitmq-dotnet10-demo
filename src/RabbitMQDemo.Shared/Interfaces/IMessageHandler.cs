namespace RabbitMQDemo.Shared.Interfaces;

/// <summary>
/// 消息处理器 —— 处理接收到的业务消息。
/// 返回 true 表示处理成功，false 表示需要重试或入死信队列。
/// </summary>
public interface IMessageHandler<in T>
{
    Task<bool> HandleAsync(T message, CancellationToken cancellationToken = default);
}
