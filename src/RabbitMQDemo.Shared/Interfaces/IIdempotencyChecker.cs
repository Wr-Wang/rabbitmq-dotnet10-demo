namespace RabbitMQDemo.Shared.Interfaces;

/// <summary>
/// 幂等检查器接口。
/// 通过 MessageId 判断消息是否已被处理，防止重复消费。
/// </summary>
public interface IIdempotencyChecker
{
    Task<bool> IsProcessedAsync(string messageId);
    Task MarkAsProcessedAsync(string messageId);
}
