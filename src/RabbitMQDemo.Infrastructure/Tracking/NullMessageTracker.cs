using RabbitMQDemo.Shared.Interfaces;
using RabbitMQDemo.Shared.Messages;

namespace RabbitMQDemo.Infrastructure.Tracking;

/// <summary>
/// 空对象模式的追踪器 —— 当未配置 Dashboard 时使用，避免 null 检查。
/// </summary>
public sealed class NullMessageTracker : IMessageTracker
{
    public Task RecordPublishedAsync(MessageRecord record, CancellationToken ct = default) => Task.CompletedTask;
    public Task RecordConsumedAsync(string messageId, string consumerQueue, string consumerWorker, CancellationToken ct = default) => Task.CompletedTask;
    public Task RecordDeadLetteredAsync(string messageId, string consumerQueue, string errorInfo, CancellationToken ct = default) => Task.CompletedTask;
    public Task RecordUnroutableAsync(string messageId, string exchange, string routingKey, string errorInfo, CancellationToken ct = default) => Task.CompletedTask;
}
