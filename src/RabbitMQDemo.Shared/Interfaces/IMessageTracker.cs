using RabbitMQDemo.Shared.Messages;

namespace RabbitMQDemo.Shared.Interfaces;

/// <summary>
/// 消息追踪接口 —— 记录消息从发布到消费的完整生命周期。
/// 实现与存储解耦（当前为 HTTP 推送到 Dashboard）。
/// </summary>
public interface IMessageTracker
{
    /// <summary>记录消息已发布。</summary>
    Task RecordPublishedAsync(MessageRecord record, CancellationToken ct = default);

    /// <summary>记录消息已消费成功。</summary>
    Task RecordConsumedAsync(string messageId, string consumerQueue, string consumerWorker, CancellationToken ct = default);

    /// <summary>记录消息已进入死信队列。</summary>
    Task RecordDeadLetteredAsync(string messageId, string consumerQueue, string errorInfo, CancellationToken ct = default);

    /// <summary>记录消息路由失败（备用交换机）。</summary>
    Task RecordUnroutableAsync(string messageId, string exchange, string routingKey, string errorInfo, CancellationToken ct = default);
}
