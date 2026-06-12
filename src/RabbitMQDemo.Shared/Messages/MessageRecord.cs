namespace RabbitMQDemo.Shared.Messages;

/// <summary>
/// 消息生命周期状态。
/// </summary>
public static class MessageStates
{
    /// <summary>已发布（未被消费）。</summary>
    public const string Published = "Published";
    /// <summary>已消费成功。</summary>
    public const string Consumed = "Consumed";
    /// <summary>已进入死信队列（消费失败）。</summary>
    public const string DeadLettered = "DeadLettered";
    /// <summary>路由失败（备用交换机）。</summary>
    public const string Unroutable = "Unroutable";
}

/// <summary>
/// 消息追踪记录 —— 记录一条消息从发布到消费的完整生命周期。
/// </summary>
public sealed class MessageRecord
{
    /// <summary>全局唯一消息 ID。</summary>
    public string MessageId { get; init; } = string.Empty;

    /// <summary>订单 ID — 用于关联同一订单的不同事件（Created→Paid→Shipped 推进）。</summary>
    public string? OrderId { get; init; }

    /// <summary>交换机名称。</summary>
    public string Exchange { get; init; } = string.Empty;

    /// <summary>路由键。</summary>
    public string RoutingKey { get; init; } = string.Empty;

    /// <summary>消息体类型（如 "Created" / "Paid" / "Shipped"）。保持发布时的原始值，不做自动推进。</summary>
    public string BodyType { get; set; } = string.Empty;

    /// <summary>原始消息体类型（发布时的初始值，永不改变）。</summary>
    public string? OriginalBodyType { get; init; }

    /// <summary>消息体摘要（如 "笔记本 x3 = $299.50"）。</summary>
    public string BodySummary { get; init; } = string.Empty;

    /// <summary>订单金额（用于统计）。</summary>
    public decimal? Amount { get; init; }

    /// <summary>发布时间。</summary>
    public DateTime PublishedAt { get; init; }

    /// <summary>消费时间。</summary>
    public DateTime? ConsumedAt { get; set; }

    /// <summary>消费队列。</summary>
    public string? ConsumerQueue { get; set; }

    /// <summary>消费者名称（如 FanoutConsumerWorker）。</summary>
    public string? ConsumerWorker { get; set; }

    /// <summary>消息状态: Published / Consumed / DeadLettered / Unroutable。</summary>
    public string State { get; set; } = MessageStates.Published;

    /// <summary>错误信息（死信/路由失败时）。</summary>
    public string? ErrorInfo { get; set; }

    /// <summary>消息体大小（字节）。</summary>
    public int BodySize { get; init; }
}
