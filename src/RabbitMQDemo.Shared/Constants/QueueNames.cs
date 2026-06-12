namespace RabbitMQDemo.Shared.Constants;

/// <summary>
/// 队列名称常量。
/// </summary>
public static class QueueNames
{
    /// <summary>Fanout 消费者队列。</summary>
    public const string Fanout = "order.fanout.q";

    /// <summary>Direct — 订单创建队列。</summary>
    public const string DirectCreated = "order.created.q";

    /// <summary>Direct — 订单支付队列。</summary>
    public const string DirectPaid = "order.paid.q";

    /// <summary>Topic 消费者队列。</summary>
    public const string Topic = "order.topic.q";

    /// <summary>死信队列。</summary>
    public const string DeadLetter = "order.dlq";

    /// <summary>备用队列 —— 路由失败的消息落在此处。</summary>
    public const string Alternate = "order.unrouted.q";
}
