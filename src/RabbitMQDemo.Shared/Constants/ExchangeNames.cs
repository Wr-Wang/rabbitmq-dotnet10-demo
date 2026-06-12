namespace RabbitMQDemo.Shared.Constants;

/// <summary>
/// 交换机名称常量。
/// </summary>
public static class ExchangeNames
{
    /// <summary>Fanout 交换机 —— 广播所有订单事件。</summary>
    public const string Fanout = "order.fanout";

    /// <summary>Direct 交换机 —— 按 EventType 精确路由。</summary>
    public const string Direct = "order.direct";

    /// <summary>Topic 交换机 —— 按通配符模式路由。</summary>
    public const string Topic = "order.topic";

    /// <summary>死信交换机 —— 接收所有死信消息。</summary>
    public const string DeadLetter = "dlx.order";

    /// <summary>备用交换机 —— 接收路由失败的消息。</summary>
    public const string AlternateExchange = "order.unrouted.ae";
}
