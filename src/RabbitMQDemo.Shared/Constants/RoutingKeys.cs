namespace RabbitMQDemo.Shared.Constants;

/// <summary>
/// 路由键常量。
/// </summary>
public static class RoutingKeys
{
    /// <summary>订单创建事件。</summary>
    public const string Created = "order.created";

    /// <summary>订单支付事件。</summary>
    public const string Paid = "order.paid";

    /// <summary>订单发货事件。</summary>
    public const string Shipped = "order.shipped";

    /// <summary>所有订单事件通配符（用于 Topic 交换机）。</summary>
    public const string AllOrders = "order.*";
}
