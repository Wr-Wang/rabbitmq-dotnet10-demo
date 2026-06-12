using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMQDemo.Infrastructure.DeadLetter;

/// <summary>
/// 死信消息封装 —— 从 BasicDeliverEventArgs 提取死信元数据。
/// </summary>
public sealed record DeadLetterMessage(
    byte[] Body,
    string Exchange,
    string RoutingKey,
    string? DeathReason,
    string? FirstDeathExchange,
    string? FirstDeathQueue,
    IReadOnlyBasicProperties? Properties)
{
    /// <summary>
    /// 从 BasicDeliverEventArgs 构造死信消息。
    /// </summary>
    public static DeadLetterMessage FromEventArgs(BasicDeliverEventArgs args)
    {
        var headers = args.BasicProperties.Headers;

        var deathReason = headers?.TryGetValue("x-first-death-reason", out var reason) == true
            ? reason?.ToString()
            : null;

        var firstDeathExchange = headers?.TryGetValue("x-first-death-exchange", out var fde) == true
            ? fde?.ToString()
            : null;

        var firstDeathQueue = headers?.TryGetValue("x-first-death-queue", out var fdq) == true
            ? fdq?.ToString()
            : null;

        return new DeadLetterMessage(
            Body: args.Body.ToArray(),
            Exchange: args.Exchange,
            RoutingKey: args.RoutingKey,
            DeathReason: deathReason,
            FirstDeathExchange: firstDeathExchange,
            FirstDeathQueue: firstDeathQueue,
            Properties: args.BasicProperties);
    }
}
