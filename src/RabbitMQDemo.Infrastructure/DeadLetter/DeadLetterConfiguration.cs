using RabbitMQ.Client;
using RabbitMQDemo.Shared.Constants;

namespace RabbitMQDemo.Infrastructure.DeadLetter;

/// <summary>
/// 死信队列配置 —— 负责声明 DLX 交换机和 DLQ 队列。
/// </summary>
public static class DeadLetterConfiguration
{
    /// <summary>
    /// 声明死信交换机（Fanout 类型）和死信队列，并进行绑定。
    /// </summary>
    public static async Task DeclareAsync(
        IChannel channel,
        CancellationToken cancellationToken = default)
    {
        // 声明死信交换机（Fanout — 所有死信都广播到绑定的队列）
        await channel.ExchangeDeclareAsync(
            exchange: ExchangeNames.DeadLetter,
            type: ExchangeType.Fanout,
            durable: true,
            autoDelete: false,
            cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        // 声明死信队列
        await channel.QueueDeclareAsync(
            queue: QueueNames.DeadLetter,
            durable: true,
            exclusive: false,
            autoDelete: false,
            cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        // 绑定死信队列到死信交换机
        await channel.QueueBindAsync(
            queue: QueueNames.DeadLetter,
            exchange: ExchangeNames.DeadLetter,
            routingKey: string.Empty,
            cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }
}
