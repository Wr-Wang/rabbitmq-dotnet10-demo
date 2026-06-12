using Microsoft.Extensions.Diagnostics.HealthChecks;
using RabbitMQ.Client;
using RabbitMQDemo.Shared.Constants;

namespace RabbitMQDemo.Infrastructure.HealthChecks;

/// <summary>
/// 死信队列健康检查 —— 通过 Management API 或 QueueDeclarePassive 监控 DLQ 消息积压。
/// 当 DLQ 消息数超过阈值时标记为 Degraded 或 Unhealthy。
/// </summary>
public sealed class DeadLetterQueueHealthCheck : IHealthCheck
{
    private readonly IConnection _connection;
    private readonly int _warningThreshold;
    private readonly int _criticalThreshold;

    public DeadLetterQueueHealthCheck(
        IConnection connection,
        int warningThreshold = 100,
        int criticalThreshold = 1000)
    {
        _connection = connection;
        _warningThreshold = warningThreshold;
        _criticalThreshold = criticalThreshold;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        if (_connection is not { IsOpen: true })
        {
            return HealthCheckResult.Unhealthy("RabbitMQ 连接不可用，无法检查 DLQ");
        }

        try
        {
            await using var channel = await _connection.CreateChannelAsync(
                cancellationToken: cancellationToken).ConfigureAwait(false);

            var queueDeclareOk = await channel.QueueDeclarePassiveAsync(
                queue: QueueNames.DeadLetter,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            var messageCount = queueDeclareOk.MessageCount;

            if (messageCount >= _criticalThreshold)
            {
                return HealthCheckResult.Unhealthy(
                    $"DLQ 消息积压严重: {messageCount} 条 (阈值: {_criticalThreshold})");
            }

            if (messageCount >= _warningThreshold)
            {
                return HealthCheckResult.Degraded(
                    $"DLQ 消息积压较多: {messageCount} 条 (警告阈值: {_warningThreshold})");
            }

            return HealthCheckResult.Healthy(
                $"DLQ 状态正常: {messageCount} 条消息");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy(
                $"检查 DLQ 时发生异常: {ex.Message}", ex);
        }
    }
}
