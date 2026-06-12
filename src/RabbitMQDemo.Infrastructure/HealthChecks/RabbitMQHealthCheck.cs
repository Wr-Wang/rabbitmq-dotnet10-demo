using Microsoft.Extensions.Diagnostics.HealthChecks;
using RabbitMQ.Client;

namespace RabbitMQDemo.Infrastructure.HealthChecks;

/// <summary>
/// RabbitMQ 连接健康检查 —— 检测连接是否处于 Open 状态。
/// </summary>
public sealed class RabbitMQHealthCheck : IHealthCheck
{
    private readonly IConnection _connection;

    public RabbitMQHealthCheck(IConnection connection)
    {
        _connection = connection;
    }

    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        if (_connection is { IsOpen: true })
        {
            return Task.FromResult(HealthCheckResult.Healthy(
                $"RabbitMQ 连接正常: {_connection.Endpoint}"));
        }

        return Task.FromResult(HealthCheckResult.Unhealthy(
            $"RabbitMQ 连接不可用: {_connection.Endpoint}"));
    }
}
