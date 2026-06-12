using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using RabbitMQDemo.Infrastructure.Configuration;
using RabbitMQDemo.Infrastructure.Connection;
using RabbitMQDemo.Infrastructure.Consuming;
using RabbitMQDemo.Infrastructure.Idempotency;
using RabbitMQDemo.Infrastructure.Publishing;
using RabbitMQDemo.Infrastructure.Serialization;
using RabbitMQDemo.Infrastructure.Tracking;
using RabbitMQDemo.Shared.Interfaces;
using RabbitMQ.Client;

namespace RabbitMQDemo.Infrastructure.DependencyInjection;

/// <summary>
/// DI 注册扩展方法。
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// 注册所有 RabbitMQ 基础设施服务。
    /// </summary>
    public static IServiceCollection AddRabbitMQ(this IServiceCollection services)
    {
        // 配置绑定
        services.AddOptions<RabbitMQOptions>()
            .BindConfiguration(RabbitMQOptions.SectionName)
            .ValidateDataAnnotations();

        // 基础设施
        services.AddSingleton<RabbitMQConnectionFactory>();
        services.AddSingleton<IMessageSerializer, JsonMessageSerializer>();
        services.AddSingleton<IIdempotencyChecker, InMemoryIdempotencyChecker>();
        services.AddSingleton<MessageProcessor>();

        // 消息追踪（HttpMessageTracker 通过 HttpClient 推送到 Dashboard）
        services.AddOptions<DashboardOptions>()
            .BindConfiguration(DashboardOptions.SectionName);
        services.AddHttpClient<IMessageTracker, HttpMessageTracker>();

        // 发布者
        services.AddSingleton<RabbitMQPublisher>();

        return services;
    }

    /// <summary>
    /// 注册并初始化发布者。
    /// </summary>
    public static async Task<IServiceProvider> UseRabbitMQPublisherAsync(
        this IServiceProvider services,
        CancellationToken cancellationToken = default)
    {
        var publisher = services.GetRequiredService<RabbitMQPublisher>();
        await publisher.InitializeAsync(cancellationToken).ConfigureAwait(false);
        return services;
    }

    /// <summary>
    /// 注册健康检查。
    /// </summary>
    public static IServiceCollection AddRabbitMQHealthChecks(
        this IServiceCollection services,
        int dlqWarningThreshold = 100,
        int dlqCriticalThreshold = 1000)
    {
        services.AddSingleton<IConnection>(sp =>
        {
            var factory = sp.GetRequiredService<RabbitMQConnectionFactory>();
            return factory.GetConnectionAsync().GetAwaiter().GetResult();
        });

        services.AddHealthChecks()
            .AddCheck<HealthChecks.RabbitMQHealthCheck>("rabbitmq-connection")
            .AddCheck<HealthChecks.DeadLetterQueueHealthCheck>("rabbitmq-dlq", tags: ["rabbitmq", "dlq"]);

        return services;
    }
}
