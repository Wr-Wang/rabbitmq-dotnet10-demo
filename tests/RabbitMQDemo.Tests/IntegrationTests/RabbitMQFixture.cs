using RabbitMQ.Client;
using RabbitMQDemo.Infrastructure.DeadLetter;

namespace RabbitMQDemo.Tests.IntegrationTests;

/// <summary>
/// RabbitMQ 集成测试夹具 —— 连接本地 RabbitMQ 实例。
/// 测试类需要实现 IClassFixture{RabbitMQFixture} 来共享连接。
/// </summary>
public sealed class RabbitMQFixture : IAsyncLifetime
{
    private IConnection? _connection;

    /// <summary>连接工厂，可在测试中创建临时 Channel。</summary>
    public IConnection Connection => _connection
        ?? throw new InvalidOperationException("RabbitMQ 连接尚未初始化");

    public async Task InitializeAsync()
    {
        var factory = new ConnectionFactory
        {
            HostName = "localhost",
            Port = 5672,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            ClientProvidedName = "rabbitmq-demo-integration-test",
            AutomaticRecoveryEnabled = false
        };

        _connection = await factory.CreateConnectionAsync().ConfigureAwait(false);
    }

    public async Task DisposeAsync()
    {
        if (_connection is not null)
        {
            await _connection.CloseAsync().ConfigureAwait(false);
            await _connection.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>获取队列消息数量。</summary>
    public async Task<uint> GetMessageCountAsync(string queueName)
    {
        await using var channel = await _connection!.CreateChannelAsync().ConfigureAwait(false);
        var result = await channel.QueueDeclarePassiveAsync(queueName).ConfigureAwait(false);
        return result.MessageCount;
    }

    /// <summary>清空队列所有消息。</summary>
    public async Task PurgeQueueAsync(string queueName)
    {
        await using var channel = await _connection!.CreateChannelAsync().ConfigureAwait(false);
        try
        {
            await channel.QueuePurgeAsync(queueName).ConfigureAwait(false);
        }
        catch
        {
            // 队列可能不存在，忽略
        }
    }

    /// <summary>声明临时队列并绑定。</summary>
    public async Task<string> DeclareAndBindQueueAsync(
        string exchange, string routingKey)
    {
        await using var channel = await _connection!.CreateChannelAsync().ConfigureAwait(false);
        var result = await channel.QueueDeclareAsync(
            queue: string.Empty, // 自动生成名称
            durable: false,
            exclusive: true,
            autoDelete: true).ConfigureAwait(false);

        await channel.QueueBindAsync(
            queue: result.QueueName!,
            exchange: exchange,
            routingKey: routingKey).ConfigureAwait(false);

        return result.QueueName!;
    }
}

/// <summary>
/// 集成测试标记接口 —— 需要 RabbitMQ 服务正在运行。
/// 使用 dotnet test --filter "Category=Integration" 单独运行。
/// </summary>
[CollectionDefinition("RabbitMQIntegration")]
public sealed class RabbitMQIntegrationCollection : ICollectionFixture<RabbitMQFixture>
{
}
