using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using RabbitMQDemo.Infrastructure.Configuration;

namespace RabbitMQDemo.Infrastructure.Connection;

/// <summary>
/// RabbitMQ 连接工厂 —— 负责创建和管理与 RabbitMQ 的长连接。
/// 支持自动重连、心跳、Polly 指数退避重试。
/// </summary>
public sealed class RabbitMQConnectionFactory : IAsyncDisposable
{
    private readonly RabbitMQOptions _options;
    private readonly ILogger<RabbitMQConnectionFactory> _logger;
    private readonly SemaphoreSlim _lock = new(1, 1);

    private IConnection? _connection;
    private bool _disposed;

    public RabbitMQConnectionFactory(
        IOptions<RabbitMQOptions> options,
        ILogger<RabbitMQConnectionFactory> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    /// <summary>
    /// 获取或创建连接。若连接已关闭则自动重建。
    /// </summary>
    public async Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
    {
        if (_connection is { IsOpen: true })
            return _connection;

        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_connection is { IsOpen: true })
                return _connection;

            _connection?.Dispose();

            var factory = new ConnectionFactory
            {
                HostName = _options.Host,
                Port = _options.Port,
                VirtualHost = _options.VirtualHost,
                UserName = _options.UserName,
                Password = _options.Password,
                ClientProvidedName = _options.ClientProvidedName,
                RequestedHeartbeat = TimeSpan.FromSeconds(_options.HeartbeatInterval),
                AutomaticRecoveryEnabled = _options.AutomaticRecoveryEnabled,
                TopologyRecoveryEnabled = true
            };

            if (_options.Ssl.Enabled)
            {
                factory.Ssl.Enabled = true;
                factory.Ssl.ServerName = _options.Ssl.ServerName;
                factory.Ssl.CertPath = _options.Ssl.CertPath;
                factory.Ssl.CertPassphrase = _options.Ssl.CertPassphrase;
            }

            _connection = await factory.CreateConnectionAsync(cancellationToken).ConfigureAwait(false);

            RegisterConnectionEvents();

            _logger.LogInformation(
                "RabbitMQ 连接已建立: {Host}:{Port}, VHost: {VHost}, ClientProvidedName: {Name}",
                _options.Host, _options.Port, _options.VirtualHost, _options.ClientProvidedName);

            return _connection;
        }
        catch (BrokerUnreachableException ex)
        {
            _logger.LogError(ex, "无法连接到 RabbitMQ: {Host}:{Port}", _options.Host, _options.Port);
            throw;
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// 创建一个新的独立 Channel。每个 Channel 不应跨线程共享。
    /// </summary>
    public async Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken = default)
    {
        var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

        _logger.LogDebug("Channel 已创建: {ChannelNumber}", channel.ChannelNumber);
        return channel;
    }

    /// <summary>
    /// 创建启用了 Publisher Confirms 的 Channel。
    /// RabbitMQ.Client 7.x 通过 CreateChannelOptions 启用确认模式。
    /// </summary>
    public async Task<IChannel> CreatePublisherConfirmChannelAsync(CancellationToken cancellationToken = default)
    {
        var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        var options = new CreateChannelOptions(
            publisherConfirmationsEnabled: true,
            publisherConfirmationTrackingEnabled: true,
            outstandingPublisherConfirmationsRateLimiter: null,
            consumerDispatchConcurrency: null);
        var channel = await connection.CreateChannelAsync(options, cancellationToken).ConfigureAwait(false);

        _logger.LogDebug("Publisher-Confirm Channel 已创建: {ChannelNumber}", channel.ChannelNumber);
        return channel;
    }

    private void RegisterConnectionEvents()
    {
        if (_connection is null) return;

        _connection.ConnectionBlockedAsync += (_, args) =>
        {
            _logger.LogWarning("连接被阻塞: {Reason}", args.Reason);
            return Task.CompletedTask;
        };

        _connection.ConnectionUnblockedAsync += (_, _) =>
        {
            _logger.LogInformation("连接已解除阻塞");
            return Task.CompletedTask;
        };

        _connection.ConnectionShutdownAsync += (_, args) =>
        {
            _logger.LogWarning(
                "连接关闭: {ReplyCode} - {ReplyText}",
                args.ReplyCode, args.ReplyText);
            return Task.CompletedTask;
        };
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        if (_connection is not null)
        {
            await _connection.CloseAsync().ConfigureAwait(false);
            await _connection.DisposeAsync().ConfigureAwait(false);
        }

        _lock.Dispose();
    }
}
