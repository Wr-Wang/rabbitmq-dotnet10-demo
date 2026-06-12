using System.ComponentModel.DataAnnotations;

namespace RabbitMQDemo.Infrastructure.Configuration;

/// <summary>
/// RabbitMQ 连接和消费者配置的强类型选项。
/// 通过 IOptions{TSource} 绑定 appsettings.json 中的 "RabbitMQ" 节。
/// </summary>
public sealed class RabbitMQOptions
{
    public const string SectionName = "RabbitMQ";

    /// <summary>RabbitMQ 主机地址，默认 localhost。</summary>
    public string Host { get; set; } = "localhost";

    /// <summary>AMQP 端口，默认 5672。</summary>
    public int Port { get; set; } = 5672;

    /// <summary>虚拟主机，默认 "/"。</summary>
    public string VirtualHost { get; set; } = "/";

    /// <summary>用户名，默认 guest。</summary>
    public string UserName { get; set; } = "guest";

    /// <summary>密码，默认 guest。</summary>
    public string Password { get; set; } = "guest";

    /// <summary>客户端连接名称（管理控制台标识用）。</summary>
    public string ClientProvidedName { get; set; } = "rabbitmq-demo";

    /// <summary>心跳间隔秒数，默认 30。</summary>
    [Range(0, 3600)]
    public ushort HeartbeatInterval { get; set; } = 30;

    /// <summary>是否启用自动恢复，默认 true。</summary>
    public bool AutomaticRecoveryEnabled { get; set; } = true;

    /// <summary>连接重试次数，默认 5。</summary>
    [Range(0, 100)]
    public int RetryCount { get; set; } = 5;

    /// <summary>连接重试基础延迟秒数，默认 1。</summary>
    [Range(0, 300)]
    public int RetryBaseDelaySeconds { get; set; } = 1;

    /// <summary>是否启用 Publisher Confirms，默认 true。</summary>
    public bool PublisherConfirms { get; set; } = true;

    /// <summary>是否启用 mandatory 标志（路由失败时触发 BasicReturn），默认 true。</summary>
    public bool Mandatory { get; set; } = true;

    /// <summary>消息投递模式：1=非持久化，2=持久化，默认 2。</summary>
    public byte DeliveryMode { get; set; } = 2;

    /// <summary>SSL 配置。</summary>
    public SslOptions Ssl { get; set; } = new();

    /// <summary>消费者配置。</summary>
    public ConsumerOptions Consumer { get; set; } = new();
}

/// <summary>SSL 配置选项。</summary>
public sealed class SslOptions
{
    /// <summary>是否启用 SSL，默认 false。</summary>
    public bool Enabled { get; set; }

    /// <summary>服务器名称。</summary>
    public string ServerName { get; set; } = string.Empty;

    /// <summary>证书路径。</summary>
    public string CertPath { get; set; } = string.Empty;

    /// <summary>证书密码。</summary>
    public string CertPassphrase { get; set; } = string.Empty;
}

/// <summary>消费者配置选项。</summary>
public sealed class ConsumerOptions
{
    /// <summary>每次预取消息数，默认 10。</summary>
    [Range(1, 1000)]
    public ushort PrefetchCount { get; set; } = 10;

    /// <summary>是否自动确认，默认 false（手动确认）。</summary>
    public bool AutoAck { get; set; }

    /// <summary>消费重试配置。</summary>
    public RetryOptions Retry { get; set; } = new();
}

/// <summary>消费重试配置选项。</summary>
public sealed class RetryOptions
{
    /// <summary>最大重试次数，默认 3。</summary>
    [Range(0, 10)]
    public int MaxRetryCount { get; set; } = 3;

    /// <summary>基础延迟秒数，默认 1。</summary>
    [Range(0, 60)]
    public int BaseDelaySeconds { get; set; } = 1;

    /// <summary>最大延迟秒数，默认 30。</summary>
    [Range(1, 300)]
    public int MaxDelaySeconds { get; set; } = 30;
}
