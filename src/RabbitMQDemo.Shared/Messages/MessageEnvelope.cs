using System.Text.Json.Serialization;

namespace RabbitMQDemo.Shared.Messages;

/// <summary>
/// 消息信封 —— 携带全局唯一 MessageId 用于幂等去重。
/// Version 字段支持向前兼容的 schema 演进。
/// </summary>
public sealed record MessageEnvelope<T>(
    [property: JsonPropertyName("messageId")] string MessageId,
    [property: JsonPropertyName("version")] string Version,
    [property: JsonPropertyName("timestamp")] DateTime Timestamp,
    [property: JsonPropertyName("source")] string Source,
    [property: JsonPropertyName("body")] T Body)
{
    /// <summary>快速构建消息信封（自动生成 MessageId 和时间戳）。</summary>
    public static MessageEnvelope<T> Create(T body, string source = "unknown")
        => new(
            MessageId: Guid.NewGuid().ToString("N"),
            Version: "1.0",
            Timestamp: DateTime.UtcNow,
            Source: source,
            Body: body);
}
