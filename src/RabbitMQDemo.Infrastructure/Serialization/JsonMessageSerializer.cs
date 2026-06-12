using System.Text;
using System.Text.Json;
using RabbitMQDemo.Shared.Interfaces;

namespace RabbitMQDemo.Infrastructure.Serialization;

/// <summary>
/// JSON 消息序列化器 —— 使用 System.Text.Json。
/// </summary>
public sealed class JsonMessageSerializer : IMessageSerializer
{
    private static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    public Encoding Encoding { get; } = Encoding.UTF8;

    public byte[] Serialize<T>(T message)
    {
        var json = JsonSerializer.Serialize(message, Options);
        return Encoding.GetBytes(json);
    }

    public T Deserialize<T>(byte[] data)
    {
        var json = Encoding.GetString(data);
        return JsonSerializer.Deserialize<T>(json, Options)
            ?? throw new InvalidOperationException($"反序列化失败: {typeof(T).Name}");
    }
}
