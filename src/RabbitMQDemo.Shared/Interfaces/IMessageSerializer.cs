using System.Text;

namespace RabbitMQDemo.Shared.Interfaces;

/// <summary>
/// 消息序列化器接口。
/// 将消息信封序列化为 byte[] 供 RabbitMQ 发送，以及反向反序列化。
/// </summary>
public interface IMessageSerializer
{
    byte[] Serialize<T>(T message);
    T Deserialize<T>(byte[] data);
    Encoding Encoding { get; }
}
