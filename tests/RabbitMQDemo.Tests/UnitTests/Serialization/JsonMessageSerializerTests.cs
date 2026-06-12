using RabbitMQDemo.Infrastructure.Serialization;
using RabbitMQDemo.Shared.Messages;

namespace RabbitMQDemo.Tests.UnitTests.Serialization;

/// <summary>
/// JsonMessageSerializer 序列化/反序列化单元测试。
/// </summary>
public sealed class JsonMessageSerializerTests
{
    private readonly JsonMessageSerializer _serializer = new();
    private static readonly DateTime TestTime = new(2026, 6, 12, 10, 0, 0, DateTimeKind.Utc);

    [Fact]
    [Trait("Category", "Unit")]
    public void Serialize_ThenDeserialize_ReturnsOriginal()
    {
        // Arrange
        var original = new MessageEnvelope<OrderEvent>(
            MessageId: Guid.NewGuid().ToString("N"),
            Version: "1.0",
            Timestamp: TestTime,
            Source: "test",
            Body: new OrderEvent(
                OrderId: Guid.NewGuid(),
                ProductName: "测试商品",
                Quantity: 3,
                Price: 299.99m,
                CreatedAt: TestTime,
                EventType: "Created"));

        // Act
        var bytes = _serializer.Serialize(original);
        var deserialized = _serializer.Deserialize<MessageEnvelope<OrderEvent>>(bytes);

        // Assert
        Assert.NotNull(deserialized);
        Assert.Equal(original.MessageId, deserialized.MessageId);
        Assert.Equal(original.Version, deserialized.Version);
        Assert.Equal(original.Source, deserialized.Source);
        Assert.Equal(original.Body.OrderId, deserialized.Body.OrderId);
        Assert.Equal(original.Body.ProductName, deserialized.Body.ProductName);
        Assert.Equal(original.Body.Quantity, deserialized.Body.Quantity);
        Assert.Equal(original.Body.Price, deserialized.Body.Price);
        Assert.Equal(original.Body.EventType, deserialized.Body.EventType);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Serialize_EmptyBody_ThrowsNoException()
    {
        // Arrange
        var envelope = new MessageEnvelope<string>(
            MessageId: Guid.NewGuid().ToString("N"),
            Version: "1.0",
            Timestamp: TestTime,
            Source: "test",
            Body: string.Empty);

        // Act
        var bytes = _serializer.Serialize(envelope);
        var deserialized = _serializer.Deserialize<MessageEnvelope<string>>(bytes);

        // Assert
        Assert.NotNull(deserialized);
        Assert.Equal(envelope.MessageId, deserialized.MessageId);
        Assert.Equal(string.Empty, deserialized.Body);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Serialize_EncodingIsUtf8()
    {
        // Arrange
        var envelope = MessageEnvelope<OrderEvent>.Create(
            new OrderEvent(Guid.NewGuid(), "商品", 1, 1.0m, TestTime, "Created"), "test");

        // Act
        var bytes = _serializer.Serialize(envelope);

        // Assert
        Assert.Equal("utf-8", _serializer.Encoding.WebName, ignoreCase: true);
        Assert.True(bytes.Length > 0, "序列化结果不能为空");
    }
}
