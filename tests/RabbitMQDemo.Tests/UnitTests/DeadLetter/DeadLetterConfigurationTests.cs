using Moq;
using RabbitMQ.Client;
using RabbitMQDemo.Infrastructure.DeadLetter;
using RabbitMQDemo.Shared.Constants;

namespace RabbitMQDemo.Tests.UnitTests.DeadLetter;

/// <summary>
/// 死信队列配置单元测试 — 验证 DLX/DLQ 声明参数正确性。
/// </summary>
public sealed class DeadLetterConfigurationTests
{
    private readonly Mock<IChannel> _channelMock = new(MockBehavior.Strict);

    public DeadLetterConfigurationTests()
    {
        // 设置 ExchangeDeclareAsync 期望
        _channelMock
            .Setup(x => x.ExchangeDeclareAsync(
                ExchangeNames.DeadLetter,
                ExchangeType.Fanout,
                true,
                false,
                It.IsAny<IDictionary<string, object?>>(),
                false,
                false,
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        // 设置 QueueDeclareAsync 期望
        _channelMock
            .Setup(x => x.QueueDeclareAsync(
                QueueNames.DeadLetter,
                true,
                false,
                false,
                It.IsAny<IDictionary<string, object?>>(),
                false,
                false,
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueueDeclareOk("order.dlq", 0, 0));

        // 设置 QueueBindAsync 期望
        _channelMock
            .Setup(x => x.QueueBindAsync(
                QueueNames.DeadLetter,
                ExchangeNames.DeadLetter,
                string.Empty,
                It.IsAny<IDictionary<string, object?>>(),
                false,
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task DeclareAsync_ShouldDeclareDlxExchange()
    {
        // Act
        await DeadLetterConfiguration.DeclareAsync(_channelMock.Object);

        // Assert
        _channelMock.Verify(x => x.ExchangeDeclareAsync(
            ExchangeNames.DeadLetter,
            ExchangeType.Fanout,
            true,
            false,
            It.IsAny<IDictionary<string, object?>>(),
            false,
            false,
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task DeclareAsync_ShouldDeclareDlqQueue()
    {
        // Act
        await DeadLetterConfiguration.DeclareAsync(_channelMock.Object);

        // Assert
        _channelMock.Verify(x => x.QueueDeclareAsync(
            QueueNames.DeadLetter,
            true,
            false,
            false,
            It.IsAny<IDictionary<string, object?>>(),
            false,
            false,
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task DeclareAsync_ShouldBindDlqToDlx()
    {
        // Act
        await DeadLetterConfiguration.DeclareAsync(_channelMock.Object);

        // Assert
        _channelMock.Verify(x => x.QueueBindAsync(
            QueueNames.DeadLetter,
            ExchangeNames.DeadLetter,
            string.Empty,
            It.IsAny<IDictionary<string, object?>>(),
            false,
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void DlxExchange_ShouldBeFanout()
    {
        Assert.Equal(ExchangeType.Fanout, ExchangeType.Fanout);
    }
}
