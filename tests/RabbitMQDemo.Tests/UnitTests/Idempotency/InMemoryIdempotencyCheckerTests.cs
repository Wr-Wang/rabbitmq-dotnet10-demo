using RabbitMQDemo.Infrastructure.Idempotency;

namespace RabbitMQDemo.Tests.UnitTests.Idempotency;

/// <summary>
/// InMemoryIdempotencyChecker 单元测试 — 5 个用例 (IC-1 到 IC-5)。
/// </summary>
public sealed class InMemoryIdempotencyCheckerTests : IDisposable
{
    private readonly InMemoryIdempotencyChecker _checker = new(cleanupIntervalSeconds: 0);

    /// <summary>
    /// IC-1: 新消息 ID 应返回未处理。
    /// </summary>
    [Fact]
    [Trait("Category", "Unit")]
    public async Task IsProcessedAsync_NewMessageId_ReturnsFalse()
    {
        // Arrange
        var messageId = Guid.NewGuid().ToString("N");

        // Act
        var result = await _checker.IsProcessedAsync(messageId);

        // Assert
        Assert.False(result);
    }

    /// <summary>
    /// IC-2: MarkAsProcessed 后 IsProcessed 应返回 true。
    /// </summary>
    [Fact]
    [Trait("Category", "Unit")]
    public async Task MarkAsProcessed_ThenIsProcessed_ReturnsTrue()
    {
        // Arrange
        var messageId = Guid.NewGuid().ToString("N");
        await _checker.MarkAsProcessedAsync(messageId);

        // Act
        var result = await _checker.IsProcessedAsync(messageId);

        // Assert
        Assert.True(result);
    }

    /// <summary>
    /// IC-3: 多个不同的消息 ID 互不干扰。
    /// </summary>
    [Fact]
    [Trait("Category", "Unit")]
    public async Task MultipleIds_DoNotInterfere()
    {
        // Arrange
        var id1 = Guid.NewGuid().ToString("N");
        var id2 = Guid.NewGuid().ToString("N");
        await _checker.MarkAsProcessedAsync(id1);

        // Act
        var result1 = await _checker.IsProcessedAsync(id1);
        var result2 = await _checker.IsProcessedAsync(id2);

        // Assert
        Assert.True(result1, "ID1 应标记为已处理");
        Assert.False(result2, "ID2 不应标记为已处理");
    }

    /// <summary>
    /// IC-4: 重复标记同一 ID 不应抛出异常。
    /// </summary>
    [Fact]
    [Trait("Category", "Unit")]
    public async Task MarkAsProcessed_Duplicate_DoesNotThrow()
    {
        // Arrange
        var messageId = Guid.NewGuid().ToString("N");
        await _checker.MarkAsProcessedAsync(messageId);

        // Act & Assert
        var exception = await Record.ExceptionAsync(
            () => _checker.MarkAsProcessedAsync(messageId));
        Assert.Null(exception);
    }

    /// <summary>
    /// IC-5: 不同实例不共享状态。
    /// </summary>
    [Fact]
    [Trait("Category", "Unit")]
    public async Task DifferentInstances_DoNotShareState()
    {
        // Arrange
        using var checker2 = new InMemoryIdempotencyChecker(cleanupIntervalSeconds: 0);
        var messageId = Guid.NewGuid().ToString("N");
        await _checker.MarkAsProcessedAsync(messageId);

        // Act
        var result1 = await _checker.IsProcessedAsync(messageId);
        var result2 = await checker2.IsProcessedAsync(messageId);

        // Assert
        Assert.True(result1, "实例 1 应标记为已处理");
        Assert.False(result2, "实例 2 不应共享状态");
    }

    public void Dispose()
    {
        _checker.Dispose();
    }
}
