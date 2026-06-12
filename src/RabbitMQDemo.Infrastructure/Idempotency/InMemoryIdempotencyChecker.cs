using System.Collections.Concurrent;
using RabbitMQDemo.Shared.Interfaces;

namespace RabbitMQDemo.Infrastructure.Idempotency;

/// <summary>
/// 内存幂等检查器 —— 基于 ConcurrentDictionary。
/// 仅用于开发和测试，生产环境请使用 Redis（SET EX 86400）或数据库去重表。
/// </summary>
public sealed class InMemoryIdempotencyChecker : IIdempotencyChecker, IDisposable
{
    private static readonly TimeSpan DefaultTtl = TimeSpan.FromHours(24);
    private readonly ConcurrentDictionary<string, DateTime> _processed = new();
    private readonly Timer? _cleanupTimer;

    /// <param name="cleanupIntervalSeconds">清理过期条目的间隔秒数。0 或负值表示禁用自动清理。</param>
    public InMemoryIdempotencyChecker(int cleanupIntervalSeconds = 300)
    {
        if (cleanupIntervalSeconds > 0)
        {
            var interval = TimeSpan.FromSeconds(cleanupIntervalSeconds);
            _cleanupTimer = new Timer(_ => CleanupExpired(), null, interval, interval);
        }
    }

    public Task<bool> IsProcessedAsync(string messageId)
    {
        var processed = _processed.TryGetValue(messageId, out var timestamp);
        var expired = processed && (DateTime.UtcNow - timestamp) > DefaultTtl;
        return Task.FromResult(processed && !expired);
    }

    public Task MarkAsProcessedAsync(string messageId)
    {
        _processed[messageId] = DateTime.UtcNow;
        return Task.CompletedTask;
    }

    private void CleanupExpired()
    {
        var cutoff = DateTime.UtcNow - DefaultTtl;
        foreach (var (key, timestamp) in _processed)
        {
            if (timestamp < cutoff)
                _processed.TryRemove(key, out _);
        }
    }

    /// <summary>仅用于测试：获取当前已处理的消息数。</summary>
    internal int Count => _processed.Count;

    public void Dispose() => _cleanupTimer?.Dispose();
}
