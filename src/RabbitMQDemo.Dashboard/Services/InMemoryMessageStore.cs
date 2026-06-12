using System.Collections.Concurrent;
using RabbitMQDemo.Shared.Messages;

namespace RabbitMQDemo.Dashboard.Services;

/// <summary>
/// 消息记录内存存储 —— 线程安全，自动清理最旧消息。
/// 每条记录的类型标签保持发布时的原始值，不做自动推进。
/// </summary>
public sealed class InMemoryMessageStore
{
    private readonly ConcurrentDictionary<string, MessageRecord> _store = new();
    private readonly ConcurrentQueue<string> _order = new();
    private readonly object _lock = new();

    // 订单最新状态追踪：OrderId → latestBodyType
    private const int MaxMessages = 2000;

    /// <summary>添加新消息记录。</summary>
    public void Add(MessageRecord record)
    {
        if (_store.TryAdd(record.MessageId, record))
        {
            lock (_lock) _order.Enqueue(record.MessageId);
            TrimExcess();
        }
    }

    /// <summary>更新消息状态。</summary>
    public bool UpdateState(string messageId, string newState, Action<MessageRecord>? updateAction = null)
    {
        if (_store.TryGetValue(messageId, out var record))
        {
            lock (record)
            {
                record.State = newState;
                updateAction?.Invoke(record);
            }
            return true;
        }
        return false;
    }

    /// <summary>获取最近 N 条消息（按发布时间升序，最早的在前）。</summary>
    public IReadOnlyList<MessageRecord> GetRecent(int count = 200)
    {
        var result = new List<MessageRecord>();
        foreach (var id in _order)
        {
            if (_store.TryGetValue(id, out var record))
                result.Add(record);
            if (result.Count >= count) break;
        }
        return result.AsReadOnly();
    }

    /// <summary>清空所有消息。</summary>
    public void Clear()
    {
        _store.Clear();
        lock (_lock)
        {
            while (_order.TryDequeue(out _)) { }
        }
    }

    private void TrimExcess()
    {
        while (_store.Count > MaxMessages)
        {
            if (_order.TryDequeue(out var id))
            {
                _store.TryRemove(id, out _);
            }
            else break;
        }
    }
}
