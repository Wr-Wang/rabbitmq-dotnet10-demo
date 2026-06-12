using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQDemo.Infrastructure.Idempotency;
using RabbitMQDemo.Infrastructure.Serialization;
using RabbitMQDemo.Shared.Constants;
using RabbitMQDemo.Shared.Messages;

namespace RabbitMQDemo.Tests.IntegrationTests;

/// <summary>
/// 幂等消费集成测试 — 3 个用例 (II-1 到 II-3)。
/// 需 RabbitMQ 服务运行在 localhost:5672。
/// </summary>
[Collection("RabbitMQIntegration")]
public sealed class IdempotencyIntegrationTests : IAsyncLifetime
{
    private readonly RabbitMQFixture _fixture;
    private readonly JsonMessageSerializer _serializer = new();
    private readonly InMemoryIdempotencyChecker _idempotencyChecker = new(cleanupIntervalSeconds: 0);
    private IConnection Connection => _fixture.Connection;

    private const string TestExchange = "test.idemp.integration";
    private const string TestQueue = "test.idemp.integration.q";
    private const string DlqExchange = "test.idemp.dlx";
    private const string DlqQueue = "test.idemp.dlq";

    public IdempotencyIntegrationTests(RabbitMQFixture fixture)
    {
        _fixture = fixture;
    }

    public async Task InitializeAsync()
    {
        await CleanupAsync();
        await using var channel = await Connection.CreateChannelAsync();

        // 声明 DLX + DLQ
        await channel.ExchangeDeclareAsync(DlqExchange, ExchangeType.Fanout, durable: true, autoDelete: true);
        await channel.QueueDeclareAsync(DlqQueue, durable: true, exclusive: false, autoDelete: true);
        await channel.QueueBindAsync(DlqQueue, DlqExchange, string.Empty);

        // 声明主队列（绑定 DLX）
        var args = new Dictionary<string, object?>
        {
            { "x-dead-letter-exchange", DlqExchange }
        };
        await channel.ExchangeDeclareAsync(TestExchange, ExchangeType.Direct, durable: true, autoDelete: true);
        await channel.QueueDeclareAsync(TestQueue, durable: true, exclusive: false, autoDelete: true, arguments: args);
        await channel.QueueBindAsync(TestQueue, TestExchange, "test");
    }

    /// <summary>
    /// II-1: 同一条消息重复投递 → 幂等检查确保业务逻辑只执行一次。
    /// </summary>
    [Fact]
    [Trait("Category", "Integration")]
    public async Task DuplicateMessage_ShouldBeProcessedOnlyOnce()
    {
        await using var pubChannel = await Connection.CreateChannelAsync();

        var processCount = 0;
        var messageId = Guid.NewGuid().ToString("N");
        var envelope = new MessageEnvelope<string>(messageId, "1.0", DateTime.UtcNow, "test", "duplicate-test");
        var body = _serializer.Serialize(envelope);

        // 发布同一条消息两次
        for (int i = 0; i < 2; i++)
        {
            await pubChannel.BasicPublishAsync(
                TestExchange, "test", false, new BasicProperties { MessageId = messageId }, body)
                ;
        }

        // 消费两条消息
        var consumer = new AsyncEventingBasicConsumer(pubChannel);
        var doneTcs = new TaskCompletionSource();
        var msgCount = 0;
        consumer.ReceivedAsync += async (_, args) =>
        {
            try
            {
                var msgId = args.BasicProperties.MessageId ?? string.Empty;
                var alreadyProcessed = await _idempotencyChecker.IsProcessedAsync(msgId);

                if (!alreadyProcessed)
                {
                    Interlocked.Increment(ref processCount);
                    await _idempotencyChecker.MarkAsProcessedAsync(msgId);
                }

                await pubChannel.BasicAckAsync(args.DeliveryTag, multiple: false);

                if (Interlocked.Increment(ref msgCount) >= 2)
                    doneTcs.TrySetResult();
            }
            catch
            {
                await pubChannel.BasicNackAsync(args.DeliveryTag, multiple: false, requeue: false)
                    ;
            }
        };

        var tag = await pubChannel.BasicConsumeAsync(TestQueue, autoAck: false, consumer: consumer)
            ;

        await doneTcs.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await pubChannel.BasicCancelAsync(tag, noWait: false);

        // Assert: 业务逻辑只执行 1 次
        Assert.Equal(1, processCount);
    }

    /// <summary>
    /// II-2: 不同 MessageId 的消息各自独立处理。
    /// </summary>
    [Fact]
    [Trait("Category", "Integration")]
    public async Task DifferentMessageIds_AreProcessedIndependently()
    {
        await using var pubChannel = await Connection.CreateChannelAsync();

        var processedIds = new HashSet<string>();
        var id1 = Guid.NewGuid().ToString("N");
        var id2 = Guid.NewGuid().ToString("N");

        // 发送两条不同的消息
        var env1 = new MessageEnvelope<string>(id1, "1.0", DateTime.UtcNow, "test", "first");
        var env2 = new MessageEnvelope<string>(id2, "1.0", DateTime.UtcNow, "test", "second");
        await pubChannel.BasicPublishAsync(
            TestExchange, "test", false, new BasicProperties { MessageId = id1 },
            _serializer.Serialize(env1));
        await pubChannel.BasicPublishAsync(
            TestExchange, "test", false, new BasicProperties { MessageId = id2 },
            _serializer.Serialize(env2));

        var consumer = new AsyncEventingBasicConsumer(pubChannel);
        var doneTcs = new TaskCompletionSource();
        var count = 0;
        consumer.ReceivedAsync += async (_, args) =>
        {
            var msgId = args.BasicProperties.MessageId ?? string.Empty;
            if (!await _idempotencyChecker.IsProcessedAsync(msgId))
            {
                lock (processedIds) processedIds.Add(msgId);
                await _idempotencyChecker.MarkAsProcessedAsync(msgId);
            }
            await pubChannel.BasicAckAsync(args.DeliveryTag, multiple: false);
            if (Interlocked.Increment(ref count) >= 2)
                doneTcs.TrySetResult();
        };

        var tag = await pubChannel.BasicConsumeAsync(TestQueue, autoAck: false, consumer: consumer)
            ;

        await doneTcs.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await pubChannel.BasicCancelAsync(tag, noWait: false);

        Assert.Equal(2, processedIds.Count);
        Assert.Contains(id1, processedIds);
        Assert.Contains(id2, processedIds);
    }

    /// <summary>
    /// II-3: 幂等 + 死信联动 — 重复消息 + 业务异常。
    /// </summary>
    [Fact]
    [Trait("Category", "Integration")]
    public async Task Idempotency_WithDeadLetter_ShouldWorkTogether()
    {
        await using var pubChannel = await Connection.CreateChannelAsync();

        var messageId = Guid.NewGuid().ToString("N");
        var envelope = new MessageEnvelope<string>(messageId, "1.0", DateTime.UtcNow, "test", "dlx-idemp-test");
        var body = _serializer.Serialize(envelope);

        // 发布同一条消息两次
        await pubChannel.BasicPublishAsync(
            TestExchange, "test", false, new BasicProperties { MessageId = messageId }, body)
            ;
        await pubChannel.BasicPublishAsync(
            TestExchange, "test", false, new BasicProperties { MessageId = messageId }, body)
            ;

        var consumer = new AsyncEventingBasicConsumer(pubChannel);
        var doneTcs = new TaskCompletionSource();
        var receivedCount = 0;
        consumer.ReceivedAsync += async (_, args) =>
        {
            try
            {
                var msgId = args.BasicProperties.MessageId ?? string.Empty;
                var alreadyProcessed = await _idempotencyChecker.IsProcessedAsync(msgId);

                if (alreadyProcessed)
                {
                    // 已处理过的消息 → Ack 不继续
                    await pubChannel.BasicAckAsync(args.DeliveryTag, multiple: false);
                }
                else
                {
                    // 未处理过 → 模拟业务失败 → Nack → DLQ
                    await _idempotencyChecker.MarkAsProcessedAsync(msgId);
                    await pubChannel.BasicNackAsync(args.DeliveryTag, multiple: false, requeue: false)
                        ;
                }

                if (Interlocked.Increment(ref receivedCount) >= 2)
                    doneTcs.TrySetResult();
            }
            catch
            {
                await pubChannel.BasicNackAsync(args.DeliveryTag, multiple: false, requeue: false)
                    ;
            }
        };

        var tag = await pubChannel.BasicConsumeAsync(TestQueue, autoAck: false, consumer: consumer)
            ;

        await doneTcs.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await pubChannel.BasicCancelAsync(tag, noWait: false);

        // 验证: DLQ 正好有 1 条消息（仅第一条失败入 DLQ，第二条幂等跳过）
        var dlqCount = await _fixture.GetMessageCountAsync(DlqQueue);
        Assert.Equal(1u, dlqCount);
    }

    private async Task CleanupAsync()
    {
        await using var ch = await Connection.CreateChannelAsync();
        foreach (var q in new[] { TestQueue, DlqQueue })
        {
            try { await ch.QueueDeleteAsync(q); } catch { }
        }
        foreach (var ex in new[] { TestExchange, DlqExchange })
        {
            try { await ch.ExchangeDeleteAsync(ex); } catch { }
        }
    }

    async Task IAsyncLifetime.DisposeAsync()
    {
        await CleanupAsync();
        _idempotencyChecker.Dispose();
    }
}
