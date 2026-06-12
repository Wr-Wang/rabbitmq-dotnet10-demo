using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQDemo.Infrastructure.DeadLetter;
using RabbitMQDemo.Infrastructure.Serialization;
using RabbitMQDemo.Shared.Constants;
using RabbitMQDemo.Shared.Messages;

namespace RabbitMQDemo.Tests.IntegrationTests;

/// <summary>
/// 死信队列集成测试 — 4 个用例 (DI-1 到 DI-4)。
/// 需 RabbitMQ 服务运行在 localhost:5672。
/// </summary>
[Collection("RabbitMQIntegration")]
public sealed class DeadLetterIntegrationTests : IAsyncLifetime
{
    private readonly RabbitMQFixture _fixture;
    private readonly JsonMessageSerializer _serializer = new();
    private IConnection Connection => _fixture.Connection;

    private const string TestExchange = "test.dlx.integration";
    private const string TestQueue = "test.dlx.integration.q";
    private const string DlqExchange = "test.dlx";
    private const string DlqQueue = "test.dlx.q";

    public DeadLetterIntegrationTests(RabbitMQFixture fixture)
    {
        _fixture = fixture;
    }

    public async Task InitializeAsync()
    {
        // 清理之前的残留
        await CleanupAsync();

        // 创建测试用的交换机、队列、DLQ
        await using var channel = await Connection.CreateChannelAsync();

        // 声明 DLX + DLQ
        await channel.ExchangeDeclareAsync(
            DlqExchange, ExchangeType.Fanout, durable: true, autoDelete: true);
        await channel.QueueDeclareAsync(
            DlqQueue, durable: true, exclusive: false, autoDelete: true);
        await channel.QueueBindAsync(DlqQueue, DlqExchange, string.Empty);

        // 声明主队列（绑定 DLX）
        var args = new Dictionary<string, object?>
        {
            { "x-dead-letter-exchange", DlqExchange }
        };
        await channel.ExchangeDeclareAsync(
            TestExchange, ExchangeType.Direct, durable: true, autoDelete: true);
        await channel.QueueDeclareAsync(
            TestQueue, durable: true, exclusive: false, autoDelete: true, arguments: args);
        await channel.QueueBindAsync(TestQueue, TestExchange, "test");
    }

    /// <summary>
    /// DI-1: 消费异常 → Nack(requeue=false) → DLQ 收到消息。
    /// </summary>
    [Fact]
    [Trait("Category", "Integration")]
    public async Task NackWithoutRequeue_ShouldMoveMessageToDlq()
    {
        await using var pubChannel = await Connection.CreateChannelAsync();

        // 发送一条消息到主队列
        var body = _serializer.Serialize(MessageEnvelope<string>.Create("hello-dlq", "test"));
        await pubChannel.BasicPublishAsync(
            exchange: TestExchange,
            routingKey: "test",
            mandatory: false,
            basicProperties: new BasicProperties(),
            body: body);

        // 消费消息（不确认，直接 Nack，requeue=false）
        var consumer = new AsyncEventingBasicConsumer(pubChannel);
        var tcs = new TaskCompletionSource();
        consumer.ReceivedAsync += async (_, args) =>
        {
            await pubChannel.BasicNackAsync(
                args.DeliveryTag, multiple: false, requeue: false);
            tcs.TrySetResult();
        };

        var tag = await pubChannel.BasicConsumeAsync(TestQueue, autoAck: false, consumer: consumer)
            ;

        await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await pubChannel.BasicCancelAsync(tag, noWait: false);

        // 验证 DLQ 中有一消息
        var dlqCount = await _fixture.GetMessageCountAsync(DlqQueue);
        Assert.Equal(1u, dlqCount);
    }

    /// <summary>
    /// DI-2: 死信消息应携带 x-death Headers。
    /// </summary>
    [Fact]
    [Trait("Category", "Integration")]
    public async Task DeadLetterMessage_ShouldHaveXDeathHeaders()
    {
        await using var pubChannel = await Connection.CreateChannelAsync();

        var body = _serializer.Serialize(MessageEnvelope<string>.Create("check-headers", "test"));
        await pubChannel.BasicPublishAsync(
            TestExchange, "test", false, new BasicProperties(), body);

        // 消费 → Nack → DLQ
        var consumer = new AsyncEventingBasicConsumer(pubChannel);
        var nackTcs = new TaskCompletionSource();
        consumer.ReceivedAsync += async (_, args) =>
        {
            await pubChannel.BasicNackAsync(
                args.DeliveryTag, multiple: false, requeue: false);
            nackTcs.TrySetResult();
        };

        var tag = await pubChannel.BasicConsumeAsync(TestQueue, autoAck: false, consumer: consumer)
            ;

        await nackTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await pubChannel.BasicCancelAsync(tag, noWait: false);

        // 从 DLQ 消费并检查 x-death
        var dlqConsumer = new AsyncEventingBasicConsumer(pubChannel);
        var dlqTcs = new TaskCompletionSource<BasicDeliverEventArgs>();
        dlqConsumer.ReceivedAsync += (_, args) =>
        {
            dlqTcs.TrySetResult(args);
            return Task.CompletedTask;
        };

        var dlqTag = await pubChannel.BasicConsumeAsync(DlqQueue, autoAck: true, consumer: dlqConsumer)
            ;

        var dlqArgs = await dlqTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await pubChannel.BasicCancelAsync(dlqTag, noWait: false);

        // Assert: x-death headers 存在
        var headers = dlqArgs.BasicProperties.Headers;
        Assert.NotNull(headers);
        Assert.True(headers.ContainsKey("x-death"), "死信消息应包含 x-death Header");
    }

    /// <summary>
    /// DI-3: 正常消费（Ack）不触发死信。
    /// </summary>
    [Fact]
    [Trait("Category", "Integration")]
    public async Task NormalAck_ShouldNotCreateDeadLetter()
    {
        await using var pubChannel = await Connection.CreateChannelAsync();

        var body = _serializer.Serialize(MessageEnvelope<string>.Create("normal-ack", "test"));
        await pubChannel.BasicPublishAsync(
            TestExchange, "test", false, new BasicProperties(), body);

        // 正常消费确认
        var consumer = new AsyncEventingBasicConsumer(pubChannel);
        var ackTcs = new TaskCompletionSource();
        consumer.ReceivedAsync += async (_, args) =>
        {
            await pubChannel.BasicAckAsync(
                args.DeliveryTag, multiple: false);
            ackTcs.TrySetResult();
        };

        var tag = await pubChannel.BasicConsumeAsync(TestQueue, autoAck: false, consumer: consumer)
            ;

        await ackTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await pubChannel.BasicCancelAsync(tag, noWait: false);

        // 验证 DLQ 中没有消息
        var dlqCount = await _fixture.GetMessageCountAsync(DlqQueue);
        Assert.Equal(0u, dlqCount);
    }

    /// <summary>
    /// DI-4: 多次异常累积 — DLQ 消息数量递增。
    /// </summary>
    [Fact]
    [Trait("Category", "Integration")]
    public async Task MultipleNacks_ShouldAccumulateInDlq()
    {
        await using var pubChannel = await Connection.CreateChannelAsync();

        // 发送 3 条消息
        for (int i = 0; i < 3; i++)
        {
            var body = _serializer.Serialize(MessageEnvelope<string>.Create($"msg-{i}", "test"));
            await pubChannel.BasicPublishAsync(
                TestExchange, "test", false, new BasicProperties(), body);
        }

        // 全部 Nack
        var consumer = new AsyncEventingBasicConsumer(pubChannel);
        var nackCount = 0;
        var nackTcs = new TaskCompletionSource();
        consumer.ReceivedAsync += async (_, args) =>
        {
            await pubChannel.BasicNackAsync(
                args.DeliveryTag, multiple: false, requeue: false);
            if (Interlocked.Increment(ref nackCount) >= 3)
                nackTcs.TrySetResult();
        };

        var tag = await pubChannel.BasicConsumeAsync(TestQueue, autoAck: false, consumer: consumer)
            ;

        await nackTcs.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await pubChannel.BasicCancelAsync(tag, noWait: false);

        // 验证 DLQ 有 3 条消息
        var dlqCount = await _fixture.GetMessageCountAsync(DlqQueue);
        Assert.Equal(3u, dlqCount);
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
    }
}
