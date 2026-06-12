using System.Diagnostics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQDemo.Infrastructure.Configuration;
using RabbitMQDemo.Infrastructure.Connection;
using RabbitMQDemo.Infrastructure.DeadLetter;
using RabbitMQDemo.Infrastructure.Serialization;
using RabbitMQDemo.Infrastructure.Tracking;
using RabbitMQDemo.Shared.Constants;
using RabbitMQDemo.Shared.Interfaces;

namespace RabbitMQDemo.Infrastructure.Consuming;

/// <summary>
/// 后台消费者基类 —— 封装了连接、Channel、消费循环、Ack/Nack 逻辑。
/// 子类只需实现交换机/队列声明和消息处理回调。
/// </summary>
public abstract class RabbitMQConsumerService : BackgroundService
{
    private readonly RabbitMQConnectionFactory _connectionFactory;
    private readonly IOptions<RabbitMQOptions> _options;
    private readonly IIdempotencyChecker _idempotencyChecker;
    private readonly MessageProcessor _messageProcessor;
    private readonly IMessageTracker _messageTracker;

    /// <summary>可供子类使用的序列化器。</summary>
    protected readonly IMessageSerializer _serializer;
    /// <summary>可供子类使用的日志记录器。</summary>
    protected readonly ILogger _logger;
    private readonly ActivitySource _activitySource;
    private readonly ResiliencePipeline _retryPipeline;

    private IChannel? _consumeChannel;
    private string _consumerTag = string.Empty;

    protected RabbitMQConsumerService(
        RabbitMQConnectionFactory connectionFactory,
        IMessageSerializer serializer,
        IOptions<RabbitMQOptions> options,
        IIdempotencyChecker idempotencyChecker,
        MessageProcessor messageProcessor,
        ILogger logger,
        string activitySourceName,
        IMessageTracker? messageTracker = null)
    {
        _connectionFactory = connectionFactory;
        _serializer = serializer;
        _options = options;
        _idempotencyChecker = idempotencyChecker;
        _messageProcessor = messageProcessor;
        _messageTracker = messageTracker ?? new NullMessageTracker();
        _logger = logger;
        _activitySource = new ActivitySource(activitySourceName);

        // 构建 Polly 重试管道
        var retryOpts = options.Value.Consumer.Retry;
        _retryPipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = retryOpts.MaxRetryCount,
                DelayGenerator = args =>
                {
                    var delay = TimeSpan.FromSeconds(retryOpts.BaseDelaySeconds);
                    // 指数退避: 1s → 3s → 9s（限制在 MaxDelaySeconds 内）
                    var exponential = TimeSpan.FromTicks(
                        Math.Min(
                            delay.Ticks * (long)Math.Pow(3, args.AttemptNumber),
                            TimeSpan.FromSeconds(retryOpts.MaxDelaySeconds).Ticks));
                    // 添加 ±25% 抖动避免惊群
                    var jitter = TimeSpan.FromMilliseconds(
                        Random.Shared.Next(
                            (int)(-exponential.TotalMilliseconds * 0.25),
                            (int)(exponential.TotalMilliseconds * 0.25)));
                    return ValueTask.FromResult<TimeSpan?>(exponential + jitter);
                }
            })
            .Build();
    }

    /// <summary>交换机名称（子类实现）。</summary>
    protected abstract string ExchangeName { get; }

    /// <summary>交换机类型（子类实现，如 "fanout"/"direct"/"topic"）。</summary>
    protected abstract string ExchangeTypeName { get; }

    /// <summary>队列名称（子类实现）。</summary>
    protected abstract string QueueName { get; }

    /// <summary>绑定路由键（子类实现）。</summary>
    protected abstract string BindingRoutingKey { get; }

    /// <summary>是否声明死信队列（子类控制，默认 true）。</summary>
    protected virtual bool DeclareDeadLetter => true;

    /// <summary>消费者的 ActivitySource 名称（子类可覆盖）。</summary>
    protected virtual string ActivitySourceName => "RabbitMQDemo.Subscriber";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // 等待连接可用
        await _connectionFactory.GetConnectionAsync(stoppingToken).ConfigureAwait(false);

        // 声明死信交换机/队列（只做一次）
        if (DeclareDeadLetter)
        {
            using var dlqChannel = await _connectionFactory.CreateChannelAsync(stoppingToken)
                .ConfigureAwait(false);
            await DeadLetterConfiguration.DeclareAsync(dlqChannel, stoppingToken)
                .ConfigureAwait(false);
        }

        // 创建消费 Channel
        _consumeChannel = await _connectionFactory.CreateChannelAsync(stoppingToken)
            .ConfigureAwait(false);

        // 设置 Prefetch
        await _consumeChannel.BasicQosAsync(
            prefetchSize: 0,
            prefetchCount: _options.Value.Consumer.PrefetchCount,
            global: false,
            cancellationToken: stoppingToken).ConfigureAwait(false);

        // 声明队列参数（含死信绑定）
        var queueArgs = new Dictionary<string, object?>
        {
            { "x-dead-letter-exchange", ExchangeNames.DeadLetter }
        };

        // 先声明备用交换机（AE）—— 供业务交换机的 alternate-exchange 引用
        using (var aeChannel = await _connectionFactory.CreateChannelAsync(stoppingToken)
            .ConfigureAwait(false))
        {
            await aeChannel.ExchangeDeclareAsync(
                exchange: ExchangeNames.AlternateExchange,
                type: ExchangeType.Fanout,
                durable: true,
                cancellationToken: stoppingToken).ConfigureAwait(false);

            await aeChannel.QueueDeclareAsync(
                queue: QueueNames.Alternate,
                durable: true,
                exclusive: false,
                autoDelete: false,
                cancellationToken: stoppingToken).ConfigureAwait(false);

            await aeChannel.QueueBindAsync(
                queue: QueueNames.Alternate,
                exchange: ExchangeNames.AlternateExchange,
                routingKey: string.Empty,
                cancellationToken: stoppingToken).ConfigureAwait(false);
        }

        // 声明业务交换机（含备用交换机参数）
        await _consumeChannel.ExchangeDeclareAsync(
            exchange: ExchangeName,
            type: ExchangeTypeName,
            durable: true,
            arguments: new Dictionary<string, object?>
            {
                { "alternate-exchange", ExchangeNames.AlternateExchange }
            },
            cancellationToken: stoppingToken).ConfigureAwait(false);

        // 声明队列
        await _consumeChannel.QueueDeclareAsync(
            queue: QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: queueArgs,
            cancellationToken: stoppingToken).ConfigureAwait(false);

        // 绑定
        await _consumeChannel.QueueBindAsync(
            queue: QueueName,
            exchange: ExchangeName,
            routingKey: BindingRoutingKey,
            cancellationToken: stoppingToken).ConfigureAwait(false);

        // 创建消费者
        var consumer = new AsyncEventingBasicConsumer(_consumeChannel);

        consumer.ReceivedAsync += OnMessageReceivedAsync;

        // 监听 Channel 关闭事件（替代 ConsumerCancelledAsync，7.x 中不再存在）
        _consumeChannel.ChannelShutdownAsync += (_, args) =>
        {
            _logger.LogWarning("消费者 Channel 关闭: {ReplyCode} - {ReplyText}",
                args.ReplyCode, args.ReplyText);
            return Task.CompletedTask;
        };

        _consumerTag = await _consumeChannel.BasicConsumeAsync(
            queue: QueueName,
            autoAck: _options.Value.Consumer.AutoAck,
            consumer: consumer,
            cancellationToken: stoppingToken).ConfigureAwait(false);

        _logger.LogInformation(
            "消费者已启动: Queue={Queue}, Exchange={Exchange}, " +
            "RoutingKey={RoutingKey}, ConsumerTag={ConsumerTag}, Prefetch={Prefetch}",
            QueueName, ExchangeName, BindingRoutingKey,
            _consumerTag, _options.Value.Consumer.PrefetchCount);

        // 等待直到取消
        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // 正常退出
        }

        _logger.LogInformation("消费者停止中: ConsumerTag={ConsumerTag}", _consumerTag);
    }

    private async Task OnMessageReceivedAsync(object? sender, BasicDeliverEventArgs args)
    {
        var messageId = args.BasicProperties.MessageId ?? Guid.NewGuid().ToString("N");
        var body = args.Body.ToArray();

        using var activity = ExtractTraceContext(args, messageId);
        activity?.SetTag("message.id", messageId);
        activity?.SetTag("messaging.system", "rabbitmq");
        activity?.SetTag("messaging.destination", args.Exchange);
        activity?.SetTag("messaging.rabbitmq.routing_key", args.RoutingKey);
        activity?.SetTag("messaging.rabbitmq.queue", QueueName);

        using var logScope = _logger.BeginScope(new Dictionary<string, object>
        {
            ["MessageId"] = messageId,
            ["Exchange"] = args.Exchange,
            ["RoutingKey"] = args.RoutingKey,
            ["Queue"] = QueueName
        });

        try
        {
            // 通过 Polly 有限重试处理消息
            var success = await _retryPipeline.ExecuteAsync(
                async ct =>
                {
                    // 由 MessageProcessor 处理幂等检查 + 业务逻辑
                    return await ProcessMessageAsync(body, messageId, ct).ConfigureAwait(false);
                },
                args.CancellationToken).ConfigureAwait(false);

            if (success)
            {
                await _consumeChannel!.BasicAckAsync(
                    deliveryTag: args.DeliveryTag,
                    multiple: false,
                    cancellationToken: args.CancellationToken).ConfigureAwait(false);

                _logger.LogInformation("消息确认完成: MessageId={MessageId}, DeliveryTag={DeliveryTag}",
                    messageId, args.DeliveryTag);

                // 追踪消费成功
                _ = _messageTracker.RecordConsumedAsync(
                    messageId, QueueName, GetType().Name, args.CancellationToken)
                    .ConfigureAwait(false);
            }
            else
            {
                // 重试耗尽 → Nack → 进入死信队列
                await _consumeChannel!.BasicNackAsync(
                    deliveryTag: args.DeliveryTag,
                    multiple: false,
                    requeue: false,
                    cancellationToken: args.CancellationToken).ConfigureAwait(false);

                _logger.LogWarning(
                    "消息处理失败，已发送至死信队列: MessageId={MessageId}, DeliveryTag={DeliveryTag}",
                    messageId, args.DeliveryTag);

                // 追踪死信
                _ = _messageTracker.RecordDeadLetteredAsync(
                    messageId, QueueName, "重试耗尽，业务处理失败", args.CancellationToken)
                    .ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "消息处理发生未捕获异常，Nack 入死信: MessageId={MessageId}", messageId);

            // 追踪未捕获异常导致的死信
            _ = _messageTracker.RecordDeadLetteredAsync(
                messageId, QueueName, $"未捕获异常: {ex.Message}", CancellationToken.None)
                .ConfigureAwait(false);

            try
            {
                await _consumeChannel!.BasicNackAsync(
                    deliveryTag: args.DeliveryTag,
                    multiple: false,
                    requeue: false,
                    cancellationToken: CancellationToken.None).ConfigureAwait(false);
            }
            catch
            {
                // 忽略 Nack 阶段的异常，避免死循环
            }
        }
    }

    /// <summary>
    /// 子类实现具体的消息反序列化和业务处理。
    /// </summary>
    protected abstract Task<bool> ProcessMessageCoreAsync(byte[] body, CancellationToken cancellationToken);

    private async Task<bool> ProcessMessageAsync(byte[] body, string messageId, CancellationToken ct)
    {
        // 先通过 MessageProcessor 进行幂等判断
        return await _messageProcessor.ProcessAsync<byte[]>(
            body,
            messageId,
            async (data, token) =>
            {
                // 幂等检查通过后，由子类处理业务
                return await ProcessMessageCoreAsync(data, token).ConfigureAwait(false);
            },
            ct).ConfigureAwait(false);
    }

    /// <summary>
    /// 从消息 Headers 手动提取 W3C TraceContext 并恢复 Activity。
    /// </summary>
    private Activity? ExtractTraceContext(BasicDeliverEventArgs args, string messageId)
    {
        var headers = args.BasicProperties.Headers;
        if (headers is null || !headers.TryGetValue("traceparent", out var tp) || tp is not string traceparent)
            return _activitySource.StartActivity(ActivityKind.Consumer, name: "Consume");

        // 手动解析 traceparent: "00-trace_id-span_id-flags"
        var parts = traceparent.Split('-');
        if (parts.Length < 3 || !ActivityContext.TryParse(traceparent, null, out var actCtx))
            return _activitySource.StartActivity(ActivityKind.Consumer, name: "Consume");

        return _activitySource.StartActivity(
            ActivityKind.Consumer,
            name: "Consume",
            parentContext: actCtx);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("消费者正在优雅关闭: ConsumerTag={ConsumerTag}", _consumerTag);

        if (_consumeChannel is not null && !string.IsNullOrEmpty(_consumerTag))
        {
            try
            {
                await _consumeChannel.BasicCancelAsync(
                    consumerTag: _consumerTag,
                    noWait: false,
                    cancellationToken: cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "取消消费者时发生异常: {ConsumerTag}", _consumerTag);
            }
        }

        await base.StopAsync(cancellationToken).ConfigureAwait(false);
    }

    public override void Dispose()
    {
        _activitySource.Dispose();
        base.Dispose();
    }
}
