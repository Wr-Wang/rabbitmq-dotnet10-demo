using System.Diagnostics;
using Microsoft.Extensions.Logging;
using RabbitMQDemo.Shared.Interfaces;

namespace RabbitMQDemo.Infrastructure.Consuming;

/// <summary>
/// 消息处理管道 —— 组合幂等检查和业务处理。
/// </summary>
public sealed class MessageProcessor
{
    private readonly IIdempotencyChecker _idempotencyChecker;
    private readonly ILogger<MessageProcessor> _logger;

    public MessageProcessor(
        IIdempotencyChecker idempotencyChecker,
        ILogger<MessageProcessor> logger)
    {
        _idempotencyChecker = idempotencyChecker;
        _logger = logger;
    }

    /// <summary>
    /// 处理消息：幂等检查 → 执行业务 → 标记已处理。
    /// </summary>
    /// <returns>处理成功返回 true；若因幂等跳过也返回 true（不视为错误）；业务异常返回 false。</returns>
    public async Task<bool> ProcessAsync<T>(
        T message,
        string messageId,
        Func<T, CancellationToken, Task<bool>> businessHandler,
        CancellationToken cancellationToken = default)
    {
        // 幂等检查
        if (await _idempotencyChecker.IsProcessedAsync(messageId).ConfigureAwait(false))
        {
            _logger.LogInformation(
                "消息已被处理，跳过 (幂等去重): MessageId={MessageId}", messageId);
            return true;
        }

        // 业务处理
        try
        {
            var success = await businessHandler(message, cancellationToken).ConfigureAwait(false);
            if (success)
            {
                await _idempotencyChecker.MarkAsProcessedAsync(messageId).ConfigureAwait(false);
                _logger.LogDebug("消息处理成功: MessageId={MessageId}", messageId);
            }
            return success;
        }
        catch (UnrecoverableMessageException)
        {
            _logger.LogWarning(
                "不可恢复异常，跳过重试直接入死信: MessageId={MessageId}", messageId);
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "消息处理异常: MessageId={MessageId}, 错误={Error}",
                messageId, ex.Message);
            return false;
        }
    }
}

/// <summary>
/// 不可恢复消息异常 —— 遇到此异常时不重试，直接入死信队列。
/// </summary>
public sealed class UnrecoverableMessageException : Exception
{
    public UnrecoverableMessageException(string message) : base(message) { }
    public UnrecoverableMessageException(string message, Exception inner) : base(message, inner) { }
}
