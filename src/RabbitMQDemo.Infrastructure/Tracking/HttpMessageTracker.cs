using System.Net.Http.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQDemo.Shared.Interfaces;
using RabbitMQDemo.Shared.Messages;

namespace RabbitMQDemo.Infrastructure.Tracking;

/// <summary>
/// 消息追踪 HTTP 客户端 —— 将消息生命周期事件推送到 Dashboard 服务。
/// 采用 fire-and-forget 模式，不阻塞业务主流程。
/// </summary>
public sealed class HttpMessageTracker : IMessageTracker, IDisposable
{
    private readonly HttpClient _httpClient;
    private readonly ILogger<HttpMessageTracker> _logger;
    private readonly string _baseUrl;

    public HttpMessageTracker(
        HttpClient httpClient,
        IOptions<DashboardOptions> options,
        ILogger<HttpMessageTracker> logger)
    {
        _httpClient = httpClient;
        _logger = logger;
        _baseUrl = options.Value.BaseUrl.TrimEnd('/');
    }

    public async Task RecordPublishedAsync(MessageRecord record, CancellationToken ct = default)
    {
        try
        {
            var response = await _httpClient.PostAsJsonAsync(
                $"{_baseUrl}/api/track/publish", record, ct).ConfigureAwait(false);

            if (!response.IsSuccessStatusCode)
            {
                _logger.LogWarning(
                    "追踪推送失败 (Published): {StatusCode}, MessageId={MessageId}",
                    response.StatusCode, record.MessageId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "追踪推送异常 (Published)，不影响主流程: {MessageId}", record.MessageId);
        }
    }

    public async Task RecordConsumedAsync(string messageId, string consumerQueue, string consumerWorker, CancellationToken ct = default)
    {
        try
        {
            var payload = new { messageId, consumerQueue, consumerWorker };
            var response = await _httpClient.PostAsJsonAsync(
                $"{_baseUrl}/api/track/consume", payload, ct).ConfigureAwait(false);

            if (!response.IsSuccessStatusCode)
            {
                _logger.LogWarning(
                    "追踪推送失败 (Consumed): {StatusCode}, MessageId={MessageId}",
                    response.StatusCode, messageId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "追踪推送异常 (Consumed)，不影响主流程: {MessageId}", messageId);
        }
    }

    public async Task RecordDeadLetteredAsync(string messageId, string consumerQueue, string errorInfo, CancellationToken ct = default)
    {
        try
        {
            var payload = new { messageId, consumerQueue, errorInfo };
            var response = await _httpClient.PostAsJsonAsync(
                $"{_baseUrl}/api/track/deadletter", payload, ct).ConfigureAwait(false);

            if (!response.IsSuccessStatusCode)
            {
                _logger.LogWarning(
                    "追踪推送失败 (DeadLetter): {StatusCode}, MessageId={MessageId}",
                    response.StatusCode, messageId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "追踪推送异常 (DeadLetter)，不影响主流程: {MessageId}", messageId);
        }
    }

    public async Task RecordUnroutableAsync(string messageId, string exchange, string routingKey, string errorInfo, CancellationToken ct = default)
    {
        try
        {
            var payload = new { messageId, exchange, routingKey, errorInfo };
            var response = await _httpClient.PostAsJsonAsync(
                $"{_baseUrl}/api/track/unroutable", payload, ct).ConfigureAwait(false);

            if (!response.IsSuccessStatusCode)
            {
                _logger.LogWarning(
                    "追踪推送失败 (Unroutable): {StatusCode}, MessageId={MessageId}",
                    response.StatusCode, messageId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "追踪推送异常 (Unroutable)，不影响主流程: {MessageId}", messageId);
        }
    }

    public void Dispose() => _httpClient.Dispose();
}

/// <summary>
/// Dashboard 连接配置。
/// </summary>
public sealed class DashboardOptions
{
    public const string SectionName = "Dashboard";

    /// <summary>Dashboard 服务地址，默认为 http://localhost:5100。</summary>
    public string BaseUrl { get; set; } = "http://localhost:5100";
}
