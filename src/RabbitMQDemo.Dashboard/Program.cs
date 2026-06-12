using RabbitMQDemo.Dashboard.Services;
using RabbitMQDemo.Shared.Messages;

var builder = WebApplication.CreateBuilder(args);

// CORS — 允许 Publisher/Subscriber 跨进程推送
builder.Services.AddCors(options =>
    options.AddDefaultPolicy(policy =>
        policy.AllowAnyOrigin().AllowAnyMethod().AllowAnyHeader()));

// 消息存储（单例，内存）
builder.Services.AddSingleton<InMemoryMessageStore>();

var app = builder.Build();
app.UseCors();
app.UseDefaultFiles();
app.UseStaticFiles(); // wwwroot/index.html

var store = app.Services.GetRequiredService<InMemoryMessageStore>();

// ─── API: 记录消息已发布 ───
app.MapPost("/api/track/publish", (MessageRecord record) =>
{
    store.Add(record);
    return Results.Ok();
});

// ─── API: 记录消息已消费 ───
app.MapPost("/api/track/consume", (ConsumeRequest req) =>
{
    var updated = store.UpdateState(req.MessageId, MessageStates.Consumed,
        record =>
        {
            record.ConsumedAt = DateTime.UtcNow;
            record.ConsumerQueue = req.ConsumerQueue;
            record.ConsumerWorker = req.ConsumerWorker;
        });
    return updated ? Results.Ok() : Results.NotFound();
});

// ─── API: 记录消息已入死信 ───
app.MapPost("/api/track/deadletter", (DeadLetterRequest req) =>
{
    var updated = store.UpdateState(req.MessageId, MessageStates.DeadLettered,
        record =>
        {
            record.ConsumerQueue = req.ConsumerQueue;
            record.ErrorInfo = req.ErrorInfo;
        });
    return updated ? Results.Ok() : Results.NotFound();
});

// ─── API: 记录消息路由失败 ───
app.MapPost("/api/track/unroutable", (UnroutableRequest req) =>
{
    store.Add(new MessageRecord
    {
        MessageId = req.MessageId,
        Exchange = req.Exchange,
        RoutingKey = req.RoutingKey,
        PublishedAt = DateTime.UtcNow,
        State = MessageStates.Unroutable,
        ErrorInfo = req.ErrorInfo,
        BodySummary = "(路由失败)"
    });
    return Results.Ok();
});

// ─── API: 获取消息列表 ───
app.MapGet("/api/messages", (string? state, int count = 200) =>
{
    var all = store.GetRecent(count);
    var messages = string.IsNullOrEmpty(state)
        ? all
        : all.Where(m => m.State == state).ToList();
    return Results.Ok(messages);
});

// ─── API: 获取统计汇总 ───
app.MapGet("/api/stats", () =>
{
    var all = store.GetRecent(int.MaxValue);
    var stats = new
    {
        total = all.Count,
        published = all.Count(m => m.State == MessageStates.Published),
        consumed = all.Count(m => m.State == MessageStates.Consumed),
        deadLettered = all.Count(m => m.State == MessageStates.DeadLettered),
        unroutable = all.Count(m => m.State == MessageStates.Unroutable),
        totalAmount = all.Sum(m => m.Amount ?? 0)
    };
    return Results.Ok(stats);
});

// ─── API: 清空消息记录 ───
app.MapDelete("/api/messages", () =>
{
    store.Clear();
    return Results.Ok();
});

app.Run();

// ─── 请求 DTO ───
record ConsumeRequest(string MessageId, string ConsumerQueue, string ConsumerWorker);
record DeadLetterRequest(string MessageId, string ConsumerQueue, string ErrorInfo);
record UnroutableRequest(string MessageId, string Exchange, string RoutingKey, string ErrorInfo);
