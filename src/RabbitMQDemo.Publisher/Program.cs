using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQDemo.Infrastructure.DependencyInjection;
using RabbitMQDemo.Publisher;

var builder = Host.CreateApplicationBuilder(args);

// 注册 RabbitMQ 基础设施（含 RabbitMQPublisher）
builder.Services.AddRabbitMQ();

// 注册发布者后台服务
builder.Services.AddHostedService<PublisherWorker>();

var host = builder.Build();

// 初始化发布者（声明交换机等）
await host.Services.UseRabbitMQPublisherAsync().ConfigureAwait(false);

await host.RunAsync().ConfigureAwait(false);
