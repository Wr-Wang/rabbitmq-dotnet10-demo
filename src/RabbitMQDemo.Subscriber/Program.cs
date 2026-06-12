using RabbitMQDemo.Infrastructure.DependencyInjection;
using RabbitMQDemo.Subscriber.Workers;

var builder = Host.CreateApplicationBuilder(args);

// 注册 RabbitMQ 基础设施
builder.Services.AddRabbitMQ();

// 注册三个消费者 Worker
builder.Services.AddHostedService<FanoutConsumerWorker>();
builder.Services.AddHostedService<DirectConsumerWorker>();
builder.Services.AddHostedService<TopicConsumerWorker>();

var host = builder.Build();

await host.RunAsync().ConfigureAwait(false);
