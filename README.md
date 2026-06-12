# RabbitMQ .NET 10 Demo

基于 **.NET 10** 的生产级 RabbitMQ 发布/订阅演示项目，涵盖订单生命周期模拟、死信队列、备用交换机、幂等消息处理和实时消息追踪仪表盘。

## 项目结构

```
├── src/
│   ├── RabbitMQDemo.Shared/          # 共享库 — 模型、接口、常量
│   ├── RabbitMQDemo.Infrastructure/  # 基础设施层 — 连接、发布、消费、序列化
│   ├── RabbitMQDemo.Publisher/       # 发布者 — 模拟订单生命周期的控制台应用
│   ├── RabbitMQDemo.Subscriber/      # 订阅者 — 3 种消费者的 Worker 服务
│   └── RabbitMQDemo.Dashboard/       # 仪表盘 — ASP.NET Core Minimal API + 浏览器 UI
└── tests/
    └── RabbitMQDemo.Tests/           # xUnit 单元测试 & 集成测试（Testcontainers）
```

## 功能特性

### 三种交换机消息模式

| 交换机 | 类型 | 路由方式 | 消费者 |
|--------|------|----------|--------|
| `order.fanout` | **Fanout** | 广播 — 所有绑定队列都收到所有事件 | `FanoutConsumerWorker` |
| `order.direct` | **Direct** | 精确路由 — `order.created` → `order.created.q` | `DirectConsumerWorker` |
| `order.topic` | **Topic** | 模式匹配 — `order.*` 匹配所有订单事件 | `TopicConsumerWorker` |

### 订单生命周期模拟

发布者模拟真实订单生命周期：

1. **创建 (Created)**（60% 概率）— 新订单发布到全部 3 个交换机
2. **支付 (Paid)**（约 24%）— 已有的 Created 订单推进到 Paid
3. **发货 (Shipped)**（约 16%）— 已有的 Paid 订单推进到 Shipped，移出活跃列表

每个周期 **3 秒**，商品随机（笔记本电脑、机械键盘、显示器等）。

### 企业级基础设施

- **发布确认 (Publisher Confirms)** — 异步确认的可靠消息投递
- **Mandatory 标志 + BasicReturn** — 检测不可路由的消息
- **备用交换机 (Alternate Exchange)** — `order.unrouted.ae` 捕获路由失败消息
- **死信交换机 (DLX)** — `dlx.order` 通过 `x-dead-letter-exchange` 接收消费失败的消息
- **幂等检查器** — 内存去重（可替换为 Redis 生产方案）
- **Polly 重试管道** — 指数退避 + 随机抖动（±25%）增强消费者弹性
- **W3C TraceContext 传播** — 跨 Publisher → RabbitMQ → Subscriber 的分布式追踪
- **健康检查** — `rabbitmq-connection`（连接状态）、`rabbitmq-dlq`（死信积压监控）

### 实时仪表盘

浏览器访问 **http://localhost:5100** 进行可视化监控：

- **4 个统计卡片**：已发布 / 已消费 / 死信 / 路由失败数量
- **13 列表格**：时间戳（毫秒精度）、耗时（分/秒/毫秒）、MessageId、交换机、路由键、内容摘要、金额、类型、状态、消费者、错误信息
- **2 秒自动刷新** + 实时指示器
- **状态筛选**（全部 / 已发布 / 已消费 / 死信 / 路由失败）
- **中英双语显示**

## 快速开始

### 前置条件

| 依赖 | 版本 |
|------|------|
| [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0) | 10.0.300+ |
| [RabbitMQ Server](https://www.rabbitmq.com/docs/install-windows) | 4.3.1+ |
| [Erlang/OTP](https://www.erlang.org/downloads) | 27.x |

安装包（`otp_win64_27.3.4.exe`、`rabbitmq-server-4.3.1.exe`）位于 `assets/` 目录，详细安装步骤参见[环境安装说明](docs/环境安装说明.md)。

### 启动 RabbitMQ

```bash
# 启用管理插件（推荐）
rabbitmq-plugins enable rabbitmq_management

# 启动服务
# Windows 上使用 start_rabbitmq.bat 脚本或 RabbitMQ 服务
```

验证：`http://localhost:15672`（guest/guest）

### 运行应用

打开 **4 个终端**，分别运行：

```bash
# 终端 1 — 仪表盘（ASP.NET Core，端口 5100）
cd src/RabbitMQDemo.Dashboard
dotnet run

# 终端 2 — 订阅者（3 个消费者：Fanout、Direct、Topic）
cd src/RabbitMQDemo.Subscriber
dotnet run

# 终端 3 — 发布者（订单生命周期模拟）
cd src/RabbitMQDemo.Publisher
dotnet run
```

打开浏览器访问 **http://localhost:5100** 查看实时消息追踪。

### 运行测试

```bash
dotnet test
```

## 架构

### 组件图

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   发布者      │────▶│  RabbitMQ    │◀────│   订阅者      │
│  (控制台)     │     │  (消息代理)   │     │  (Worker)    │
└──────┬───────┘     └──────┬───────┘     └──────┬───────┘
       │                    │                     │
       │   POST /api/track  │                     │
       └────────────────────┼─────────────────────┘
                            ▼
                   ┌────────────────┐
                   │   仪表盘        │
                   │ (ASP.NET, :5100) │
                   └────────────────┘
```

- **发布者** 发送订单事件 → RabbitMQ（3 个交换机）
- **订阅者** 从 3 个队列消费 → 追踪消费事件 → Dashboard HTTP API
- **仪表盘** 内存存储 → 提供 REST API + 静态 HTML（浏览器每 2 秒轮询）
- 所有追踪调用均是 **fire-and-forget** 模式，不阻塞业务逻辑

### 基础设施设计

```csharp
// 1. 配置服务
services.AddRabbitMQ();

// 2. 初始化发布者（声明交换机、备用交换机、发布确认）
await host.Services.UseRabbitMQPublisherAsync();

// 3. 消费者基类（自动处理：连接、Channel、Ack/Nack、死信、重试）
public class MyConsumer : RabbitMQConsumerService { ... }
```

### RabbitMQ 拓扑

```
交换机:
  order.fanout (fanout) ──────┬── alternate-exchange → order.unrouted.ae (fanout)
  order.direct (direct) ──────┤                              │
  order.topic  (topic)  ──────┤                              └── order.unrouted.q
                              │
  dlx.order    (fanout)  ←────┘ (队列的 x-dead-letter-exchange 设置)

队列:
  order.fanout.q     ← 绑定到 order.fanout
  order.created.q    ← 绑定到 order.direct（路由键: order.created）
  order.topic.q      ← 绑定到 order.topic（路由键: order.*）
  order.dlq          ← 绑定到 dlx.order
  order.unrouted.q   ← 绑定到 order.unrouted.ae
```

## 关键包

| 包 | 版本 | 用途 |
|----|------|------|
| `RabbitMQ.Client` | 7.2.1 | 异步优先的 AMQP 客户端 |
| `Polly.Core` | 8.7.0 | 弹性和重试管道 |
| `System.Text.Json` | 内置 | JSON 序列化 |
| `xunit` / `Moq` / `Testcontainers.RabbitMq` | 最新 | 测试 |

## 配置

### `appsettings.json`

```json
{
  "RabbitMQ": {
    "Host": "localhost",
    "Port": 5672,
    "UserName": "guest",
    "Password": "guest",
    "PublisherConfirms": true,
    "Mandatory": true,
    "DeliveryMode": 2,
    "Consumer": {
      "PrefetchCount": 10,
      "AutoAck": false,
      "Retry": {
        "MaxRetryCount": 3,
        "BaseDelaySeconds": 1,
        "MaxDelaySeconds": 30
      }
    }
  },
  "Dashboard": {
    "BaseUrl": "http://localhost:5100"
  }
}
```

## 项目文件说明

| 文件 | 用途 |
|------|------|
| `Shared/Messages/OrderEvent.cs` | 订单事件数据模型 |
| `Shared/Messages/MessageEnvelope.cs` | 带 MessageId 的通用消息信封 |
| `Shared/Messages/MessageRecord.cs` | 完整生命周期追踪记录 |
| `Infrastructure/Connection/` | 带自动重连的连接工厂 |
| `Infrastructure/Publishing/` | 含确认、Mandatory、追踪的发布者 |
| `Infrastructure/Consuming/` | 含死信和重试的消费者基类 |
| `Infrastructure/DeadLetter/` | DLX/DLQ 拓扑声明 |
| `Infrastructure/HealthChecks/` | 连接 + 死信健康检查 |
| `Infrastructure/Tracking/` | HTTP 追踪客户端 → Dashboard API |
| `Dashboard/Program.cs` | Minimal API REST 接口 |
| `Dashboard/wwwroot/index.html` | 中英双语浏览器界面 |
| `Publisher/PublisherWorker.cs` | 订单生命周期模拟 |
