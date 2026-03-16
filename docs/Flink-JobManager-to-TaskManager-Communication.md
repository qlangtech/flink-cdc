# Flink 外部消息下推机制：外部 → JobManager → TaskManager 算子

## 概述

Flink 提供了多种机制，支持从外部系统将消息发送到 JobManager，再由 JobManager 主动通过 RPC 下推到 TaskManager 中对应 Job 的算子上。以下逐一分析这些机制的原理、接口和使用场景。

## 一、OperatorCoordinator + SubtaskGateway（通用算子）

这是 Flink 的 `OperatorCoordinator` 框架提供的**通用下行通道**，适用于任意类型的算子。

### 1.1 通信路径

```
外部系统
   │
   │  REST API: POST /jobs/:jobid/coordinators/:operatorid
   │  (发送 CoordinationRequest)
   ▼
JobManager
   │  OperatorCoordinatorHolder
   │  → OperatorCoordinator.handleCoordinationRequest()
   │
   │  可以通过 SubtaskGateway.sendEvent(OperatorEvent) 主动下推消息
   │  (Pekko RPC)
   ▼
TaskManager
   │  OperatorEventHandler.handleOperatorEvent(OperatorEvent)
   ▼
具体算子 (Source / Sink / 任意算子)
```

### 1.2 关键接口

**JobManager 端 — Coordinator 获取 gateway 后可以主动推送：**

```java
// OperatorCoordinator 接口方法
// 当 TaskManager 上的算子实例就绪时，Flink 框架会回调此方法并传入 gateway
public void executionAttemptReady(int subTaskId, int attemptNumber, SubtaskGateway gateway) {
    // 保存 gateway，后续可以随时调用：
    gateway.sendEvent(new MyCustomOperatorEvent(...));
}
```

**TaskManager 端 — 算子接收来自 Coordinator 的事件：**

```java
// 算子实现 OperatorEventHandler 接口
public void handleOperatorEvent(OperatorEvent event) {
    // 处理来自 JobManager 的下推消息
}
```

### 1.3 备注

flink-cdc 的 `SchemaRegistry` 在 `executionAttemptReady()` 中注释写了：

```java
@Override
public final void executionAttemptReady(
        int subTaskId, int attemptNumber, SubtaskGateway gateway) {
    // Needless to do anything. SchemaRegistry does not post message to the coordinator
    // spontaneously.
}
```

即 SchemaRegistry 没有使用这个下行通道，但这个能力在框架层面是完整存在的。

## 二、FLIP-27 Source API（专门针对 Source 算子）

这是 Flink 新 Source API（FLIP-27）专门为 Source 设计的**双向通信机制**，也是 flink-cdc 的 Source Connector 实际使用的方式。

### 2.1 通信路径

```
                    JobManager 端                          TaskManager 端
              ┌──────────────────────┐              ┌──────────────────────┐
              │   SplitEnumerator    │              │    SourceReader      │
              │   (分片枚举器)        │              │    (数据读取器)       │
              └──────────────────────┘              └──────────────────────┘
                        │                                     ▲
                        │  ① assignSplit(split, subtask)      │
                        │     分配数据分片给 Reader            │
                        ├────────────────────────────────────►│
                        │                                     │
                        │  ② sendEventToSourceReader(         │
                        │       subtask, SourceEvent)         │
                        │     发送自定义事件给 Reader           │
                        ├────────────────────────────────────►│
                        │                                     │
                        │  ③ handleSourceEvent(               │
                        │       subtask, SourceEvent)         │
                        │     接收 Reader 上报的事件           │
                        │◄────────────────────────────────────┤
                        │                                     │
```

### 2.2 三种通信方式

| 方法 | 方向 | 说明 |
|------|------|------|
| `assignSplit(split, subtask)` | Enumerator → Reader | 分配数据分片给指定 Reader |
| `sendEventToSourceReader(subtask, SourceEvent)` | Enumerator → Reader | 发送自定义事件给指定 Reader |
| `sendSourceEventToCoordinator(SourceEvent)` | Reader → Enumerator | Reader 上报事件给 Enumerator |

### 2.3 flink-cdc 中的实际例子

以 Kingbase/Postgres Connector 为例：

**JobManager 端 — Enumerator 主动向 Reader 发送 OffsetCommitEvent：**

```java
// PostgresSourceEnumerator.java
public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
    if (sourceEvent instanceof OffsetCommitAckEvent) {
        // Reader 确认后，向 Reader 发送新的 commit 事件
        context.sendEventToSourceReader(subtaskId, new OffsetCommitEvent(...));
    } else {
        super.handleSourceEvent(subtaskId, sourceEvent);
    }
}
```

**TaskManager 端 — Reader 接收并处理：**

```java
// PostgresSourceReader.java
public void handleSourceEvents(SourceEvent sourceEvent) {
    if (sourceEvent instanceof OffsetCommitEvent) {
        // 处理来自 Enumerator 的指令
        context.sendSourceEventToCoordinator(new OffsetCommitAckEvent());
    } else {
        super.handleSourceEvents(sourceEvent);
    }
}
```

## 三、REST API 直接触发 Coordinator

从 Flink 1.18 开始，外部系统可以通过 REST API 直接向 Coordinator 发送 `CoordinationRequest`：

```bash
curl -X POST http://jobmanager:8081/jobs/{jobId}/coordinators/{operatorId} \
  -d '{"serializedCoordinationRequest": "<base64-encoded-request>"}'
```

这使得外部系统（如管控平台）可以直接与 JobManager 中的 Coordinator 通信，Coordinator 再通过 `SubtaskGateway` 下推到 TaskManager 上的算子。

### 3.1 完整链路

```
外部管控平台 / 运维系统
   │
   │  HTTP POST (REST API)
   ▼
Flink REST Endpoint (JobManager)
   │
   │  反序列化 CoordinationRequest
   ▼
OperatorCoordinatorHolder
   │
   │  调用 coordinator.handleCoordinationRequest(request)
   ▼
OperatorCoordinator (如 SchemaRegistry)
   │
   │  处理请求后，可通过 SubtaskGateway.sendEvent() 下推
   ▼
TaskManager 上的算子实例
```

## 四、总结对比

| 机制 | 适用场景 | 外部可触发 | 方向 |
|------|---------|-----------|------|
| `SubtaskGateway.sendEvent()` | 任意算子 | 通过 REST API 间接触发 | JobManager → TaskManager |
| `SplitEnumeratorContext.assignSplit()` | Source 算子 | 否（内部调度） | JobManager → TaskManager |
| `SplitEnumeratorContext.sendEventToSourceReader()` | Source 算子 | 否（内部调度） | JobManager → TaskManager |
| REST `/coordinators/:id` | 任意 Coordinator | 是 | 外部 → JobManager → TaskManager |

最直接的外部触发路径是：**REST API → OperatorCoordinator → SubtaskGateway.sendEvent() → 算子**。

对于 Source 算子，还有专门的 FLIP-27 Source API 提供更丰富的双向通信能力（分片分配 + 自定义事件）。

## 五、底层 RPC 实现

所有 JobManager 与 TaskManager 之间的跨进程通信，底层均通过 **Apache Pekko**（Akka 的 Apache 许可证分支）实现：

| Flink 版本 | RPC 框架 | 说明 |
|-----------|----------|------|
| < 1.18 | Akka | Flink 早期使用 Akka 作为底层 RPC |
| 1.18+ | Apache Pekko | 由于 Akka 更改为 BSL 商业许可证，Flink 迁移到 Pekko |

本项目使用 **Flink 1.20.1**，因此底层 RPC 框架为 **Apache Pekko**。
