# SchemaRegistry.handleCoordinationRequest() 调用链路分析

## 一、方法概览

`handleCoordinationRequest()` 位于 `SchemaRegistry.java:L243`，是 Flink CDC Schema 演化协调机制的**统一入口**。它实现了 Flink 的 `CoordinationRequestHandler` 接口，接收来自 TaskManager 端算子的 RPC 请求，并根据请求类型分发到不同的处理逻辑。

**文件路径：**
```
flink-cdc-runtime/src/main/java/org/apache/flink/cdc/runtime/operators/schema/common/SchemaRegistry.java
```

## 二、请求来源

请求通过 Flink 的 `TaskOperatorEventGateway.sendRequestToCoordinator()` RPC 机制，从 TaskManager 端发送到 JobManager 端的 SchemaRegistry Coordinator。

### 2.1 发送者 1：SchemaEvolutionClient（Sink Writer 端）

**文件路径：**
```
flink-cdc-runtime/src/main/java/org/apache/flink/cdc/runtime/operators/sink/SchemaEvolutionClient.java
```

`DataSinkWriterOperator` 通过 `SchemaEvolutionClient` 发送 **Schema 查询请求**：

| 方法 | 发送的请求类型 | 用途 |
|------|---------------|------|
| `getLatestEvolvedSchema()` | `GetEvolvedSchemaRequest` | 查询某张表最新的演化后 Schema |
| `getLatestOriginalSchema()` | `GetOriginalSchemaRequest` | 查询某张表最新的原始 Schema |

**调用场景：** 当 `DataSinkWriterOperator` 收到一个新表的数据，但本地缓存中没有该表的 Schema 时，会通过 `emitLatestSchema()` 方法向 SchemaRegistry 查询，确保 Sink 在处理数据前拥有正确的 Schema 视图。

### 2.2 发送者 2：SchemaOperator（Schema 变更算子）

SchemaOperator 存在两种拓扑模式，均通过 `sendRequestToCoordinator()` 发送 `SchemaChangeRequest`：

#### Regular 模式

**文件路径：**
```
flink-cdc-runtime/src/main/java/org/apache/flink/cdc/runtime/operators/schema/regular/SchemaOperator.java
```

- 当收到上游的 `SchemaChangeEvent` 时，调用 `requestSchemaChange()` 方法（L225-228）
- 发送 `regular.event.SchemaChangeRequest`，包含：`tableId`、`schemaChangeEvent`、`subTaskId`

#### Distributed 模式

**文件路径：**
```
flink-cdc-runtime/src/main/java/org/apache/flink/cdc/runtime/operators/schema/distributed/SchemaOperator.java
```

- 当收到上游的 `SchemaChangeEvent` 时，调用 `requestSchemaChange()` 方法（L192-203）
- 发送 `distributed.event.SchemaChangeRequest`，包含：`sourcePartition`、`sinkSubTaskId`、`schemaChangeEvent`

## 三、请求分发逻辑

`handleCoordinationRequest()` 的核心逻辑是一个**请求分发器（Dispatcher）**：

```java
@Override
public final CompletableFuture<CoordinationResponse> handleCoordinationRequest(
        CoordinationRequest request) {
    CompletableFuture<CoordinationResponse> future = new CompletableFuture<>();
    runInEventLoop(() -> {
        if (request instanceof GetEvolvedSchemaRequest) {
            handleGetEvolvedSchemaRequest((GetEvolvedSchemaRequest) request, future);
        } else if (request instanceof GetOriginalSchemaRequest) {
            handleGetOriginalSchemaRequest((GetOriginalSchemaRequest) request, future);
        } else {
            handleCustomCoordinationRequest(request, future);
        }
    }, "Handling request - %s", request);
    return future;
}
```

分发路径如下：

```
handleCoordinationRequest(request)
   ├── GetEvolvedSchemaRequest   → handleGetEvolvedSchemaRequest()   // 查询演化后 Schema
   ├── GetOriginalSchemaRequest  → handleGetOriginalSchemaRequest()  // 查询原始 Schema
   └── 其他 (SchemaChangeRequest) → handleCustomCoordinationRequest() // 处理 Schema 变更
```

其中 `handleCustomCoordinationRequest()` 是抽象方法，由两个子类分别实现：

| 子类 | 文件路径 |
|------|----------|
| `regular.SchemaCoordinator` | `.../schema/regular/SchemaCoordinator.java` |
| `distributed.SchemaCoordinator` | `.../schema/distributed/SchemaCoordinator.java` |

## 四、架构全景图

```
数据流拓扑：

  Source → SchemaOperator → [Partitioner] → DataSinkWriterOperator → Sink
               │                                    │
               │  SchemaChangeRequest               │  GetEvolvedSchemaRequest
               │  (请求变更 Schema)                  │  GetOriginalSchemaRequest
               │                                    │  (查询最新 Schema)
               ▼                                    ▼
          ┌─────────────────────────────────────────────┐
          │        SchemaRegistry (Coordinator)          │
          │          运行在 JobManager 端                │
          │                                             │
          │  handleCoordinationRequest()  ← 统一入口    │
          │    ├── 处理 Schema 查询请求                  │
          │    │    → 从 SchemaManager 读取缓存返回      │
          │    └── 处理 Schema 变更请求                  │
          │         → 等待所有 Sink flush 完成           │
          │         → 推导演化后的 Schema                │
          │         → 通过 MetadataApplier 应用到外部系统│
          │         → 返回变更结果给 SchemaOperator      │
          └─────────────────────────────────────────────┘
```

## 五、核心作用总结

`handleCoordinationRequest()` 在 Flink CDC 的 Schema 演化机制中承担三个关键职责：

1. **Schema 查询服务**：Sink Writer 在处理数据前需要知道目标表的 Schema，通过此入口查询 `SchemaManager` 中维护的 Schema 信息，确保数据写入时 Schema 一致。

2. **Schema 变更协调**：当上游 Source 检测到 DDL 变更（如 `ALTER TABLE`），`SchemaOperator` 通过此入口发送 `SchemaChangeRequest`。Coordinator 收到后会协调等待所有下游 Sink 完成 flush，然后推导目标表应有的 Schema，通过 `MetadataApplier` 应用到外部系统（如 MySQL、StarRocks 等），最后将变更结果返回。

3. **并发安全保证**：所有请求通过 `runInEventLoop()` 在单线程的 `coordinatorExecutor` 中串行处理，避免并发修改 Schema 状态导致数据不一致。

## 六、关键文件索引

| 文件 | 职责 |
|------|------|
| `schema/common/SchemaRegistry.java` | 抽象基类，定义 `handleCoordinationRequest()` 分发逻辑 |
| `schema/common/SchemaManager.java` | Schema 状态管理，存储原始和演化后的 Schema |
| `schema/regular/SchemaCoordinator.java` | Regular 模式下的 Coordinator 实现 |
| `schema/distributed/SchemaCoordinator.java` | Distributed 模式下的 Coordinator 实现 |
| `schema/regular/SchemaOperator.java` | Regular 模式下的 Schema 变更算子（请求发送方） |
| `schema/distributed/SchemaOperator.java` | Distributed 模式下的 Schema 变更算子（请求发送方） |
| `sink/SchemaEvolutionClient.java` | Sink Writer 端的 Schema 查询客户端（请求发送方） |
| `sink/DataSinkWriterOperator.java` | Sink Writer 算子，通过 SchemaEvolutionClient 查询 Schema |
| `schema/common/event/GetEvolvedSchemaRequest.java` | 查询演化后 Schema 的请求类 |
| `schema/common/event/GetOriginalSchemaRequest.java` | 查询原始 Schema 的请求类 |
| `schema/regular/event/SchemaChangeRequest.java` | Regular 模式下的 Schema 变更请求类 |
| `schema/distributed/event/SchemaChangeRequest.java` | Distributed 模式下的 Schema 变更请求类 |
