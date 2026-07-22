# Kafka 深度剖析：存储引擎、复制协议、控制器、消费组

> 面向高级开发者的 Kafka 内核原理，基于 Kafka 4.0 源码

---

## 一、核心架构

### 1.1 控制器（Controller）与 KRaft 模式

> Kafka 4.0 已完全移除 ZooKeeper，KRaft 是唯一支持的元数据管理模式。

```java
// Kafka 4.0 控制器使用 KRaft（基于 Raft 协议）
// 不再需要 ZooKeeper 集群

// KRaft 架构
// 1. 部分 Broker 充当 Controller 角色（controller quorum）
// 2. 使用 Raft 协议在 Controller 之间复制元数据
// 3. 所有 Broker 直接从 KRaft 元数据日志读取元数据
// 4. 无需维护额外的 ZooKeeper 集群
```

**KIP-996: Pre-Vote 机制**：Kafka 4.0 引入 Pre-Vote 机制，节点在发起选举前先检查自己是否有资格成为 Leader，减少网络分区或瞬态故障导致的不必要选举。

**控制器职责**：
1. 分区 Leader 选举
2. 分区副本分配
3. ISR 变更处理
4. 主题管理（创建/删除/修改）
5. Broker 上下线处理

**Controller Quorum 要点**：
- Controller 节点数量推荐 3 或 5（奇数个）
- controller.quorum.voters 配置指定初始 voters
- 元数据日志存储在本地磁盘（`metadata.log.dir`）
- 使用 `kafka-metadata-shell.sh` 工具查看元数据状态

### 1.2 事件处理模型

```java
// 控制器使用事件队列模式
// 所有操作通过事件驱动

// 事件类型
// LeaderAndIsrRequest
// UpdateMetadataRequest
// TopicChange
// PartitionModifications
// BrokerChange
// LogDirChange
// ControlledShutdown
```

---

## 二、存储引擎

### 2.1 日志文件结构

```java
// Kafka 日志文件结构
// base_offset.log        — 消息数据
// base_offset.index      — 偏移量索引（稀疏）
// base_offset.timeindex  — 时间戳索引
// base_offset.txnindex   — 事务索引（事务性消息）

// 日志段（LogSegment）
// 每个分区由多个日志段组成
// 活跃段：正在写入的段
// 非活跃段：只读的段（可被清理）

// 索引文件结构
// 偏移量索引：相对偏移量 → 物理位置
// 时间戳索引：时间戳 → 相对偏移量
```

### 2.2 日志写入流程

```java
// 写入流程
1. 追加消息到活跃段
2. 更新 Leader 的 LEO（Log End Offset）
3. 等待 ISR 副本同步
4. 更新 HW（High Watermark）
5. 返回写入结果给生产者

// 刷盘策略
// 1. 基于时间：log.flush.interval.ms（默认无限制）
// 2. 基于消息数：log.flush.interval.messages
// 3. 基于 OS 页面缓存：依赖 OS 自动刷盘（推荐）
```

### 2.3 日志清理

```java
// 两种清理策略

// 1. DELETE 策略
// 删除超过保留时间的日志段
// 检查条件：
// - log.retention.hours（默认 168 小时）
// - log.retention.bytes（默认无限制）
// - log.retention.check.interval.ms（默认 5 分钟）

// 2. COMPACT 策略
// 保留每个 key 的最新版本
// 使用名为 Cleaner 的线程执行
// 清理过程：
// 1. 读取日志段，构建 key → 最新偏移量的映射
// 2. 删除旧版本的记录
// 3. 生成新的日志段
```

---

## 三、复制协议

### 3.1 ISR 管理

```java
// ISR（In-Sync Replicas）
// 与 Leader 保持同步的副本集合

// 同步条件
// 1. 副本在 replica.lag.time.max.ms（默认 30 秒）内
//    从 Leader 获取了最新的消息
// 2. 副本的 LEO ≥ Leader 的 LEO - replica.lag.max.messages（已废弃）

// LEO（Log End Offset）：最后一条消息的偏移量
// HW（High Watermark）：所有 ISR 副本都确认的偏移量
// 消费者只能读取到 HW 之前的消息
```

### 3.2 Leader 选举

```java
// 优先副本选举
// 每个分区有优先副本（preferred leader）
// 优先副本通常是第一个副本
// 当优先副本落后时，不会自动选举
// 需要通过 kafka-preferred-replica-election 命令触发

// ISR 选举
// 从 ISR 中选择 LEO 最大的副本
// 保证数据不丢失

// Unclean 选举
// 从非 ISR 中选择
// 可能丢失数据
// 通过 unclean.leader.election.enable 控制
```

---

## 四、消费组协议

### 4.1 消费组协调器（KIP-848 新一代协议）

> Kafka 4.0 正式 GA 了 KIP-848 新一代消费者重平衡协议，
> 将协调逻辑从客户端迁移到 Broker 端，实现真正的增量式、异步重平衡。

```java
// 新一代消费者重平衡协议（group.protocol=consumer）
// 对比传统协议：

// 传统协议（Classic）：
//   1. JoinGroup → 所有消费者暂停
//   2. Leader 计算分配 → SyncGroup
//   3. 停止整个世界（stop-the-world）
//   4. 所有消费者受影响

// 新协议（Consumer / KIP-848）：
//   1. 消费者通过心跳声明订阅和确认分配
//   2. 协调器（Broker）计算目标分配
//   3. 增量式协调，无需全局暂停
//   4. 只有涉及分区的消费者受影响
```

**KIP-848 核心创新**：
1. **服务端驱动的协调**：Group Coordinator 成为中央智能，维护组成员、监控主题元数据、计算目标分配、驱动增量协调
2. **声明式状态**：消费者通过心跳机制声明订阅并确认分区分配/撤销，不再自行计算分配
3. **增量式协调**：协调器比较当前状态与目标状态，通过心跳响应下发具体的撤销/分配指令。无需全局同步屏障
4. **无停止世界暂停**：消费者只暂停需要撤销的分区，其他消费者继续处理不受影响的分区
5. **服务端配置**：`session.timeout.ms` 和 `heartbeat.interval.ms` 现在在 Broker 端动态配置

**启用方式**：
```properties
# 消费者端配置
group.protocol=consumer
# 以下配置已迁移到 Broker 端，不再需要在客户端设置：
# partition.assignment.strategy
# session.timeout.ms
# heartbeat.interval.ms
```

**KIP-932: Queues for Kafka（Early Access）**：
- 引入 Share Group 概念，支持传统队列语义
- 多个消费者可以消费同一分区（竞争消费）
- 类似 RabbitMQ 的 work queue 模式
- 使用 `kafka-share-groups.sh` 管理

**KIP-1043: 新的管理工具**：
- `kafka-groups.sh`：查看所有类型的 Group（consumer、share）
- `kafka-consumer-groups.sh` 和 `kafka-share-groups.sh`：支持新协议组的管理

### 4.2 Rebalance 协议（新旧对比）

| 特性 | Classic 协议 | Consumer 协议（KIP-848） |
|------|-------------|------------------------|
| 协调方式 | 客户端驱动 JoinGroup/SyncGroup | Broker 端驱动，心跳机制 |
| 应用影响 | 停止世界（stop-the-world） | 无停止世界暂停 |
| Fetch 处理 | 暂停 | 可继续处理 |
| Commit 处理 | 暂停 | 可继续处理 |
| 影响范围 | 组内所有消费者 | 只影响涉及分区的消费者 |
| 配置管理 | 客户端配置 | 服务端动态配置 |

**新协议流程**：
```
1. 消费者通过心跳声明订阅
2. 协调器维护组成员、监控主题元数据
3. 协调器计算目标分配（服务端 assignor）
4. 协调器比较当前状态与目标状态
5. 通过心跳响应下发撤销/分配指令
6. 消费者独立完成撤销/分配，进入新 epoch
```

**分区分配策略**：
- 传统协议：RangeAssignor、RoundRobinAssignor、StickyAssignor、CooperativeStickyAssignor
- 新协议：服务端 assignor（默认 range、uniform），客户端无需配置 `partition.assignment.strategy`

### 4.3 偏移量管理

```java
// 偏移量存储在 __consumer_offsets 主题
// 默认 50 个分区
// 自动提交（enable.auto.commit=true）时
// 每隔 auto.commit.interval.ms 提交一次

// 偏移量提交格式
// key: group_id + topic + partition
// value: offset + metadata + timestamp
```

---

## 五、性能优化

### 5.1 零拷贝技术

```java
// Kafka 使用零拷贝（zero-copy）技术
// 传统读取：磁盘 → OS 内核 → 应用 → OS 内核 → 网卡
// 零拷贝：磁盘 → OS 内核 → 网卡

// Java 实现
// FileChannel.transferTo() 方法
// 底层使用 sendfile 系统调用
```

### 5.2 批处理与压缩

```java
// 生产者批处理
// batch.size（默认 16KB）：每个分区批次大小
// linger.ms（默认 0）：等待时间，凑够批次发送
// 压缩：compression.type（gzip/snappy/lz4/zstd）

// 服务端解压
// 消息以压缩格式存储
// 消费者拉取时，Kafka 不主动解压
// 消费者自己解压（减少服务端 CPU 开销）
```

---

> **总结**：Kafka 通过控制器实现集群管理，通过日志段和索引实现高效存储，通过 ISR 机制保证数据可靠性，通过消费组协调器实现消费管理。零拷贝、批处理、压缩等优化技术使其达到百万级吞吐量。理解这些底层实现是优化 Kafka 性能和排查问题的关键。

---

## 面试题精选

**Q1: Kafka 如何保证消息不丢失？**

A: 生产者端：`acks=all` + 重试 + 幂等性。Broker 端：`replication.factor≥3` + `min.insync.replicas≥2` + 禁用 unclean 选举。消费者端：处理完再提交偏移量。

**Q2: Kafka 如何保证消息顺序消费？**

A: 单分区内天然有序。相同 key 的消息发到同一分区，单线程消费一个分区。

**Q3: ISR 机制是什么？**

A: ISR（In-Sync Replicas）是与 Leader 保持同步的副本集合。只有 ISR 中的副本才能被选举为 Leader。`replica.lag.time.max.ms` 控制同步超时。

**Q4: 如何解决消息重复消费？**

A: 消费端实现幂等性：唯一 ID 去重 + 数据库唯一约束 + Redis 记录已处理消息 ID。

**Q5: Kafka 消息积压如何处理？**

A: 增加消费者（提高分区数）→ 优化消费者处理逻辑 → 临时扩容 Topic 分区 → 限流生产者 → 建立监控告警。

**Q6: Kafka 为什么吞吐量高？**

A: 顺序写磁盘、零拷贝（sendfile）、批处理、压缩、分区并行。

**Q7: Kafka 4.0 的 KRaft 模式是什么？和 ZooKeeper 模式有什么区别？**

A: KRaft 是 Kafka 4.0 唯一支持的元数据管理模式，完全移除了 ZooKeeper。优势：1）简化部署，无需维护独立的 ZK 集群；2）更快的启动速度；3）更少的运维成本；4）更好的扩展性。Controller 节点使用 Raft 协议复制元数据。

**Q8: KIP-848 新一代重平衡协议有什么优势？**

A: 1）无停止世界暂停，只有涉及分区的消费者受影响；2）增量式协调，不阻塞 Fetch 和 Commit；3）服务端驱动，客户端更轻量；4）重平衡速度提升显著，适合大规模部署。启用方式：`group.protocol=consumer`。

**Q9: KIP-932 Queues for Kafka 是什么？**

A: 引入 Share Group 概念，支持传统队列语义。同个 partition 可以被多个消费者消费（竞争消费），类似 RabbitMQ 的 work queue 模式。Kafka 4.0 中为 Early Access 阶段。

**Q10: Kafka 4.0 升级需要注意什么？**

A: 1）必须先迁移到 KRaft 模式；2）Broker 需要 Java 17+，Clients 需要 Java 11+；3）最低客户端协议版本 2.1；4）消息格式 v0/v1 已移除；5）Log4j 已升级到 Log4j2。