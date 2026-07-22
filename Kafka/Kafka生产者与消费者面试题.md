# Kafka 生产者、消费者与高级特性深度剖析

> 面向高级开发者的 Kafka 客户端与内核源码分析

---

## 一、生产者

### 1.1 发送流程

```java
// Kafka 生产者发送流程
1. 序列化 key 和 value
2. 分区器选择分区
3. 追加到 RecordAccumulator（批次累积器）
4. Sender 线程从 RecordAccumulator 拉取批次
5. 发送到 Broker
6. 处理响应

// RecordAccumulator 结构
// 每个分区对应一个 Deque<ProducerBatch>
// 每个 ProducerBatch 包含多个消息
// 批次满（batch.size）或超时（linger.ms）时发送
```

### 1.2 幂等性实现

```java
// 幂等性生产者（enable.idempotence=true）

// 核心组件
// 1. ProducerId（PID）：每个生产者会话分配唯一 ID
// 2. Sequence Number：每个分区递增的序列号
// 3. Producer State：Broker 维护的 <PID, Partition> → 序列号状态

// 去重机制
// Broker 收到消息时，检查序列号
// 如果序列号 > 期望值 → 丢失消息，返回 OutOfOrderSequenceException
// 如果序列号 = 期望值 → 正常处理
// 如果序列号 < 期望值 → 重复消息，忽略
```

### 1.3 事务实现

```java
// 事务性生产者（transactional.id）

// 事务协调器（Transaction Coordinator）
// 每个 transactional.id 对应一个协调器
// 协调器负责：

// 事务流程
1. initTransactions()：获取 PID，初始化事务
2. beginTransaction()：开始事务
3. send()：发送消息（写入事务缓冲区）
4. commitTransaction()：提交事务
   a. 向协调器发送 PrepareCommit
   b. 协调器写入事务日志
   c. 向所有分区发送 MarkCommit
   d. 协调器标记事务完成

// 事务保证
// 1. 跨分区原子性
// 2. 跨会话幂等性
// 3. 消费者读已提交消息（isolation.level=read_committed）
```

---

## 二、消费者

### 2.1 拉取模型

```java
// Kafka 使用拉取（pull）模型
// 消费者主动拉取消息

// 拉取请求
// 1. 消费者发送 FetchRequest
// 2. Broker 检查是否有可用数据
// 3. 如果有 → 返回数据
// 4. 如果没有 → 等待（fetch.max.wait.ms）或返回空

// 拉取参数
// fetch.min.bytes（默认 1）：最小拉取字节数
// fetch.max.wait.ms（默认 500）：最大等待时间
// max.partition.fetch.bytes（默认 1MB）：单分区最大拉取
// max.poll.records（默认 500）：单次最大拉取消息数
```

### 2.2 消费者协调器

```java
// 消费者协调器（ConsumerCoordinator）
// 负责：Rebalance、偏移量提交、心跳

// 心跳机制
// 消费者每隔 heartbeat.interval.ms 发送心跳
// 如果 session.timeout.ms 未收到心跳，协调器认为消费者死亡
// 触发 Rebalance

// 最大轮询间隔
// max.poll.interval.ms（默认 5 分钟）
// 如果两次 poll 调用间隔超过此值，协调器认为消费者有故障
// 触发 Rebalance
```

---

## 三、消息语义

### 3.1 Exactly Once 实现

```java
// Exactly Once = 幂等性 + 事务 + 消费端幂等

// 消费端幂等实现
// 1. 基于数据库唯一键去重
// 2. 基于 Redis 记录已处理消息 ID
// 3. 基于事务：消费和业务操作在同一个事务中

// 典型方案
// 1. 消费消息
// 2. 开启本地事务
// 3. 执行业务操作（INSERT/UPDATE）
// 4. 提交偏移量（在同一个事务中）
// 5. 提交事务
```

---

> **总结**：Kafka 的生产者通过幂等性和事务实现精确一次投递，消费者通过拉取模型和协调器实现弹性消费，消息语义通过幂等消费和事务机制保证。理解这些机制是构建可靠消息系统的关键。

---

## 面试题精选

**Q1: acks 参数的三种取值分别代表什么？**

A: acks=0（不等待确认，可能丢消息）、acks=1（Leader 写入成功即确认，中可靠）、acks=all（等待所有 ISR 确认，最可靠）。

**Q2: 幂等性 Producer 的原理是什么？**

A: 每个 Producer 分配唯一 PID，每条消息带递增序列号。Broker 按 `<PID, Partition>` 去重，小于等于已接收序列号的消息被忽略。

**Q3: Kafka 4.0 的消费者组 Rebalance 协议有什么变化？**

A: Kafka 4.0 正式 GA 了 KIP-848 新一代消费者重平衡协议（`group.protocol=consumer`）。核心变化：1）协调逻辑从客户端迁移到 Broker 端；2）增量式协调，无停止世界暂停；3）只有涉及分区的消费者受影响，其他消费者不受影响；4）`session.timeout.ms` 等配置迁移到服务端。

**Q4: 如何保证 Exactly Once 语义？**

A: 幂等 Producer + 事务 + 消费端幂等（唯一键去重或事务性消费）。