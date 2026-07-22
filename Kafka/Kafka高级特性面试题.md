# Kafka 高级特性与监控运维深度剖析

> 面向高级开发者的 Kafka 可靠性、监控、运维实战，基于 Kafka 4.0

---

## 一、消息可靠性

### 1.1 可靠性配置

```java
// 生产端
acks=all                          // 等待所有 ISR 确认
enable.idempotence=true           // 启用幂等性
retries=Integer.MAX_VALUE         // 无限重试
max.in.flight.requests.per.connection=5

// 服务端（Kafka 4.0）
min.insync.replicas=2            // 最小 ISR 副本数
default.replication.factor=3     // 副本因子
unclean.leader.election.enable=false  // 禁止 unclean 选举

// KIP-966: Eligible Leader Replicas (ELR, Preview)
// ELR 是 ISR 的子集，保证有完整的截至 HW 的数据
// 比 ISR 更安全，防止数据丢失
```

### 1.2 数据丢失场景

```java
// 1. acks=0：网络闪断导致丢失
// 2. acks=1：Leader 宕机，未同步副本
// 3. 事务未提交：生产者崩溃
// 4. Unclean 选举：非 ISR 副本成为 Leader
// 5. 日志清理：过期数据被删除
```

## 二、Kafka 4.0 新特性

### 2.1 KRaft 模式（唯一模式）

```java
// Kafka 4.0 已完全移除 ZooKeeper
// KRaft 是唯一支持的元数据管理模式

// 优势
// 1. 简化部署：无需维护独立的 ZooKeeper 集群
// 2. 更快的启动速度
// 3. 更少的运维成本
// 4. 更好的扩展性

// 配置示例
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093,2@localhost:9094,3@localhost:9095
```

### 2.2 KIP-848: 新一代消费者重平衡协议

```java
// 正式 GA，默认启用
// 消费者端配置 group.protocol=consumer 即可使用

// 核心改进
// 1. 无停止世界暂停
// 2. 增量式协调
// 3. 服务端驱动，更轻量的客户端
// 4. 更适合大规模部署
```

### 2.3 KIP-932: Queues for Kafka（Early Access）

```java
// 引入 Share Group，支持传统队列语义
// 同个 partition 可以被多个消费者消费（竞争消费）
// 类似 RabbitMQ 的 work queue 模式

// 使用方式
kafka-share-groups.sh --bootstrap-server localhost:9092 --share-group my-group --topic my-topic
```

### 2.4 KIP-890: 事务服务端防御

```java
// 第二阶段完成
// 减少生产者故障时的 "zombie transactions"
// 提高事务一致性
```

### 2.5 KIP-996: Pre-Vote 机制

```java
// 减少 KRaft 不必要的 Leader 选举
// 节点在发起选举前先检查自己是否有资格
// 减少网络分区或瞬态故障导致的选举
```

### 2.6 KIP-1106: 基于时间的偏移量重置

```java
// auto.offset.reset 新增 duration 选项
// 消费者可以基于固定的时间窗口重置偏移量
// 避免重新处理大量历史数据
```

## 三、监控运维

### 3.1 关键指标

```java
// Broker 指标
// BytesInPerSec：每秒流入
// BytesOutPerSec：每秒流出
// UnderReplicatedPartitions：未完全复制分区数
// LeaderElectionRateAndTime：Leader 选举频率
// TotalTimeMs：请求处理时间

// 消费者指标（KIP-848 新增）
// ConsumerGroup.Rebalance.Time：重平衡耗时
// ConsumerGroup.Rebalance.Rate：重平衡频率
// ConsumerGroup.Consumer.Lag：消费者延迟
```

### 3.2 集群管理

```java
// Kafka 4.0 新 CLIn工具

// 查看所有 Group（支持 consumer 和 share 类型）
kafka-groups.sh --bootstrap-server localhost:9092 --list

// 管理消费者组（支持新协议）
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-group

// 管理 Share Group
kafka-share-groups.sh --bootstrap-server localhost:9092 --describe --share-group my-group
```

### 3.3 升级注意事项

```java
// Kafka 4.0 升级要求
// 1. 必须先迁移到 KRaft 模式（如果还在用 ZooKeeper）
// 2. Java 17 要求（Broker/Connect/Tools）
// 3. Java 11 要求（Clients/Streams）
// 4. 最低客户端协议版本 2.1
// 5. 消息格式 v0/v1 已移除
// 6. Log4j 已升级到 Log4j2
```