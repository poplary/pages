# Kafka 监控运维深度剖析

> 面向高级开发者的 Kafka 集群管理与运维实践，基于 Kafka 4.0

---

## 一、集群部署（KRaft 模式）

### 1.1 硬件选型

```java
// 磁盘
// 推荐：多块 SSD，RAID 10
// 避免：NAS/NFS 网络存储
// 每 Broker 推荐 4-8 块磁盘

// 内存
// 充分利用 OS 页面缓存
// 推荐 32GB-128GB
// 避免堆内存过大（JVM GC 问题）

// 网络
// 万兆网卡
// 低延迟网络
```

### 1.2 KRaft 配置（Kafka 4.0）

```properties
# Kafka 4.0 不再需要 ZooKeeper
# 使用 KRaft 模式，部分 Broker 承担 Controller 角色

# 节点配置
process.roles=broker,controller  # 同时承担 Broker 和 Controller 角色
node.id=1

# Controller Quorum（推荐 3 或 5 个节点）
controller.quorum.voters=1@host1:9093,2@host2:9093,3@host3:9093

# 元数据日志目录
metadata.log.dir=/data/kafka/metadata

# 监听器
listeners=PLAINTEXT://0.0.0.0:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
```

### 1.3 关键配置

```properties
# 日志配置
log.retention.hours=168          // 7 天
log.segment.bytes=1073741824     // 1GB
log.retention.check.interval.ms=300000

# 网络配置
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400

# 副本配置
num.replica.fetchers=2
replica.fetch.max.bytes=1048576
```

## 二、性能调优

### 2.1 生产者调优

```properties
batch.size=16384       // 16KB，增大减少请求数
linger.ms=5            // 等待 5ms 凑批次
compression.type=snappy  // CPU 换带宽
buffer.memory=33554432  // 32MB
```

### 2.2 消费者调优（KIP-848 新协议）

```properties
# 客户端配置
group.protocol=consumer  // 使用新一代重平衡协议

# 以下配置已迁移到 Broker 端，无需在客户端配置：
# partition.assignment.strategy
# session.timeout.ms
# heartbeat.interval.ms

# 拉取性能
fetch.min.bytes=1
fetch.max.wait.ms=500
max.partition.fetch.bytes=1048576
```

## 三、故障排查

### 3.1 常见问题

```java
// 1. 磁盘空间不足
// 检查 log.retention 配置
// 手动删除旧日志段
// 扩容磁盘

// 2. 消费者延迟
// 检查消费者处理速度
// 增加分区数
// 优化消费者逻辑

// 3. Leader 选举频繁
// 检查 Broker 稳定性
// 检查网络延迟
// 调整 KRaft 相关配置

// 4. KRaft 元数据问题
// 使用 kafka-metadata-shell.sh 查看元数据状态
// 检查 controller.quorum 状态
```

### 3.2 Kafka 4.0 升级排查

```java
// 确认已在 KRaft 模式下运行
// 检查 broker 和客户端版本兼容性
// 确认 Java 版本满足要求（Broker: Java 17+）
// 检查 Log4j 配置（已升级到 Log4j2）
// 测试新协议（group.protocol=consumer）
```

---

> **总结**：Kafka 4.0 运维的核心变化：1）KRaft 模式已完全替代 ZooKeeper；2）KIP-848 新一代重平衡协议正式 GA；3）KIP-932 Share Group 支持队列语义；4）Java 17 要求。监控 ConsumerLag 和重平衡频率是预防集群故障的关键。