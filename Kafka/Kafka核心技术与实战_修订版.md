---
title: Kafka 核心技术与实战（修订版）
---

# Kafka 核心技术与实战 · 学习笔记（基于最新版本修订）

> 原专栏基于 Kafka 2.3（2019 年），本文档在总结每篇内容的基础上，依据 **Apache Kafka 4.0（2024.12）** 对过时内容进行修正与补充，并新增 KRaft 章节。

---

## 版本演进总览（2.3 → 4.0）

| 版本 | 时间 | 关键变化 |
|------|------|---------|
| 2.3 | 2019.06 | 专栏基线 |
| 2.4 | 2019.12 | MirrorMaker 2 引入；增量协作式 rebalance |
| 2.5–2.8 | 2020–2021 | 弃用旧客户端、ZK 稳步增强 |
| **3.0** | 2021.08 | **消息格式 v0/v1 弃用**（仅保留 v2）；Java 8 移除，最低 Java 11 |
| **3.3** | 2022.10 | **KRaft 模式 GA（生产可用）**，ZooKeeper 弃用 |
| 3.5–3.6 | 2023 | 分层存储（Tiered Storage）预览；JBOD 增强 |
| 3.9 | 2024.11 | ZK→KRaft 迁移路径完善 |
| **4.0** | 2024.12 | **彻底移除 ZooKeeper**，KRaft 为唯一元数据模式；KIP-848 新消费者组协议 GA；消息格式 v0/v1 移除；Broker 最低 Java 17，客户端/Streams 最低 Java 11 |

### 影响原专栏内容的核心变化

1. **ZooKeeper 全面退出**：所有基于 ZK 的 Controller、元数据、集群管理章节均需以 KRaft 视角重新审视。
2. **消息格式 v0/v1 已移除**：日志格式/压缩相关讨论中旧版本格式不再有效，4.0 仅支持 v2（自 0.11 起的默认格式）。
3. **新消费者组协议（KIP-848）GA**：消费者组重平衡由"stop-the-world"改为增量式，4.0 默认服务端启用，客户端需 `group.protocol=consumer` 主动开启。
4. **队列语义（KIP-932）早期访问**：引入 share group，支持点对点队列语义。
5. **Authorizer 更替**：旧的 `SimpleAclAuthorizer`（ZK 版）早已弃用，3.x 起使用 `StandardAuthorizer`（KRaft 版）。
6. **MirrorMaker 2 取代 MirrorMaker 1**：MM1 弃用，MM2 成默认。
7. **Controller 重写**：ZK 时代的"每个 Broker 一个 Controller 候选、靠 ZK 选举"被 KRaft 的"独立 Controller 仲裁"取代。
8. **Java 版本要求提升**：Broker/Connect/Tools 最低 Java 17，客户端/Streams 最低 Java 11。
9. **ELR（KIP-966）预览**：eligible leader replicas，保证选举的 Leader 数据完整到高水位，避免丢数据。

### 详细解析与示例

#### 版本演进时间轴

```text
2019 ──2.3(专栏基线)──2.4(MM2/增量rebalance)
  │
2021 ──3.0(消息格式v0/v1弃用, Java11)
  │
2022 ──3.3(KRaft GA, ZK弃用) ←里程碑
  │
2023 ──3.5/3.6(分层存储预览, JBOD增强)
  │
2024 ──3.9(ZK→KRaft迁移完善)──4.0(ZK移除/KIP-848 GA/Java17)
```

#### 元数据管理演进对比

| 维度 | ZK时代 (≤3.2) | KRaft时代 (3.3+, 4.0唯一) |
|------|---------------|--------------------------|
| 元数据存储 | 外部 ZooKeeper znode | Kafka 内部主题 `__cluster_metadata` |
| Controller 选举 | 各 Broker 抢 `/controller` 节点 | Raft 仲裁选 active controller |
| 成员感知 | ZK 临时节点 + Watch | 元数据日志变更同步 |
| 启动速度 | 大集群数十分钟 | 毫秒~秒级 |
| 分区上限 | 受限于元数据加载(数万) | 百万级 |

#### 升级路径示例（从 2.x 到 4.0）

```bash
# 1. 先升至 3.3+（获得 KRaft GA）
# 2. 在 3.6-3.9 执行 ZK→KRaft 迁移（kafka-cluster.sh）
bin/kafka-cluster.sh cluster-id --bootstrap-server broker:9092
# 3. 迁移完成、确认 KRaft 稳定后，再升 4.0
# ⚠️ 4.0 不再提供 ZK 入口，未迁的 ZK 集群无法直跳 4.0
```

> 后文每篇笔记中，凡涉及上述变化的，以「📌 版本提示」标注修订。

---

## 开篇词 · 为什么要学习 Kafka？

### 内容总结
作者胡夕（Apache Kafka Committer）自述 5 年 Kafka 实战经历（0.8→2.3），阐述 Kafka 在数据密集型应用中的价值，给出从客户端样例到流处理的学习路径，并勾勒专栏六大模块：入门、基本使用、客户端、原理、运维监控、高级应用。

### 重点
- Kafka 是应对「数据量激增 / 复杂度 / 变化速率」数据密集型场景的利器，核心能力是削峰填谷、隔离上下游。
- 一套框架覆盖消息引擎、应用集成、分布式存储、流处理四大用途。
- 学习路径：选客户端→跑官网样例→改 API→优化可靠性/性能→学流处理高级功能。
- 运维侧重 JMX 监控 + 找瓶颈提吞吐。

### 详细解析与示例

#### 学习路径图

```text
[选客户端]→[跑官网样例]→[改API]→[优化可靠性/性能]→[学流处理]
  Java/librdkafka   Producer/Consumer   参数调优      Kafka Streams
```

#### Kafka 四大用途定位

```text
┌─────────────────────────────────────────┐
│                Apache Kafka              │
├──────────┬──────────┬──────────┬──────────┤
│ 消息引擎  │ 应用集成 │ 分布式存储 │ 流处理   │
│ 削峰填谷 │ 解耦上下游│ 日志留存 │ Streams  │
└──────────┴──────────┴──────────┴──────────┘
```

#### 快速上手示例（生产+消费）

```bash
# 启动单机 KRaft Kafka（4.0 无需 ZK）
bin/kafka-storage.sh format --config=config/kraft/server.properties --cluster-id=$(bin/kafka-storage.sh random-uuid)
bin/kafka-server-start.sh config/kraft/server.properties

# 建主题
bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic demo --partitions 3 --replication-factor 1

# 生产
bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo

# 消费（另一终端）
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic demo --from-beginning
```

#### 选型对比

| 场景 | 推荐 | 理由 |
|------|------|------|
| 高吞吐流式数据管道 | Kafka | 顺序写+零拷贝，百万 TPS |
| 复杂路由/AMQP 协议 | RabbitMQ | 成熟路由能力 |
| 事务消息+顺序+百万积压 | RocketMQ | 金融场景优化 |

### 📌 版本提示
- 文中「2.3」为 2019 基线，当前已 4.0；运维监控部分需以 KRaft 视角重新审视（不再有 ZK）。

---

## 01 · 消息引擎系统 ABC

### 内容总结
定义 Kafka 为开源消息引擎系统，消息以纯二进制字节序列编码。解释消息引擎两大价值——削峰填谷与松耦合，并以秒杀订单场景说明。介绍两种消息模型：点对点（P2P）与发布/订阅，Kafka 两者皆支持。

### 重点
- 消息引擎 = 一组在系统间传递语义准确消息的规范，实现异步、松耦合数据传递。
- 消息格式：Kafka 用纯二进制字节序列（结构化，使用前序列化）。
- 两种模型：点对点（一条消息只被一个消费者消费）、发布/订阅（Topic + 多发布者/多订阅者）。
- 削峰填谷：上游瞬时流量缓存于消息引擎，平滑传导至下游，避免雪崩。

### 详细解析与示例

#### 两种消息模型对比

```text
点对点(P2P)              发布/订阅(Pub/Sub)
┌──────┐   msg   ┌──────┐     ┌──────┐   Topic   ┌──────┐
│Prod A│────────▶│Cons B│     │Prod 1│──────────┐│Sub 1 │
└──────┘  只给B  └──────┘     │Prod 2│──────────┤│Sub 2 │
                              └──────┘          ▼└──────┘
                            (多发布者→同主题→多订阅者各收一份)
```

Kafka 用「消费者组」一套机制同时实现两种模型：同组=P2P，不同组=Pub/Sub。

#### 削峰填谷示例（秒杀订单）

```text
秒杀开始  上游订单TPS=10万/s
   │  ┌──────────────────────────┐
   └─▶│ Kafka Topic: orders      │← 缓冲，容量充足
      │ (保留1小时，多副本)       │
   ┌──┴──────────────────────────┘
   ▼   下游支付/库存 TPS=1万/s
  Consumer Group 按各自能力消费，不压垮下游
```

#### 消息格式演进（v0/v1 → v2）

```text
v1(0.10): 每条消息各自存CRC/时间戳  → 浪费空间
v2(0.11+): 公共字段抽到消息集合层  → 压缩整个集合，省空间
         └─ 4.0 仅保留 v2，v0/v1 已移除
```

示例：v2 消息集合结构
```text
[RecordBatch]
  ├─ BatchHeader(公共: CRC/producerId/epoch/baseSeq)
  ├─ Record1 (length/attrs/timestamp/offset/key/value)
  ├─ Record2 ...
  └─ 整个 Batch 可被压缩传输
```

### 📌 版本提示
- 消息格式：4.0 已移除 v0/v1 格式，仅保留 v2（0.11 起默认），日志格式/压缩讨论需更新。
- 削峰填谷、P2P/Pub-Sub 模型核心仍完全成立。

---

## 02 · 一篇文章带你快速搞定 Kafka 术语

### 内容总结
盘点 Kafka 核心术语，给出三层消息架构：主题层（Topic→M 分区→N 副本）、分区层（1 Leader + N-1 Follower）、消息层（Offset）。Broker 用 append-only 日志 + 日志段持久化。消费者组实现 P2P 与负载均衡，引入 Rebalance，并区分分区位移与消费者位移。

### 重点
- 副本分 Leader（对外读写）和 Follower（仅同步、不对外服务，与 MySQL 从库读不同）。
- 分区机制解决伸缩性：Topic 划分多 Partition 分布于多 Broker，分区号从 0 起。
- 消费者组：组内每分区只被一个消费者实例消费，提升消费端 TPS。
- Rebalance：实例挂掉后分区自动重分配，是消费者高可用手段（也易出 Bug）。
- 日志段（Log Segment）机制：append-only 顺序写 + 定期删除老段回收磁盘。
- 严格区分「分区位移（消息位置，不变）」与「消费者位移（消费进度，可变）」。

### 详细解析与示例

#### 三层消息架构

```text
Topic: orders (3分区, 副本因子2)
├─ Partition 0
│   ├─ Leader(broker1)   ◀─ 客户端只读写Leader
│   └─ Follower(broker2) ◀─ 仅异步拉取同步，不对外
├─ Partition 1
│   ├─ Leader(broker2)
│   └─ Follower(broker3)
└─ Partition 2
    ├─ Leader(broker3)
    └─ Follower(broker1)

消息层: Partition0 内消息位移 0,1,2,... 单调递增不变
```

#### 分区位移 vs 消费者位移

```text
分区位移(消息位置,不可变)     消费者位移(下次要读的位置,可变)
┌──┬──┬──┬──┬──┬──┬──┬──┐    消费到位移3 → 消费者位移=4
│0 │1 │2 │3 │4 │5 │6 │7 │    重启后从位移4继续
└──┴──┴──┴──┴──┴──┴──┴──┘
        ▲
      已消费到3
```

#### 日志段(Log Segment)与磁盘回收

```text
logs/orders-0/
├─ 00000000000000000000.log   ← .log 段1(封存,可删)
├─ 00000000000000000000.index
├─ 00000000000000123456.log   ← .log 段2(当前写入)
└─ 00000000000000123456.index
   触发删除: 超过 retention.ms(默认7天) 或 retention.bytes
```

#### 副本读写模型（为何 Follower 不抗读）

```text
Producer ──写──▶ Leader(读写入口)
Consumer ──读──▶ Leader
Follower ◀──拉取── Leader  (异步,可能落后)
```
- 保证 Read-your-writes（写了立即可读）
- 保证单调读（不会时有时无）
- 分区已跨 Broker 分散，读写负载天然均衡，无需主从读分离

#### 消费者组分区分配示例

```text
Topic: T (6分区)  Group: G
场景A: 3个消费者 → 每人2分区
  c0:{0,1}  c1:{2,3}  c2:{4,5}
场景B: 8个消费者 → 2个空闲(浪费资源,应避免)
  c0~c5 各1分区, c6/c7 空闲
```

### 📌 版本提示
- 副本 Leader/Follower 读写模型仍成立。
- 4.0 KIP-848 新消费者组协议 GA：Rebalance 改为增量式，大幅减少 stop-the-world，客户端需 `group.protocol=consumer` 开启。
- Controller 选举底层由 ZK 改为 KRaft，影响第 25 讲「重平衡全流程」的底层机制描述。

---

## 03 · Kafka 只是消息引擎系统吗？

### 内容总结
梳理 Kafka 演进史：LinkedIn 内部孵化（解决轮询数据收集的不准确与高定制成本），2011 入 Apache 孵化，2012 成为顶级项目。0.10.0.0 推出 Kafka Streams，正式成为分布式流处理平台。相比 Spark/Flink 的两大优势：端到端精确一次语义、Kafka Streams 定位为轻量客户端库。

### 重点
- Kafka 定位演进：消息引擎 → 流处理平台（0.10 起）。
- 端到端 EOS：外部框架（Spark/Flink）只能保证框架内 EOS，无法控制写回 Kafka 的语义；Kafka 全流程内部完成可实现端到端 EOS。
- Kafka Streams 是库（client library）而非完整系统，不提供集群调度/弹性部署，需自行搭配。
- 轻量定位适合中小企业流处理场景。

### 详细解析与示例

#### Kafka 定位演进时间轴

```text
2010 LinkedIn内部孵化(解决轮询数据收集问题)
  │
2011 入Apache孵化 ── 2012 毕业(顶级项目)
  │
0.10(2016) ── Kafka Streams → 正式为「流处理平台」
  │
0.11(2017) ── 幂等Producer+事务 → 端到端EOS基石
  │
4.0(2024) ── Streams持续强化(KIP-1104/1112/1065/1076)
```

#### 端到端 EOS vs 框架内 EOS

```text
场景: Spark/Flink 从Kafka读 → 计算 → 写回Kafka

[Spark内部]  可保证EOS(状态变更幂等)
[写回Kafka]  ✗ Spark控制不了 → 可能重试导致重复写入
───────────────────────────────────────────
场景: Kafka Streams 全流程在Kafka内部
[读→计算→写]  全程Kafka事务覆盖 → 端到端EOS ✓
```

#### Kafka Streams vs Spark/Flink 定位

| 维度 | Kafka Streams | Spark/Flink |
|------|---------------|-------------|
| 形态 | 轻量客户端库 | 完整集群系统 |
| 集群调度 | 不提供(需自行配) | 内置 |
| 部署 | 与应用同进程 | 独立集群 |
| 适合 | 中小流量、逻辑简单 | 大规模、复杂拓扑 |
| 端到端EOS | 原生支持 | 需借助Kafka事务(如Flink 1.4+) |

#### 事务型 Producer 示例（EOS）

```java
props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-tx-producer");
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
KafkaProducer<String,String> p = new KafkaProducer<>(props);
p.initTransactions();
try {
    p.beginTransaction();
    p.send(new ProducerRecord<>("out", "k1", "v1"));
    p.send(new ProducerRecord<>("out", "k2", "v2"));
    p.commitTransaction();   // 两条原子提交
} catch (Exception e) { p.abortTransaction(); }
```

### 📌 版本提示
- EOS 自 0.11 事务机制起支持，4.0 强化（KIP-890 事务服务端防御，降低 producer 失败时的「僵尸事务」）。
- Kafka Streams 4.0 持续演进：KIP-1104（外键从 value 提取）、KIP-1112（自定义 Processor 包装）、KIP-1065（RETRY 错误处理）、KIP-1076（状态指标）。
- 演进史判断（自建流处理）方向正确，4.0 仍在强化。

---

## 04 · 我应该选择哪种 Kafka？

### 内容总结
介绍 Kafka 生态圈（Kafka Connect 串联上下游外部系统）和三种「发行版」：Apache Kafka（社区版，迭代快但仅基础组件）、Confluent Kafka（Schema Registry/REST proxy/跨数据中心备份/监控，国内普及低）、CDH/HDP Kafka（大数据平台集成，操作简单但把控度低、版本滞后）。

### 重点
- Kafka Connect 通过 Connector 串联上下游，生态牢固度的关键。
- Apache Kafka：迭代最快、社区响应高、把控度高；缺高级特性与监控框架。
- Confluent Kafka：企业特性丰富（Schema Registry、REST proxy、跨 DC 备份、监控）；中文资料少、国内普及低。
- CDH/HDP Kafka：UI 化运维、省成本；把控度低、版本滞后（如 CDH 6.1 时仍用 2.0.0）。

### 详细解析与示例

#### 三发行版对比

| 维度 | Apache Kafka | Confluent Kafka | CDP(原CDH/HDP) |
|------|--------------|------------------|----------------|
| 迭代速度 | 最快 | 快 | 滞后 |
| 高级特性 | 仅基础 | Schema Registry/REST proxy/跨DC备份/监控 | 平台集成监控 |
| 把控度 | 高 | 中 | 低 |
| 中文资料 | 丰富 | 少 | 中(平台文档) |
| 费用 | 免费 | 免费+企业版 | 商业 |
| 适合 | 需把控/自建 | 企业级特性需求 | 已用CDP平台 |

#### Kafka Connect 生态

```text
         ┌─ JDBC Connector ── MySQL/PG
         ├─ Debezium CDC ──── MySQL/Oracle binlog
Kafka ◀──┤  (Source)
Connect  │
  │      ├─ Elasticsearch Sink
  └──────┤ S3/HDFS Sink
         └─ …(上下游生态越多越牢固)
```

#### 选型决策树

```text
需高级特性(Schema管理/跨DC)？
  ├─是→ Confluent (2021已上市,文档改善)
  └─否→ 已用CDP大数据平台？
        ├─是→ CDP内置Kafka(省运维)
        └─否→ Apache Kafka自建 (4.0 KRaft后成本大降)
```

### 📌 版本提示
- Cloudera 与 Hortonworks 已合并（2018），现为 CDP；选型建议中 CDH/HDP 提法应更新为 CDP。
- Confluent 已于 2021 年 IPO 上市，国内业务与文档已有改善。
- 4.0 后自建 Apache Kafka 成本因 KRaft（无需独立 ZK 集群）显著降低，自建方案吸引力上升。

---

## 05 · 聊聊 Kafka 的版本号

### 内容总结
详解 Kafka 版本号命名：`kafka-2.11-2.1.1` 中 2.11 是 Scala 编译器版本，2.1.1 才是真版本（Major.Minor.Patch）。回顾 0.7→2.0 各大版本功能：0.8 副本机制、0.9 安全认证+新 Consumer+Connect、0.10 Kafka Streams、0.11 幂等 Producer+事务+消息格式重构、1.0/2.0 Streams 改进。

### 重点
- 版本号 3 位构成：大版本-小版本-Patch（如 0.11.0.0 视为大版本 0.11）。
- 客户端演进：老版本连 ZK、新版本连 Broker；0.8.2 新 Producer、0.9 新 Consumer、0.10.2.2 新 Consumer 稳定。
- 0.11 幂等 Producer + 事务 API 是实现 EOS（精确一次）的基石。
- 服务端与客户端版本尽量一致，否则损失性能优化。
- 「不要当最新版本小白鼠」的选型原则。

### 详细解析与示例

#### 版本号命名解析

```text
下载包: kafka_2.13-4.0.0.tgz
        ─┬─  ─┬─
         │    └─ Kafka真版本(Major.Minor.Patch)
         └─ Scala编译器版本(2.13)，不是Kafka版本!

⚠️ 初学者常误读为「Kafka 2.13版」
```

#### 各大版本功能里程碑

```text
0.7  上古(仅消息队列,无副本) ─── 别用
0.8  副本机制(高可靠MQ) ─── 0.8.2.2新Producer
0.9  安全认证+Connect+新Consumer(0.9新Consumer坑多)
0.10 Kafka Streams ─── 0.10.2.2新Consumer稳定
0.11 幂等Producer+事务+消息格式v2 ← EOS基石
1.0/2.0 Streams改进(消息引擎无大变化)
3.0  弃用v0/v1格式,Java11
3.3  KRaft GA
4.0  移除ZK, KIP-848 GA, Java17
```

#### 客户端兼容性示例

```bash
# 查Broker支持的API版本
bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092
# Produce(0): 0 to 7 [usable: 7]  ← 2.2服务端,可用到v7
# 0.10.2起双向兼容: 低版本Broker也能处理高版本Client
```

#### 版本一致性建议

```text
服务端 4.0  ←┐  版本不一致会损失性能优化
客户端 4.0  ←┘  (如压缩、新协议字段)
⚠️ 升级4.0前: Broker≥2.1 且 Client≥2.1 (KIP-896基线)
```
- 版本演进已到 4.0，0.7–1.0 内容现为历史背景。
- 4.0 已移除：消息格式 v0/v1、ZooKeeper 模式、大量弃用超 12 个月的 API；老客户端连 ZK 的方式彻底退出。
- 「不追最新」原则仍适用，但 4.0 是成熟稳定大版本，可放心上生产。

---

## 06 · Kafka 线上集群部署方案怎么做？

### 内容总结
从操作系统、磁盘、容量、带宽四维度规划生产集群。OS 选 Linux（epoll vs select、零拷贝、社区支持）；磁盘用普通机械盘即可（顺序写规避随机慢），可不上 RAID（1.1 起 JBOD 单盘 Failover）；容量按「消息数×大小×留存×副本×压缩」算；带宽按 70%×2/3 估算服务器台数。

### 重点
- Linux 优于 Windows：I/O 模型 epoll、零拷贝（sendfile）、社区修复 Bug。
- 机械盘性价比高，Kafka 顺序写规避了机械盘随机读写慢的劣势。
- Kafka 1.1 引入 JBOD 单盘 Failover，使 RAID 非必需（坏盘数据自动迁移、Broker 不退出）。
- 容量公式：1 亿×1KB×2(副本)×14(天)×0.75(压缩) ≈ 2.25TB。
- 带宽规划：千兆网单机可用 ~240Mbps（70%×2/3），1TB/h 需 ~30 台（含 3 副本）。

### 详细解析与示例

#### 部署四维度

```text
┌────────────────────────────────────┐
│  1.OS选型   Linux > Windows        │
│  2.磁盘      机械盘/JBOD           │
│  3.容量      消息数×大小×留存×副本×压缩 │
│  4.带宽      70%×2/3 估算服务器台数 │
└────────────────────────────────────┘
```

#### Linux 优势三要点

```text
1.I/O模型   epoll(多路复用) vs Windows select
2.零拷贝     sendfile: 磁盘→网卡,跳过内核态拷贝
3.社区支持   Windows Bug社区不承诺修复(仅测试用)
```

#### 容量计算示例

```text
需求: 每天1亿条1KB消息, 存2副本, 留存14天, 压缩0.75

计算: 1亿×1KB×2(副本)÷1000÷1000 = 200GB/天
  +10%(索引等) = 220GB/天
  ×14天 = 3.08TB
  ×0.75(压缩) ≈ 2.25TB  ← 规划容量
```

#### 带宽→服务器台数示例

```text
目标: 千兆网(1Gbps) 1小时内处理1TB业务数据

单机可用带宽: 1Gbps×70% = 700Mb/s (超70%易丢包)
  再预留2/3余量: 700÷3 ≈ 240Mbps

需求: 1TB/h = 2336Mb/s (1TB=8×1024×1024÷3600≈2330Mb/s)
服务器数: 2336÷240 ≈ 10台
  含3副本: 10×3 = 30台
```

#### JBOD vs RAID

| 方案 | 优势 | 劣势 |
|------|------|------|
| RAID | 冗余+负载均衡 | 成本高, Kafka已有副本机制重复 |
| JBOD(多log.dirs) | 性价比高, 1.1起单盘Failover | 坏盘数据需迁移 |

```text
log.dirs=/k1,/k2,/k3   (挂不同物理盘)
  1.1起: 任一盘坏 → 数据自动迁移到其他盘, Broker不退出
  ← 舍弃RAID的基础
```

### 📌 版本提示
- JBOD Failover（1.1）在 4.0 仍强化；分层存储（3.5 Tiered Storage 预览）是新选项。
- KRaft 模式下部署更简单：无需独立 ZK 节点规划，controller 与 broker 可同进程。
- 4.0 Broker 需 Java 17，容量/JVM 规划需相应调整。

---

## 07 · 最最最重要的集群参数配置（上）

### 内容总结
Broker 端静态参数。`log.dirs` 多路径多物理盘（1.1 Failover）；`zookeeper.connect` + chroot 多集群隔离；`listeners`/`advertised.listeners`（用主机名不用 IP）；Topic 管理：`auto.create.topics.enable=false`、`unclean.leader.election.enable=false`、`auto.leader.rebalance.enable=false`；留存 `log.retention.bytes` / `message.max.bytes`。

### 重点
- `log.dirs` 必须多路径且挂不同物理盘（提升吞吐 + 1.1 起单盘 Failover）。
- `zookeeper.connect` 的 chroot 写在末尾（如 `…:2181/kafka1`），多集群共用 ZK 隔离。
- `listeners` 三元组 `<协议,主机名,端口>`，自定义协议需配 `listener.security.protocol.map`。
- 三个 false 参数：禁自动建 Topic、禁 Unclean Leader 选举（防丢数据）、禁 Leader 重平衡（无收益代价高）。
- `message.max.bytes` 默认 ~1MB 太小，生产建议调大（仅标尺不耗盘）。

### 详细解析与示例

#### 关键 Broker 参数分组

| 分组 | 参数 | 说明 |
|------|------|------|
| 存储 | `log.dirs` | 多路径逗号分隔，挂不同物理盘 |
| 连接 | `listeners`/`advertised.listeners` | 监听器三元组<协议,主机,端口> |
| Topic管理 | `auto.create.topics.enable` | 生产建议 false |
| | `unclean.leader.election.enable` | 建议 false 防丢数据 |
| | `auto.leader.rebalance.enable` | 建议 false 无收益代价高 |
| 留存 | `log.retention.hours` / `log.retention.bytes` / `message.max.bytes` | 留存策略与消息大小上限 |

#### listeners 三元组示例

```properties
# 自定义协议名需配映射
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://broker1.host:9092
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
# ⚠️ 主机名全部用主机名不用IP(Broker源码按主机名)
```

#### 三个 false 参数决策

```text
auto.create.topics.enable=false
  └ 防拼写错误自动建稀奇古怪Topic(如test→tst)
unclean.leader.election.enable=false
  └ 防落后副本当Leader丢数据(宁可分区不可用)
  └ 4.0 KIP-966 ELR提供更安全的选举补充
auto.leader.rebalance.enable=false
  └ 换Leader代价高(客户端重连)且无性能收益
```

#### KRaft 配置对照（4.0）

```properties
# 旧(ZK时代,4.0已不存在):
# zookeeper.connect=zk1:2181,zk2:2181,zk3:2181/kafka1

# 新(KRaft):
process.roles=broker,controller   # combined模式
node.id=1
controller.quorum.voters=1@broker1:9093,2@broker2:9093,3@broker3:9093
controller.listener.names=CONTROLLER
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
```

### 📌 版本提示
- **`zookeeper.connect` 在 4.0 已不存在**（ZK 移除），改为 KRaft 配置：`process.roles`、`node.id`、`controller.quorum.voters`、`controller.listener.names`。
- `listeners` 在 KRaft 下必须区分 `CONTROLLER` 和 `BROKER` 两类监听器。
- `unclean.leader.election.enable` 仍为 false；4.0 KIP-966 ELR（预览）提供更安全的 Leader 选举。

---

## 08 · 最最最重要的集群参数配置（下）

### 内容总结
Topic 级参数（`retention.ms/bytes`、`max.message.bytes`，覆盖 Broker 全局）；用 `kafka-configs` 脚本设置；JVM：Java 8+（2.0 弃 Java 7）、堆 6GB、G1 GC（`KAFKA_HEAP_OPTS`/`KAFKA_JVM_PERFORMANCE_OPTS`）；OS：`ulimit -n 1000000`、XFS 文件系统、swappiness≈1（非 0）、flush 间隔可调大换性能。

### 重点
- Topic 级参数覆盖全局 Broker 参数；`kafka-configs` 是统一设置方式（社区倾向统一用它）。
- JVM 堆 6GB 业界公认；默认 1GB 太小（Broker 交互创建大量 ByteBuffer）。
- GC：Java 8+ 用 G1（比 CMS Full GC 少、调参少）。
- OS：`ulimit -n` 调大防 'Too many open files'；XFS 强于 ext4；swappiness 设 1 非 0（防 OOM killer 无预警杀进程）；页缓存落盘默认 5s 可拉大换性能（多副本兜底）。

### 详细解析与示例

#### Topic级 vs Broker全局 参数优先级

```text
优先级: Topic级参数 > Broker全局参数

需求: 大部分Topic存7天，但交易Topic需存半年
  全局: log.retention.hours=168
  Topic级(覆盖): kafka-configs设置retention.ms=...半年
```

#### kafka-configs 命令示例

```bash
# 查看Topic配置
bin/kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name transaction --describe

# 设置Topic级留存(半年)
bin/kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name transaction \
  --alter --add-config retention.ms=15552000000

# KRaft下用 --bootstrap-server (不再 --zookeeper)
```

#### JVM 调优示例

```bash
# 堆6GB + G1GC
export KAFKA_HEAP_OPTS="-Xms6g -Xmx6g"
export KAFKA_JVM_PERFORMANCE_OPTS="-server -XX:+UseG1GC \
  -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35"
bin/kafka-server-start.sh config/kraft/server.properties
```

#### OS 调优清单

```bash
# 1.文件描述符
ulimit -n 1000000   # 防 'Too many open files'

# 2.文件系统(XFS优于ext4)
mount -o noatime /dev/sdb1 /kafka  # 禁atime减少inode写

# 3.swappiness (设1不设0,防OOM killer无预警杀进程)
sudo sysctl vm.swappiness=1

# 4.页缓存映射数(主题超多时)
echo 'vm.max_map_count=655360' >> /etc/sysctl.conf

# 5.页缓存≥1个日志段(log.segment.bytes默认1GB)
```

#### 内存分配原则

```text
总内存64GB示例:
  JVM堆: 6GB (ByteBuffer交互,不宜太小)
  页缓存: ≥1个日志段(1GB+)  ← Kafka性能关键
  其余留给OS页缓存

⚠️ 不要给Broker堆开太大,抢了页缓存反而降性能
```

### 📌 版本提示
- **Java 要求提升**：4.0 Broker/Connect/Tools 需 Java 17，客户端/Streams 需 Java 11（2.0 弃 Java 7 已是历史）。
- G1 仍推荐；4.0 日志框架从 Log4j 迁移到 Log4j2（KIP-653），配置需转换。
- `kafka-configs` 在 KRaft 下用 `--bootstrap-server` 而非 `--zookeeper`。
- 分层存储（Tiered Storage，3.5+）是应对「Kafka 当存储用」的新方案，可缓解留存调大的磁盘压力。

---

## 09 · 生产者消息分区机制原理剖析

### 内容总结
分区是为负载均衡与高伸缩性，三级结构 主题-分区-消息。分区策略：轮询（无 Key 默认）、按 Key 保序（有 Key 默认）、随机（老版本）、自定义（实现 `Partitioner` 接口，配 `partitioner.class`）、基于地理位置。国企案例由单分区保序→改按标志位分区，吞吐提升 40 倍。

### 重点
- 分区核心价值=负载均衡 + 高伸缩；不同分区置于不同 Broker，可加节点提吞吐。
- 默认策略：有 Key 按 Key hash 保序、无 Key 轮询（Sticky Partitioner 在新版有优化）。
- 自定义 Partitioner：实现 `partition()` 方法，可利用 topic/key/cluster 信息。
- 保业务顺序用 Key 分区而非单分区（单分区牺牲吞吐与负载均衡）。

### 详细解析与示例

#### 分区策略图

```text
Producer ──选择分区策略──┐
                         ├─有Key? ─是→ hash(Key)%分区数 (按Key保序)
                         │       └─否→ 轮询/Sticky (默认)
                         ├─自定义Partitioner → partitioner.class
                         └─地理位置 → 按Broker IP选址
```

#### 默认分区策略实现要点

```java
// 无Key: 轮询(2.4起Sticky Partitioner,批发送更高效)
List<PartitionInfo> ps = cluster.partitionsForTopic(topic);
return ThreadLocalRandom.current().nextInt(ps.size());

// 有Key: 按Key哈希保序(同Key始终同分区→分区内有序)
return Math.abs(key.hashCode()) % ps.size();
```

#### 国企案例改造对比

```text
改造前: 1个分区保顺序 → 全局有序但无负载均衡
  吞吐: ~X

改造后: 按标志位(用户ID)分Key → 同用户同分区有序
  多分区并行 → 吞吐提升40倍+
  
  逻辑: 相同因果关系的消息 → 提取标志位到Key → 同Key同分区
```

#### Sticky Partitioner（2.4优化）

```text
老轮询: 每条消息可能落到不同分区 → 每分区一个小batch → 压缩/请求效率低
Sticky: 一批消息绑定同一分区 → 大batch → 批量发送更高效
         (分区变化时才重新选分区)
```

#### 分区数选择参考

```text
目标吞吐 ÷ 单分区吞吐 ≈ 所需分区数
  例: 目标100MB/s, 单分区~10MB/s → ≥10分区
  + 兼顾消费者数(建议消费者数≤分区数)
  + 未来扩容余量
```

### 📌 版本提示
- 分区机制核心不变；4.0 KIP-932（Queues for Kafka，早期访问）引入 share group，为点对点队列语义提供新消费模型，分区与消费组关系有新扩展。
- 自定义分区器仍支持；生产者默认分区器后续有 Sticky Partitioner（2.4）优化，批发送更高效。

---

## 10 · 生产者压缩算法面面观

### 内容总结
压缩用 CPU 换磁盘/带宽。消息格式 V1（老）与 V2（0.11 引入，抽公共部分、压缩整个消息集合）。压缩发生处：Producer 端（`compression.type`）、Broker 端（仅当与 Producer 算法不同或发生格式转换时重压缩，会丧失零拷贝）。解压在 Consumer 端（算法封装在消息集合中）。算法：GZIP/Snappy/LZ4/zstd（2.1.0+）。吞吐 LZ4>Snappy>zstd/GZIP；压缩比 zstd>LZ4>GZIP>Snappy。

### 重点
- V2 比 V1 省空间（启用压缩更明显）；公共字段抽到消息集合层、CRC 上移。
- 链路：Producer 压缩、Broker 保持、Consumer 解压缩。
- 避免消息格式转换（否则丧失 Zero Copy + CPU 飙升）。
- 选型：CPU 充足/带宽紧张→zstd；重吞吐→LZ4。

### 详细解析与示例

#### 压缩链路

```text
Producer ──压缩──▶ Broker ──保持(同算法)──▶ Consumer ──解压──▶
    compression.type       (一般原样存)      (按消息集合中的算法标识)

例外: Broker重压缩(丧失零拷贝):
  1. Broker配了与Producer不同的compression.type
  2. 消息格式转换(兼容老版本Consumer) → V2转V1
```

#### 算法对比

| 算法 | 压缩比 | 吞吐 | 适合 |
|------|--------|------|------|
| zstd(2.1+) | 最高↑ | 中 | CPU足/带宽紧 |
| LZ4 | 中 | 最高↑ | 重吞吐 |
| Snappy | 低 | 中高 | (较少用) |
| GZIP | 中低 | 低 | (较少用) |

```text
吞吐: LZ4 > Snappy > zstd ≈ GZIP
压缩比: zstd > LZ4 > GZIP > Snappy
```

#### 启用压缩示例

```java
props.put("compression.type", "zstd");  // 或 gzip/snappy/lz4
KafkaProducer<String,String> p = new KafkaProducer<>(props);
// Producer端压缩整个消息集合, 节省网络+磁盘
```

#### 零拷贝与格式转换

```text
正常: 磁盘→sendfile→网卡 (零拷贝, 跳过内核态拷贝)
格式转换时: 需解压→转格式→重压 → CPU飙升, 零拷贝丧失

4.0: 已移除v0/v1格式 → 格式转换场景大幅减少, 零拷贝更稳定
```

### 📌 版本提示
- **4.0 已移除消息格式 v0/v1**（即文中 V1），仅保留 V2；这反而消除了「格式转换」痛点，Broker 重压缩场景大幅减少，零拷贝更稳定。
- zstd（2.1.0+）仍是压缩比首选；零拷贝机制 4.0 仍重要。

---

## 11 · 无消息丢失配置怎么实现？

### 内容总结
明确 Kafka 持久化保证边界=对「已提交」消息的有限度保证。常见丢失场景：Producer「fire and forget」（`send(msg)` 无回调）、Consumer 先提交位移后消费（书签放错页）、多线程异步+自动提交。给出 8 条无丢失配置。

### 重点
- 必须用 `send(msg, callback)` 带回调，不用 fire-and-forget。
- `acks=all`：所有 ISR 副本收到才算已提交（最高等级）。
- `retries` 调大（应对瞬时网络抖动）。
- `unclean.leader.election.enable=false`（防落后副本当 Leader 丢数据）。
- `replication.factor>=3`；`min.insync.replicas>1`；且 `RF > MIR`（推荐 `RF=MIR+1`，兼顾持久性与可用性）。
- `enable.auto.commit=false`，手动提交位移；多线程消费尤其重要。

### 详细解析与示例

#### Kafka 持久化保证边界

```text
「已提交」消息: 若干Broker成功写入并应答给Producer
有限度保证: N个副本中至少1个存活 → 不丢

✗ 未提交消息(send后未应答) → Kafka不保证
✗ Producer未用回调(fire-and-forget) → 丢了不知道
```

#### 三大常见丢失场景

| 环节 | 场景 | 解决 |
|------|------|------|
| Producer | `send(msg)`无回调(发射后不管) | 用 `send(msg, callback)` |
| Consumer | 先提交位移后消费(书签放错页) | 先消费后提交 |
| Consumer | 多线程异步+自动提交 | 关自动提交,手动提交 |

#### 8条无丢失配置

```properties
# Producer端
acks=all                  # 所有ISR副本收到才算已提交
retries=2147483647        # 调大,应对瞬时抖动
enable.idempotence=true   # 幂等,防重试重复(4.0默认true)
max.in.flight.requests.per.connection=5  # 幂等下可>1

# Broker端
replication.factor=3              # 3副本
min.insync.replicas=2              # 至少2副本写入才算已提交
unclean.leader.election.enable=false  # 禁落后副本当Leader

# Consumer端
enable.auto.commit=false           # 手动提交位移
```

#### RF 与 MIR 关系

```text
replication.factor(RF) vs min.insync.replicas(MIR)

RF=3, MIR=2 (推荐):
  写入: 至少2副本确认 → 挂1副本仍可用 ✓
  持久: 2副本同时才安全

RF=3, MIR=3 (不推荐):
  挂1副本 → MIR不满足 → 分区不可用 ✗
  
✅ 推荐: RF = MIR + 1 (持久性+可用性平衡)
```

### 📌 版本提示
- `acks=all`（即 -1）仍最高等级；`min.insync.replicas` 概念不变。
- KIP-848 新消费者组协议（4.0 GA）下位移提交机制更安全（增量协同 rebalance，减少 stop-the-world）。
- ELR（KIP-966，4.0 预览）增强 Leader 选举安全性，与 `unclean=false` 互补，保证当选 Leader 数据完整到 HW。

---

## 12 · 客户端都有哪些不常见但是很高级的功能？

### 内容总结
Kafka 拦截器（0.10+）。生产者拦截器 `ProducerInterceptor`（`onSend`/`onAcknowledgement`）、消费者拦截器 `ConsumerInterceptor`（`onConsume`/`onCommit`），支持链式。配置 `interceptor.classes`（全限定名）。用例：端到端延时监控、消息审计。案例：Redis 统计端到端平均延时。

### 重点
- 拦截器可插拔、链式、按添加顺序执行；配全限定类名。
- `onAcknowledgement` 与 `onSend` 不同线程（注意线程安全），且在主路径别放重逻辑（否则 TPS 下降）。
- 典型场景：端到端延时监控、消息审计（多租户 PaaS 强制接入）。

### 详细解析与示例

#### 拦截器机制

```text
Producer拦截器(链式,按序):
  onSend(record) ──发送前──▶ [拦截逻辑1] ──▶ [拦截逻辑2] ──▶ 发送
  onAcknowledgement ──提交成功/失败后──▶ (早于callback调用)

Consumer拦截器(链式,按序):
  onConsume(records) ──返回给Consumer前──▶ [拦截逻辑]
  onCommit(offsets) ──提交位移后──▶ [拦截逻辑]
```

#### 接口方法与注意事项

```java
public class MyInterceptor implements ProducerInterceptor<String,String> {
    @Override public ProducerRecord<String,String> onSend(ProducerRecord<String,String> r){
        // 发送前修改消息(加header等)
        return r;
    }
    @Override public void onAcknowledgement(RecordMetadata m, Exception e){
        // ⚠️ 与onSend不同线程! 注意线程安全
        // ⚠️ 在主路径,别放重逻辑(TPS会降)
    }
}
// 配置: interceptor.classes (全限定名列表)
```

#### 端到端延时监控案例

```text
目标: 统计消息从生产到消费的平均总时长

Producer拦截器: onSend → jedis.incr(totalSent)
Consumer拦截器: onConsume →
  latency += now - record.timestamp()
  jedis.set(avgLatency, totalLatency/totalSent)

架构: Producer/Consumer拦截器 + 共享Redis
```

#### vs KIP-714 (4.0 Broker收集客户端指标)

| 方案 | 优点 | 劣势 |
|------|------|------|
| 拦截器自建 | 灵活,可自定义 | 需开发,侵入客户端 |
| KIP-714 | 官方,从Broker收集 | 仅admin/consumer/producer标准指标 |

### 📌 版本提示
- 拦截器机制不变。
- 4.0 KIP-714 可直接从 Broker 收集客户端指标（admin/consumer/producer），部分替代自建拦截器监控方案。
- 新消费者组协议（KIP-848）下消费拦截时点与协同 rebalance 配合，监控更精细。

---

## 13 · Java 生产者是如何管理 TCP 连接的？

### 内容总结
Kafka 全基于 TCP（多路复用、HTTP 库简陋）。Producer 在创建 `KafkaProducer` 实例时启动 Sender 线程，建立与 `bootstrap.servers` 所有 Broker 的连接并拉取元数据。连接还可能在更新元数据后/发送消息时按需创建。关闭：用户主动 `close`、Kafka 自动关闭空闲连接。

### 重点
- TCP 连接在 `new KafkaProducer` 时即建立（Sender 线程启动）。
- `bootstrap.servers` 不必配全部 Broker（3-4 台即可，连上任一即可获全集群元数据）。
- 元数据更新：找不到主题时拉取、`metadata.max.age.ms` 默认 5 分钟强刷。
- 作者质疑「连所有 Broker」设计浪费（1000 台集群只与 3-5 台通信却全连）。

### 详细解析与示例

#### TCP 连接建立时机

```text
new KafkaProducer(props)
  └启动Sender线程──▶ 连接bootstrap.servers所有Broker
                      └发送METADATA请求, 获全集群元数据

后续按需建连接:
  1. 更新元数据后(发现新Broker)
  2. 发送消息时(目标Broker未连)
  3. 元数据强刷(metadata.max.age.ms=5min)
```

#### 连接数估算

```text
bootstrap.servers配3台 → 启动建3连接
  连上1台即获全集群信息
  实际只与3-5台Leader通信

⚠️ 若配1000台bootstrap → 启动建1000连接(浪费)
  → 结论: bootstrap配3-4台足够
```

#### 关闭连接

```text
1. 用户主动: producer.close()  (推荐)
2. Kafka自动: connections.max.idle.ms (默认9分钟)
   超时空闲连接自动关闭
```

#### Producer线程模型

```text
KafkaProducer (线程安全, 可多线程共享)
  ├─ 用户主线程 (调用send)
  └─ Sender线程 (后台, 网络IO)
      └─ RecordAccumulator (共享, ConcurrentMap<TopicPartition,Deque>)
         ← 线程安全的关键: Deque有锁保护
```

#### Consumer连接(对比, 21讲)

```text
new KafkaConsumer ─ 不建连接(优于Producer)
poll时建连接:
  1. FindCoordinator(连负载最小Broker)
  2. 连Coordinator
  3. 连各分区Leader所在Broker
```

### 📌 版本提示
- KRaft 下元数据获取底层从 ZK/Controller 改为 KRaft metadata log，Producer 拉元数据路径基本不变但 controller 不同。
- KIP-1102（4.0）增强 client rebootstrap 能力（超时/错误码触发重新引导），解决「元数据卡住」问题。

---

## 14 · 幂等生产者和事务生产者是一回事吗？

### 内容总结
三种交付语义：at most once / at least once（默认）/ exactly once。EOS 由幂等性（0.11）+ 事务（0.11）实现。幂等 Producer：`enable.idempotence=true`，仅单分区单会话幂等。事务 Producer：加 `transactional.id` + `initTransactions`/`beginTransaction`/`commitTransaction`/`abortTransaction`，跨分区跨会话。Consumer `isolation.level=read_committed` 只读已提交事务消息。

### 重点
- 幂等=单分区单会话；事务=跨分区跨会话，两者作用范围不同。
- 事务 API：init/begin/commit/abort；Consumer 需配 `read_committed`。
- 事务型 Producer 性能更差，需评估开销，不可无脑启用。

### 详细解析与示例

#### 三种交付语义

```text
at most once  : 可能丢, 不重复 (禁重试即可)
at least once : 不丢, 可能重复 (默认, 重试导致)
exactly once  : 不丢不重复 (幂等+事务)
```

#### 幂等 vs 事务作用范围

```text
幂等Producer(enable.idempotence=true):
  └ 范围: 单分区 + 单会话(进程一次运行)
  └ 机制: Broker端多存PID+seq, 重复消息自动去重

事务Producer(加transactional.id):
  └ 范围: 跨分区 + 跨会话(重启后仍保幂等)
  └ 机制: 多消息原子提交, Consumer只读已提交事务

        幂等 ◀ ── 事务包含幂等
```

#### PID/Epoch 机制(幂等底层)

```text
Producer启动 → Broker分配PID(Producer ID)
  每条消息带 <PID, partition, seqNum>
  Broker: 同<PID,partition> seqNum必须递增, 重复的被丢弃

事务: 加epoch, 重启用同transactional.id → epoch+1
  → 防止“僵尸Producer”同时写(老epoch的请求被拒)
```

#### 事务代码示例

```java
props.put("transactional.id", "my-tx-1");
props.put("enable.idempotence", "true");
KafkaProducer<String,String> p = new KafkaProducer<>(props);
p.initTransactions();
try {
    p.beginTransaction();
    p.send(new ProducerRecord<>("topicA","k1","v1"));
    p.send(new ProducerRecord<>("topicB","k2","v2"));  // 跨Topic原子
    p.commitTransaction();
} catch(Exception e) { p.abortTransaction(); }
```

#### Consumer 隔离级别

```properties
isolation.level=read_committed  # 只读已提交事务消息(默认read_uncommitted)
# 也能读非事务型Producer的所有消息
```

### 📌 版本提示
- 4.0 KIP-890 事务服务端防御（phase 2 完成）降低 producer 失败时的「僵尸事务」。
- EOS 机制仍以 0.11 为基础但持续加固；`read_committed` 仍主流隔离级别。

---

## 15 · 消费者组到底是什么？

### 内容总结
Consumer Group = 可扩展 + 容错的消费者机制。三大特性：组内多实例、Group ID 唯一、单分区单组内一实例消费。用一种机制同时实现 P2P（同组）与 Pub/Sub（不同组）。实例数建议 ≤ 分区总数。位移管理演进：老版本存 ZK（高频写拖慢 ZK）→ 新版本存内部主题 `__consumer_offsets`。Rebalance 触发：成员数/订阅主题数/分区数变更。

### 重点
- 一机制两模型：同 Group=消息队列模型，不同 Group=发布订阅模型。
- 实例数=分区数为理想；超过分区数的实例空闲。
- 位移管理演进：ZK（不适合高频写）→ `__consumer_offsets` 内部主题。
- Rebalance 三触发条件；痛点：STW 停消费、全量重分配、慢（几百实例几小时）。

### 详细解析与示例

#### 一机制两模型

```text
同一Group实例               不同Group实例
  ↓                            ↓
每分区1实例消费(P2P)        各Group各收一份(Pub/Sub)

Topic T(2分区):
  Group G1(2实例): p0→c1, p1→c2  (P2P)
  Group G2 + G3 同时订阅 → 各得全量消息 (Pub/Sub)
```

#### 实例数与分区数关系

```text
6分区Topic:
  3实例: 每人2分区 ✅
  6实例: 每人1分区 ✅(理想,最大吞吐)
  8实例: 6人各1分区, 2人空闲 ⚠️(浪费)
  2实例: 每人3分区 (吞吐受限于实例数)
```

#### 位移管理演进

```text
老版本Consumer(≤0.8.2): 位移存ZK
  问题: ZK不适合高频写(位移更新频繁) → 拖慢ZK集群
新版本Consumer(0.9+): 位移存内部主题__consumer_offsets
  优势: Kafka主题天然高持久+高吞吐写
```

#### Rebalance 触发与痛点

```text
触发条件:
  1. 组成员数变化(加入/退出/崩溃被踢) ← 99%的rebalance
  2. 订阅主题数变化(正则订阅新匹配主题)
  3. 订阅主题分区数变化(只能增)

痛点:
  ✗ STW: rebalance期间所有实例停消费
  ✗ 全量重分配: 不保留原分配(0.11 StickyAssignor部分缓解)
  ✗ 慢: 几百实例几小时

4.0 KIP-848: 增量协同rebalance → 消除STW ⭐
```

### 📌 版本提示
- **4.0 KIP-848 新消费者组协议 GA，正是为解决本讲痛点的最大升级**：增量协同 rebalance、消除 STW，客户端需 `group.protocol=consumer` 开启。
- 老 ZK 存位移设计早已废弃；`__consumer_offsets` 仍是位移存储基础。

---

## 16 · 揭开神秘的「位移主题」面纱

### 内容总结
`__consumer_offsets` 位移主题。背景：老版本位移存 ZK 不适合高频写。新版本把位移作为普通 Kafka 消息存内部主题。消息格式 3 种：key=`<Group ID,主题名,分区号>` + value(位移+元数据)、Group 注册消息、tombstone(墓碑，value=null，彻底删 Group)。创建：首个 Consumer 启动时自动创建，默认 50 分区 3 副本。提交：自动（`enable.auto.commit=true`）/手动（`commitSync`）。删除：Compact（压实/整理），同 Key 仅留最新。

### 重点
- 位移主题就是普通主题（可手动建/删），但消息格式 Kafka 自定义，勿自行写入。
- 3 种消息格式；tombstone 删 Group。
- 自动创建默认 50 分区 3 副本（`offsets.topic.num.partitions`/`offsets.topic.replication.factor`）。
- Compact 删过期消息避免撑爆磁盘（注意 Compaction≠Compression）。

### 详细解析与示例

#### 位移主题 3 种消息格式

```text
格式1(位移): Key=<GroupID, topic, partition>  Value=<offset+metadata>
格式2(Group注册): 记录Consumer Group信息(神秘,源码内)
格式3(tombstone墓碑): Value=null → 彻底删Group
```

#### tombstone 删除流程

```text
Consumer Group下所有实例停止 + 位移都已删
  → Kafka向位移主题写tombstone消息(Value=null)
  → Compact时该Key被清除 → Group信息彻底删除
```

#### Compact 压实机制

```text
同Key多条消息 M1(早) M2(晚):
  Compact前: [M1][M2][M3]... 
  Compact后: 仅留最新的 [M3]  (同Key留最新)

⚠️ Compact ≠ Compression(压缩)
  Compact: 同Key去重,删旧版本
  Compression: 用算法减小消息体积
```

```text
避免位移主题撑爆磁盘:
  Consumer自动提交 → 不停写位移=100的消息
  Compact → 同Key只留最新1条 → 磁盘可控
```

#### 创建与提交

```properties
# 自动创建: 首个Consumer启动时
offsets.topic.num.partitions=50          # 默认50分区
offsets.topic.replication.factor=3      # 默认3副本

# 提交方式
enable.auto.commit=true   # 后台每5s(auto.commit.interval.ms)自动提交
enable.auto.commit=false  # 手动: commitSync()/commitAsync()
```

#### Coordinator 定位算法

```text
Group的Coordinator在哪台Broker?
  1. partitionId = abs(groupId.hashCode() % 50)  # 位移主题分区号
  2. 该分区Leader所在Broker = Coordinator

例: group.id="test-group" → hashCode=627841412
    abs(627841412 % 50) = 12 → 位移主题分区12的Leader所在Broker
```

### 📌 版本提示
- 位移主题机制 4.0 仍沿用；Compact 策略不变。
- KIP-848 新消费者组协议下位移存储与组管理演进（更高效的位移管理协同），但 `__consumer_offsets` 仍是基础。

---

## 17 · 消费者组重平衡能避免吗？

### 内容总结
Rebalance 三弊端：影响 TPS（STW）、慢（几百实例几小时）、效率不高（全量重分配不考虑局部性，0.11 推 StickyAssignor 保留原方案）。Coordinator：每个 Broker 都有，Group 的 Coordinator 由 `abs(groupId.hashCode()%50)` 定位移主题分区 + 该分区 Leader 所在 Broker。避免不必要 Rebalance 主要盯成员数变化——被误判「踢出」。关键参数：`session.timeout.ms`（默认 10s）、`heartbeat.interval.ms`、`max.poll.interval.ms`（默认 5min）。推荐 session=6s heartbeat=2s，保证 session≥3×heartbeat。

### 重点
- Coordinator 定位算法：位移主题分区号 + 该分区 Leader 所在 Broker（便于排查日志）。
- Rebalance 三弊端：STW、慢、全量重分配无局部性。
- StickyAssignor（0.11）：尽量保留原分配方案，减少变动。
- 避免被踢出：合理设 `session.timeout.ms`/`heartbeat.interval.ms`/`max.poll.interval.ms`。

### 详细解析与示例

#### Coordinator 定位

```text
Coordinator确定算法:
  1. partitionId = abs(groupId.hashCode() % 50)  # 位移主题分区
  2. 该分区Leader所在Broker = Coordinator

意义: 排查Group问题 → 根据算法定位Broker看日志
```

#### Rebalance 三弊端

```text
1. STW: rebalance期间所有Consumer停消费 (影响TPS)
2. 慢: 几百实例→几小时
3. 无局部性: 全量重分配, 不保留原方案
   例: 1实例退出 → 50分区(10人×5)重新打散给9人
   更好做法: 9人保留原分配, 仅退出者的5分区转移
   ← 0.11 StickyAssignor部分实现(有bug)
```

#### 避免不必要 Rebalance(被误踢)参数

| 参数 | 默认 | 推荐 | 作用 |
|------|------|------|------|
| session.timeout.ms | 10s | 6s | 心跳超时→被踢 |
| heartbeat.interval.ms | 3s | 2s | 心跳频率 |
| max.poll.interval.ms | 5min | 调 | 两次poll最大间隔 |

```text
保证: session.timeout.ms ≥ 3 × heartbeat.interval.ms
  (判定dead前能发送≥3轮心跳)

避免误踢场景:
  ✗ Consumer处理慢 → poll间隔超max.poll.interval.ms → 主动退组
  ✗ GC/网络抖动 → 心跳送不出 → 被踢
```

#### Rebalance触发与避免

```text
触发:
  1. 组成员数变化(99%的不必要rebalance)
  2. 订阅主题数变化(正则订阅)
  3. 分区数增加

可避免的(误踢):
  ✓ 调对session/heartbeat/max.poll.interval
  ✓ 保证处理速度 ≤ poll能力
不可避免的(主动增减实例/分区): 接受

4.0 KIP-848: 增量协同rebalance → 根治STW
```

### 📌 版本提示
- **4.0 KIP-848 新消费者组协议 GA 是本讲痛点的根本解决**：增量协同 rebalance 消除 STW、提升效率，`group.protocol=consumer` 开启。
- StickyAssignor 在 KIP-848 下演化为 sticky + cooperative；`max.poll.interval.ms` 等参数在新协议下行为优化，被误踢出场景大幅减少。

---

## 18 · Kafka 中位移提交那些事儿

### 内容总结
位移=下一条消息位移（非最新消费的）。提交语义由用户负责，Kafka「无脑」接受。自动提交（`enable.auto.commit=true`，默认 5s）省事但可能重复消费；手动提交 `commitSync`（同步阻塞+自动重试）/`commitAsync`（异步不阻塞+不重试）。组合用：平时 `commitAsync`，关闭前 `commitSync`。

### 重点
- 位移=下一条消息位置；提交语义用户负责（可提交任意值，后果自负）。
- 自动提交：poll 时先提交上一批，不丢消息但可能重复消费（rebalance 时窗口）。
- `commitSync`：阻塞+自动重试；`commitAsync`：不阻塞但不重试（重试位移可能过期）。
- 最佳实践：平时 `commitAsync` 规避阻塞，关闭前 `commitSync` 保底。

### 详细解析与示例

#### 位移 = 下一条消息位置

```text
分区消息位移: 0 1 2 3 4 5 6 7
              ◀─已消费到3─▶
Consumer位移 = 4 (下次要读的位置, 非最新已消费的3)

⚠️ 语义用户负责: 提交4 → Kafka认为<4都已消费
  提交20 → 位移5-19可能丢
  提交5 → 位移4-9可能重复消费
```

#### 自动 vs 手动提交

| 方式 | 优点 | 缺点 |
|------|------|------|
| 自动(enable.auto.commit=true) | 省事 | 可能重复消费(rebalance窗口) |
| commitSync(同步) | 自动重试,可靠 | 阻塞, 影响TPS |
| commitAsync(异步) | 不阻塞 | 不重试(位移可能过期) |

#### 自动提交重复消费窗口

```text
自动提交每5s:
  T=0 提交位移=100
  T=3s 发生rebalance → Consumer从位移100继续消费
  但实际已消费到103 → 位移100-103重复消费

→ 减小auto.commit.interval.ms只能缩小窗口,不能消除
```

#### 组合提交最佳实践

```java
try {
    while(true) {
        ConsumerRecords<String,String> records = consumer.poll(Duration.ofSeconds(1));
        process(records);
        consumer.commitAsync((offsets, e) -> {   // 平时异步,不阻塞
            if (e != null) handle(e);
        });
    }
} catch(Exception e) {
    handle(e);
} finally {
    try { consumer.commitSync(); }   // 关闭前同步保底
    finally { consumer.close(); }
}
```

### 📌 版本提示
- 位移提交 API 不变；KIP-848 下异步增量提交更安全。
- 自动提交重复消费窗口问题在新协议下通过协同 rebalance（不再 STW）大幅缓解。

---

## 19 · CommitFailedException 异常怎么处理？

### 内容总结
`CommitFailedException`=不可恢复的位移提交异常（组已 rebalance、分区给别人）。原因：两次 poll 间隔超 `max.poll.interval.ms`。4 种应对：缩短单条处理时间、增大 `max.poll.interval.ms`、减小 `max.poll.records`、多线程加速。冷门场景：相同 `group.id` 的独立消费者+消费者组共存会触发。

### 重点
- 异常本质=poll 间隔超 `max.poll.interval.ms`，组已 rebalance。
- 4 种应对（推荐先优化下游消费逻辑，再调参）。
- `max.poll.interval.ms` 与 `max.poll.records` 调参估算（单条延时×条数 < 间隔）。
- 冷门 bug：独立消费者与消费者组 `group.id` 冲突。

### 详细解析与示例

#### 异常触发流程

```text
Consumer调用commitSync()
  ↓
两次poll间隔 > max.poll.interval.ms?
  └是→ Group已rebalance, 分区分配给别的实例
      → 抛CommitFailedException (不可恢复)

代码复现:
  max.poll.interval.ms=5s, poll间sleep(6s) → 必抛
```

#### 4种应对方法

```text
1. 缩短单条处理时间 (优化下游消费逻辑) ← 推荐
   例: 100ms/条 → 50ms/条, TPS×2

2. 增大max.poll.interval.ms
   估算: 单条延时×条数 < 间隔
   例: 下游MongoDB 2s/条, max.poll.records=500
       总时长1000s → max.poll.interval.ms > 1000000

3. 减小max.poll.records
   例: 设150条 → 总时长300s < 5min默认 ✓

4. 下游多线程加速 (如Flink多KafkaConsumerThread)
   ⚠️ 多线程位移提交难, 易重复消费
```

#### 调参估算示例

```text
场景: 消费后写MongoDB(平均2s/条)

max.poll.records=500(默认):
  一批总时长 = 500×2s = 1000s ≈ 16.7min
  → 默认max.poll.interval.ms=5min不够 → 必抛异常

方案2: max.poll.interval.ms = 1000000 (1000s+)
方案3: max.poll.records=150 → 150×2s=300s < 5min ✓ (最简单)
```

#### 冷门场景(group.id冲突)

```text
应用中同时有:
  - Consumer Group程序(group.id=test)
  - 独立消费者(Standalone, 也设group.id=test)

独立消费者手动提交 → Kafka不识别它为合法成员 → 抛异常
  (4种应对均无效, 需改独立消费者的group.id避免重复)
```

### 📌 版本提示
- KIP-848 新协议下 rebalance 不再 STW，CommitFailedException 触发概率大幅下降。
- `max.poll.interval.ms` 语义在新协议下有调整；独立消费者（Standalone）机制仍存在。

---

## 20 · 多线程开发消费者实例

### 内容总结
KafkaConsumer 单线程（0.10.1 起双线程：用户主线程+心跳线程），非线程安全。两套多线程方案：方案1 每线程专属 Consumer（粗粒度，简单无交互）；方案2 单/多线程获取+线程池处理（细粒度，解耦获取与处理，难在位移提交）。`KafkaConsumer` 非线程安全，唯一例外 `wakeup()`。

### 重点
- KafkaConsumer 单线程设计（0.10.1 心跳线程分离心跳与消息处理）。
- 非线程安全：共享实例抛 `ConcurrentModificationException`；`wakeup()` 可跨线程调用。
- 方案1（粗粒度）：每线程独立 Consumer，简单、无交互，但受分区数限制。
- 方案2（细粒度）：获取线程+处理线程池，解耦、扩展性好，难在位移提交与顺序保证。

### 详细解析与示例

#### KafkaConsumer 线程模型

```text
0.10.1起双线程:
  ├─ 用户主线程 (调用poll, 消息获取+处理)
  └─ 心跳线程 (Heartbeat Thread, 只发心跳)

← 分离心跳与消息处理: 消息处理慢不再误判死亡
但消息获取仍在主线程 → 消费层面仍单线程
```

#### 非线程安全

```text
KafkaConsumer 非线程安全:
  多线程共享实例 → ConcurrentModificationException
  唯一例外: wakeup() 可跨线程调用唤醒Consumer
```

#### 两套多线程方案对比

| 维度 | 方案1(粗粒度) | 方案2(细粒度) |
|------|---------------|---------------|
| 结构 | 每线程独立Consumer | 获取线程+处理线程池 |
| 实现 | 简单 | 复杂(位移提交难) |
| 交互 | 无(各自完整流) | 有(需共享队列) |
| 扩展 | 受分区数限制 | 灵活(线程池可调) |
| 位移 | 各Consumer独立提交 | 需额外管理 |
| 顺序 | 分区内有序 | 需额外保证 |

#### 方案1 代码骨架

```java
// 每线程独立Consumer, 受分区数限制
final int threadCount = 5;
for (int i = 0; i < threadCount; i++) {
    new Thread(() -> {
        KafkaConsumer<String,String> c = new KafkaConsumer<>(props);
        c.subscribe(Arrays.asList("topic"));
        while(true) {
            ConsumerRecords<String,String> rs = c.poll(Duration.ofSeconds(1));
            process(rs);  // 完整流, 各线程独立提交位移
        }
    }).start();
}
```

#### 方案2 骨架与位移难点

```text
获取线程(1个KafkaConsumer) → 阻塞队列 → 处理线程池
  
难点: 处理线程异步, 位移何时提交?
  ✗ 提早提交 → 处理失败的消息丢失
  ✓ 需跟踪每条消息处理完成 → 复杂
  ✓ 或用分区级位移表+定期提交
```

### 📌 版本提示
- KIP-848 新协议下心跳与 poll 进一步解耦（rebalance 不阻塞 poll）。
- 多线程消费仍是推荐做法；新协议下方案2 的位移管理更安全（协同提交）。

---

## 21 · Java 消费者是如何管理 TCP 连接的

### 内容总结
Consumer 在 `new KafkaConsumer` 时不创建连接（优于 Producer 不在构造器启动线程）。连接在 `poll` 时创建，3 时机：FindCoordinator 请求（连负载最小的 Broker）、连接 Coordinator、消费数据（连各分区 Leader 所在 Broker）。

### 重点
- Consumer 构造时不建连接（设计优于 Producer 的 this 逃逸隐患）。
- `poll` 内 3 时机建连接：FindCoordinator → 连 Coordinator → 连各分区 Leader。
- 连接数 ≈ 1(找协调者) + 1(协调者) + 各分区 Leader 所在 Broker 数。

### 详细解析与示例

#### Consumer 连接建立 3 时机（poll 内）

```text
new KafkaConsumer ─ 不建连接（优于 Producer 不在构造器启线程）

poll() 首次调用时建连接:
  1. FindCoordinator 请求 → 连负载最小的 Broker
  2. 连上后的 Coordinator（该Group的协调者）
  3. 消费数据 → 连各分区Leader所在Broker

→ 连接数 ≈ 1 + 1 + 各Leader所在Broker数
```

#### vs Producer 连接设计

```text
Producer:
  new KafkaProducer → 启动Sender线程 → 立即连bootstrap所有Broker
  隐患: this逃逸（构造器中this被传给线程共享）

Consumer:
  new KafkaConsumer → 不启线程、不建连接（延迟到poll）
  避免this逃逸，构造时不阻塞
```

#### 连接数估算示例

```text
集群10台Broker, Topic有6分区:
  FindCoordinator: 1连接
  连Coordinator: 1连接
  各分区Leader分布在6台Broker: 6连接
  总连接数 ≈ 1 + 1 + 6 = 8连接（远少于全集群）
```

#### KRaft 下连接变化

```text
KRaft: Controller 是单独节点
  FindCoordinator 仍连Broker（Broker代理元数据请求）
  底层Controller不再依赖ZK，客户端无感知
```

### 📌 版本提示
- KRaft 下 Coordinator 仍驻 Broker 内存，机制不变。
- KIP-848 新协议下消费连接管理优化；KIP-1102（4.0）增强 rebootstrap，避免连接卡住。

---

## 22 · 消费者组消费进度监控都怎么实现？

### 内容总结
Lag=消费者落后生产者的程度，分区级别监控、主题级汇总。3 种方法：`kafka-consumer-groups` 脚本（`--describe --group`，输出 LOG-END-OFFSET/CURRENT-OFFSET/LAG）、Java Consumer API 编程（查分区最新位移与消费位移算 Lag）、JMX 监控指标。Lag 大→读盘→马太效应越来越慢。

### 重点
- Lag 是消费者最重要监控指标；Lag 大会导致读盘马太效应（越慢 Lag 越大）。
- 3 种监控方法：脚本、API、JMX。
- `kafka-consumer-groups` 输出列含义；非 active 组也能查 Lag（升级版本后）。

### 详细解析与示例

#### Lag 定义

```text
Lag = 生产者写入的最新位移 - 消费者已提交位移
     = 未消费的消息条数

分区级: 每分区各算Lag
主题级: 各分区Lag汇总
```

#### 3 种监控方法

| 方法 | 命令/示例 | 适合场景 |
|------|----------|----------|
| 脚本 | `kafka-consumer-groups --describe --group <g>` | 运维/排查 |
| Java API | `endOffsets()` + `committed()` 算差值 | 应用内监控 |
| JMX | Broker/Consumer指标 | 集中监控 |

#### 脚本输出示例

```text
$ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group mygroup

GROUP        TOPIC     PARTITION  CURRENT-OFFSET   LOG-END-OFFSET   LAG
mygroup      orders    0          1000             1500             500
mygroup      orders    1          990              1500             510
mygroup      orders    2          1005             1500             495

LOG-END-OFFSET: 分区最新位移（生产者写入）
CURRENT-OFFSET: 消费者已提交位移
LAG: 两者差值（积压量）
```

#### Java API 监控示例

```java
Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(partitions);
for (TopicPartition tp : partitions) {
    long lag = endOffsets.get(tp) - committed.get(tp).offset();
    System.out.println(tp + " lag=" + lag);
}
```

#### 马太效应（Lag 大→越慢）

```text
Lag小 → 消息在页缓存 → 快
Lag变大 → 消息落盘 → 读盘IO慢 → 消费更慢
→ Lag继续变大 → 恶性循环（马太效应）

应对: 及时扩容Consumer / 优化消费逻辑 / 多分区
```

### 📌 版本提示
- 4.0 新增 `kafka-groups.sh`（KIP-1043）统一管理 consumer/share group；`kafka-consumer-groups` 增强。
- KIP-714（4.0）可从 Broker 收集客户端指标，补充 Lag 监控。

---

## 23 · Kafka 副本机制详解

### 内容总结
副本=分区级 append-only 提交日志，分散多 Broker 抗宕机。基于领导者副本机制：Leader 处理所有读写，Follower 异步拉取同步、不对外服务。Follower 不抗读的原因：保证 Read-your-writes 和单调读一致性。Leader 挂靠 ZK 监控触发新一轮选举。

### 重点
- 副本三好处 Kafka 只享第 1（数据冗余），无读扩展/局部性。
- Leader/Follower：Follower 不对外服务，仅异步拉取同步。
- Read-your-writes + 单调读一致性是 Follower 不抗读的设计原因。
- Leader 挂靠 ZK 监控触发选举。

### 详细解析与示例

#### 副本机制架构

```text
Topic: orders (RF=3)

┌───────────┐  ┌───────────┐  ┌───────────┐
│ Broker 1  │  │ Broker 2  │  │ Broker 3  │
│ Leader    │◀─│ Follower  │  │ Follower  │
│ (读写入口) │  │ (仅拉取)  │  │ (仅拉取)  │
└─────▲─────┘  └─────▲─────┘  └─────▲─────┘
      │              │              │
Producer ──写──▶ Leader ──拉取──▶ Follower
Consumer ──读──▶ Leader（不读Follower）
```

#### Follower 不抗读的 2 大原因

```text
1. Read-your-writes（读你写的）
   Producer写Leader后立即读 → 必须读Leader才看得到

2. 单调读一致性
   Consumer读Follower → 可能读到旧数据/时有时无
   (Leader新写入尚未同步到Follower)

→ 结论: 分区已跨Broker分散, 读写负载天然均衡, 无需主从读
```

#### 副本三好处 Kafka 只享第1

```text
副本通用三好处:
  1. 数据冗余（抗宕机）      ✓ Kafka享受
  2. 读扩展（多Follower读）  ✗ 未实现
  3. 数据局部性              ✗ 未实现

Kafka: 仅数据冗余；读负载靠分区分散而非副本读
```

#### ISR 机制（副本同步）

```text
ISR(In-Sync Replicas): 与Leader同步的副本集合

Producer(acks=all) → Leader写入
  → ISR中其他副本拉取同步
  → 至少min.insync.replicas个副本确认 → 才算已提交

Follower落后超过replica.lag.time.max.ms(默认30s)
  → 移出ISR (可能触发选举变化)
```

#### 副本选举（ZK vs KRaft）

```text
ZK时代: Leader副本挂靠ZK
  挂ZK路径 /brokers/topics/<topic>/partitions/<p>/state
  ZK Watch感知Leader异常 → 触发新一轮选举

KRaft时代(4.0): Controller仲裁主动选举
  Controller监听Broker心跳
  Broker异常 → Controller主动发起选举
  ⭐ 不再依赖ZK
```

### 📌 版本提示
- **Leader 挂靠 ZK 监控选举 → KRaft 下改为 Controller 仲裁主动选举**，不再依赖 ZK。
- 4.0 KIP-966 ELR（预览）保证当选 Leader 数据完整到 HW，与 `unclean=false` 互补。
- Follower 不抗读的设计仍不变。

---

## 24 · 请求是怎么被处理的？

### 内容总结
Kafka 自定义 45 种请求协议（2.3）。顺序处理/每请求一线程都不行，用 Reactor 模式：Acceptor 线程分发 → 网络线程池（`num.network.threads` 默认 3） → 共享请求队列 → IO 线程池处理（PRODUCE 写盘/FETCH 读）。

### 重点
- Reactor 模式：Acceptor + 网络线程池 + IO 线程池。
- `num.network.threads` 默认 3，Acceptor 轮询分发到网络线程。
- 网络线程放请求到共享队列，IO 线程取请求真正处理（避免阻塞）。
- 慢操作可转交 RequestHandler 异步处理。

### 详细解析与示例

#### Reactor 架构

```text
                ┌──────────┐
                │Acceptor  │  监听请求
                │线程(1个) │
                └────┬─────┘
                     │轮询分发
        ┌────────────┼────────────┐
        ▼            ▼            ▼
   ┌──────────┐ ┌──────────┐ ┌──────────┐
   │网络线程1│ │网络线程2│ │网络线程3│  num.network.threads=3
   │接收请求 │ │接收请求 │ │接收请求 │
   └────┬─────┘ └────┬─────┘ └────┬─────┘
        └────────────┼────────────┘
                     ▼
              ┌───────────┐
              │共享请求队列│
              └─────┬─────┘
                    ▼
        ┌──────────┼──────────┐
        ▼          ▼          ▼
   ┌─────────┐ ┌─────────┐ ┌─────────┐
   │IO线程1  │ │IO线程2  │ │IO线程N  │  num.io.threads=8
   │处理请求 │ │处理请求 │ │处理请求 │
   │PRODUCE │ │FETCH   │ │...      │
   │写盘    │ │读     │ │         │
   └─────────┘ └─────────┘ └─────────┘
```

#### 为何用 Reactor

```text
方案1: 顺序处理(1线程) → 请求排队,吞吐低
方案2: 每请求一线程 → 线程爆炸,上下文切换开销
方案3: Reactor(线程池) → 网络线程接收,IO线程处理
         避免阻塞: 网络线程只接收放队列,不处理
         ← 选此方案
```

#### 关键参数

```properties
num.network.threads=3   # 网络线程(接收请求放队列,默认3)
num.io.threads=8        # IO线程(真正处理请求,默认8)
request.queue.size=500  # 请求队列容量(满则拒绝)
```

#### 慢操作异步处理

```text
RequestHandler处理请求:
  PRODUCE → 写盘(同步)
  FETCH  → 读(可零拷贝sendfile)
  慢操作 → 转交RequestHandler Pool异步处理
  → 不阻塞IO线程处理后续请求
```

### 📌 版本提示
- KRaft 下请求处理架构不变。
- 4.0 请求数增加（新增 share group、client metrics 等）；KIP-714 客户端指标收集涉及请求协议扩展。

---

## 25 · 消费者组重平衡全流程解析

### 内容总结
重平衡通知靠心跳线程（0.10.1 起独立心跳线程，避免消息处理慢导致误判死亡）。`REBALANCE_IN_PROGRESS` 封装在心跳响应。`heartbeat.interval.ms` 控制通知频率。消费者组状态机 5 状态：Empty/Dead/PreparingRebalance/CompletingRebalance/Stable。过期位移删除仅在 Empty 状态。

### 重点
- 心跳线程通知机制（0.10.1 独立心跳线程，解耦消息处理与存活检测）。
- 状态机 5 状态流转：Empty→PreparingRebalance→CompletingRebalance→Stable。
- 过期位移删除需 Empty 状态（停超 7 天可能删位移）。
- 重平衡全流程消费者与 Coordinator 共同参与（JoinGroup/SyncGroup）。

### 详细解析与示例

#### 心跳通知机制

```text
0.10.1前: 主线程既poll消息又发心跳
  问题: 消息处理慢 → 心跳跟不上 → 误判死亡 → 踢出

0.10.1起: 独立心跳线程
  用户主线程: poll消息+处理
  心跳线程: 只发心跳 (与消息处理解耦)

通知: REBALANCE_IN_PROGRESS封装在心跳响应中
  → 控制频率: heartbeat.interval.ms
```

#### 消费者组状态机 5 状态

```text
┌────────┐   成员加入    ┌───────────────────┐
│Empty   │────────────▶│PreparingRebalance  │
└────────┘             └────────┬──────────┘
                                 │
                          ┌──────▼───────────┐
                          │CompletingRebalance│
                          └──────┬───────────┘
                                 │
                          ┌──────▼──────┐
                          │   Stable    │
                          └─────────────┘

Dead: Group下所有Consumer停止 + 位移已删 + tombstone
```

#### 过期位移删除条件

```text
仅Empty状态下删除:
  Group存活超offsets.retention.minutes(默认7天)
  → Group转为Empty → 删除位移(可恢复)
  → 再停止+tombstone → Dead (彻底删除)

⚠️ 长时间不启动Consumer可能丢位移
```

#### JoinGroup/SyncGroup 流程

```text
1. Consumer发JoinGroup → Coordinator
2. Coordinator选Coordinator(负载均衡分配算法)
3. Coordinator发SyncGroup给所有成员
4. 每个Consumer从SyncGroup响应获自己的分区分配
5. Consumer开始消费对应分区

4.0 KIP-848: 增量协同版JoinGroup/SyncGroup
  → 只重分配变化部分, 消除STW
```

### 📌 版本提示
- **4.0 KIP-848 重写状态机与流程**：增量协同 rebalance 消除 STW，状态流转更平滑，心跳与 poll 解耦更彻底。

---

## 26 · 你一定不能错过的 Kafka 控制器

### 内容总结
Controller 重度依赖 ZK。选举：第一个成功创建 `/controller` 节点的 Broker。5 职责：主题管理、分区重分配、Preferred Leader 选举、集群成员管理（Watch+临时节点感知 Broker 增删）、数据服务（保存最全元数据，定期推给其他 Broker）。ZK：持久/临时 znode、Watch 通知。

### 重点
- Controller 由 ZK 选举（抢 `/controller` 节点，每集群唯一）。
- 5 大职责：主题管理、分区重分配、Preferred Leader 选举、集群成员管理、数据服务。
- 成员管理靠 Watch + 临时节点（Broker 宕机会话结束、znode 删除、Watch 通知 Controller）。
- `activeController` JMX 指标监控 Controller 存活。

### 详细解析与示例

#### Controller 5 大职责

```text
┌────────────────────────────────────────┐
│              Controller                │
├──────────┬──────────┬──────────┬───────┤
│主题管理   │分区重分配  │Preferred │集群成员 │
│创建/删除  │扩缩分区   │Leader选举 │增删感知 │
├──────────┴──────────┴──────────┴───────┤
│  数据服务: 保存最全元数据,定期推给其他Broker  │
└────────────────────────────────────────┘
```

#### ZK 选举机制（2.x 时代）

```text
每个Broker启动:
  尝试创建ZK持久节点 /controller
  ↓
成功创建的Broker成为唯一Controller
  其他Broker作为Observer监控/controller变化

节点类型:
  持久znode: /brokers/topics/... (主题元数据)
  临时znode: /brokers/ids/<id> (Broker存活,会话结束自动删)
  Watch: Controller监听临时节点变化 → 感知Broker增删
```

#### ZK vs KRaft Controller 对比

| 维度 | ZK Controller | KRaft Controller |
|------|--------------|-----------------|
| 选举 | 抢 `/controller` 节点 | Raft共识选active |
| 元数据 | 存ZK持久节点 | 存内部主题 `__cluster_metadata` |
| 成员感知 | Watch临时节点 | 元数据日志变更 |
| 架构 | 每个Broker都能当 | 独立controller节点(推荐) |
| 启动速度 | 大集群数十分钟 | 毫秒~秒级 |

#### KRaft 部署示例

```properties
# 独立Controller模式(推荐)
process.roles=controller
node.id=0
controller.quorum.voters=0@controller1:9093,1@controller2:9093,2@controller3:9093
listeners=CONTROLLER://:9093
controller.listener.names=CONTROLLER

# Broker只连Controller:
process.roles=broker
node.id=100
controller.quorum.voters=0@controller1:9093,1@controller2:9093,2@controller3:9093
listeners=PLAINTEXT://:9092
```

#### 监控 Controller

```bash
# JMX查询 activeController
# MBean: kafka.controller:type=KafkaController,name=ActiveControllerCount
# ActiveControllerCount=1 表示存活
```

### 📌 版本提示
- **这是受 KRaft 影响最大的章节**：ZK 选举 Controller → KRaft Controller 仲裁（独立 controller 节点，Raft 共识）。
- 不再有「每 Broker 抢 `/controller`」和 ZK Watch；元数据由 KRaft metadata log 统一管理推送。
- `activeController` 概念变为 active controller（仲裁 leader），监控方式演进。

---

## 27 · 关于高水位和 Leader Epoch 的讨论

### 内容总结
HW（高水位）=消息位移标识，作用：定义消息可见性、副本同步。HW 以下=已提交可消费，HW 上=未提交不可消费。LEO=日志末端位移（下一条消息位移）。Leader 副本保存所有 Follower（远程副本）LEO 帮助确定分区 HW。Leader Epoch（0.11）弥补 HW 缺陷（防止日志截断丢数据）。

### 重点
- HW 与 LEO 概念；HW 定义可见性与副本同步；HW≤LEO。
- Leader 保存远程副本 LEO 帮助确定分区 HW。
- 位移=HW 的消息不可消费。
- Leader Epoch（0.11）防 HW 更新错乱导致的丢数据。

### 详细解析与示例

#### HW / LEO 概念

```text
日志消息位移: 0 1 2 3 4 5 6 7
                 ▲       ▲
                 HW      LEO
              (高水位)  (日志末端位移)

HW以下: 已提交, 可消费
HW以上: 未提交, 不可消费
HW ≤ LEO  (不变式)
```

#### HW 双重作用

```text
1. 消息可见性
   Consumer只能读HW以下的消息
   HW=3 → 位移0,1,2可读, 位移3及以上不可读

2. 副本同步
   Leader保存所有Follower的LEO
   HW = min(所有ISR副本的LEO)
   确保HW以下的数据在所有ISR中都存在
```

#### Leader Epoch 防丢数据

```text
问题: HW更新错乱场景
  T1: Leader1选举, HW=10
  T2: Leader2选举, 但Leader2的日志只到5
  T3: Leader2基于旧HW=10 → 错截断 → 丢数据

解决: Leader Epoch(0.11引入)
  每次选举递增epoch
  副本记录epoch→LEO映射
  Leader截断日志时参考对应epoch的LEO
  → 防止基于错乱的HW截断
```

#### 副本同步与HW更新

```text
Producer写 → Leader记录(offset=10, LEO=11)
              └─ ISR中Follower异步拉取
              └─ 所有Follower LEO都≥10
              └─ HW更新为10
              └─ 位移0-9对Consumer可见
```

### 📌 版本提示
- HW/LEO 机制 4.0 仍沿用；Leader Epoch 仍是防日志截断丢数据的关键。
- 4.0 KIP-966 ELR（预览）在 HW 基础上提供更安全的 Leader 选举（保证当选 Leader 数据完整到 HW）。

---

## 28 · 主题管理知多少

### 内容总结
`kafka-topics` 脚本增删改查。2.2 起 `--bootstrap-server` 替换 `--zookeeper`（安全认证+统一连接）。增分区（`--alter`，不能减）、改 Topic 级参数（`kafka-configs`）、删主题、查主题。特殊主题：内部主题 `__consumer_offsets`、事务 `__transaction_state`。

### 重点
- 2.2 起用 `--bootstrap-server` 而非 `--zookeeper`（安全认证+统一连接信息）。
- 分区只能增不能减（减抛 `InvalidPartitionsException`）。
- `kafka-configs` 改 Topic 级参数；内部主题管理。

### 详细解析与示例

#### kafka-topics 增删改查

```bash
# 查主题列表
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# 查主题详情(分区/副本分布)
bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic orders

# 创建主题(指定分区/副本/参数)
bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic orders \
  --partitions 6 --replication-factor 3 \
  --config retention.ms=604800000

# 增分区(只能增不能减)
bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --alter --topic orders --partitions 12

# 删除主题
bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --delete --topic orders
```

#### 改 Topic 级参数

```bash
# 改留存为7天
bin/kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name orders \
  --alter --add-config retention.ms=604800000

# 查参数
bin/kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name orders --describe
```

#### 增分区的影响

```text
增分区: --partitions 6 → 12
  ✓ 新分区可用
  ⚠️ 原分区消息不变, 但hash(key)%分区数 结果变化
     → 同key可能映射到新分区 → 分区内顺序可能乱
     → 建议: 创建时预留分区, 不轻易增
```

#### 内部主题

```text
__consumer_offsets (位移):
  50分区, 3副本, 50分钟留存
  手动查询: kafka-console-consumer --topic __consumer_offsets

__transaction_state (事务):
  50分区, 3副本
  存储事务状态(活跃/完成/中止)

查看: kafka-topics --list 会显示这些内部主题
```

### 📌 版本提示
- **4.0 ZK 移除，`--zookeeper` 参数彻底不可用**，必须 `--bootstrap-server`。
- KRaft 下主题管理命令统一走 `--bootstrap-server`；事务状态主题 `__transaction_state` 仍存在。

---

## 29 · Kafka 动态配置了解下？

### 内容总结
1.1 引入动态 Broker 参数（Dynamic Broker Configs），无需重启即生效。官网 Dynamic Update Mode 列 3 类：read-only（重启生效）、per-broker（单 Broker）、cluster-wide（全集群）。使用场景：动态调线程池应对突发流量、调连接/安全、更新 SSL、调 Compact。动态配置保存在 ZK。

### 重点
- 动态 vs 静态参数；3 类 update mode（read-only/per-broker/cluster-wide）。
- 动态调网络/IO 线程应对突发流量最实用（可封装定时任务自动扩缩容）。
- 动态配置保存在 ZK（changes/topics/users/clients znode，含客户端配额）。

### 详细解析与示例

#### 动态 vs 静态参数

```text
静态参数: 改后必须重启生效
  例: broker.id, num.partitions

动态参数: 1.1起, 无需重启即时生效
  例: num.network.threads, num.io.threads, log.retention.hours

查官网: Configs → Dynamic Update Mode 列
```

#### 3 类 Update Mode

| Mode | 作用范围 | 示例 |
|------|---------|------|
| read-only | 仅重启生效 | broker.id |
| per-broker | 单个Broker | 该Broker参数 |
| cluster-wide | 全集群 | log.retention.hours |

#### 动态配置命令示例

```bash
# 动态调网络线程(单Broker, 不重启)
bin/kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type brokers --entity-name 0 \
  --alter --add-config num.network.threads=10

# 动态调全集群留存
bin/kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name orders \
  --alter --add-config retention.ms=1209600000

# 查配置
bin/kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type brokers --entity-name 0 --describe
```

#### 动态调线程应对突发流量

```text
监控TPS上升 → 告警 → 自动调大num.io.threads
  封装定时任务/运维平台脚本
  避免重启中断服务
```

#### KRaft 下存储变化

```text
ZK时代: 动态配置存ZK znode
  /kafka/config/changes
  /kafka/config/topics
  /kafka/config/users
  /kafka/config/clients (配额)

KRaft时代(4.0): 改存__cluster_metadata内部主题
  不再存ZK
  kafka-configs用--bootstrap-server
```

### 📌 版本提示
- **KRaft 下动态配置改为存 KRaft metadata log（非 ZK）**；`kafka-configs` 用 `--bootstrap-server`。
- 动态参数范围 4.0 进一步扩展。

---

## 30 · 怎么重设消费者组位移？

### 内容总结
Kafka 消费可重演（log-based 只读，位移可控）。2 维度×7 策略：位移维度（Earliest/Latest/Current/Specified-Offset/Shift-By-N）、时间维度（DateTime/Duration）。两种方式：Consumer API（`seek`）、`kafka-consumer-groups` 脚本（`--reset-offsets`）。

### 重点
- Kafka 可重消费（vs 传统 MQ 破坏性删除）。
- 7 种重设策略；跳过坏消息用 Specified-Offset。
- Duration 格式 `PnDTnHnMnS`（ISO-8601）。

### 详细解析与示例

#### 7 种重设位移策略

```text
位移维度:
  Earliest  → 最早位移(0)
  Latest    → 最新位移
  Current   → 当前位移不变
  Specified-Offset → 指定位移值
  Shift-By-N → 当前±N条

时间维度:
  DateTime → 指定时间点
  Duration → 距当前n时间(P1D=1天)
```

#### kafka-consumer-groups 重设示例

```bash
# 重置到最早
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group mygroup --reset-offsets \
  --to-earliest --execute

# 重置到最新
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group mygroup --reset-offsets \
  --to-latest --execute

# 跳过3条坏消息(当前+3)
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group mygroup --reset-offsets \
  --shift-by -3 --execute

# 重置到1天前
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group mygroup --reset-offsets \
  --to-duration P1D --execute

# 干运行预览(不执行)
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group mygroup --reset-offsets \
  --to-earliest --dry-run
```

#### Consumer API seek 示例

```java
// 编程方式重置位移(仅当前实例)
consumer.assign(Arrays.asList(tp));
consumer.seek(tp, 0);    // 重置到最早
consumer.seekToBeginning(Collections.singletonList(tp));
consumer.seekToEnd(Collections.singletonList(tp));
consumer.seek(tp, specificOffset);  // 指定位移
// 下次poll()生效
```

#### Duration 格式示例

```text
P1D     = 1天
P1DT2H  = 1天2小时
PT30M   = 30分钟
PT1H30M = 1小时30分钟
PN      = 无单位(P开头,D天H小时M分钟S秒)
```

### 📌 版本提示
- `kafka-consumer-groups --reset-offsets` 在 KRaft 下用 `--bootstrap-server`。
- KIP-848 下位移重设与新协议协同；4.0 新增 duration-based `auto.offset.reset`（按时长重置，KIP-1010）。

---

## 31 · 常见工具脚本大汇总

### 内容总结
2.2 版 30 个 shell 脚本。`connect-standalone/distributed`（Connect 启动）、`kafka-acls`（权限）、`kafka-broker-api-versions`（版本兼容性，0.10.2 起双向兼容）、`kafka-configs`、`kafka-console-consumer/producer`、`kafka-producer-perf-test/consumer-perf-test`、`kafka-consumer-groups`、`kafka-topics` 等。

### 重点
- 30 个脚本盘点；`kafka-broker-api-versions` 验证客户端/服务端版本兼容。
- 0.10.2 起双向兼容（低版本 Broker 也能处理高版本 Client 请求）。
- `console-consumer/producer` 最常用；`perf-test` 性能测试。

### 详细解析与示例

#### 30 脚本分类

```text
Connect:
  connect-standalone / connect-distributed

权限:
  kafka-acls

元数据:
  kafka-broker-api-versions  kafka-topics  kafka-configs

生产/消费:
  kafka-console-producer    kafka-console-consumer
  kafka-producer-perf-test  kafka-consumer-perf-test
  kafka-streams-application-reset

消费者组:
  kafka-consumer-groups     kafka-groups (4.0新增)

工具:
  kafka-log-dirs            kafka-leader-election
  kafka-reassign-partitions  kafka-delete-records
  kafka-cluster             kafka-groups (KIP-1043)
```

#### 版本兼容性验证

```bash
# 查Broker支持的API版本
bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092
# Produce(0): 0 to 9 [usable: 9]
# → 2.2服务端, 可用到v9
#
# 0.10.2起双向兼容:
#   低版本Broker也能处理高版本Client请求
#   查usable最大值 → 确认客户端格式版本
```

#### 性能测试示例

```bash
# Producer性能压测
bin/kafka-producer-perf-test.sh \
  --broker-list localhost:9092 \
  --topic test --messages 1000000 \
  --record-size 1000 --throughput -1

# Consumer性能压测
bin/kafka-consumer-perf-test.sh \
  --broker-list localhost:9092 \
  --topic test --messages 1000000

# Streams应用状态重置
bin/kafka-streams-application-reset.sh \
  --bootstrap-server localhost:9092 \
  --application-id my-stream
```

### 📌 版本提示
- 4.0 脚本数增加（新增 `kafka-groups.sh`、`kafka-share-groups.sh`）。
- 所有脚本统一 `--bootstrap-server`，`--zookeeper` 彻底移除；KIP-848/KIP-932 引入新 group 类型管理脚本。

---

## 32 · KafkaAdminClient：Kafka 的运维利器

### 内容总结
命令行脚本弊端（控制台受限、直连 ZK 绕过安全、用服务器端代码）。0.11 推出客户端 `AdminClient`（`kafka-clients` 依赖），9 大类功能：主题/权限/配置参数/副本日志/分区/消息删除/Delegation Token/消费者组管理。统一走请求机制，纳入安全认证。

### 重点
- `AdminClient`（0.11）替代命令行直连 ZK。
- 走客户端请求机制 + 安全认证，便于集成到应用/运维平台。
- 9 大类运维功能；老 `kafka.admin.AdminClient` 已弃用。

### 详细解析与示例

#### AdminClient vs 命令行脚本

```text
命令行脚本弊端:
  ✗ 控制台受限,难集成
  ✗ 直连ZK绕过安全认证
  ✗ 用服务器端代码(耦合)

AdminClient(0.11):
  ✓ kafka-clients依赖,嵌入应用
  ✓ 走客户端请求机制+安全认证
  ✓ 统一客户端API
```

#### 9 大类功能

```text
1. 主题管理: createTopic/deleteTopics
2. 权限管理: createAcls/deleteAcls
3. 配置参数: configure/describeConfigs
4. 副本日志: deleteRecords/logDir
5. 分区管理: createPartitions/reassignPartitions
6. 消息删除: deleteRecords
7. Delegation Token: createToken/deleteToken
8. 消费者组: listConsumerGroups/describeGroups
9. 客户端配额: 4.0新增
```

#### Java 代码示例

```java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
AdminClient admin = AdminClient.create(props);

// 创建主题
NewTopic topic = new NewTopic("orders", 6, (short)3);
admin.createTopics(Collections.singleton(topic));

// 删除主题
admin.deleteTopics(Collections.singletonList("orders"));

// 改配置
Map<TopicCollection, Config> configs = ...;
admin.incrementalAlterConfigs(configs);

// 查消费者组
admin.listConsumerGroups().all().get();
```

### 📌 版本提示
- `AdminClient` 在 KRaft 下全面生效（无 ZK 可连），仍是推荐的运维集成方式。
- 4.0 功能扩展（支持 share group、client metrics 等新资源管理）。

---

## 33 · Kafka 认证机制用哪家？

### 内容总结
认证（authentication，你是谁） vs 授权（authorization，能做什么）。0.9 引入认证。SSL 双路认证（Broker+客户端互认证）。SASL 5 种机制：GSSAPI/Kerberos(0.9)、PLAIN(0.10)、SCRAM(0.10.2)、OAUTHBEARER(2.0)、Delegation Token(1.1)。建议 SSL 做加密 + SASL 做认证。SASL/PLAIN 不能动态增减用户需重启。

### 重点
- 认证 vs 授权区别；SSL 加密 + SASL 认证是推荐组合。
- 5 种 SASL 机制选型：Kerberos 场景用 GSSAPI，小公司用 PLAIN，安全敏感用 SCRAM。
- PLAIN 用户存静态文件需重启 Broker；SCRAM 支持动态增减。

### 详细解析与示例

#### 认证 vs 授权

```text
认证(Authentication): 你是谁
  身份校验: 密码/证书/Ticket

授权(Authorization): 能做什么
  权限校验: 能否读写某主题/分区

典型组合: SSL(加密) + SASL(认证) + ACL(授权)
```

#### SSL vs SASL 对比

| 维度 | SSL | SASL |
|------|-----|------|
| 作用 | 加密+双路认证 | 仅认证(需配SSL加密) |
| 性能 | 双路证书验证开销大 | 较轻 |
| 互认证 | 是(双向) | 否(需配SSL) |
| 动态用户 | 证书管理复杂 | PLAIN否/SCRAM是 |
| 场景 | 高安全场景 | 常规场景 |

#### 5 种 SASL 机制

| 机制 | 版本 | 特点 | 适合 |
|------|------|------|------|
| GSSAPI(Kerberos) | 0.9 | Ticket认证,需KDC | 大型企业/域 |
| PLAIN | 0.10 | 用户名+密码明文 | 小公司(配SSL) |
| SCRAM | 0.10.2 | 挑战-响应,密码不传 | 安全敏感 |
| OAUTHBEARER | 2.0 | OAuth令牌 | 云/微服务 |
| Delegation Token | 1.1 | 令牌授权 | 临时访问 |

#### 配置示例

```properties
# Broker端: SASL/SCRAM
listeners=SASL_SSL://:9092
listener.name.sasl_ssl.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="admin" password="admin_pass";
listener.name.sasl_ssl.security.protocol=SASL_SSL
sasl.enabled.mechanisms=SCRAM-SHA-512

# Client端
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="producer1" password="pass1";
```

### 📌 版本提示
- KRaft 下认证机制不变（仍 SSL+SASL）。
- SCRAM 凭据存储从 ZK 改为 KRaft metadata；4.0 仍支持 OAUTHBEARER。

---

## 34 · 云环境下的授权该怎么做？

### 内容总结
4 种权限模型 ACL/RBAC/ABAC/PBAC，Kafka 用 ACL。模型：'Principal P is Allowed/Denied Operation O From Host H On Resource R'。5 种 Resource：TOPIC/CLUSTER/GROUP/TRANSACTIONALID/DELEGATION TOKEN。授权保存在 ZK `/kafka-acl` 节点，`kafka-acls` 脚本动态增删改查。`authorizer.class.name=SimpleAclAuthorizer`。

### 重点
- Kafka 用 ACL 模型（非 RBAC）；5 种 Resource。
- `kafka-acls` 动态管理 ACL，存 ZK `/kafka-acl`。
- `SimpleAclAuthorizer` 开启授权。

### 详细解析与示例

#### 4 种权限模型对比

| 模型 | 说明 | 适合 |
|------|------|------|
| ACL | 主体-操作-资源 直接映射 | Kafka采用 |
| RBAC | 主体→角色→权限 | 大型企业 |
| ABAC | 基于属性动态判断 | 复杂场景 |
| PBAC | 基于关系判断 | 组织层级 |

#### ACL 模型详解

```text
形式: 'Principal P is Allowed/Denied Operation O
      From Host H On Resource R'

例: 'User:alice is Allowed READ From Host 10.0.0.1
    On Resource Topic:orders'

5 种 Resource:
  TOPIC / CLUSTER / GROUP / TRANSACTIONALID / DELEGATION_TOKEN

4 种 Operation:
  READ / WRITE / CREATE / DELETE / ALTER / DESCRIBE
  ALL / IDEMPOTENT_WRITE
```

#### kafka-acls 命令示例

```bash
# 授予alice读orders主题权限
bin/kafka-acls.sh --bootstrap-server localhost:9092 \
  --add --allow-principal User:alice \
  --operation Read --topic orders

# 授予写权限(指定主机)
bin/kafka-acls.sh --bootstrap-server localhost:9092 \
  --add --allow-principal User:bob \
  --operation Write --topic orders \
  --host 10.0.0.1

# 查权限
bin/kafka-acls.sh --bootstrap-server localhost:9092 \
  --list --topic orders

# 删除权限
bin/kafka-acls.sh --bootstrap-server localhost:9092 \
  --remove --allow-principal User:alice \
  --operation Read --topic orders
```

#### KRaft 下配置

```properties
# 4.0推荐: StandardAuthorizer (KRaft版)
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer

# 旧版本(ZK版,已弃用):
# authorizer.class.name=kafka.security.authorizer.SimpleAclAuthorizer

# KRaft: ACL存__cluster_metadata主题
# ZK时代: ACL存/kafka-acl znode
```

### 📌 版本提示
- **`SimpleAclAuthorizer`（ZK 版）早已弃用，3.x 起用 `StandardAuthorizer`（KRaft 版）**，ACL 存 KRaft metadata 而非 ZK。
- 4.0 强制 KRaft，授权全走 metadata log；Resource 类型 4.0 扩展。

---

## 35 · 跨集群备份解决方案 MirrorMaker

### 内容总结
备份（单集群节点间） vs 镜像（集群间）。MirrorMaker=消费者+生产者，从源集群消费→目标集群生产。`kafka-mirror-maker` 脚本：`consumer.config`/`producer.config`/`num.streams`/`whitelist`。多套集群场景：灾备、低延时、数据分析。

### 重点
- MirrorMaker 本质 Consumer+Producer。
- 4 参数（consumer/producer config、num.streams、whitelist 正则）。
- 多集群拓扑：灾备/分析/热备。

### 详细解析与示例

#### 备份 vs 镜像

```text
备份(Backup): 单集群节点间复制
  → Kafka副本机制(RF=3)自动完成

镜像(Mirror): 集群间复制
  → MirrorMaker实现
```

#### MirrorMaker 架构

```text
源集群(Source)                 目标集群(Target)
┌──────────┐                  ┌──────────┐
│ Cluster A │                  │ Cluster B │
│ Topic T1  │ ──MirrorMaker──▶│ Topic T1  │
└──────────┘                  └──────────┘
        本质: Consumer(源) + Producer(目标)

MM2(2.4+): 基于Kafka Connect框架
  源集群 ◀── Connect Source ──┐
                              ▼
                         Kafka Topic
                              │
                         Connect Sink ──▶ 目标集群
```

#### MM2 配置示例

```properties
# MirrorMaker 2 配置
clusters=source,target
source.consumer.config.bootstrap.servers=source:9092
target.producer.config.bootstrap.servers=target:9092

# Topic映射规则
source.topic.regex=.*
target.topic.remap.pattern=^(.*)$
target.topic.remap.replace=$1_replica

# 监控心跳主题
num.streams=3
topic.whitelist=orders,payments

# MM2: 启用ACL
source.consumer.config.sasl.mechanism=SCRAM-SHA-512
target.producer.config.sasl.mechanism=SCRAM-SHA-512
```

#### MM1 vs MM2

| 维度 | MM1 | MM2 |
|------|-----|-----|
| 框架 | 独立Consumer+Producer | Kafka Connect |
| 版本 | ≤2.3 | 2.4+ |
| 双向复制 | 不支持 | 支持 |
| ACL/位移同步 | 不支持 | 支持 |
| 状态 | 已弃用 | 推荐 |

### 📌 版本提示
- **MirrorMaker 1(MM1) 已弃用，MirrorMaker 2(MM2，2.4 引入) 成默认**：MM2 基于 Connect 框架，支持 ACL/位移/Topic 同步、双向复制。
- 4.0 MM2 进一步完善（可复制 .internal 主题、可禁用 heartbeat 主题复制等）。

---

## 36 · 你应该怎么监控 Kafka？

### 内容总结
从主机、JVM、Kafka 集群三维度监控。主机：机器负载（load average 1/5/15min）、CPU 使用率（top %CPU 多核可超 100）、内存、磁盘/网络 IO、TCP 连接数、打开文件数、inode。JVM：GC、堆。Kafka 集群：JMX 指标。

### 重点
- 三维度监控（主机/JVM/集群），勿只盯 Broker。
- top %CPU 是所有 CPU 平均转单核（多核可超 100，除以核数得真实使用率）。
- load average 三时段（1/5/15min）；JVM GC 监控。

### 详细解析与示例

#### 三维度监控清单

```text
1. 主机维度
   load average(1/5/15min) │ CPU % │ 内存
   磁盘IO │ 网络IO │ TCP连接数
   打开文件数 │ inode

2. JVM维度
   GC频率/耗时 │ 堆使用率
   堆溢出风险 │ 线程数

3. Kafka集群维度
   JMX指标 │ 自定义监控
```

#### 关键指标采集示例

```bash
# 主机load
uptime
# load average: 1.20 0.85 0.60 (1/5/15min)
# 建议: load < CPU核数

# CPU使用率(多核)
top
# %CPU=150 (2核) → 真实使用率75%

# TCP连接数
ss -s
# tcp: 12345 (estab 8000)

# 磁盘使用
df -h
# inode
df -i
```

#### JMX 指标采集

```bash
# 查JMX MBean
jconsole  → kafka.server:type=BrokerTopicsMetrics

# 关键MBean:
# BrokerTopicMetrics: Produce/Request/Fetch 指标
# Topic: RecordRate/BytesInPerSec/BytesOutPerSec
# ClientQuotas: 客户端配额

# Prometheus JMX Exporter(常用):
# JVM MBean → Prometheus → Grafana
```

#### 监控告警阈值参考

| 指标 | 告警阈值 | 说明 |
|------|---------|------|
| load average | >CPU核数 | 过载 |
| GC耗时 | >2s/次 | 频繁GC |
| 堆使用率 | >80% | 内存风险 |
| Lag | >10万 | 消费积压 |
| 磁盘使用 | >80% | 磁盘风险 |

### 📌 版本提示
- 4.0 KIP-714 可从 Broker 直接收集客户端指标（补充 JMX）。
- KIP-1076 Streams 状态指标；监控维度建议增加 KRaft controller 仲裁指标（新引入）。

---

## 37 · 主流的 Kafka 监控框架

### 内容总结
社区未投入监控框架（500+ KIP 无监控提议），依赖第三方。JMXTool（社区自带工具，临时救急，`bin/kafka-run-class.sh kafka.tools.JmxTool`），通过 `--object-name` 查 JMX 指标（如 BytesInPerSec、ActiveController）。主流第三方框架：CMAK（原 Kafka Manager）、Kafka Eagle、Burrow（Lag 监控）、Confluent Control Center、JMXTrans+InfluxDB+Grafana。

### 重点
- JMXTool 临时查看 JMX 指标；`--object-name` 指标查询。
- 社区无官方监控框架，靠第三方（CMAK/Grafana/Burrow 等）。

### 详细解析与示例

#### JMXTool 使用

```bash
# 查指定MBean指标
bin/kafka-run-class.sh kafka.tools.JmxTool \
  --host localhost --port 9999 \
  --object-name 'kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec'

# 查Controller是否存活
bin/kafka-run-class.sh kafka.tools.JmxTool \
  --host localhost --port 9999 \
  --object-name 'kafka.controller:type=KafkaController,name=ActiveControllerCount'
```

#### 监控框架对比

| 框架 | 特点 | 适合 |
|------|------|------|
| CMAK(原Kafka Manager) | UI直观,社区活跃 | 主流选择 |
| Kafka Eagle | 实时Lag监控 | 消费进度 |
| Burrow(Rust) | Lag监控,轻量 | 专用Lag监控 |
| Confluent Control Center | 企业级,官方 | Confluent用户 |
| JMXTrans+InfluxDB+Grafana | 灵活组合 | 已有监控栈 |

#### Prometheus + Grafana 监控栈

```text
Broker JMX → JMX Exporter → Prometheus → Grafana
Consumer Lag → Kafka Exporter → Prometheus → Grafana
KRaft Controller → Controller Exporter → Prometheus

优势: 生态成熟,告警完善,多集群支持
```

### 📌 版本提示
- 4.0 KIP-714 Broker 收集客户端指标补充 JMX；监控仍需第三方框架。
- CMAK 仍主流；KRaft 下需监控 controller 仲裁指标。

---

## 38 · 调优 Kafka，你做到了吗？

### 内容总结
调优目标=高吞吐+低延时。优化漏斗 4 层（效果自上而下衰减）：应用层（最明显）、框架层（参数配置）、JVM 层、操作系统层。OS 调优：禁 atime、XFS/ext4、swappiness 1-10、ulimit -n、vm.max_map_count、页缓存 ≥ 一个日志段（`log.segment.bytes` 默认 1GB）。

### 重点
- 优化漏斗（应用 > 框架 > JVM > OS），优先优化应用层。
- OS 调优项：noatime/XFS/swappiness/ulimit/max_map_count。
- 页缓存越大越好，至少容纳一个日志段（保证消费命中缓存避免物理 IO）。

### 详细解析与示例

#### 优化漏斗

```text
┌─────────────────────────┐
│  1.应用层(效果最明显)   │  ← 优先优化
├─────────────────────────┤
│  2.框架层(参数配置)     │  ← 参数调优
├─────────────────────────┤
│  3.JVM层(堆/GC)        │  ← 内存调优
├─────────────────────────┤
│  4.OS层(文件系统/IO)   │  ← 基础设施
└─────────────────────────┘
效果自上而下递减,但成本低也递减
```

#### 应用层优化

```text
1. 批量发送: linger.ms 调大,攒批减少请求数
2. 压缩: compression.type 按场景选
3. 分区: 合理分区数,避免单分区瓶颈
4. 客户端连接池: 复用连接,不频繁重建
5. 避免同步send: 异步send+回调
```

#### OS 调优清单

```bash
# 1. 禁atime(减少inode写)
echo 'noatime' >> /etc/fstab
# mount -o remount,noatime /kafka

# 2. XFS文件系统(优于ext4)
mkfs.xfs /dev/sdb1

# 3. swappiness 1-10
sudo sysctl vm.swappiness=1

# 4. 文件描述符
ulimit -n 1000000

# 5. 页缓存映射数(主题超多时)
echo 'vm.max_map_count=655360' >> /etc/sysctl.conf

# 6. 页缓存≥1个日志段
log.segment.bytes=1073741824  # 默认1GB
# 页缓存应≥此值,保证消费命中缓存
```

#### 页缓存原理

```text
写入: Producer → 页缓存 → 延迟刷盘(默认5s)
读取: Consumer → 先查页缓存 → 命中则零IO

页缓存越大 → 命中率越高 → 性能越好
页缓存 < 日志段 → 消费旧数据需读盘 → 性能骤降

建议: 页缓存 ≥ log.segment.bytes(1GB+)
```

### 📌 版本提示
- 4.0 Broker 需 Java 17（JVM 调优相应）；KRaft 下元数据内存占用变化。
- 调优原则不变；分层漏斗仍适用。

---

## 39 · 从 0 搭建基于 Kafka 的企业级实时日志流处理平台

### 内容总结
用 Kafka 一个框架（Connect+Core+Streams）替代 Flume+Kafka+Storm/Flink 栈。流程：Web 日志 → Kafka Connect(File Connector) → Kafka 主题 → Kafka Streams 实时分析 → 结果主题。案例：实时统计 Nginx access 日志 ios/android 请求数。

### 重点
- 单框架栈（Connect+Core+Streams）降复杂度与运维成本。
- Connect 收集、Streams 计算；File Connector 读日志。

### 详细解析与示例

#### 单框架栈 vs 多框架栈

```text
多框架栈(传统):
  Flume(采集) → Kafka(队列) → Storm/Flink(计算)
  ✗ 三个组件,运维复杂,数据格式不统一

单框架栈(Kafka全家桶):
  Kafka Connect(采集) → Kafka(队列) → Kafka Streams(计算)
  ✓ 一个框架,运维简单,数据格式统一
```

#### Kafka Connect File Connector 配置

```properties
# 读取Nginx access.log
name=file-source
connector.class=FileStreamSource
file=/var/log/nginx/access.log
topic=nginx-logs
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.storage.StringConverter
```

#### Kafka Streams 实时统计示例

```java
// 实时统计ios/android请求数
KStream<String,String> logs = builder.stream("nginx-logs");

KTable<String,Long> iosCount = logs
    .filter((k,v) -> v.contains("iPhone"))
    .groupBy((k,v) -> "ios")
    .count();

KTable<String,Long> androidCount = logs
    .filter((k,v) -> v.contains("Android"))
    .groupBy((k,v) -> "android")
    .count();

// 结果输出到主题
androidCount.toStream().to("android-count");
iosCount.toStream().to("ios-count");
```

### 📌 版本提示
- Kafka Connect 4.0 升级 Jakarta EE（KIP-1032）；Kafka Streams 4.0 增强（KIP-1104 外键提取等）。
- 单框架栈思路仍有效。

---

## 40 · Kafka Streams 与其他流处理平台的差异在哪里？

### 内容总结
流处理（无限数据集、低延时但曾不准） vs 批处理（有限数据集、准但延时高）。Lambda 架构=流+批结合。正确性是流取代批的障碍，基石=EOS。3 种一致性：at most/least/exactly once。副作用操作难 EOS。

### 重点
- 流 vs 批处理区别；Lambda 架构（流+批）。
- EOS 是正确性基石；3 种语义。
- 副作用操作难精确一次（如发邮件）。

### 详细解析与示例

#### 流处理 vs 批处理

| 维度 | 流处理 | 批处理 |
|------|--------|--------|
| 数据量 | 无限数据集 | 有限数据集 |
| 延时 | 低(实时) | 高(分钟~小时) |
| 正确性 | 曾不准(现在可EOS) | 准确 |
| 场景 | 实时风控/监控 | 报表/ETL |

#### Lambda 架构

```text
数据源
  ├─▶ 流处理层(实时) → 实时视图
  └─▶ 批处理层(离线) → 批视图
        ↓
    统一视图(批处理纠正流处理错误)

问题: 双链路,数据一致性难保证
趋势: 流处理EOS成熟后,单流处理链路取代Lambda
```

#### 3 种 EOS 语义

```text
at most once: 可能丢,不重复 (禁重试)
at least once: 不丢,可能重复 (默认,重试导致)
exactly once: 不丢不重复 (幂等+事务)

Kafka Streams实现EOS:
  ✓ 消费位移: 自动提交到__consumer_offsets
  ✓ 中间状态: 变更日志同步到状态存储
  ✓ 输出: 事务型Producer提交到输出主题
  ✗ 副作用: 发邮件/短信等无法事务回滚
```

#### 副作用处理策略

```text
发邮件/短信(不可回滚):
  方案1: at-least-once + 幂等发送(去重)
  方案2: 发邮件到中间主题,再由邮件服务消费
         Kafka保证主题EOS,邮件服务自己保证幂等
  方案3: 事务+outbox pattern
```

### 📌 版本提示
- Kafka Streams 实现 EOS（0.11 事务）；4.0 持续演进（KIP-1104/1112/1065/1076）。
- Kafka Streams vs Spark/Flink 定位仍为轻量库，适合中小企业。

---

## 41 · Kafka Streams DSL 开发实例

### 内容总结
DSL=领域特定语言，声明式函数式 API（类 SQL）。拓扑=有向无环图 DAG，节点=Processor（map/filter/join/aggregation）。两大 API：DSL（声明式）+ Processor API（命令式低阶）。有状态（连接/聚合/时间窗口需保存状态） vs 无状态（转换/过滤）。流表二元性、时间窗口。

### 重点
- DSL 声明式 vs Processor API 命令式；拓扑=DAG。
- 有状态 vs 无状态应用。
- 流表二元性（stream-table duality）、时间窗口。

### 详细解析与示例

#### DSL 拓扑图

```text
DSL拓扑 = 有向无环图(DAG)

Source Topic A ──▶ [map] ──▶ [filter] ──▶ [groupBy] ──▶ Sink Topic B
                (转换)     (过滤)      (聚合)          (输出)

两种API:
  DSL(声明式): map/filter/join/count ← 类SQL
  Processor API(命令式): 低阶,灵活但复杂
```

#### 有状态 vs 无状态应用

| 类型 | 示例 | 需状态存储 |
|------|------|----------|
| 无状态 | 转换/过滤/映射 | 否 |
| 有状态 | 连接/聚合/窗口 | 是(RocksDB) |

```text
无状态: map/filter/transform
  → 每条消息独立处理,无需记忆

有状态: join/aggregation/window
  → 需保存历史数据(如窗口内累计)
  → 状态存储: RocksDB(嵌入式)
```

#### 流表二元性

```text
流(Stream): 无限序列
表(Table): 当前状态快照

流⇔表 可互相转换:
  stream.toTable() → 查最新值
  table.toStream() → 发变更事件

DSL支持: KStream/KTable 互转
```

#### 时间窗口示例

```java
// 每分钟统计订单数
KTable<Windowed<String>, Long> windowed = ordersStream
    .windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
    .groupBy((k, v) -> v.get("userId"))
    .count();

// 滑动窗口
TimeWindows.of(Duration.ofMinutes(5))
    .slideBy(Duration.ofMinutes(1))  // 每1分钟滑动

// 会话窗口(自动检测活动间隙)
TimeWindows.ofTimeDifference(Duration.ofMinutes(5))
```

### 📌 版本提示
- 4.0 KIP-1104（外键从 key/value 提取，简化 join）、KIP-1112（自定义 Processor 包装 `ProcessorWrapper`）。
- DSL 仍持续丰富；KIP-1065（RETRY 错误处理）、KIP-1076（状态指标）。

---

## 42 · Kafka Streams 在金融领域的应用

### 内容总结
金融获客成本高，需 KYC + 用户画像（打标签）实现精准营销。ID 识别 5 种：身份证号/手机号/设备 ID(IDFA/IMEI)/应用注册账号/Cookie。ID Mapping 打通多端用户信息。Kafka Streams 实时构建用户画像。

### 重点
- 用户画像=打标签（KYC）。
- ID 识别 5 种；ID Mapping 跨端打通用户信息。
- Kafka Streams 实时画像构建。

### 详细解析与示例

#### 用户画像 vs KYC

```text
KYC(了解你的客户): 监管合规,识别客户身份
用户画像: 打标签,精准营销

流程: 识别 → 打标 → 存储 → 应用
  
Kafka Streams: 实时流式打标签
```

#### ID 识别 5 种

```text
1. 身份证号 (唯一,稳定)
2. 手机号 (唯一,可换)
3. 设备ID (IDFA/IMEI,多端)
4. 应用注册账号 (平台内唯一)
5. Cookie (会话级,临时)

问题: 同一用户多端登录,不同ID
  → 需ID Mapping打通
```

#### ID Mapping 流程

```text
用户设备A ──ID1──┐
用户设备B ──ID2──┤── ID Mapping ──▶ 统一用户UID
用户设备C ──ID3──┘

ID Mapping算法:
  1. 多源ID关联(共同出现/登录)
  2. 图算法(连通分量)
  3. 实时合并(同一会话的多ID)

Kafka Streams:
  源Topic(多ID) → Join(ID映射表) → 统一UID → 画像Topic
```

#### Kafka Streams 画像架构

```text
┌──────────┐   ┌────────────┐   ┌────────────┐
│ 行为数据  │──▶│ ID Mapping  │──▶│ 用户标签    │
│(多源)    │   │(Join表)    │   │(RocksDB)   │
└──────────┘   └────────────┘   └─────┬──────┘
                                       │
                                ┌─────▼──────┐
                                │ 画像输出    │
                                │ Topic       │
                                └────────────┘

特点: 实时性,低延时,可回溯
```

### 📌 版本提示
- 4.0 Kafka Streams 持续演进（外键 join KIP-1104 简化 ID Mapping）。
- 流处理画像思路仍有效；隐私法规（GDPR/个保法）下 ID 收集需合规调整。

---

## 加餐 · 搭建开发环境、阅读源码方法、经典学习资料大揭秘

### 内容总结
3 话题：搭建 Kafka 开发环境（Java+Gradle+Scala 插件+IDEA，`git clone` trunk）、阅读源码方法、学习资料。从 `kafka.log` 包开始读源码。

### 重点
- 开发环境搭建（Gradle Wrapper + Scala 插件）。
- trunk 分支贡献代码；从 `kafka.log` 包入手读源码。
- 学习资料汇总。

### 详细解析与示例

#### 开发环境搭建

```bash
# 1. 安装 JDK 17+ (4.0 Broker 要求)
java -version

# 2. 安装 Gradle (或用 Gradle Wrapper)
git clone https://github.com/apache/kafka.git
cd kafka

# 3. 构建项目
./gradlew compileKafka

# 4. IDEA 导入 Scala 插件 + Gradle 项目
# 5. 运行 Kafka:
./gradlew runKafka
```

#### 源码阅读路径

```text
推荐阅读顺序:
  1. kafka.log 包     ← 日志存储核心
     ├─ LogManager    (日志管理器)
     ├─ Log           (单分区日志)
     ├─ LogSegment    (日志段)
     └─ LazyAppendBuffer
  
  2. kafka.network 包 ← 网络层
     └─ SocketServer  (Reactor模式)

  3. kafka.cluster   ← 元数据
     └─ Partition      (分区)

  4. 4.0新重点: KafkaRaft / MetadataLog 相关类
```

#### 学习资料

```text
官方:
  - Kafka 官网文档 (kafka.apache.org)
  - KIP 提案 (kafka.apache.org/protocol#kips)
  - 源码 (github.com/apache/kafka)

书籍:
  - 《Kafka: The Definitive Guide》(2nd Edition)
  - 《企业级 Kafka 架构分析与实践》

社区:
  - Confluent 博客
  - Apache Kafka 邮件列表
```

### 📌 版本提示
- 4.0 源码持续演进；Gradle 仍为构建工具；Broker 需 Java 17。
- 从 `kafka.log` 包读源码的建议仍有效；KRaft 元数据相关代码是新重点。

---

## 期末测试 · 这些 Kafka 核心要点，你都掌握了吗？

### 内容总结
结课测试，20 选择题+5 简答题，覆盖 42 讲。简答要点：副本长时间不在 ISR 说明、acks 参数、重要组件、消费者组、为什么不读写分离。

### 重点
- ISR：副本长时间不在 ISR 说明 Follower 跟不上，查 Broker 连接与负载。
- `acks`=0/1/-1(all)，三档可靠性。
- 消费者组定义（一机制两模型）。
- 不读写分离：分区已负载均衡 + 消费位移复杂 + 性能权衡。

### 详细解析与示例

#### 简答要点速查

| 问题 | 答案要点 |
|------|----------|
| ISR 副本长时间不在 | Follower拉取跟不上Leader，查Broker负载/网络/磁盘IO |
| acks 参数 | 0=发后不管，1=Leader确认，-1(all)=所有ISR确认 |
| 重要组件 | Broker/Topic/Partition/Replica/Controller/Consumer Group |
| 消费者组 | 多实例消费一组Topic，每分区仅一个实例消费 |
| 不读写分离 | 分区已跨Broker分散，读负载均衡；消费位移复杂；Follower落后 |

#### ISR 排查示例

```bash
# 查分区ISR状态
bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic orders --partition 3

# 输出:
# Partition: 3 Leader: 1 Isr: 1,2,3
# 正常: Isr包含所有副本
# 异常: Isr减少 → Follower掉队
#
# 排查:
# 1. 查Follower Broker负载(top)
# 2. 查网络(ss -s)
# 3. 查磁盘IO(iostat)
# 4. 查副本拉取延迟(replica.lag.time.max.ms)
```

#### acks 三档可靠性

```text
acks=0: 发后不管
  → 最快，可能丢(Leader未写就应答)
  → 适合: 可丢消息(日志类)

acks=1: Leader确认
  → Leader写入即应答
  → 适合: 一般业务

acks=all(-1): 所有ISR确认
  → 最可靠，min.insync.replicas控制最低确认数
  → 适合: 重要业务(支付/订单)
```

### 📌 版本提示
- 测试要点 4.0 仍适用；`acks=all` 仍最高可靠性。
- 消费者组 KIP-848 增强；不读写分离设计仍不变。

---

## 用户故事 · 黄云：行百里者半九十

### 内容总结
后端工程师学习心得。大数据时代 Kafka 必备技能，公司用 Kafka 对接实时流量 20GB/天。痛点：只知用不知用好，资料重理论轻实践。专栏做到了「道」与「术」平衡。每天清晨 20 分钟学习，看评论二次梳理。

### 重点
- 理论与实践结合（道与术平衡）。
- 核心参数掌握（不必贪多求全，抓 3 个「最」）。
- 评论区二次梳理、今日疑今日解。

### 详细解析与示例

#### 学习心得总结

```text
黄云的三个核心实践:

1. 抓核心(不必贪多)
   3个「最」: 最重要参数、最常见问题、最核心概念
   例: 3个最重要参数 → 副本因子/留存/分区数

2. 道与术平衡
   道: 架构设计思想(为何用Kafka)
   术: 具体操作(如何部署/配置/排障)
   专栏特点: 既有原理又有实战

3. 学习节奏
   每天清晨20分钟(习惯养成)
   评论区二次梳理(消化理解)
   今日疑今日解(及时提问)
```

#### 20GB/天 场景参考

```text
公司场景: 实时流量20GB/天
  → Kafka削峰填谷 + 日志存储
  → 20GB/天 ÷ 7天留存 ≈ 140GB
  → 1KB消息 ≈ 2亿条/天
  → 平均TPS ≈ 2300/s (可轻松承载)
```

### 📌 版本提示
- 学习方法论不过时；4.0 参数更多，抓核心原则仍适用。

---

## 结束语 · 以梦为马，莫负韶华！

### 内容总结
感谢+新开始。「Stay focused and work hard」、10000 小时定律。学习大数据框架经验：精通 Java 是基石（语言规范/JVM 规范），基本功（OS/数据结构），持之以恒。

### 重点
- 坚持胜过速成（10000 小时定律）。
- 精通 Java+JVM 是基石；熟读语言规范。
- 基本功（OS/数据结构）重要。

### 详细解析与示例

#### 10000 小时定律

```text
精通 = 长期投入(10000小时)
  ≈ 每天2小时 × 5年
  
Kafka学习路径:
  入门(1-2周) → 熟练使用(1-3月)
  → 深入原理(3-6月) → 精通(1-2年)
```

#### 精通 Java 是基石

```text
Java 精通要点:
  1. 语言规范: 深入理解JLS(Java Language Specification)
     例: volatile/synchronized/ThreadLocal 底层机制
  2. JVM 规范: 理解JVM内存模型/GC/类加载
     例: Kafka调优依赖JVM堆/GC知识
  3. 并发编程: 线程池/锁/AQS
     例: Kafka Reactor模式依赖并发知识
```

#### 基本功重要

```text
OS: 文件系统/IO模型/内存管理
  例: Kafka顺序写/零拷贝/页缓存都依赖OS知识

数据结构: 哈希/树/堆
  例: Kafka分区hash/索引结构/状态存储

网络: TCP/拥塞控制/多路复用
  例: Kafka连接管理/请求处理
```

#### 学习建议

```text
Stay focused and work hard:
  1. 专注: 选定技术栈深入
  2. 坚持: 每天学习,积少成多
  3. 实战: 理论+实践结合
  4. 4.0新建议: 提升到Java 17(Kafka Broker要求)
```

### 📌 版本提示
- 学习方法论不过时；4.0 仍 JVM 系，Java 精通建议仍有效（且需提升到 Java 17）。

---

# 附录 · KRaft 模式详解（新增章节）

> 原专栏基于 Kafka 2.3，ZooKeeper 仍是元数据管理唯一方式；KRaft 是 3.x→4.0 最重要的架构演进，本章按相同格式补充。

## 一、背景：为什么需要 KRaft

### 内容总结
ZooKeeper 作为 Kafka 元数据存储的三大痛点：①元数据分散在 ZK，Broker 启动需从 ZK 拉取，集群规模大时启动慢；②ZK 单点写入（Controller 单点），元数据更新受限；③多一套 ZK 集群运维复杂。社区自 2.8 起预览 KRaft，3.3 GA，4.0 彻底移除 ZK。

### 重点
- ZK 三痛点：启动慢、Controller 单点写入、双集群运维复杂。
- KRaft 把元数据收回 Kafka 内部（内部主题 `__cluster_metadata`），用 Raft 共识替代 ZK。
- 4.0 是第一个完全无 ZK 的大版本。

### 详细解析与示例

#### ZK 三痛点

```text
痛点1: 启动慢
  ZK时代: Broker启动 → 从ZK拉取全量元数据
  大集群(1000+分区): 启动可达数十分钟

痛点2: Controller单点写入
  ZK时代: 只有Leader Controller写元数据
  → 元数据更新串行化,限制扩展

痛点3: 双集群运维
  ZK集群 + Kafka集群
  → 两套监控/部署/备份/容量规划
```

#### KRaft 解决方式

```text
KRaft = Kafka Raft Mode

元数据: Kafka内部主题 __cluster_metadata
  → append-only日志 + snapshot
  → 所有节点订阅同步

共识: Raft算法
  → Controller仲裁(quorum)选active
  → 奇数节点(3或5),多数派可写

优势: 一套系统解决所有问题
```

#### 演进时间线

```text
2020 2.8  预览(KIP-831/832)
2021 3.0  预览(生产不推荐)
2022 3.3  GA(生产可用) ← 里程碑
2023 3.4-3.9  迁移路径完善
2024 4.0  移除ZK(唯一模式) ← 里程碑
```

### 📌 版本提示
- 时间线：2.8 预览 → 3.0 预览 → **3.3 GA（生产可用）** → 3.4-3.9 完善 ZK→KRaft 迁移 → **4.0 移除 ZK**。

---

## 二、核心概念与架构

### 内容总结
KRaft = Kafka Raft Mode。元数据存于内部主题 `__cluster_metadata`（日志结构），由 Controller 仲裁（quorum）用 Raft 共识维护。进程角色 `process.roles`：`broker`（只做 Broker）、`controller`（只做 Controller）、`combined`（同进程兼任两者）。`node.id` 替代 `broker.id`；`controller.quorum.voters` 列出 Controller 节点；`controller.listener.names` 指定 Controller 监听器。

### 重点
- **Controller 仲裁**：奇数个 Controller 节点（3 或 5），Raft 共识选出 active controller（leader），其余为 voter。
- `process.roles` 三种模式：`broker`/`controller`/`combined`（combined 适合小集群，减节点数）。
- 元数据存 `__cluster_metadata` 内部主题（append-only 日志 + snapshot），所有 Controller/Broker 订阅同步。
- 配置关键项：`node.id`、`process.roles`、`controller.quorum.voters`、`controller.listener.names`、`listeners`（需区分 BROKER 和 CONTROLLER）。

### 详细解析与示例

#### 架构图

```text
┌─────────────────────────────────────────────────┐
│                KRaft 集群                        │
├─────────────────────────────────────────────────┤
│                                                 │
│  ┌──────────────────────────────────┐          │
│  │     Controller Quorum (Raft)     │          │
│  │  ┌─────────┐  ┌─────────┐       │          │
│  │  │C0(active)│  │C1(voter)│       │          │
│  │  └────┬────┘  └────┬────┘       │          │
│  │       │  Raft共识    │           │          │
│  │  ┌────▼────────────▼────┐       │          │
│  │  │ __cluster_metadata   │       │          │
│  │  │ (内部主题,日志格式)   │       │          │
│  │  └──────────────────────┘       │          │
│  └──────────────────────────────────┘          │
│                    │                             │
│            元数据同步(订阅)                      │
│                    │                             │
│  ┌─────────────────▼─────────────────┐          │
│  │     Broker 节点                   │          │
│  │  ┌──────┐  ┌──────┐  ┌──────┐    │          │
│  │  │B100  │  │B101  │  │B102  │    │          │
│  │  └──────┘  └──────┘  └──────┘    │          │
│  └───────────────────────────────────┘          │
└─────────────────────────────────────────────────┘
```

#### process.roles 三种模式

```text
broker:     只做Broker(生产数据节点)
controller: 只做Controller(元数据仲裁)
combined:   同进程兼任两者(小集群推荐)

示例:
  # 分离模式(生产推荐)
  Broker:    process.roles=broker
  Controller: process.roles=controller
  
  # 合并模式(小集群/测试)
  All nodes:  process.roles=broker,controller
```

#### 配置示例

```properties
# Broker节点
node.id=100
process.roles=broker
controller.quorum.voters=0@controller1:9093,1@controller2:9093,2@controller3:9093
listeners=PLAINTEXT://:9092
advertised.listeners=PLAINTEXT://broker1:9092

# Controller节点
node.id=0
process.roles=controller
controller.quorum.voters=0@controller1:9093,1@controller2:9093,2@controller3:9093
listeners=CONTROLLER://:9093
controller.listener.names=CONTROLLER
```

### 📌 版本提示
- `__cluster_metadata` 是正式主题名（开发期曾用 `__kafka_metadata`）。
- combined 模式（broker+controller 同进程）适合小集群；大规模生产建议分离 controller 节点。

---

## 三、与原专栏 ZK 机制的关键差异

### 内容总结
对照原专栏受影响章节，KRaft 的替代方式：Controller 选举（ZK 抢 `/controller` → Raft 仲裁选 active controller）；成员管理（ZK Watch+临时节点 → 元数据日志变更）；元数据传播（Controller 推送 → metadata log 同步）；位移/ACL/动态配置存储（ZK znode → KRaft metadata log）。

### 重点
| 原专栏机制（ZK 时代） | KRaft 时代 |
|---|---|
| Controller 选举：抢 `/controller` | Raft 仲裁选 active controller |
| Broker 存活：ZK 临时节点+Watch | metadata log 中 Broker 注册+心跳 |
| 元数据传播：Controller 推送 | metadata log 复制同步 |
| 位移 `__consumer_offsets` | 不变（仍是 Kafka 主题） |
| ACL `SimpleAclAuthorizer` 存 ZK | `StandardAuthorizer` 存 KRaft metadata |
| 动态配置存 ZK znode | 存 KRaft metadata log |
| `--zookeeper` 参数 | `--bootstrap-server` |

### 详细解析与示例

#### 受影响章节清单

```text
原专栏受影响章节:
  第07讲: 集群参数(zookeeper.connect → KRaft配置)
  第15讲: 消费者组(位移存储不变,底层协调者变化)
  第25讲: 重平衡(ZK协调 → KRaft协调)
  第26讲: Controller(最大变化:ZK选举→Raft仲裁)
  第29讲: 动态配置(ZK znode → KRaft metadata)
  第34讲: 授权(SimpleAclAuthorizer → StandardAuthorizer)
  第28讲: 主题管理(--zookeeper → --bootstrap-server)
```

#### 底层影响对比

```text
Controller选举:
  ZK: 各Broker抢 /controller 节点(临时节点)
  KRaft: Controller Quorum用Raft选active(日志共识)

元数据传播:
  ZK: Controller推送给其他Broker
  KRaft: metadata log复制到所有节点

动态配置:
  ZK: 存/kafka/config znode
  KRaft: 存__cluster_metadata主题

ACL:
  ZK: 存/kafka-acl znode
  KRaft: 存__cluster_metadata主题
```

#### 迁移影响评估

```text
代码层面: 客户端无感知(Kafka API不变)
运维层面: 移除ZK集群(简化部署)
配置层面: 新增KRaft参数,移除ZK参数
监控层面: 新增Controller仲裁监控
升级层面: 需迁移路径(3.4+)再升4.0
```

### 📌 版本提示
- 第 26 讲（Controller）受影响最大；第 07/29 讲（动态配置）、第 34 讲（授权）存储层全部从 ZK 迁到 KRaft metadata。
- `activeController` JMX 概念演化为 active controller（Raft leader）。

---

## 四、KRaft 的核心优势

### 内容总结
相比 ZK 模式的五大优势：①元数据传播更快（Raft 日志复制，毫秒级）；②支持更多分区（百万级，ZK 模式受限于元数据加载）；③无 ZK 单点与扩展瓶颈；④部署更简单（无需独立 ZK 集群）；⑤启动与故障恢复更快（元数据本地日志）。

### 重点
- 元数据传播毫秒级（ZK 秒级），Controller 切换更快。
- 支持百万级分区（ZK 模式大集群启动可达数十分钟）。
- 去除 ZK 外部依赖，部署/监控/运维统一到 Kafka 一套。
- 故障恢复快：active controller failover 由 Raft 仲裁快速选出新 leader。

### 详细解析与示例

#### 五大优势对比

| 维度 | ZK模式 | KRaft模式 |
|------|--------|----------|
| 元数据传播 | 秒级(Controller推送) | 毫秒级(Raft日志复制) |
| 分区上限 | 数万(元数据加载受限) | 百万级 |
| 扩展性 | ZK写入瓶颈 | 无单点,多Controller可写 |
| 部署 | 双集群(ZK+Kafka) | 单集群 |
| 启动/恢复 | 大集群数十分钟 | 秒级 |

#### 启动速度对比

```text
1000分区集群:
  ZK模式: 从ZK拉取全量元数据 → 数十分钟
  KRaft: 本地日志+snapshot → 秒级

10000分区集群:
  ZK模式: 可能超过5分钟
  KRaft: 仍为秒级
```

#### 故障恢复对比

```text
Controller故障:
  ZK: 新Broker抢ZK /controller节点
       → 需等待 + 元数据重新加载
  
  KRaft: Raft仲裁(3个节点)
       → 剩余2个节点多数派
       → 快速选出新active(秒级)

Broker故障:
  ZK: Controller通过ZK Watch感知
  KRaft: Controller通过心跳感知
       → 触发分区选举
```

#### 分区上限对比

```text
ZK模式: 元数据存ZK
  → Broker启动需加载全量元数据到内存
  → 大集群(10000+分区)启动极慢
  → 实际生产建议<5000分区

KRaft: 元数据存本地日志
  → 无需全量加载
  → 支持百万级分区
```

### 📌 版本提示
- 4.0 KIP-996 Pre-Vote 机制减少不必要的 KRaft leader 选举（网络分区场景）。
- 4.0 KIP-966 ELR（预览）保证当选 Leader 数据完整到 HW，与 `unclean=false` 互补。

---

## 五、部署与迁移

### 内容总结
全新部署：直接配 `process.roles`+KRaft 参数，无 ZK。迁移路径（3.4-3.9 支持）：ZK 模式集群 → `kafka-cluster` 工具迁移 → KRaft 模式，支持不停机/滚动迁移。4.0 后无新 ZK 迁移入口（必须先迁到 KRaft 再升级 4.0）。

### 重点
- 全新部署：`process.roles`、`node.id`、`controller.quorum.voters`、`controller.listener.names`，无 `zookeeper.connect`。
- 迁移用 `kafka-storage.sh` + `kafka-cluster.sh`（3.4+）。
- 4.0 升级前提：必须先从 ZK 迁到 KRaft（3.6+ 迁移路径成熟），再升 4.0。
- 客户端/服务端版本兼容：升 4.0 前确保 broker 与 client 均 ≥2.1（KIP-896 基线）。

### 详细解析与示例

#### 全新部署配置

```properties
# 最小化配置(单节点combined模式)
node.id=0
process.roles=broker,controller
controller.quorum.voters=0@localhost:9093
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
controller.listener.names=CONTROLLER
log.dirs=/var/lib/kafka/data

# 三节点生产配置(分离模式)
# Controller节点(3个,奇数):
node.id=0
process.roles=controller
controller.quorum.voters=0@controller1:9093,1@controller2:9093,2@controller3:9093
listeners=CONTROLLER://:9093

# Broker节点(多个):
node.id=100
process.roles=broker
controller.quorum.voters=0@controller1:9093,1@controller2:9093,2@controller3:9093
listeners=PLAINTEXT://:9092
```

#### 格式化与启动

```bash
# 1. 生成集群ID
CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)

# 2. 格式化存储
bin/kafka-storage.sh format -c config/kraft/server.properties \
  --cluster-id $CLUSTER_ID

# 3. 启动
bin/kafka-server-start.sh config/kraft/server.properties

# 4. 验证
bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092
bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 --describe
```

#### ZK→KRaft 迁移步骤

```bash
# 前提: 集群版本≥3.4 (推荐3.6+)

# 1. 准备KRaft Controller节点(新增)
# 2. 配置迁移参数(server.properties)
# 3. 使用kafka-cluster.sh迁移
bin/kafka-cluster.sh migrate-metadata-from-kafka-metadata-topic \
  --bootstrap-server old-broker:9092 \
  --controller-connection-url new-controller:9093

# 4. 验证迁移状态
bin/kafka-cluster.sh verify-migration-status \
  --bootstrap-server new-controller:9093

# 5. 切换ZK配置指向KRaft
# 6. 逐步移除ZK集群
```

#### 4.0 升级前提检查

```bash
# 1. 确保已迁KRaft(不能从ZK直跳4.0)
bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# 2. 检查客户端版本≥2.1
# 3. 备份配置
# 4. 升级Broker(滚动)
# 5. 验证
```

### 📌 版本提示
- 3.9 迁移路径完善；4.0 移除 ZK，**未迁的 ZK 集群无法直接升 4.0**。
- Confluent 提供自动化迁移工具包（商业）。

---

## 六、运维要点

### 内容总结
KRaft 运维关键：监控 active controller（JMX `kafka.controller:type=KafkaController,name=ActiveControllerCount`）、Controller 仲裁健康、metadata log 延迟（follower 落后）。工具：`kafka-metadata-quorum.sh` 查仲裁状态、`kafka-storage.sh` 格式化节点。日志框架 4.0 迁 Log4j2（KIP-653）。

### 重点
- 监控 active controller 唯一性（应为 1）。
- `kafka-metadata-quorum.sh --describe` 查仲裁状态、落后情况。
- metadata log 同 `__cluster_metadata` 主题，可查分区。
- 4.0 日志框架 Log4j→Log4j2，需转换配置（可用 log4j-transform-cli）。

### 详细解析与示例

#### 运维监控清单

```text
1. Controller仲裁健康
   - active controller唯一(应为1)
   - voter是否存活
   - 仲裁是否多数派

2. metadata log延迟
   - Follower落后多少
   - 快照是否定期生成

3. Broker健康
   - 分区分配均衡
   - ISR完整性

4. 客户端指标(4.0 KIP-714)
   - 从Broker收集客户端指标
```

#### 关键命令

```bash
# 查仲裁状态
bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 --describe

# 输出示例:
# ControllerEpoch: 5
# Voters: [0, 1, 2]
# Observers: []
# Leader: 0
# LastOffset: 12345

# 查节点信息
bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 --describe --controller-id 0

# 查__cluster_metadata主题
bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic __cluster_metadata

# 格式化新节点
bin/kafka-storage.sh format -c config/server.properties \
  --cluster-id <现有集群ID>
```

#### Log4j2 迁移

```bash
# 转换配置(可用工具)
java -jar log4j-transform-cli.jar \
  -s log4j.properties \
  -t log4j2.properties

# 或手动转换:
# Log4j:  log4j.appender.KAFKA=org.apache.log4j.RollingFileAppender
# Log4j2: <RollingFile name="KAFKA" ...>

# 4.0默认使用Log4j2
```

#### 4.0 新增监控指标

```text
KRaft Controller仲裁:
  - ActiveControllerCount (应为1)
  - ControllerEpoch
  - RaftFollowerLag

metadata log:
  - __cluster_metadata 分区Lag
  - Snapshot创建频率

KIP-714 客户端指标:
  - Broker端收集Producer/Consumer/Streams指标
```

### 📌 版本提示
- 4.0 新增 KRaft 仲裁相关指标；监控维度需新增 controller 仲裁健康检查。
- `kafka-groups.sh`（KIP-1043）统一管理 consumer/share group。

---

## 七、与 KIP-848 新消费者组协议的协同

### 内容总结
KRaft 与 KIP-848（新消费者组协议）是 4.0 两大协同升级。KIP-848 位移仍存 `__consumer_offsets`，但组管理与 rebalance 协议重写：增量协同 rebalance（消除 STW）、`group.protocol=consumer` 开启、服务端默认启用。与 KRaft 元数据机制互补。

### 重点
- KIP-848 是 4.0 GA，解决第 15/17/25 讲的 rebalance 痛点（STW、慢、全量重分配）。
- 与 KRaft 独立但协同：元数据走 KRaft，消费者组协议走 KIP-848。
- 客户端需主动开 `group.protocol=consumer`。

### 详细解析与示例

#### 新旧消费者组协议对比

| 维度 | 旧协议(classic) | 新协议(KIP-848) |
|------|----------------|-----------------|
| 重平衡 | 全量STW(stop-the-world) | 增量协同(不停消费) |
| 成员变更 | 所有成员重分配 | 仅变化部分重分配 |
| 速度 | 几百实例几小时 | 秒级 |
| 默认 | 3.x默认 | 4.0默认启用 |
| 开启 | 无需 | `group.protocol=consumer` |

#### 重平衡流程对比

```text
旧协议(全量STW):
  Consumer退出
    → 所有Consumer暂停消费(STW)
    → 重新分配所有分区
    → 所有Consumer恢复
  痛点: 全量重分配,停机时间长

新协议(增量协同):
  Consumer退出
    → 仅受影响分区暂停(非全量)
    → 仅重分配变化部分
    → 其他分区继续消费
  优势: 增量重分配,近无停机
```

#### 配置示例

```java
// 启用新消费者组协议
Properties props = new Properties();
props.put("group.id", "my-group");
props.put("group.protocol", "consumer");  // 4.0关键配置

// 旧协议(默认,3.x):
props.put("group.protocol", "classic");
```

#### KRaft + KIP-848 + KIP-932 三亮点

```text
KRaft:           元数据管理(ZK→Kafka内部)
KIP-848:         消费者组协议(增量协同重平衡)
KIP-932:         Queues for Kafka(share group,点对点队列语义)

三者协同:
  - KRaft提供稳定元数据基础
  - KIP-848提升消费可靠性
  - KIP-932扩展消费模型(队列语义)
```

### 📌 版本提示
- KIP-848 + KRaft + Queues for Kafka（KIP-932 早期访问）构成 4.0 三大亮点。
- share group（KIP-932）引入点对点队列语义，扩展消费模型。

---

## 八、学习与上手建议

### 内容总结
上手 KRaft：本地 3 节点 combined 模式快速体验；生产建议分离 controller 节点。学习资料：Kafka 官网 KRaft 文档、KIP-832（KRaft）、KIP-595 等。迁移前充分测试。源码重点：`kafka.server` 包 controller 相关、metadata log 相关类。

### 重点
- 本地 3 节点 combined 模式快速起步。
- 生产分离 controller；奇数个 controller 节点。
- 迁移前在测试环境完整演练。
- 源码重点：KRaft metadata log、controller quorum 相关类。

### 详细解析与示例

#### 快速上手步骤

```bash
# 1. 下载4.0
wget https://downloads.apache.org/kafka/4.0.0/kafka_2.13-4.0.0.tgz
tar -xzf kafka_2.13-4.0.0.tgz
cd kafka_2.13-4.0.0

# 2. 启动KRaft(单节点)
CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
bin/kafka-storage.sh format -c config/kraft/server.properties --cluster-id $CLUSTER_ID
bin/kafka-server-start.sh config/kraft/server.properties

# 3. 创建主题
bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic test --partitions 3 --replication-factor 1

# 4. 验证
bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 --describe
```

#### 生产部署建议

```text
最小配置(小集群):
  3节点combined模式
  node.id=0,1,2
  process.roles=broker,controller
  controller.quorum.voters=0@n1:9093,1@n2:9093,2@n3:9093

推荐配置(生产):
  3个Controller节点(分离)
  N个Broker节点
  controller.quorum.voters=0@c1:9093,1@c2:9093,2@c3:9093
```

#### 源码阅读重点

```text
KRaft相关包:
  org.apache.kafka.server.fault     ← 容错
  org.apache.kafka.server.rack      ← 机架感知
  org.apache.kafka.server.raft      ← Raft实现
    ├─ KafkaRaftClient              (Raft客户端)
    ├─ RaftClient                   (Raft接口)
    └─ metadata                     (元数据管理)
      ├─ MetadataLog                (元数据日志)
      ├─ SnapshotGenerator          (快照生成)
      └─ snapshot                   (快照处理)

org.apache.kafka.coordinator     ← 消费者组(新协议)
  └─ group                          (组管理)
```

#### 学习资料

```text
官方:
  - KRaft文档: kafka.apache.org/documentation/kraft
  - KIP-831/832 (KRaft设计)
  - KIP-848 (新消费者组)
  - KIP-932 (Queues for Kafka)

迁移:
  - 3.4+迁移指南
  - Confluent迁移工具包(商业)
```

### 📌 版本提示
- KIP-832（KRaft）、KIP-595 是核心提案；4.0 Release Notes 是最权威变更来源。
- 原《Apache Kafka 实战》书（基于 1.0）与专栏（2.3）的 ZK 内容现属历史背景，实操以 4.0 KRaft 为准。

