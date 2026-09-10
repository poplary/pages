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

### 📌 版本提示
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

### 📌 版本提示
- KIP-832（KRaft）、KIP-595 是核心提案；4.0 Release Notes 是最权威变更来源。
- 原《Apache Kafka 实战》书（基于 1.0）与专栏（2.3）的 ZK 内容现属历史背景，实操以 4.0 KRaft 为准。

