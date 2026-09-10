---
title: Kafka KRaft 与新版特性
---

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
|------|------|
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

---

## 九、KRaft 发展流程与核心原理（详细版）

> 本节补充 KRaft 从提出到 4.0 落地的完整发展历程与核心设计原理。

### 内容总结

KRaft 经历了 5 年演进：2020 年 KIP-831/832 提出设计（预览），2021 年 3.0 继续预览，2022 年 3.3 GA 达到生产可用，2023 年 3.4-3.9 完善迁移工具链，2024 年 4.0 彻底移除 ZooKeeper。核心设计原理包括：元数据日志（Raft 共识的 append-only 日志）、快照机制（加速恢复）、仲裁 quorum（奇数 Controller 节点）、元数据传播（所有节点订阅同步）。KRaft 用 Kafka 自身的复制机制管理元数据，实现了"用 Kafka 管理 Kafka"的自举设计。

### 重点

- 5 年演进：预览(2.8) → GA(3.3) → 迁移完善(3.4-3.9) → 移除 ZK(4.0)。
- 核心原理：Raft 共识 + 元数据日志 + 快照 + 仲裁 quorum。
- 自举设计：用 Kafka 主题管理 Kafka 元数据。
- 关键 KIP：831/832（基础设计）、595（元数据传播）、890（事务防御）、996（Pre-Vote）、1043（统一组管理）。
- 迁移路径：3.4+ 支持 `kafka-cluster.sh` 工具从 ZK 迁移到 KRaft。

### 详细解析与示例

#### KRaft 五年演进时间线

```text
┌─────────────────────────────────────────────────────────────────┐
│                    KRaft 演进时间线                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  2020.06  Kafka 2.8  预览(KIP-831/832)                          │
│  │       ├─ 核心设计完成                                         │
│  │       ├─ process.roles 引入                                   │
│  │       ├─ 内部主题 __cluster_metadata 定义                     │
│  │       └─ 生产不推荐，仅供测试                                 │
│  │                                                               │
│  2021.08  Kafka 3.0  预览继续                                    │
│  │       ├─ 功能增强，但仍有已知问题                             │
│  │       ├─ 部分特性仍不稳定                                     │
│  │       └─ 建议生产环境使用 ZK 模式                             │
│  │                                                               │
│  2022.10  Kafka 3.3  ★ GA 生产可用 ★                            │
│  │       ├─ 完整功能集，生产环境可用                             │
│  │       ├─ ZK 弃用标记（但仍支持）                             │
│  │       ├─ 迁移工具初步可用                                     │
│  │       └─ 里程碑：KRaft 正式进入生产时代                       │
│  │                                                               │
│  2023  3.4-3.6  迁移路径成熟                                     │
│  │       ├─ 3.4: kafka-cluster.sh 迁移工具                      │
│  │       ├─ 3.5: 分层存储(Tiered Storage)预览                   │
│  │       ├─ 3.6: 迁移路径完善，支持不停机滚动迁移               │
│  │       └─ Confluent 提供商业迁移工具包                         │
│  │                                                               │
│  2024.11  Kafka 3.9  ZK 迁移最后支持版本                         │
│  │       ├─ ZK→KRaft 迁移路径最完善                             │
│  │       ├─ KIP-996 Pre-Vote 减少不必要选举                     │
│  │       └─ 最后一个支持 ZK 的版本                               │
│  │                                                               │
│  2024.12  Kafka 4.0  ★ 移除 ZooKeeper ★                         │
│          ├─ ZK 代码彻底删除                                     │
│          ├─ KRaft 为唯一元数据模式                               │
│          ├─ KIP-848 新消费者组协议 GA                           │
│          ├─ KIP-932 Queues for Kafka 早期访问                  │
│          ├─ Broker 最低 Java 17                                  │
│          └─ 里程碑：Kafka 自举完成                               │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

#### 关键 KIP 提案一览

| KIP | 标题 | 版本 | 内容 |
|-----|------|------|------|
| 831 | KRaft 核心设计 | 2.8 | 元数据日志、Raft 共识、仲裁 |
| 832 | KRaft 元数据格式 | 2.8 | 元数据记录类型、序列化 |
| 595 | 元数据传播 | 3.0 | Broker 订阅元数据日志 |
| 848 | 新消费者组协议 | 4.0 | 服务端驱动增量重平衡 |
| 890 | 事务防御 | 3.4 | 事务状态防御机制 |
| 932 | Queues for Kafka | 4.0 | share group 队列语义 |
| 966 | ELR | 4.0 | eligible leader replicas 预览 |
| 996 | Pre-Vote | 4.0 | 减少不必要的 Raft 选举 |
| 1043 | 统一组管理 | 4.0 | kafka-groups.sh 管理所有组 |
| 714 | 客户端指标 | 4.0 | Broker 收集客户端指标 |

#### 核心原理详解

##### 1. 元数据日志（Metadata Log）

```text
传统 ZK 模式:
  元数据存 ZK znode
  → 非日志格式，不可回放
  → 快照需要全量导出
  → Broker 启动需从 ZK 拉取全量

KRaft 模式:
  元数据存 __cluster_metadata 主题
  → append-only 日志格式
  → 支持快照 + 增量恢复
  → 所有节点订阅同步，无需拉取

元数据记录类型(部分):
  RegisterBrokerRecord      ← Broker 注册
  UnregisterBrokerRecord    ← Broker 注销
  TopicRecord              ← Topic 创建
  DeleteTopicRecord        ← Topic 删除
  PartitionRecord          ← 分区配置
  AccessControlRecord      ← ACL
  ConfigurationRecord      ← 动态配置
  ConsumerGroupRecord      ← 消费者组元数据

每条记录:
  ┌─────────────────────────────────────┐
  │ RecordHeader (key, header fields)   │
  │ RecordBody (具体记录内容)            │
  │ CRC (校验)                           │
  └─────────────────────────────────────┘
```

##### 2. Raft 共识与仲裁 Quorum

```text
Controller Quorum:
  奇数个 Controller 节点(3 或 5)
  → 多数派(≥N/2+1) 可写
  → 少数派可服务读

Raft 选举流程:
  1. 各 Controller 启动，处于 Follower 状态
  2. 随机超时后变为 Candidate
  3. 向其他 Controller 发起投票请求
  4. 获得多数票 → 当选 Leader (active controller)
  5. Leader 开始接收日志条目，复制到 Follower
  6. 复制成功 → 提交日志

KIP-996 Pre-Vote:
  问题: 网络分区时，少数派选出一个新 Leader
       → 网络恢复后与多数派冲突，触发不必要选举
  解决: Pre-Vote 阶段先预检是否能获得多数票
       → 不能则不发起正式选举
  → 减少网络分区场景下的不必要选举

故障场景:
  3个 Controller: C0(active), C1, C2
  C0 宕机 → C1/C2 多数派选出新 Leader
  C0 恢复 → 发现新 Leader 存在，降级为 Follower
  → 快速恢复，无数据丢失
```

##### 3. 快照机制（Snapshot）

```text
元数据日志增长问题:
  __cluster_metadata 持续增长
  → 新 Broker 加入需回放全部日志 → 慢

快照机制:
  定期生成元数据快照
  → 新节点从最近快照 + 增量日志恢复
  → 大幅加速启动

快照流程:
  1. 达到触发条件(时间间隔/日志条数)
  2. 暂停日志追加
  3. 导出当前元数据状态为快照文件
  4. 记录快照对应的日志 offset
  5. 恢复日志追加

恢复流程:
  1. 加载最近快照
  2. 从快照 offset 后回放增量日志
  3. 应用到内存元数据结构
  4. 完成恢复，开始服务

配置:
  metadata.log.retention.ms=604800000  (默认7天)
  snapshot.auto.period=3600000           (默认1小时)
```

##### 4. 元数据传播机制

```text
ZK 模式传播:
  Controller(Leader) 修改元数据
  → 写入 ZK
  → 其他 Broker 通过 Watch 感知
  → 从 ZK 读取新元数据
  → 延迟: 秒级(Watch 通知 + 读取)

KRaft 模式传播:
  Controller(active) 写入元数据日志
  → Raft 复制到所有 Controller
  → 所有节点订阅 __cluster_metadata 主题
  → 本地消费日志，更新元数据缓存
  → 延迟: 毫秒级(日志复制)

传播架构:
  ┌──────────────────────────────────┐
  │  Active Controller               │
  │  ┌────────────────────────────┐  │
  │  │ 写入元数据日志(本地)       │  │
  │  └──────────┬─────────────────┘  │
  │             │ Raft 复制          │
  │  ┌──────────▼─────────────────┐  │
  │  │ __cluster_metadata         │  │
  │  │ (内部主题)                  │  │
  │  └──┬──────┬──────┬───────────┘  │
  │     │      │      │              │
  │  订阅  订阅  订阅                         │
  │     │      │      │              │
  │  ┌──▼──┐┌──▼──┐┌──▼──┐           │
  │  │C1   ││C2   ││B100 │           │
  │  │(voter)││(voter)││(broker)│    │
  │  └─────┘└─────┘└─────┘           │
  └──────────────────────────────────┘
```

#### 迁移策略详解

##### 从 ZK 迁移到 KRaft（3.4+）

```text
前提条件:
  - 当前集群版本 ≥ 3.4（推荐 3.6+）
  - 已有稳定的 ZK 模式集群
  - 计划新增 Controller 节点

迁移步骤:

步骤1: 准备 KRaft Controller 节点
  ├─ 新增 3 个 Controller 节点(或复用 Broker)
  ├─ 配置 process.roles=controller
  ├─ 配置 controller.quorum.voters
  └─ 格式化存储: kafka-storage.sh format

步骤2: 配置迁移参数
  ├─ 在现有 Broker 的 server.properties 中添加:
  │   process.roles=broker,controller  (过渡期)
  │   controller.quorum.voters=0@controller1:9093,...
  ├─ 配置迁移开关
  └─ 重启 Broker（滚动）

步骤3: 执行元数据迁移
  ├─ 使用 kafka-cluster.sh 迁移工具
  ├─ 从 ZK 导出元数据到 __cluster_metadata
  └─ 验证迁移状态: verify-migration-status

步骤4: 切换元数据源
  ├─ 配置指向 KRaft Controller
  ├─ 逐步移除 ZK 依赖
  └─ 监控迁移进度

步骤5: 清理 ZK
  ├─ 确认所有 Broker 使用 KRaft
  ├─ 停止 ZK 集群
  └─ 清理 ZK 数据

命令示例:
  # 迁移元数据
  bin/kafka-cluster.sh migrate-metadata-from-kafka-metadata-topic \
    --bootstrap-server old-broker:9092 \
    --controller-connection-url new-controller:9093

  # 验证迁移
  bin/kafka-cluster.sh verify-migration-status \
    --bootstrap-server new-controller:9093

  # 检查状态
  bin/kafka-metadata-quorum.sh \
    --bootstrap-server new-controller:9093 --describe
```

##### 全新部署 KRaft 集群

```properties
# server.properties (全新 KRaft 部署)

# 节点标识
node.id=100

# 进程角色
process.roles=broker

# Controller 仲裁配置
controller.quorum.voters=0@controller1:9093,1@controller2:9093,2@controller3:9093

# 监听器
listeners=PLAINTEXT://:9092
advertised.listeners=PLAINTEXT://broker1:9092

# 日志目录
log.dirs=/var/lib/kafka/data

# Controller 节点配置
node.id=0
process.roles=controller
controller.quorum.voters=0@controller1:9093,1@controller2:9093,2@controller3:9093
listeners=CONTROLLER://:9093
controller.listener.names=CONTROLLER

# 格式化与启动
# 1. 生成集群 ID
CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)

# 2. 格式化存储
bin/kafka-storage.sh format -c server.properties --cluster-id $CLUSTER_ID

# 3. 启动 Controller 节点
bin/kafka-server-start.sh config/controller1.properties
bin/kafka-server-start.sh config/controller2.properties
bin/kafka-server-start.sh config/controller3.properties

# 4. 启动 Broker 节点
bin/kafka-server-start.sh config/broker1.properties
```

#### KRaft vs ZK 性能对比

```text
场景1: 集群启动
  ZK 模式(1000分区): 从ZK拉取全量元数据 → 5-10分钟
  KRaft模式(1000分区): 本地快照+增量日志 → 10-30秒
  KRaft模式(10000分区): 本地快照+增量日志 → 30-60秒
  → 提升: 10-100倍

场景2: 元数据更新
  ZK 模式: Controller写ZK → Watch通知 → Broker读取 → 秒级
  KRaft模式: Raft日志复制 → 毫秒级
  → 提升: 100倍

场景3: Controller 故障恢复
  ZK 模式: 新Broker抢ZK节点 → 加载元数据 → 30秒-5分钟
  KRaft模式: Raft仲裁选新Leader → 继续服务 → 1-3秒
  → 提升: 10-100倍

场景4: 扩展性
  ZK 模式: ZK写入瓶颈，Controller单点 → 数万分区上限
  KRaft模式: 多Controller仲裁，无单点 → 百万级分区
  → 提升: 100倍

场景5: 运维复杂度
  ZK 模式: 2套系统(ZK+Kafka)，双监控双备份
  KRaft模式: 1套系统，统一运维
  → 简化: 50%运维成本
```

### 📌 版本提示
- KRaft 从 2.8 预览到 4.0 移除 ZK，历时 5 年，经过充分验证。
- 关键里程碑：3.3 GA（生产可用）、3.6（迁移成熟）、4.0（ZK 移除）。
- 4.0 不支持从 ZK 直接升级，必须先迁移到 KRaft 再升 4.0。
- KRaft 与 KIP-848、KIP-932 共同构成 4.0 的三大架构亮点。