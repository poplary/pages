---
title: Kafka Streams 流处理
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


---

## 附录 · Kafka Streams 内核深入解析

> 本节从源码层面深入流处理机制，补充第 39-42 讲的讲解。

### 深入解析：Streams 架构

```text
Kafka Streams 架构:

┌─────────────────────────────────────────────────┐
│              应用进程 (同进程, 无独立集群)          │
│  ┌──────────────────────────────────────────┐    │
│  │  StreamThread (多个, 默认1)               │    │
│  │  ┌─────────────┐  ┌─────────────┐        │    │
│  │  │ StreamTask │  │ StreamTask │  ...    │    │
│  │  │ (1个分区对)  │  │ (1个分区对)  │        │    │
│  │  └──────┬──────┘  └──────┬──────┘        │    │
│  │         │                │                │    │
│  │  ┌──────▼────────────────▼──────┐        │    │
│  │  │     状态存储 (RocksDB)         │        │    │
│  │  │  ┌────────┐  ┌────────┐      │        │    │
│  │  │  │ Store   │  │ Store   │ ... │        │    │
│  │  │  └────────┘  └────────┘      │        │    │
│  │  └──────────────────────────────┘        │    │
│  └──────────────────────────────────────────┘    │
│                       │                          │
│              变更日志主题(changelog)               │
│              (容错, 恢复状态)                      │
└─────────────────────────────────────────────────┘
            │ 消费/生产
            ▼
        Kafka 集群

核心组件:
1. StreamThread: 执行线程, 每个线程处理多个Task
2. StreamTask: 任务单元, 一个输入分区一个Task
3. StateStore: 状态存储(RocksDB), 有状态操作用
4. Changelog: 变更日志主题, 用于状态容错恢复
```

### 深入解析：状态存储与容错

```text
状态存储 (StateStore):

无状态操作: map/filter/branch
  → 不需状态存储, 每条消息独立处理

有状态操作: count/aggregate/join/window
  → 需保存历史数据(如窗口内累计)
  → 状态存储: RocksDB (嵌入式, 本地磁盘)

RocksDB 优势:
  - 嵌入式(无独立进程)
  - 写入快(LSM树)
  - 内存+磁盘混合(热数据在内存)

容错机制:
  每个状态存储对应一个 changelog 主题:
  → 每次状态变更 → 写入 changelog
  → Task故障 → 从changelog重放 → 恢复状态

恢复流程:
┌─────────────────────────────────────────┐
│ Task 故障 (如消费者实例宕机)             │
│   → rebalance, Task迁移到其他实例        │
│   → 新实例创建空StateStore               │
│   → 从changelog开头重放                  │
│   → 重建RocksDB状态                      │
│   → 恢复完成, 开始消费                   │
└─────────────────────────────────────────┘

优化: 周期性创建changelog快照
  → 恢复时从快照+增量日志, 加速恢复
```

### 深入解析：DSL 与 Processor API

```text
两种API对比:

DSL (声明式, 类SQL):
  优点: 简洁, 快速开发, 自带优化
  操作: stream/table, map/filter, groupBy/count,
        join, window, toStream/toTable

Processor API (命令式, 低阶):
  优点: 灵活, 可完全自定义处理逻辑
  操作: 实现Processor接口, 手动管理状态存储

DSL 示例:
  builder.stream("input")
    .filter((k,v) -> v.length() > 0)
    .mapValues(v -> v.toUpperCase())
    .groupBy((k,v) -> v)
    .count()
    .toStream()
    .to("output");

Processor API 示例:
  Topology topology = new Topology();
  topology.addSource("source", "input-topic");
  topology.addProcessor("process", () -> new MyProcessor(), "source");
  topology.addSink("sink", "output-topic", "process");
```

### 深入解析：窗口机制

```text
4种窗口类型:

1. 翻转窗口 (Tumbling Window):
   固定大小, 不重叠
   ┌────┬────┬────┐
   │ W1 │ W2 │ W3 │
   └────┴────┴────┘
   t=0  t=5  t=10 t=15
   → 每条消息只属于1个窗口

2. 跳跃窗口 (Hopping Window):
   固定大小, 可重叠
   ┌────┐
   │ W1 ├────┐
   │    │ W2 ├────┐
   └────┴────┴ W3 │
   → 每条消息可属于多个窗口

3. 滑动窗口 (Sliding Window):
   按时间差滑动, 用于join
   → 适合流-流join (join窗口内匹配)

4. 会话窗口 (Session Window):
   按活动间隙自动切分
   ┌───┐     ┌──────┐  ┌──┐
   │S1 │     │  S2  │  │S3│
   └───┘     └──────┘  └──┘
   gap=5min, 间隙>5min切分新会话
   → 适合用户会话分析

代码示例:
  // 1分钟翻转窗口统计
  KTable<Windowed<String>, Long> counts = stream
    .groupBy((k, v) -> v.getKey())
    .windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
    .count();

  // 会话窗口(5分钟gap)
  KTable<Windowed<String>, Long> sessions = stream
    .groupBy((k, v) -> v.getUserId())
    .windowedBy(SessionWindows.with(Duration.ofMinutes(5)))
    .count();
```

### 深入解析：流表二元性

```text
流(Stream) ⇄ 表(Table) 可互相转换:

流 → 表: 聚合(accumulate)
  stream.groupBy().count() → KTable
  (每条消息更新表状态)

表 → 流: 变更流(changelog)
  table.toStream() → KStream
  (表的每次变更产生一条消息)

DSL双视图:
  KStream: 事实流(每条都是独立事件)
    → map/filter/join其他stream
  
  KTable: 状态表(保留最新值)
    → aggregate/window/join其他table

应用:
  实时PV: stream → groupBy → count(KTable)
  实时UV: stream → groupBy → count(KTable) + 去重
  用户画像: stream → join(KTable用户属性) → 输出
```

---

## 面试题精选

**Q1: Kafka Streams 和 Flink/Spark Streaming 有什么区别？**

A: ①形态：Streams 是轻量客户端库（与应用同进程，无独立集群），Flink/Spark 是完整集群系统；②部署：Streams 零运维（集成进应用），Flink/Spark 需独立集群；③EOS：Streams 原生支持（基于 Kafka 事务），Flink 需借助 Kafka 事务；④适合：Streams 适合中小流量+简单拓扑，Flink 适合大规模+复杂窗口/状态计算。

**Q2: Kafka Streams 的状态存储是什么？如何容错？**

A: 有状态操作（count/aggregate/join）用 RocksDB 作为本地状态存储。容错机制：每次状态变更写入 changelog 主题（内部 topic），Task 故障后从 changelog 重放恢复状态。4.0+ 支持周期性快照加速恢复。

**Q3: DSL 和 Processor API 有什么区别？怎么选？**

A: DSL 是声明式 API（map/filter/groupBy），简洁且自带优化，适合大多数场景。Processor API 是命令式低阶 API（实现 Processor 接口），灵活可自定义处理逻辑和状态管理，适合复杂自定义拓扑。建议先用 DSL，需要细粒度控制时用 Processor API。

**Q4: Kafka Streams 如何实现 Exactly Once？**

A: 基于 Kafka 事务：消费位移提交到 `__consumer_offsets`、中间状态变更写 changelog、输出到目标 topic，三者通过事务 Producer 提交，要么全成功要么全回滚。副作用操作（如写外部 DB）需自行幂等。

**Q5: 翻转窗口和会话窗口有什么区别？**

A: 翻转窗口（Tumbling）固定大小不重叠，每条消息属于一个窗口，适合固定周期统计。会话窗口（Session）按活动间隙自动切分，无活动时关闭，适合用户会话分析（如用户操作间隔超过 5 分钟则切分新会话）。