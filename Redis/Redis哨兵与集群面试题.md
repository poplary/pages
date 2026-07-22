# Redis 哨兵与集群深度剖析

> 面向高级开发者的 Redis 高可用架构源码分析

---

## 一、主从复制

### 1.1 复制积压缓冲区

```c
// replication.c
// 复制积压缓冲区是一个环形缓冲区
// 默认大小：repl-backlog-size（默认 1MB）

// 结构
struct replBacklog {
    char *buf;           // 环形缓冲区
    long long offset;    // 缓冲区对应的偏移量
    long long histlen;   // 历史数据长度
    int refcount;        // 引用计数
};

// 部分重同步流程
// 从库发送 PSYNC {master_replid} {offset}
// 主库检查：
// 1. replid 是否匹配
// 2. offset 是否在积压缓冲区范围内
// 匹配 → 增量同步
// 不匹配 → 全量同步
```

### 1.2 全量同步流程

```c
// 全量同步步骤
1. 主库执行 BGSAVE 生成 RDB
2. 发送 RDB 给从库
3. 从库加载 RDB
4. 主库将 BGSAVE 期间的写命令发送给从库
5. 从库执行这些命令

// 优化
// 无盘复制（Diskless replication）：主库直接通过网络发送 RDB
// 适用：磁盘慢但网络快的场景
// repl-diskless-sync yes
```

---

## 二、哨兵模式

### 2.1 哨兵数据结构

```c
// sentinel.c
typedef struct sentinelRedisInstance {
    int flags;              // 类型和状态
    char *name;             // 实例名称
    char *runid;            // 运行 ID
    uint64_t config_epoch;  // 配置纪元
    sentinelAddr *addr;     // 地址
    mstime_t last_pub_time; // 最后发布消息时间
    mstime_t last_hello_time; // 最后收到 hello 时间
    mstime_t last_master_down_reply_time; // 最后回复时间
    mstime_t s_down_since_time; // 主观下线时间
    mstime_t o_down_since_time; // 客观下线时间
    dict *sentinels;        // 监视同一个 master 的哨兵
    dict *slaves;           // 从库
} sentinelRedisInstance;
```

### 2.2 故障转移流程

```c
// 选主算法
1. 从 replicas 中排除：
   - 断线超过 down-after-milliseconds 的
   - 最近 10 秒内没有回复 INFO 的
   - 与 master 断开时间超过 down-after-milliseconds * 10 的
2. 按优先级排序：
   - slave-priority（越小越优先）
   - 复制偏移量（越大越优先）
   - runid（越小越优先）
3. 选择最优先的从库提升为主库
```

---

## 三、Cluster 模式

### 3.1 槽位分配算法

```c
// 槽位分配
// 16384 个槽平均分配到所有主节点
// 每个主节点负责的槽位连续

// 槽位迁移的最小单位
// 不能只迁移单个槽
// 也不能批量迁移，每次迁移一个槽
// 一个槽的迁移涉及该槽中的所有 key
```

### 3.2 集群通信

```c
// 集群总线
// 每个节点额外开启一个端口（默认 10000+port）
// 用于集群节点间通信

// 消息类型
// PING, PONG, MEET, FAIL, PUBLISH, 
// UPDATE, MIGRATE, REQUEST, REPLY

// Gossip 更新
// 每个节点维护一个集群状态
// 周期性发送 PING 到随机节点
// 接收 PONG 更新状态
```

---

> **总结**：Redis 的高可用方案从主从复制到哨兵再到集群，层层递进。主从复制实现数据备份，哨兵实现自动故障转移，集群实现数据分片和水平扩展。理解这些方案的工作原理和适用场景，是设计高可用 Redis 架构的关键。