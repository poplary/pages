# Redis 高级特性与集群模式深度剖析

> 面向高级开发者的 Redis 事务、Lua、哨兵、集群源码分析

---

## 一、事务实现

### 1.1 MULTI/EXEC 实现

```c
// multi.c
// Redis 事务的实现

// 事务状态
typedef struct multiState {
    multiCmd *commands;    // 命令队列
    int count;             // 命令数
    int cmd_flags;         // 命令标志
    int minreplicas;        // 最小从库数
    time_t minreplicas_timeout; // 超时时间
} multiState;

typedef struct multiCmd {
    robj **argv;           // 参数
    int argc;              // 参数数
    struct redisCommand *cmd; // 命令
} multiCmd;
```

**MULTI 执行流程**：
```c
1. MULTI 命令 → 设置 REDIS_MULTI 标志
2. 后续命令不入队的情况：
   - WATCH, MULTI, EXEC, DISCARD 直接执行
   - 其他命令入队到 commands 数组
3. 语法错误 → 入队时返回 QUEUED，但记录错误
4. 运行时错误 → 执行时返回错误，不影响其他命令
5. EXEC → 依次执行队列中的命令
6. DISCARD → 清空命令队列，释放 REDIS_MULTI 标志
```

### 1.2 WATCH 实现

```c
// WATCH 使用乐观锁
// 原理：记录被监视 key 的修改次数

// 每个 key 维护一个 64 位计数器
// 每次修改 key 时，计数器递增
// EXEC 前检查计数器是否变化
// 变化则事务失败

// 实现细节
// 1. WATCH 将 key 注册到客户端的 watched_keys 列表
// 2. 每个 db 维护一个 watched_keys 字典
// 3. 修改 key 时，通知所有监视该 key 的客户端
// 4. 被通知的客户端设置 CLIENT_DIRTY_CAS 标志
// 5. EXEC 时检查该标志，标志存在则事务失败
```

---

## 二、Lua 脚本执行

### 2.1 Lua 环境

```c
// scripting.c
// Redis 创建独立的 Lua 环境
// 初始化时注册 Redis 的 API

// Lua 沙箱
// 1. 限制执行时间（lua-time-limit，默认 5 秒）
// 2. 禁用危险函数（dofile, loadfile, require, os.execute 等）
// 3. 限制内存使用
// 4. 限制递归深度

// 注册的 Redis API
// redis.call()     — 同步调用，错误时抛出异常
// redis.pcall()    — 安全调用，错误时返回错误对象
// redis.log()      — 写日志
// redis.setresp()  — 设置响应协议
// redis.error_reply() — 创建错误回复
// redis.status_reply() — 创建状态回复
```

### 2.2 脚本复制

```c
// 脚本传播模式
// 1. 脚本模式（默认）：复制脚本本身
// 2. 效果模式：复制脚本产生的写命令

// 脚本模式
// EVAL "redis.call('SET', KEYS[1], ARGV[1])" 1 key value
// → 复制 EVAL 命令到从库

// 效果模式
// 使用 redis.setresp(3) 或脚本中包含随机值
// → 复制 SET key value 命令到从库
```

---

## 三、哨兵模式

### 3.1 哨兵间通信

```c
// sentinel.c
// 哨兵使用 Redis 的 Pub/Sub 进行通信
// 频道：__sentinel__:hello

// 哨兵每隔 2 秒向监视的 master 发布：
// sentinel_ip, sentinel_port, sentinel_runid, 
// sentinel_epoch, master_name, master_ip, master_port, master_epoch

// 其他哨兵订阅该频道，发现新哨兵
```

### 3.2 主观下线判定

```c
// 哨兵向所有节点（master/slave/sentinel）每秒发送 PING
// 超过 down-after-milliseconds 未回复 → 主观下线（SDOWN）

// 客观下线判定（ODOWN）
// 哨兵向其他哨兵发送 SENTINEL is-master-down-by-addr
// 收到 quorum 个确认 → 客观下线
```

### 3.3 Leader 选举（Raft 协议）

```c
// 哨兵 Leader 选举流程
1. 每个哨兵向其他哨兵发送投票请求
2. 每个哨兵在一个 epoch 中只能投一票
3. 获得多数票的哨兵成为 Leader
4. Leader 执行故障转移

// 投票规则
// 1. 先到先得：第一个收到投票请求的哨兵获得投票
// 2. epoch 递增：每次选举使用新的 epoch
// 3. 自增规则：如果未收到投票，等下次选举
```

---

## 四、Redis Cluster

### 4.1 槽位迁移

```c
// cluster.c
// 槽位迁移流程
1. 目标节点：cluster setslot {slot} IMPORTING {source_node_id}
2. 源节点：cluster setslot {slot} MIGRATING {target_node_id}
3. 源节点：migrate key 到目标节点
4. 任意节点：cluster setslot {slot} NODE {target_node_id}

// 迁移过程中的请求处理
// 1. 如果 key 还在源节点 → 正常处理
// 2. 如果 key 已迁移到目标节点 → 返回 ASK 重定向
// 3. 客户端收到 ASK 后，先发送 ASKING 命令，再发送请求
```

### 4.2 集群故障检测

```c
// 节点间使用 Gossip 协议交换信息
// 每个节点每秒发送 PING 到其他节点
// 如果 5 秒未收到 PONG，标记为疑似下线（PFAIL）
// 如果半数以上主节点标记为 PFAIL，标记为 FAIL

// 从节点提升
// 1. 主节点被标记为 FAIL
// 2. 从节点检查复制偏移量
// 3. 从节点发起投票
// 4. 获得多数票 → 提升为主节点
```

---

> **总结**：Redis 事务通过命令队列和乐观锁实现，Lua 脚本通过沙箱执行保证原子性，哨兵通过 Raft 协议实现可靠的故障转移，集群通过槽位迁移和 Gossip 协议实现分布式扩展。理解这些高级特性是构建高可用 Redis 系统的基础。