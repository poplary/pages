# MySQL 架构与运维深度：复制、分片、高可用、备份恢复

> 面向高级开发者的 MySQL 架构设计与运维实践

---

## 一、主从复制原理

### 1.1 复制线程详解

**主库线程**：
```c
// Binlog Dump Thread
// 当从库连接时，主库创建 dump 线程
// 读取 binlog 并发送给从库
// 有三种发送方式：
// 1. 异步：发送后不等待确认
// 2. 半同步：等待至少一个从库确认
// 3. 同步：等待所有从库确认

// 主库线程执行流程
1. 从库发送 COM_BINLOG_DUMP 命令
2. 主库检查 binlog 文件是否存在
3. 从指定位置读取 binlog 事件
4. 发送给从库
5. 等待新事件
```

**从库线程**：
```c
// I/O Thread
// 接收主库的 binlog 事件，写入 relay log
// 执行流程：
1. 连接主库，发送 dump 请求
2. 接收 binlog 事件
3. 写入 relay log
4. 更新 master.info

// SQL Thread
// 读取 relay log，执行事件
// 执行流程：
1. 读取 relay log 事件
2. 执行 SQL 语句
3. 更新 relay-log.info
```

### 1.2 Binlog 格式对比

```c
// 1. STATEMENT 格式
// 记录 SQL 语句
// 优点：日志量小
// 缺点：非确定性函数（NOW(), UUID()）可能导致不一致
// 示例：
# at 1234
#2024-01-01 10:00:00 server id 1
SET TIMESTAMP=1704088800;
INSERT INTO orders VALUES (1, NOW(), 100);

// 2. ROW 格式（默认，推荐）
// 记录每行变更
// 优点：一致性最好
// 缺点：日志量大
// 示例：
# at 1234
#2024-01-01 10:00:00 server id 1
### INSERT INTO `db`.`orders`
### SET
###   @1=1 /* INT */
###   @2='2024-01-01 10:00:00' /* DATETIME */
###   @3=100 /* DECIMAL */

// 3. MIXED 格式
// 混合模式，MySQL 自动选择
// 确定性语句用 STATEMENT
// 非确定性语句用 ROW
```

### 1.3 半同步复制

```c
// 半同步复制流程
1. 主库执行事务，写入 binlog
2. 等待至少一个从库确认收到 binlog（rpl_semi_sync_master_wait_for_slave_count）
3. 从库 I/O 线程接收并写入 relay log
4. 从库返回 ACK 给主库
5. 主库返回事务成功给客户端

// 超时处理
// 如果等待超时（rpl_semi_sync_master_timeout），降级为异步复制
// 主库继续执行，不再等待从库确认
```

### 1.4 复制延迟分析

**延迟来源**：
```
1. 主库 binlog 写入（磁盘 I/O）
2. 网络传输延迟
3. 从库 relay log 写入（磁盘 I/O）
4. 从库 SQL 线程回放（单线程瓶颈）
5. 从库锁冲突
6. 从库硬件性能
```

**并行复制（MySQL 5.7+）**：
```c
// slave_parallel_workers > 0 时启用并行复制
// 三种并行策略：
// 1. DATABASE（5.7）：不同数据库并行
// 2. LOGICAL_CLOCK（5.7+）：同一时间点的事务并行
// 3. WRITESET（8.0+）：没有冲突的事务并行

// LOGICAL_CLOCK 原理
// 主库在提交时记录事务的 last_committed 和 sequence_number
// 如果事务 A 的 sequence_number 小于事务 B 的 last_committed
// 则事务 A 必须在事务 B 之前提交
// 否则可以并行执行
```

---

## 二、分库分表

### 2.1 分片策略

**分片键选择**：
```c
// 1. 高频查询条件
// 2. 数据分布均匀
// 3. 不可变（或很少变化）
// 4. 业务相关性

// 常见的分片算法：
// 1. 取模：hash(key) % N
// 2. 范围：key 在某个范围
// 3. 一致性哈希：减少扩缩容的迁移数据量
// 4. 地理位置：按地域分片
// 5. 时间：按时间分片（适合日志类数据）
```

### 2.2 分布式事务

**XA 事务（两阶段提交）**：
```c
// 第一阶段：准备
// 所有分片执行 prepare，写入 redo log
// 第二阶段：提交
// 所有分片执行 commit

// 事务管理器协调流程：
1. TM 向所有 RM 发送 prepare
2. 所有 RM 返回 OK
3. TM 向所有 RM 发送 commit
4. 所有 RM 返回 OK

// 问题：
// 1. 性能差（多次网络交互）
// 2. 锁定资源时间长
// 3. 协调者单点故障
```

**最终一致性方案**：
```c
// 本地消息表
// 1. 在业务库创建消息表
// 2. 业务操作和消息写入在同一个事务中
// 3. 后台定时扫描消息表，发送到消息队列
// 4. 下游服务消费消息，执行操作
// 5. 成功后标记消息为已处理
```

### 2.3 全局 ID 生成

**雪花算法（Snowflake）**：
```c
// 64-bit ID 结构
// | 1 bit 符号位 | 41 bit 时间戳 | 10 bit 工作节点 | 12 bit 序列号 |
// |   0          |   毫秒级时间   |   机器 ID      |   自增号      |

// 每秒可生成 4096 个 ID
// 时间戳可用 69 年
// 支持 1024 个节点

// 常见问题：
// 1. 时钟回拨：记录上次生成时间，回拨时等待
// 2. 节点分配：ZK 或数据库记录
```

---

## 三、高可用架构

### 3.1 MHA（Master High Availability）

```c
// MHA 故障转移流程
1. 监控节点检测主库故障
2. 从候选从库中选择数据最新的
3. 应用差异的 relay log
4. 提升为新主库
5. 其他从库指向新主库
6. 通知应用程序

// 切换时间：10-30 秒
// 最大数据丢失：1 个事务（如果使用半同步）
```

### 3.2 MGR（MySQL Group Replication）

```c
// MGR 基于 Paxos 协议
// 有两种模式：
// 1. 单主模式（Single-Primary）：只有一个节点可写
// 2. 多主模式（Multi-Primary）：所有节点可写

// 组成员管理
// 1. 加入组：新节点发送 JOIN 请求
// 2. 组视图：成员变更时生成新视图
// 3. 故障检测：5 秒超时检测

// 冲突检测
// 多主模式下，使用乐观锁
// 如果两个事务修改同一行，后提交的会回滚
```

### 3.3 读写分离

```c
// 读写分离的挑战
// 1. 主从延迟导致读不到最新数据
// 2. 事务内读一致性

// 解决方案：
// 1. 强制读主库：关键数据或事务内的读
// 2. 等待延迟：从库追上主库后再读
// 3. 缓存标记：记录数据更新时间，从库未追上时读主库

// 中间件方案：
// 1. ProxySQL：功能完善的代理
// 2. ShardingSphere：可插拔的中间件
// 3. Atlas：360 开源的代理
```

---

## 四、备份恢复

### 4.1 备份策略

```c
// 推荐备份策略
// 每周：全量备份（XtraBackup）
// 每天：增量备份（XtraBackup）
// 实时：binlog 备份

// 备份保留周期
// 全量备份：4 周
// 增量备份：2 周
// binlog：7 天
```

### 4.2 XtraBackup 原理

```c
// XtraBackup 备份流程
1. 记录 LSN（Log Sequence Number）
2. 启动后台线程复制 redo log
3. 拷贝数据文件（并行）
4. 停止复制 redo log
5. 应用 redo log（prepare）

// 恢复流程
1. Prepare：应用 redo log，回滚未提交事务
2. Copy-back：将文件拷贝到数据目录
3. 启动 MySQL
4. 应用 binlog 到需要的时间点
```

---

## 五、监控体系

### 5.1 核心监控指标

```sql
-- QPS/TPS
SHOW GLOBAL STATUS LIKE 'Questions';           -- 总查询数（含内部）
SHOW GLOBAL STATUS LIKE 'Queries';             -- 总查询数（不含内部）
SHOW GLOBAL STATUS LIKE 'Com_commit';          -- 提交数
SHOW GLOBAL STATUS LIKE 'Com_rollback';        -- 回滚数

-- 连接
SHOW GLOBAL STATUS LIKE 'Threads_connected';   -- 当前连接数
SHOW GLOBAL STATUS LIKE 'Threads_running';     -- 正在运行的线程
SHOW GLOBAL STATUS LIKE 'Aborted_connects';    -- 连接失败数

-- 缓冲池
SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool_read_requests';     -- 读请求数
SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool_reads';             -- 磁盘读次数
-- 命中率 = (read_requests - reads) / read_requests * 100%

-- 锁
SHOW GLOBAL STATUS LIKE 'Innodb_row_lock_current_waits';        -- 当前等待
SHOW GLOBAL STATUS LIKE 'Innodb_row_lock_time';                 -- 锁等待总时间

-- 临时表
SHOW GLOBAL STATUS LIKE 'Created_tmp_tables';       -- 临时表数
SHOW GLOBAL STATUS LIKE 'Created_tmp_disk_tables';  -- 磁盘临时表数
```

---

> **总结**：MySQL 架构与运维的核心是理解复制原理、分片策略和高可用方案。主从复制从 binlog 格式到并行复制，分库分表从分片键选择到全局 ID 生成，高可用从 MHA 到 MGR，每个环节都需要深入理解其原理。备份恢复是数据安全的最后防线，XtraBackup 的热备份能力是生产环境的关键工具。

---

## 面试题精选

**Q1: 主从复制的三种模式有什么区别？**

A: 异步复制（主库不等待从库确认，性能好但可能丢数据）、半同步复制（等待至少一个从库确认，平衡安全与性能）、全同步复制（等待所有从库确认，最安全但最慢）。

**Q2: 主从延迟如何解决？**

A: 1）使用并行复制（`slave_parallel_workers`）；2）拆分大事务为小事务；3）升级从库硬件；4）读写分离时读从库可接受延迟；5）强制读主库。

**Q3: 分库分表的分片键如何选择？**

A: 1）高频查询条件作为分片键；2）数据分布均匀；3）不可变或很少变化；4）避免跨分片查询。

**Q4: 分布式全局 ID 如何生成？**

A: 雪花算法（64bit：1 bit 符号 + 41 bit 时间戳 + 10 bit 节点 + 12 bit 序列号）、Redis 自增、号段模式。

**Q5: XtraBackup 备份原理是什么？**

A: 记录 LSN → 后台线程复制 redo log → 拷贝数据文件 → 停止复制 redo log → 应用 redo log（prepare）。

**Q6: 脑裂如何解决？**

A: 设置 `min-slaves-to-write` 和 `min-slaves-max-lag`，当从库数量或延迟不满足条件时，主库禁止写操作。