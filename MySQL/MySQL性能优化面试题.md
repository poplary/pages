# MySQL 性能优化深度剖析：执行计划、索引优化、查询优化器

> 面向高级开发者的 MySQL 性能优化，基于优化器源码分析

---

## 一、查询优化器

### 1.1 优化器架构

```
SQL → 解析器 → 预处理 → 优化器 → 执行器
                          ↓
                    ┌─────────────┐
                    │   优化器     │
                    │ 1. 逻辑变换  │
                    │ 2. 物理优化  │
                    │ 3. 成本估算  │
                    └─────────────┘
```

**优化器输入**：解析后的查询树（Query Tree）
**优化器输出**：执行计划（Execution Plan）

### 1.2 成本模型

```c
// 优化器使用成本模型选择执行计划
// 成本 = I/O 成本 + CPU 成本

// 默认成本值
// 一次顺序读取数据页 = 1.0
// 一次随机读取数据页 = 1.0
// 处理一行数据 = 0.2
// 比较一次 = 0.2

// 全表扫描成本 = 总页数 × 1.0 + 总行数 × 0.2
// 索引扫描成本 = 索引页数 × 1.0 + 匹配行数 × 0.2 + 回表行数 × 1.0

// 查看表的统计信息
SHOW TABLE STATUS LIKE 'orders';
SHOW INDEX FROM orders;
```

### 1.3 统计信息

```c
// 优化器依赖统计信息做决策
// 表的统计信息
record_count:    总行数
data_length:     数据大小
index_length:    索引大小
avg_row_length:  平均行长度

// 索引的统计信息
cardinality:     索引的基数（唯一值数量）
// 使用 show index 查看
// 基数估算方法：采样（默认 20 个页）

// 统计信息更新
ANALYZE TABLE orders;  // 手动更新统计信息
// 自动更新：当表修改行数超过 10% 时触发
```

### 1.4 索引选择

**优化器选择索引的依据**：
```c
// 1. 索引选择性 = cardinality / record_count
// 选择性越接近 1，索引越好

// 2. 索引覆盖度
// 索引是否包含所有查询字段（覆盖索引）
// 如果覆盖，避免回表

// 3. 索引扫描行数
// 估算通过索引扫描的行数
// 如果超过表的 20-30%，可能选择全表扫描
```

**优化器选择错误的常见原因**：
```
1. 统计信息过时（未更新）
2. 索引基数估算不准
3. 成本模型参数不合理
4. 查询条件复杂（多表 JOIN）
```

**强制指定索引**：
```sql
SELECT * FROM orders FORCE INDEX (idx_created_at) WHERE created_at > '2024-01-01';
SELECT * FROM orders USE INDEX (idx_created_at) WHERE created_at > '2024-01-01';
SELECT * FROM orders IGNORE INDEX (idx_created_at) WHERE status = 1;
```

---

## 二、EXPLAIN 深度分析

### 2.1 EXPLAIN 格式

```sql
-- 传统格式
EXPLAIN SELECT * FROM orders o JOIN users u ON o.user_id = u.id WHERE o.status = 1\G

-- JSON 格式（更详细）
EXPLAIN FORMAT=JSON SELECT * FROM orders o JOIN users u ON o.user_id = u.id WHERE o.status = 1\G

-- 分析执行计划
EXPLAIN ANALYZE SELECT * FROM orders WHERE status = 1\G
```

### 2.2 EXPLAIN ANALYZE 输出

```sql
EXPLAIN ANALYZE SELECT * FROM orders WHERE status = 1\G
-- 输出示例：
-- -> Filter: (orders.status = 1)  (cost=100.5 rows=1000)
--    -> Table scan on orders  (cost=100.5 rows=10000)
--        (actual time=0.12..5.23 rows=1000 loops=1)

-- 解读：
-- cost: 优化器估算的成本
-- rows: 优化器估算的行数
-- actual time: 实际执行时间（开始..结束）
-- actual rows: 实际返回行数
-- loops: 循环次数
```

### 2.3 type 列详解

```sql
-- 性能从好到差：
system > const > eq_ref > ref > fulltext > ref_or_null 
       > index_merge > unique_subquery > index_subquery 
       > range > index > ALL

-- 各类型详解：
-- system: 表只有一行（系统表）
-- const: 主键或唯一索引等值查询，只返回一行
-- eq_ref: JOIN 中的主键/唯一索引匹配，每行只匹配一次
-- ref: 非唯一索引等值查询
-- range: 索引范围扫描（> < BETWEEN IN）
-- index: 索引全扫描（比 ALL 快，因为索引比数据小）
-- ALL: 全表扫描（需要优化）
```

### 2.4 Extra 列详解

```sql
-- Using index: 覆盖索引，无需回表（最佳）
-- Using where: 在存储引擎返回后，Server 层过滤
-- Using index condition: 索引下推（ICP），在存储引擎层过滤
-- Using filesort: 需要额外排序（优化方向：用索引排序）
-- Using temporary: 使用临时表（优化方向：优化 GROUP BY/DISTINCT）
-- Using join buffer (Block Nested Loop): JOIN 没有使用索引
-- Using MRR: 多范围读取优化
-- Using index for group-by: 使用松散索引扫描
-- Using index for skip scan: 使用索引跳跃扫描（MySQL 8.0.13+）
```

---

## 三、索引优化深入

### 3.1 索引跳跃扫描（Skip Scan Scan）

```sql
-- MySQL 8.0.13+ 支持
-- 联合索引 (a, b)，查询条件是 b 而没有 a 的条件
-- 优化器会遍历 a 的不同值，在每个 a 值下对 b 做索引扫描

-- 示例：
ALTER TABLE t ADD INDEX idx_a_b (a, b);
SELECT * FROM t WHERE b = 5;
-- 没有 idx_a_b 时：全表扫描
-- 有 idx_a_b 时：优化器可能使用 Skip Scan
-- 对每个 a 值，执行：SELECT * FROM t WHERE a = X AND b = 5
```

### 3.2 索引下推（ICP）

```sql
-- MySQL 5.6+ 引入
-- 在没有 ICP 时，存储引擎根据索引条件返回行，Server 层再过滤
-- 有 ICP 时，存储引擎层直接过滤不符合条件的行

-- 示例：
CREATE INDEX idx_zip_lastname ON people (zipcode, lastname);
SELECT * FROM people WHERE zipcode = '10001' AND lastname LIKE '%Smith%';

-- 没有 ICP：存储引擎用 zipcode 找到行，返回 Server，Server 过滤 lastname
-- 有 ICP：存储引擎用 zipcode 找到行后，在引擎层过滤 lastname，减少回表
```

### 3.3 MRR（Multi-Range Read）

```sql
-- MySQL 5.6+ 引入
-- 将随机 I/O 转换为顺序 I/O
-- 先收集所有需要回表的主键值，排序后再批量回表

-- 适用场景：
-- 1. 范围查询（二级索引）
-- 2. 需要回表大量数据

-- 优化原理：
-- 步骤 1: 从二级索引获取所有匹配的主键值
-- 步骤 2: 对主键值排序（按主键顺序）
-- 步骤 3: 按排序后的主键顺序回表

-- 开启 MRR
SET optimizer_switch = 'mrr=on';
SET optimizer_switch = 'mrr_cost_based=on';  -- 基于成本选择
```

---

## 四、JOIN 优化

### 4.1 JOIN 算法

```sql
-- 1. Nested Loop Join（NLJ，嵌套循环连接）
-- 驱动表逐行匹配被驱动表
-- 被驱动表有索引时效率高

-- 2. Block Nested Loop Join（BNL，块嵌套循环连接）
-- 驱动表一批行传给被驱动表
-- 被驱动表没有索引时使用

-- 3. Hash Join（哈希连接，MySQL 8.0.18+）
-- 驱动表构建哈希表，被驱动表逐行探测
-- 适合大表等值连接
```

### 4.2 JOIN 优化策略

```sql
-- 1. 小表驱动大表
-- 优化器选择小表作为驱动表

-- 2. 连接条件建立索引
-- 被驱动表的连接列必须有索引

-- 3. 使用 STRAIGHT_JOIN 强制驱动表顺序
SELECT * FROM small_table STRAIGHT_JOIN big_table ON small_table.id = big_table.ref_id;

-- 4. 避免多表 JOIN
-- 3 表以上 JOIN 考虑拆分或汇总
```

---

## 五、SQL 优化实战

### 5.1 分页优化

```sql
-- 传统分页（越往后越慢）
SELECT * FROM orders ORDER BY id LIMIT 100000, 20;

-- 优化 1：子查询 + 覆盖索引
SELECT * FROM orders 
WHERE id > (SELECT id FROM orders ORDER BY id LIMIT 100000, 1)
ORDER BY id LIMIT 20;

-- 优化 2：JOIN 延迟关联
SELECT * FROM orders o
INNER JOIN (SELECT id FROM orders ORDER BY id LIMIT 100000, 20) AS tmp
ON o.id = tmp.id;

-- 优化 3：游标分页（基于最后一条记录）
SELECT * FROM orders WHERE id > 100000 ORDER BY id LIMIT 20;
```

### 5.2 COUNT 优化

```sql
-- 慢：COUNT(*) 走全表扫描
SELECT COUNT(*) FROM orders;

-- 快：使用二级索引（比主键索引小）
-- 优化器会自动选择最小的索引

-- 精确计数维护
-- 使用计数表：
CREATE TABLE order_count (
    date DATE PRIMARY KEY,
    count INT
);
-- 每次插入/删除时更新计数表

-- 使用 Redis 维护计数
```

### 5.3 ORDER BY 优化

```sql
-- 文件排序（filesort）的两种算法
-- 1. 单路排序：读取所有需要的字段，排序后返回
-- 2. 双路排序：只读取排序列和主键，排序后回表取数据

-- 优化方向：
-- 1. 用索引排序
-- 2. 增加 sort_buffer_size
-- 3. 减少 SELECT 列数（避免排序大字段）

-- 示例：
-- 索引 (status, created_at)
-- 好（用索引排序）
SELECT * FROM orders WHERE status = 1 ORDER BY created_at LIMIT 100;

-- 坏（文件排序）
SELECT * FROM orders ORDER BY status, created_at LIMIT 100;
-- 没有 WHERE 条件，全表扫描 + 排序
```

---

## 六、性能监控

### 6.1 性能 Schema

```sql
-- 查看未使用索引的查询
SELECT * FROM sys.schema_unused_indexes;

-- 查看全表扫描的查询
SELECT * FROM sys.statements_with_full_table_scans;

-- 查看排序相关的查询
SELECT * FROM sys.statements_with_sorting;

-- 查看最耗时的查询
SELECT * FROM sys.statement_analysis ORDER BY avg_latency DESC LIMIT 10;

-- 查看索引使用统计
SELECT * FROM sys.schema_index_statistics WHERE table_schema = 'mydb';
```

### 6.2 锁监控

```sql
-- 查看当前锁等待
SELECT * FROM performance_schema.data_lock_waits\G

-- 查看事务信息
SELECT * FROM information_schema.INNODB_TRX\G

-- 查看 InnoDB 状态
SHOW ENGINE INNODB STATUS\G
```

---

> **总结**：MySQL 性能优化需要深入理解优化器的工作机制。通过 EXPLAIN 分析执行计划、理解成本模型、掌握索引优化技术（Skip Scan、ICP、MRR）和 JOIN 优化策略，才能写出高效的 SQL。实际优化中，建议先使用 sys 系统库定位问题，再针对性地优化。

---

## 面试题精选

**Q1: 如何定位和优化慢查询？**

A: 1）开启慢查询日志（`long_query_time=1`）；2）用 `EXPLAIN` 分析执行计划，重点看 type、key、rows、Extra；3）优化索引（加索引/覆盖索引/索引失效排查）；4）优化 SQL（避免 SELECT *、分页优化、EXISTS 代替 IN）。

**Q2: type 字段各值的含义是什么？**

A: ALL（全表扫描）→ index（全索引扫描）→ range（索引范围扫描）→ ref（非唯一索引等值）→ eq_ref（唯一索引 JOIN）→ const（主键/唯一索引等值）。

**Q3: 哪些情况会导致索引失效？**

A: 1）对索引列使用函数；2）隐式类型转换；3）前导模糊查询 `like %xx`；4）联合索引未用最左列；5）OR 条件中有非索引列；6）索引列参与运算。

**Q4: 如何优化分页查询？**

A: 1）子查询 + 覆盖索引：`SELECT * FROM orders WHERE id > (SELECT id FROM orders LIMIT 100000,1) LIMIT 20`；2）JOIN 延迟关联；3）游标分页（基于最后一条记录）。

**Q5: JOIN 查询时如何优化？**

A: 1）小表驱动大表；2）被驱动表连接列建立索引；3）避免 3 表以上 JOIN，考虑拆分。