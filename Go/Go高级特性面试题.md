# Go Runtime 深度剖析：GMP、GC、内存管理

> 面向高级开发者的运行时内部原理，基于 Go 1.26 源码分析

---

## 一、Goroutine 调度器（GMP）

### 1.1 核心数据结构

#### 1.1.1 G（Goroutine）
```go
// src/runtime/runtime2.go
type g struct {
    stack       stack       // 栈空间 [lo, hi]
    stackguard0 uintptr     // 栈扩张检测阈值（= stack.lo + StackGuard，被抢占时设为 StackPreempt）
    stackguard1 uintptr     // systemstack 栈增长阈值
    _panic    *_panic       // 最内层的 panic
    _defer    *_defer       // 最内层的 defer
    m         *m            // 当前绑定的 M
    sched     gobuf         // 调度上下文（保存 sp/pc/bp 用于恢复执行）
    atomicstatus atomic.Uint32 // 状态：_Gidle/_Grunnable/_Grunning/_Gsyscall/_Gwaiting/_Gpreempted
    goid         uint64
    preempt       bool      // 抢占信号，与 stackguard0 = StackPreempt 配合
    preemptStop   bool      // 抢占时转为 _Gpreempted（而非直接调度）
    preemptShrink bool      // 在同步安全点收缩栈
    param        unsafe.Pointer // 用于 channel 唤醒时传递 sudog
    labels       unsafe.Pointer // 分析标签
    timer        *timer      // 缓存的 time.Timer（用于 time.Sleep）
    parkingOnChan atomic.Bool // 即将在 chansend/chanrecv 上 park
    tracking     bool        // 是否在追踪调度延迟
    runnableTime int64       // 可运行状态的累计时间（用于调度延迟监控）
    gcAssistBytes int64      // GC 辅助额度（负值表示需要协助 GC 扫描）
    xRegs        xRegPerG   // 异步抢占时保存的扩展寄存器状态
    bubble       *synctestBubble // 测试用
}

type gobuf struct {
    sp   uintptr  // 栈指针
    pc   uintptr  // 程序计数器
    bp   uintptr  // 基址指针
    ret  uintptr  // 系统调用的返回值
    lr   uintptr  // 链接寄存器（arm 架构）
}
```

**G 的状态机**：
```
_Gidle ─→ _Grunnable ─→ _Grunning ─→ _Gsyscall
    ↑           ↑            ↓              ↓
    └───────────┴───── _Gwaiting ←──────────┘
                           ↓
                       _Gpreempted
```

#### 1.1.2 M（Machine / OS Thread）
```go
type m struct {
    g0      *g      // 系统栈上的 goroutine（执行调度、栈扩张、GC）
    curg    *g      // 当前运行的用户 goroutine
    p       puintptr // 关联的 P（nil 表示不在执行 Go 代码）
    nextp   puintptr // 下一个要安装的 P
    oldp    puintptr // 系统调用前绑定的 P
    tls     [tlsSlots]uintptr // 线程本地存储
    spinning bool    // 是否在自旋寻找可运行的 G
    blocked  bool    // M 是否阻塞在 note 上
    park     note    // 挂起/唤醒机制（futex/sema）
    mstartfn func()  // M 启动时执行的函数
    procid   uint64  // 调试用的线程 ID
    allpSnapshot []*p // 释放 P 后用于 findRunnable 的 allp 快照
    mWaitList     mWaitList // 运行时锁等待列表（用于饥饿检测）
    ditEnabled    bool      // 是否启用 DIT（数据独立定时）
    cgoCallersUse atomic.Uint32
    lockedg       guintptr  // 通过 LockOSThread 锁定的 G
    freelink      *m        // 空闲 M 链表
}
```

- **M0**：Go 程序启动时的第一个线程
- **g0**：每个 M 有一个 g0，运行在系统栈上，用于执行调度、GC 等运行时任务

#### 1.1.3 P（Processor — 逻辑处理器）
```go
type p struct {
    id          int32
    status      uint32 // _Pidle/_Prunning/_Psyscall/_Pgcstop/_Pdead
    m           muintptr       // 关联的 M
    mcache      *mcache        // 每 P 专属内存缓存（无锁分配）
    pcache      pageCache      // 空闲页缓存
    runqhead    uint32         // 本地可运行队列头部
    runqtail    uint32         // 本地可运行队列尾部
    runq        [256]guintptr  // 本地可运行队列（环形数组，无锁）
    runnext     guintptr       // 下一个要运行的 G（优先级最高，会继承时间片）
    gcw         gcWork         // GC 标记工作缓冲区
    wbBuf       wbBuf          // 写屏障缓冲区
    goidcache    uint64         // goroutine ID 缓存
    goidcacheend uint64         // 缓存上限
    gFree        gList          // 空闲 G 列表（状态 _Gdead）
    deferpool    []*_defer      // defer 结构体池
    mspancache   struct {       // mspan 缓存，减少锁竞争
        len int
        buf [128]*mspan
    }
    pinnerCache  *pinner        // Pinner 对象缓存
    oldm         mWeakPointer   // 之前的 M（用于调试）
}
```

`GOMAXPROCS` 控制 P 的数量。P 是 G 能运行的前提——没有 P 的 G 无法被调度。

### 1.2 调度循环

#### 1.2.1 schedule() — 核心调度函数
```go
// src/runtime/proc.go
func schedule() {
    mp := getg().m

    // 持有锁时不能调度
    if mp.locks != 0 {
        throw("schedule: holding locks")
    }

    // 如果当前 M 锁定了 G（LockOSThread），直接执行该 G
    if mp.lockedg != 0 {
        stoplockedm()
        execute(mp.lockedg.ptr(), false)
    }

top:
    pp := mp.p.ptr()
    pp.preempt = false

    // 关键：调用 findRunnable 阻塞式查找可运行的 G
    // 若所有 G 都阻塞，M 会休眠（stopm）
    gp, inheritTime, tryWakeP := findRunnable()

    // 清理 allpSnapshot，让 GC 回收
    mp.clearAllpSnapshot()

    // 释放 GC 标记 worker（如果没有被选中）
    gcController.releaseNextGCMarkWorker(pp)

    // 如果 M 之前是自旋状态，现在要运行 G 了，重置自旋
    if mp.spinning {
        resetspinning()
    }

    // 执行 G
    execute(gp, inheritTime)
}
```

**调度发生时机**：
```
1. 函数调用入口（编译器插入的栈检查）
2. 阻塞系统调用（进入 _Gsyscall）
3. time.Sleep（G 进入 _Gwaiting）
4. channel 操作（G 阻塞在 sendq/recvq）
5. mutex 操作（semacquire 阻塞）
6. runtime.Gosched() 主动让出
7. GC 的栈扫描
8. 抢占信号（SIGURG，10ms 超时）
```

#### 1.2.2 execute() — 执行 G
```go
// src/runtime/proc.go
func execute(gp *g, inheritTime bool) {
    mp := getg().m

    // 绑定 G 和 M
    mp.curg = gp
    gp.m = mp

    // 状态转换：_Grunnable → _Grunning
    casgstatus(gp, _Grunnable, _Grunning)
    gp.waitsince = 0
    gp.preempt = false
    gp.stackguard0 = gp.stack.lo + stackGuard // 重置栈检查

    // 不继承时间片则递增调度计数
    if !inheritTime {
        mp.p.ptr().schedtick++
    }

    // 通过 gogo 汇编函数恢复 G 的上下文（sp/pc/bp）
    // 这会从 gp.sched 中恢复寄存器，跳转到 G 上次执行的位置
    gogo(&gp.sched)
}
```

#### 1.2.3 findRunnable() — 阻塞式查找可运行 G
```go
// src/runtime/proc.go
func findRunnable() (gp *g, inheritTime, tryWakeP bool) {
    mp := getg().m

top:
    mp.clearAllpSnapshot()
    pp := mp.p.ptr()

    // 1. 检查是否需要 GC（若正在 GC，M 停车等待）
    if sched.gcwaiting.Load() {
        gcstopm()
        goto top
    }

    // 2. 优先调度 trace reader（跟踪调试）
    if traceEnabled() || traceShuttingDown() {
        if gp := traceReader(); gp != nil {
            return gp, false, true
        }
    }

    // 3. 优先调度 GC 标记工作者
    if gcBlackenEnabled != 0 {
        if gp, tnow := gcController.findRunnableGCWorker(pp, now); gp != nil {
            return gp, false, true
        }
    }

    // 4. 公平性：每 61 次调度检查一次全局队列
    if pp.schedtick%61 == 0 && !sched.runq.empty() {
        lock(&sched.lock)
        gp := globrunqget()
        unlock(&sched.lock)
        if gp != nil {
            return gp, false, false
        }
    }

    // 5. 从本地可运行队列取（runq 环形数组）
    if gp, inheritTime := runqget(pp); gp != nil {
        return gp, inheritTime, false
    }

    // 6. 从全局队列批量取
    if !sched.runq.empty() {
        lock(&sched.lock)
        gp, q := globrunqgetbatch(int32(len(pp.runq)) / 2)
        unlock(&sched.lock)
        if gp != nil {
            runqputbatch(pp, &q)
            return gp, false, false
        }
    }

    // 7. 网络轮询（非阻塞）
    if netpollinited() && netpollAnyWaiters() && sched.lastpoll.Load() != 0 {
        if sched.pollingNet.Swap(1) == 0 {
            list, delta := netpoll(0)
            sched.pollingNet.Store(0)
            if !list.empty() {
                gp := list.pop()
                injectglist(&list)
                return gp, false, false
            }
        }
    }

    // 8. ★ 核心：工作窃取（Work Stealing）
    // 随机遍历其他 P，偷取一半的 G
    if mp.spinning || mp.allpSnapshot == nil {
        // 收集所有 P 的快照
        mp.collectAllpSnapshot()
        mp.spinning = true
        sched.nmspinning.Add(1)
    }

    for i := 0; i < 4; i++ {
        // 随机顺序遍历，避免窃取热点
        for enum := stealOrder.start(fastrand()); !enum.done(); enum.next() {
            p2 := mp.allpSnapshot[enum.position()]
            if gp := runqsteal(pp, p2, i == 0); gp != nil {
                return gp, false, false
            }
        }
    }

    // 9. 阻塞式网络轮询（等待网络事件）
    if netpollinited() && netpollAnyWaiters() && sched.lastpoll.Load() != 0 {
        sched.lastpoll.Store(0)
        list, delta := netpoll(-1)
        sched.lastpoll.Store(nanotime())
        if !list.empty() {
            gp := list.pop()
            injectglist(&list)
            return gp, false, false
        }
    }

    // 10. 实在没有可运行的 G，M 休眠
    stopm()
    goto top
}
```

**工作窃取策略**：
- 遍历顺序随机化（`stealOrder.start(fastrand())`），避免所有 P 同时窃取同一个 P
- 每次偷取目标 P 本地队列的一半 G
- 自旋状态（`mp.spinning`）的 M 会持续尝试窃取，非自旋 M 会直接 `stopm()`

### 1.3 抢占式调度（Go 1.14+）

```go
// src/runtime/proc.go
// 后台监控线程，运行在独立 M 上
func sysmon() {
    for {
        usleep(20 * 1000) // 20us 轮询一次

        // 检查是否有 P 运行时间超过 10ms
        for _, pp := range allp {
            if pp.syscalltick != pp.sysmontick.syscalltick {
                // 系统调用中，检查是否需要抢占
                pp.sysmontick.syscalltick = pp.syscalltick
                pp.sysmontick.syscallwhen = now
            } else if pp.sysmontick.syscallwhen != 0 &&
                now-pp.sysmontick.syscallwhen > 10e6 {
                // 超过 10ms 的 P，发送抢占信号
                preemptone(pp)
            }
        }
    }
}

// 向 P 发送抢占请求
func preemptone(pp *p) bool {
    mp := pp.m.ptr()
    if mp == nil || mp == getg().m {
        return false
    }
    gp := mp.curg
    if gp == nil || gp == mp.g0 {
        return false
    }
    // 设置抢占标记
    gp.preempt = true
    // 设置 stackguard0 为 StackPreempt，下次函数调用时触发
    gp.stackguard0 = stackPreempt
    return true
}
```

**抢占流程**：
```
1. sysmon 检测到 G 运行超过 10ms
2. 设置 gp.preempt = true, gp.stackguard0 = StackPreempt
3. 发送 SIGURG 信号给目标 M
4. 信号处理函数 sigtramp 在安全点保存上下文
5. G 在下一个函数调用时触发 morestack → newstack → schedule
```

### 1.4 系统调用处理

**同步系统调用**（如文件 I/O）：
```
G running → entersyscall() → P 和 M 解绑 → P 去找其他 M 或空闲 M
                              ↓
                           M 在 _Gsyscall 等待
                              ↓
                        syscall 返回 → exitsyscall() → 尝试重新绑定 P
                                                   ↓
                                           成功 → 继续执行
                                           失败 → G 进入全局队列，M 找新 P 或休眠
```

**异步系统调用**（网络 I/O）：
```
goroutine 调用 net.Read → 注册 fd 到 epoll → G 进入 _Gwaiting
                                                ↓
                              sysmon/findRunnable 时 netpoll 检查就绪 fd
                                                ↓
                              G 变为 _Grunnable → 放入运行队列
```

---

## 二、sync.Mutex 内部实现

> Go 1.26 的 `sync.Mutex` 是 `isync.Mutex`（`internal/sync`）的薄封装。
> 使用经典的 `state` + `sema` 架构，支持正常模式和饥饿模式。

### 2.1 数据结构

```go
// src/sync/mutex.go — 对外封装
type Mutex struct {
    _ noCopy
    mu isync.Mutex
}
func (m *Mutex) Lock()   { m.mu.Lock() }
func (m *Mutex) Unlock() { m.mu.Unlock() }

// src/internal/sync/mutex.go — 内部实现
type Mutex struct {
    state int32   // 锁状态（位字段编码）
    sema  uint32  // 信号量（用于休眠/唤醒 goroutine）
}

// state 字段位编码：
// bit 0: mutexLocked   — 锁定标志
// bit 1: mutexWoken    — 有等待者被唤醒标记
// bit 2: mutexStarving — 饥饿模式
// bits 3+: waiter count — 等待者数量
const (
    mutexLocked   = 1 << iota  // 1
    mutexWoken                 // 2
    mutexStarving              // 4
    mutexWaiterShift = iota    // 3
)

// 饥饿阈值（1ms）
const starvationThresholdNs = 1e6
```

### 2.2 Lock 快慢路径

```go
// src/internal/sync/mutex.go

func (m *Mutex) Lock() {
    // 快速路径：CAS 尝试获取锁
    // state 从 0 变更为 mutexLocked（1），成功则直接返回
    if atomic.CompareAndSwapInt32(&m.state, 0, mutexLocked) {
        return
    }
    // 慢路径（outlined 便于快速路径内联）
    m.lockSlow()
}

func (m *Mutex) lockSlow() {
    var waitStartTime int64
    starving := false
    awoke := false
    iter := 0
    old := m.state

    for {
        // 阶段 1：自旋
        // 条件：锁被持有且未饥饿
        // 自旋期间设置 mutexWoken 标志，防止 unlock 重复唤醒
        if old&(mutexLocked|mutexStarving) == mutexLocked && runtime_canSpin(iter) {
            if !awoke && old&mutexWoken == 0 &&
                old>>mutexWaiterShift != 0 &&
                atomic.CompareAndSwapInt32(&m.state, old, old|mutexWoken) {
                awoke = true
            }
            runtime_doSpin()
            iter++
            old = m.state
            continue
        }

        // 阶段 2：计算新状态
        new := old
        if old&mutexStarving == 0 {
            new |= mutexLocked
        }
        if old&(mutexLocked|mutexStarving) != 0 {
            new += 1 << mutexWaiterShift
        }
        if starving && old&mutexLocked != 0 {
            new |= mutexStarving
        }
        if awoke {
            if new&mutexWoken == 0 {
                throw("sync: inconsistent mutex state")
            }
            new &^= mutexWoken
        }

        // 阶段 3：CAS 更新状态
        if atomic.CompareAndSwapInt32(&m.state, old, new) {
            if old&(mutexLocked|mutexStarving) == 0 {
                break  // 获取锁成功
            }

            // 阶段 4：通过信号量休眠
            queueLifo := waitStartTime != 0
            if waitStartTime == 0 {
                waitStartTime = runtime_nanotime()
            }
            runtime_SemacquireMutex(&m.sema, queueLifo, 2)

            starving = starving ||
                runtime_nanotime()-waitStartTime > starvationThresholdNs
            old = m.state

            // 阶段 5：饥饿模式处理
            if old&mutexStarving != 0 {
                delta := int32(mutexLocked - 1<<mutexWaiterShift)
                if !starving || old>>mutexWaiterShift == 1 {
                    delta -= mutexStarving  // 退出饥饿模式
                }
                atomic.AddInt32(&m.state, delta)
                break
            }
            awoke = true
            iter = 0
        } else {
            old = m.state
        }
    }
}
```

### 2.3 Unlock 快慢路径

```go
func (m *Mutex) Unlock() {
    new := atomic.AddInt32(&m.state, -mutexLocked)
    if new != 0 {
        m.unlockSlow(new)
    }
}

func (m *Mutex) unlockSlow(new int32) {
    if (new+mutexLocked)&mutexLocked == 0 {
        fatal("sync: unlock of unlocked mutex")
    }

    if new&mutexStarving == 0 {
        // 正常模式：循环 CAS，递减等待者计数，设置 mutexWoken，唤醒一个等待者
        for {
            if new>>mutexWaiterShift == 0 ||
                new&(mutexLocked|mutexWoken|mutexStarving) != 0 {
                return
            }
            new = (new - 1<<mutexWaiterShift) | mutexWoken
            if atomic.CompareAndSwapInt32(&m.state, m.state, new) {
                runtime_Semrelease(&m.sema, false, 2)
                return
            }
        }
    } else {
        // 饥饿模式：直接移交锁所有权，mutexLocked 由等待者自己设置
        runtime_Semrelease(&m.sema, true, 2)
    }
}
```

### 2.4 正常模式 vs 饥饿模式

| 模式 | 触发条件 | 行为 |
|------|---------|------|
| **正常模式** | 默认 | 等待者 FIFO 排队，新来的 goroutine 有 CPU 优势可能插队，被唤醒后竞争失败则排到队首 |
| **饥饿模式** | 等待 > 1ms | 直接移交锁所有权给队首等待者，新来的不自旋、不获取锁，直接排队尾 |

**退出饥饿模式**：该等待者成为最后一个等待者，或等待时间 < 1ms。

### 2.5 自旋条件

```go
func runtime_canSpin(i int) bool {
    return i < active_spin && ncpu > 1 && gomaxprocs > 1
        && sched.runqsize == 0 && runqempty(getg().m.p.ptr())
}
```
## 三、Map 内部实现（Go 1.26 全新 Swiss Table 风格）

### 3.1 新架构：目录表（Directory of Tables）

Go 1.26 的 map 底层实现已从 `hmap`/`bmap`（桶链表）完全重写为 `maps.Map`（目录表架构）。

```go
// src/internal/runtime/maps/map.go
type Map struct {
    used       uint64   // 已填充槽数（即元素数）
    seed       uintptr  // 哈希种子（每个 map 运行时随机生成）

    dirPtr     unsafe.Pointer // 目录表指针
    // 正常模式：*[dirLen]*table（指向 table 指针数组）
    // 小 map 模式：*group（≤ abi.MapGroupSlots 个元素）

    dirLen     int      // 目录长度 = 1 << globalDepth
    globalDepth uint8  // 目录深度（用于哈希索引）
    globalShift uint8  // 哈希移位 = 64 - globalDepth

    writing    uint8   // 写入标志（检测并发写入）
    tombstonePossible bool  // 是否存在墓碑标记（删除的槽）
    clearSeq   uint64  // Clear 操作序列号（用于迭代检测）
}
```

### 3.2 查找操作
```go
// src/internal/runtime/maps/map.go (简化)
// 查找 key 对应的 value
func (m *Map) Lookup(ptr unsafe.Pointer, key unsafe.Pointer) unsafe.Pointer {
    // 1. 计算哈希
    hash := m.keyHash(key, m.seed)

    if m.dirLen == 0 {
        // ★ 小 map 优化：直接查 group
        // 当元素 ≤ abi.MapGroupSlots（8）时
        // dirPtr 直接指向一个 group，无需经过 table
        return m.lookupSmall(hash, key, ptr)
    }

    // 2. 根据哈希高位索引目录
    idx := hash >> m.globalShift  // 取高 globalDepth 位
    tbl := (*table)(atomic.Loadp(unsafe.Add(m.dirPtr, idx*goarch.PtrSize)))

    // 3. 在 table 中查找
    // 使用哈希低位进行探测
    // 每个 table 包含多个 group（每个 group 8 个槽）
    // 探测方式类似 Swiss Table：
    //   - 每个槽有 1 字节控制位（7 位哈希 + 1 位占用/空）
    //   - 通过 SIMD 或位运算快速匹配
    return tbl.lookup(hash, key, ptr)
}
```

### 3.3 插入操作
```go
// src/internal/runtime/maps/map.go (简化)
func (m *Map) Insert(ptr unsafe.Pointer, key, value unsafe.Pointer) {
    // 检测并发写入
    m.writing ^= 1

    hash := m.keyHash(key, m.seed)

    if m.dirLen == 0 {
        // 小 map 插入
        if m.tryInsertSmall(hash, key, value, ptr) {
            m.writing ^= 1
            return
        }
        // 小 map 满了，需要扩容（创建 table）
        m.growSmall(hash, key, value, ptr)
        m.writing ^= 1
        return
    }

    // 正常插入
    idx := hash >> m.globalShift
    tbl := (*table)(atomic.Loadp(unsafe.Add(m.dirPtr, idx*goarch.PtrSize)))

    if tbl.insert(hash, key, value) {
        m.used++
        // 检查是否需要扩容（负载因子）
        if m.used > uint64(m.dirLen)*abi.MapGroupSlots*7/8 {
            m.grow()
        }
    }

    m.writing ^= 1
}
```

### 3.4 删除操作
```go
// src/internal/runtime/maps/map.go (简化)
func (m *Map) Delete(ptr unsafe.Pointer, key unsafe.Pointer) {
    m.writing ^= 1

    hash := m.keyHash(key, m.seed)

    if m.dirLen == 0 {
        m.deleteSmall(hash, key)
        m.writing ^= 1
        return
    }

    idx := hash >> m.globalShift
    tbl := (*table)(atomic.Loadp(unsafe.Add(m.dirPtr, idx*goarch.PtrSize)))

    if tbl.delete(hash, key) {
        m.used--
        m.tombstonePossible = true
    }

    m.writing ^= 1
}
```

### 3.5 扩容机制
```go
// src/internal/runtime/maps/map.go (简化)
func (m *Map) grow() {
    // 1. globalDepth++，目录长度翻倍
    oldDirLen := 1 << m.globalDepth
    m.globalDepth++
    m.globalShift = 64 - m.globalDepth
    newDirLen := 1 << m.globalDepth

    // 2. 创建新目录
    newDir := make([]*table, newDirLen)

    // 3. 逐个分裂旧的 table
    for i := 0; i < oldDirLen; i++ {
        oldTbl := m.dirPtrAsSlice()[i]
        // 每个 table 分裂为两个新 table
        // 根据哈希的 globalDepth 位决定去哪个新 table
        t1, t2 := oldTbl.split(m.globalDepth)
        newDir[i*2] = t1
        newDir[i*2+1] = t2
    }

    // 4. 更新目录
    m.dirPtr = unsafe.Pointer(&newDir[0])
    m.dirLen = newDirLen
}
```

**扩容 vs 旧版**：
```
旧版（hmap）：
  翻倍扩容：B++，oldbuckets 保留，逐桶迁移（evacuate）
  等量扩容：B 不变，减少溢出桶

新版（maps.Map）：
  目录加倍：globalDepth++，table 逐步分裂
  无 oldbuckets，分裂过程原子化
  小 map 优化：≤8 元素直接使用 group，无需 table
```

### 3.6 遍历随机性
```go
// 遍历时随机选择起始目录索引和组内偏移
// 保证每次遍历顺序不同
func (m *Map) Iterate(fn func(key, value unsafe.Pointer) bool) {
    // 随机选择起始点和偏移
    r := fastrand()
    startIdx := r % uint32(m.dirLen)
    offset := uint8(r >> 16)
    // 从 startIdx 开始遍历，offset 控制组内偏移
}
```

---

## 四、同步原语（sync 包）

### 4.1 sync.Mutex（Go 1.26 基于 HashTrieMap）

Go 1.26 的 `sync.Mutex` 仍然是 `Mutex` 结构体，但内部实现已使用 `internal/sync.Mutex`。

```go
// src/sync/mutex.go
type Mutex struct {
    _ noCopy
    m isync.Mutex
}

func (m *Mutex) Lock()   { m.m.Lock() }
func (m *Mutex) Unlock() { m.m.Unlock() }
```

### 4.2 sync.RWMutex
```go
type RWMutex struct {
    w           Mutex   // 写锁
    writerSem   uint32  // 写等待信号量
    readerSem   uint32  // 读等待信号量
    readerCount int32   // 读者计数（负数表示有写者等待）
    readerWait  int32   // 写者需要等待的读者数
}

func (rw *RWMutex) RLock() {
    // 原子递增 readerCount
    if atomic.AddInt32(&rw.readerCount, 1) < 0 {
        // 有写者等待，读者需要休眠
        runtime_Semacquire(&rw.readerSem)
    }
}

func (rw *RWMutex) Lock() {
    // 先获取写锁
    rw.w.Lock()
    // 等待所有读者完成
    r := atomic.AddInt32(&rw.readerCount, -rwmutexMaxReaders) + rwmutexMaxReaders
    if r != 0 && atomic.AddInt32(&rw.readerWait, r) != 0 {
        runtime_Semacquire(&rw.writerSem)
    }
}
```

### 4.3 sync.WaitGroup
```go
type WaitGroup struct {
    noCopy noCopy
    state atomic.Uint64  // 高 32 位：计数器，低 32 位：等待者
    sema  uint32
}

func (wg *WaitGroup) Add(delta int) {
    state := wg.state.Add(uint64(delta) << 32)
    v := int32(state >> 32)  // 计数器
    w := uint32(state)       // 等待者
    if v < 0 {
        panic("negative WaitGroup counter")
    }
    if v > 0 || w == 0 {
        return  // 还有任务未完成，或无等待者，直接返回
    }
    // 所有任务完成，唤醒所有等待者
    wg.state.Store(0)
    for ; w != 0; w-- {
        runtime_Semrelease(&wg.sema, false, 0)
    }
}

func (wg *WaitGroup) Done() { wg.Add(-1) }

func (wg *WaitGroup) Wait() {
    for {
        state := wg.state.Load()
        v := int32(state >> 32)
        w := uint32(state)
        if v == 0 {
            return  // 所有任务已完成
        }
        // 增加等待者计数，休眠
        if wg.state.CompareAndSwap(state, (uint64(v)<<32)|uint64(w+1)) {
            runtime_Semacquire(&wg.sema)
        }
    }
}
```

### 4.4 sync.Once
```go
type Once struct {
    done atomic.Uint32
    m    Mutex
}

func (o *Once) Do(f func()) {
    // 快速路径：已执行过，直接返回
    if o.done.Load() == 0 {
        o.doSlow(f)
    }
}

func (o *Once) doSlow(f func()) {
    o.m.Lock()
    defer o.m.Unlock()
    if o.done.Load() == 0 { // 双检锁
        defer o.done.Store(1)
        f()
    }
}
```

### 4.5 sync.Pool（ObjCache 模式）
```go
type Pool struct {
    noCopy noCopy
    local     unsafe.Pointer // per-P 的 poolLocal 数组
    localSize uintptr
    New       func() any
}

type poolLocal struct {
    poolLocalInternal
    pad [128 - unsafe.Sizeof(poolLocalInternal{})%128]byte // 缓存行填充
}

type poolLocalInternal struct {
    private any        // 仅当前 P 使用（无锁）
    shared  poolChain  // 可被其他 P 偷取
}

func (p *Pool) Get() any {
    l := p.pin()
    x := l.private
    l.private = nil
    if x == nil {
        x, _ = l.shared.popHead()
        if x == nil {
            x = p.getSlow()  // 从其他 P 偷取或调用 New
        }
    }
    runtime_procUnpin()
    return x
}
```

---

## 五、Channel 内部实现

### 5.1 hchan 结构
```go
// src/runtime/chan.go
type hchan struct {
    qcount   uint           // 队列中元素数量
    dataqsiz uint           // 缓冲区大小（0 表示无缓冲）
    buf      unsafe.Pointer // 指向缓冲区数组
    elemsize uint16
    closed   uint32
    timer    *timer         // 用于 select 超时的定时器
    elemtype *_type
    sendx    uint           // 发送索引（环形缓冲区写入位置）
    recvx    uint           // 接收索引（环形缓冲区读取位置）
    recvq    waitq          // 接收等待队列（sudog 链表）
    sendq    waitq          // 发送等待队列（sudog 链表）
    lock     mutex          // 保护 hchan 的互斥锁
    bubble   *synctestBubble
}

type sudog struct {
    g    *g          // 等待的 goroutine
    next *sudog
    prev *sudog
    elem unsafe.Pointer // 数据元素指针
    isSelect bool      // 是否在 select 中
    c    *hchan        // 关联的 channel
}
```

### 5.2 发送操作（chansend）
```go
// src/runtime/chan.go
func chansend(c *hchan, ep unsafe.Pointer, block bool, callerpc uintptr) bool {
    lock(&c.lock)

    // 1. 向已关闭的 channel 发送 → panic
    if c.closed != 0 {
        unlock(&c.lock)
        panic(plainError("send on closed channel"))
    }

    // 2. ★ 直接交给等待的接收者（recvq 不为空）
    //    不经过缓冲区，减少内存拷贝
    if sg := c.recvq.dequeue(); sg != nil {
        send(c, sg, ep, func() { unlock(&c.lock) }, 3)
        return true
    }

    // 3. 缓冲区有空位 → 放入缓冲区（环形队列）
    if c.qcount < c.dataqsiz {
        qp := chanbuf(c, c.sendx)
        typedmemmove(c.elemtype, qp, ep)
        c.sendx++
        if c.sendx == c.dataqsiz {
            c.sendx = 0  // 环形回绕
        }
        c.qcount++
        unlock(&c.lock)
        return true
    }

    // 4. 缓冲区满 → 阻塞发送者
    if !block {
        unlock(&c.lock)
        return false  // 非阻塞发送失败
    }

    // 创建 sudog，加入 sendq 等待队列
    gp := getg()
    mysg := acquireSudog()
    mysg.elem = ep
    mysg.g = gp
    mysg.c = c
    c.sendq.enqueue(mysg)

    // ★ 挂起 goroutine，等待接收者唤醒
    gopark(chanparkcommit, unsafe.Pointer(&c.lock),
        waitReasonChanSend, traceEvGoBlockSend, 2)

    // 被唤醒后继续
    // 检查 sudog 是否已被正常处理
    if mysg.releasetime > 0 {
        blockevent(mysg.releasetime-t0, 2)
    }
    mysg.c = nil
    releaseSudog(mysg)
    return true
}
```

### 5.3 接收操作（chanrecv）
```go
// src/runtime/chan.go
func chanrecv(c *hchan, ep unsafe.Pointer, block bool) (selected, received bool) {
    lock(&c.lock)

    // 1. 从已关闭且空的 channel 接收 → 返回零值
    if c.closed != 0 && c.qcount == 0 {
        unlock(&c.lock)
        if ep != nil {
            typedmemclr(c.elemtype, ep)
        }
        return true, false
    }

    // 2. ★ 从等待的发送者直接接收（sendq 不为空）
    //    不经过缓冲区，直接拷贝
    if sg := c.sendq.dequeue(); sg != nil {
        recv(c, sg, ep, func() { unlock(&c.lock) }, 3)
        return true, true
    }

    // 3. 缓冲区有数据 → 从环形缓冲区取
    if c.qcount > 0 {
        qp := chanbuf(c, c.recvx)
        if ep != nil {
            typedmemmove(c.elemtype, ep, qp)
        }
        typedmemclr(c.elemtype, qp) // 清空缓冲区槽
        c.recvx++
        if c.recvx == c.dataqsiz {
            c.recvx = 0
        }
        c.qcount--
        unlock(&c.lock)
        return true, true
    }

    // 4. 缓冲区空 → 阻塞接收者
    if !block {
        unlock(&c.lock)
        return false, false
    }

    // 创建 sudog，加入 recvq 等待队列
    gp := getg()
    mysg := acquireSudog()
    mysg.elem = ep
    mysg.g = gp
    mysg.c = c
    c.recvq.enqueue(mysg)
    gopark(chanparkcommit, unsafe.Pointer(&c.lock),
        waitReasonChanReceive, traceEvGoBlockRecv, 2)

    // 被唤醒后
    if mysg.releasetime > 0 {
        blockevent(mysg.releasetime-t0, 2)
    }
    return true, true
}
```

### 5.4 select 实现
```go
// src/runtime/select.go
func selectgo(cas0 *scase, order0 *uint16, pc0 *uintptr, ncases int, block bool) int {
    // 1. 生成轮询顺序和加锁顺序
    pollorder = order0[:ncases:ncases]
    lockorder = order0[ncases:][:ncases:ncases]

    // 2. 按 channel 地址排序（加锁顺序，避免死锁）
    // 3. 加锁所有 channel
    // 4. 遍历 pollorder（随机顺序），检查是否有 case 可执行
    // 5. 如果没有，将所有 sudog 注册到对应 channel 的等待队列
    // 6. gopark 挂起
    // 7. 被唤醒后，从所有等待队列中移除
    // 8. 解锁所有 channel
    // 9. 返回选中的 case 索引
}
```

---

## 六、GC 垃圾回收

### 6.1 GC 阶段
```
1. 标记准备（Mark Setup）    — STW，开启写屏障
2. 并发标记（Concurrent Mark）— 并发，三色标记
3. 标记终止（Mark Termination）— STW，关闭写屏障
4. 并发清扫（Concurrent Sweep）— 并发，回收白色对象
```

### 6.2 GC 触发条件
```go
// src/runtime/mgc.go
func (t gcTrigger) test() bool {
    if !memstats.enablegc || panicking.Load() != 0 || gcphase != _GCoff {
        return false
    }
    switch t.kind {
    case gcTriggerHeap:
        // 堆大小达到阈值
        trigger, _ := gcController.trigger()
        return gcController.heapLive.Load() >= trigger
    case gcTriggerTime:
        // 超过 forcegcperiod（约 2 分钟）未 GC
        if gcController.gcPercent.Load() < 0 {
            return false
        }
        lastgc := int64(atomic.Load64(&memstats.last_gc_nanotime))
        return lastgc != 0 && t.now-lastgc > forcegcperiod
    case gcTriggerCycle:
        // 手动触发 runtime.GC()
        return int32(t.n-work.cycles.Load()) > 0
    }
    return true
}
```

**GC 控制器**：`gcController.trigger()` 使用目标堆增长率计算触发阈值。

### 6.3 三色标记 + 混合写屏障
```go
// 写屏障（在指针写入时由编译器插入）
// 混合写屏障（Go 1.8+）：
// 对旧值置灰（删除屏障）+ 对新值置灰（插入屏障）
//
// 在 GC 标记阶段（gcphase == _GCmark）时启用
// 主要开销约 5-15%

// src/runtime/mbarrier.go
func writeBarrierEnabled() bool {
    return gcphase == _GCmark || gcphase == _GCmarktermination
}

// 写屏障实现（简化）
func writePointer(slot *unsafe.Pointer, ptr unsafe.Pointer) {
    if writeBarrierEnabled() {
        // 1. 对旧值置灰（删除屏障）
        shade(*slot)
        // 2. 对新值置灰（插入屏障）
        shade(ptr)
    }
    // 3. 实际写入
    *slot = ptr
}
```

### 6.4 GC 辅助（GC Assist）
```go
// 分配内存时，如果 GC 辅助额度不足，需要协助 GC 标记
// 确保"标记速度 ≥ 分配速度"

func deductAssistCredit(size uintptr) {
    gp := getg()
    if gp.gcAssistBytes < int64(size) {
        // 额度不足，强制协助 GC
        gcAssistAlloc(gp)
    }
    gp.gcAssistBytes -= int64(size)
}
```

---

## 七、内存分配器

### 7.1 分级架构
```
mcache（每 P 一个，无锁分配）
    ↓ mcache 无空闲 span 时
mcentral（每个 size class 一个，加锁分配）
    ↓ mcentral 无空闲 span 时
mheap（全局堆，从 OS 申请）
    ↓ 大对象（> 32KB）
直接分配多页（largeAlloc）
```

### 7.2 Size Class 系统
```go
// src/internal/runtime/gc/sizeclasses.go
const NumSizeClasses = 68  // 0 ~ 67, 0 是空闲

// class 1: 8 bytes → 1 页 1024 个对象
// class 2: 16 bytes → 1 页 512 个对象
// ...
// class 67: 32768 bytes → 1 页 1 个对象

const MaxSmallSize = 32768  // 32KB
const TinySize = 16         // 16 字节以下的 tiny 对象
```

### 7.3 分配流程
```go
// src/runtime/malloc.go
func mallocgc(size uintptr, typ *_type, needzero bool) unsafe.Pointer {
    // 1. 检查 GC 辅助额度
    deductAssistCredit(size)

    // 2. 小对象分配（≤ 32KB）
    if size <= maxSmallSize {
        if size <= maxTinySize && typ == nil {
            // ★ Tiny Allocator：≤ 16 字节，合并到 16 字节槽
            // 减少内存碎片和 GC 扫描压力
            return mallocgcTiny(mcache, size)
        }
        // 按 size class 分配
        c := mcache
        span := c.alloc[sizeclass]
        if span.freeindex == span.nelems {
            // mcache 无空闲，从 mcentral 获取新 span
            span = c.refill(sizeclass)
        }
        return span.allocFromCache()
    }

    // 3. 大对象分配（> 32KB）
    span := mheap.allocLarge(npages)
    return span.base()
}
```

### 7.4 mspan 结构
```go
type mspan struct {
    next       *mspan      // 双向链表
    prev       *mspan
    startAddr  uintptr     // 起始页地址
    npages     uintptr     // 页数
    spanclass  spanClass   // size class
    state      mSpanStateBox // mSpanInUse/mSpanFree/mSpanManual
    nelems     uintptr     // 总对象数
    freeindex  uintptr     // 下一个空闲对象的索引
    allocBits  *gcBits     // 分配位图
    gcmarkBits *gcBits     // GC 标记位图
    allocCache uint64      // 分配缓存（加速小对象分配）
}
```

---

> **总结**：Go 1.26 的运行时通过 GMP 调度器（sysmon + work stealing + 信号式抢占）实现高效并发，通过三色标记 + 混合写屏障实现低延迟 GC，通过分级内存分配器（mcache → mcentral → mheap）实现高性能内存管理，通过 Swiss Table 风格的新 map 实现更优的哈希性能。理解这些源码细节是成为 Go 高级开发者的关键。