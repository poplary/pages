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
    stackguard0 uintptr     // 栈扩张检测阈值
    stackguard1 uintptr     // systemstack 栈增长阈值
    m           *m          // 当前绑定的 M
    sched       gobuf       // 调度上下文（sp, pc, bp, ret）
    atomicstatus atomic.Uint32 // 状态：_Gidle, _Grunnable, _Grunning, _Gsyscall, _Gwaiting
    goid         uint64
    preempt       bool      // 抢占信号
    preemptStop   bool      // 抢占时转为 _Gpreempted
    preemptShrink bool      // 在同步安全点收缩栈
    param        unsafe.Pointer
    labels       unsafe.Pointer // 分析标签
    timer        *timer      // 与 G 关联的定时器
    parkingOnChan atomic.Bool // 是否即将在 channel 上 park
    tracking     bool        // 是否在追踪调度延迟
    runnableTime int64       // 可运行状态的累计时间
    gcAssistBytes int64      // GC 辅助额度
    xRegs        xRegPerG   // 异步抢占时的扩展寄存器状态
    bubble       *synctestBubble
}

type gobuf struct {
    sp   uintptr  // 栈指针
    pc   uintptr  // 程序计数器
    bp   uintptr  // 基址指针（Go 1.7+）
    ret  uintptr  // 系统调用的返回值
    lr   uintptr  // 链接寄存器
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
    g0      *g      // 系统栈上的 goroutine（执行调度、栈扩张）
    curg    *g      // 当前运行的用户 goroutine
    p       puintptr // 关联的 P（没有时为 nil）
    tls     [tlsSlots]uintptr // 线程本地存储
    spinning bool   // 是否在自旋寻找可运行的 G
    blocked  bool    // M 是否阻塞在 note 上
    park     note   // 挂起/唤醒机制
    mstartfn func()
    procid   uint64  // 调试用的线程 ID
    allpSnapshot []*p // 在 release P 后用于 findRunnable 的 allp 快照
    mWaitList     mWaitList // 运行时锁等待列表
    ditEnabled    bool      // 是否启用 DIT
    cgoCallersUse atomic.Uint32
}
```

- **M0**：Go 程序启动时的第一个线程，也称为主线程
- **g0**：每个 M 有自己的 g0，用于执行调度、栈扩张、GC 等运行时任务
- 一个 M 对应一个 OS 线程，通常不会超过 10000 个

#### 1.1.3 P（Processor — 逻辑处理器）
```go
type p struct {
    id          int32
    status      uint32 // _Pidle, _Prunning, _Psyscall, _Pgcstop, _Pdead
    m           muintptr       // 关联的 M（没有时为 nil）
    mcache      *mcache        // 每个 P 的内存缓存（无锁分配）
    pcache      pageCache      // 页缓存
    runqhead    uint32         // 本地可运行队列头部
    runqtail    uint32         // 本地可运行队列尾部
    runq        [256]guintptr  // 本地可运行队列（环形数组）
    runnext     guintptr       // 下一个要运行的 G（优先级最高）
    sudogcache  []*sudog
    gcw         gcWork         // GC 标记工作缓冲区
    wbBuf       wbBuf          // 写屏障缓冲区
    goidcache    uint64         // goroutine ID 缓存
    goidcacheend uint64
    gFree        gList          // 空闲 G 列表
    deferpool    []*_defer      // defer 结构体池
    mspancache   struct { len int; buf [128]*mspan } // mspan 缓存
    pinnerCache  *pinner        // Pinner 对象缓存
    oldm         mWeakPointer   // 之前运行的 M
}
```

**GOMAXPROCS** 控制 P 的数量，默认等于 CPU 核数。P 是 G 能运行的关键——没有 P 的 G 无法被调度。

### 1.2 调度循环

#### 1.2.1 schedule() — 核心调度函数
```go
// src/runtime/proc.go
func schedule() {
    _g_ := getg()  // 当前 g0

top:
    // 检查是否需要 GC（抢占式调度触发点之一）
    if sched.gcwaiting.Load() {
        gcstopm()
        goto top
    }

    var gp *g
    // 1. 每 61 次调度检查一次全局队列（公平性）
    if gp == nil && sched.runqsize.Load() != 0 {
        if _g_.m.p.ptr().schedtick%61 == 0 {
            gp = globrunqget(_g_.m.p.ptr(), 1)
        }
    }

    // 2. 先尝试 runnext（最高优先级）
    if gp == nil {
        gp, _ = runqget(_g_.m.p.ptr())
    }

    // 3. 尝试从本地队列、全局队列、网络轮询、其他 P 偷取
    if gp == nil {
        gp = findRunnable() // 阻塞式查找
    }

    // 4. 执行找到的 G
    execute(gp)
}
```

**关键调度点**：
- 函数调用（编译器插入的栈检查）
- GC 的栈扫描
- 阻塞系统调用
- time.Sleep
- channel 操作
- mutex 操作
- `runtime.Gosched()`
- 抢占信号（`SIGURG`，Go 1.14+）

#### 1.2.2 execute() — 执行切换
```go
func execute(gp *g) {
    _g_ := getg()
    _g_.m.curg = gp
    gp.m = _g_.m
    casgstatus(gp, _Grunnable, _Grunning) // 状态切换
    gp.waitsince = 0
    gp.preempt = false
    gp.stackguard0 = gp.stack.lo + _StackGuard // 重置栈检查
    _g_.m.p.ptr().schedtick++

    // 汇编实现：将 g 的 sched 上下文恢复到 CPU 寄存器
    gogo(&gp.sched)
}
```

### 1.3 工作窃取（Work Stealing）

当 P 从本地队列找不到可运行的 G 时，进入 `findRunnable()`：

```go
func findRunnable() (gp *g, inheritTime, tryWakeP bool) {
    _g_ := getg()
top:
    // 1. 检查 GC 和追踪
    if sched.gcwaiting.Load() { ... }

    // 2. 从本地队列取
    if gp, inheritTime := runqget(_g_.m.p.ptr()); gp != nil {
        return gp, inheritTime
    }

    // 3. 从全局队列取
    if sched.runqsize.Load() != 0 {
        if gp := globrunqget(_g_.m.p.ptr(), 0); gp != nil {
            return gp, false
        }
    }

    // 4. 从网络轮询器获取
    if netpollinited() && netpollWaiters.Load() > 0 {
        if list := netpoll(0); !list.empty() {
            // 把从 netpoll 拿到的 G 放入本地队列
            gp := list.pop()
            injectglist(&list)
            return gp, false
        }
    }

    // 5. 尝试窃取其他 P 的 G（核心）
    if !_g_.m.spinning {
        _g_.m.spinning = true
        sched.nmspinning.Add(1)
    }

    for i := 0; i < 4; i++ {
        // 随机遍历 P，尝试偷取一半的任务
        for enum := stealOrder.start(fastrand()); !enum.done(); enum.next() {
            if gp := runqsteal(_g_.m.p.ptr(), allp[enum.position()], stealTimers); gp != nil {
                return gp, false
            }
        }
    }

    // 6. 没有找到任何 G，M 休眠
    stopm()
    goto top
}
```

**窃取顺序**：随机顺序遍历，避免窃取热点。每次偷取一半的 G（`runqgrabs` 取 128 个中的一半）。

### 1.4 抢占式调度（Go 1.14+）

**基于信号的异步抢占**：
```
1. 后台监控线程 sysmon 检测到 G 运行超过 10ms
2. 设置 G 的 preempt 标记和 stackguard0
3. 发送 SIGURG 信号给目标 M
4. 信号处理函数 sigtramp 在安全点保存上下文
5. G 在下一个函数调用时被调度器抢占
```

```go
// src/runtime/proc.go - sysmon
func sysmon() {
    for {
        usleep(20 * 1000) // 20us 轮询一次
        lastpoll := sched.lastpoll.Load()
        if lastpoll != 0 {
            // 检查是否超过 10ms
            if now - _g_.m.p.ptr().syscalltick > 10e6 {
                // 发出抢占请求
                preemptone(_g_.m.p.ptr())
            }
        }
    }
}
```

**安全点**：函数调用、栈检查、GC 安全点。非安全点处（如寄存器密集型循环）的抢占需等待到达安全点。

### 1.5 系统调用处理

**同步系统调用**（如文件 I/O）：
```
G 运行 → 系统调用 → P 与 M 解绑 → P 去找其他 M 或空闲 M
                     ↓
                  M 在 _Gsyscall 状态等待
                     ↓
                  syscall 返回 → G 变为 _Grunnable → 尝试重新绑定 P 或放入全局队列
```

**异步系统调用**（网络 I/O）：
```
goroutine 调用 net.Read → 注册 fd 到 epoll → G 进入 _Gwaiting
                                                ↓
                                            sysmon 或 findRunnable 时
                                            netpoll 检查就绪 fd
                                                ↓
                                            G 变为 _Grunnable → 被调度
```

### 1.6 网络轮询器（Netpoller）

```
┌─────────────────────────────────────────────────────┐
│                 Go Netpoller (netpoll_epoll.go)       │
│                                                       │
│  goroutine A ──→ net.Read ──→ epoll_wait ──→ 阻塞     │
│                                                       │
│  sysmon / findRunnable:                               │
│    └─→ netpoll() ──→ epoll_wait(0) ──→ 就绪 fd       │
│         └─→ 唤醒对应的 goroutine 放入 runq            │
└─────────────────────────────────────────────────────┘
```

- Linux 使用 `epoll`，macOS 使用 `kqueue`，Windows 使用 `IOCP`
- 非阻塞 I/O，避免每个 goroutine 消耗一个 OS 线程

---

## 二、内存管理

### 2.1 整体架构

```
┌──────────────────────────────────────────────────┐
│                    mheap                          │
│  (全局堆，从 OS 申请大块内存，page 为单位)          │
│  ┌────────────────────────────────────────────────┐│
│  │  mcentral [136]   每个 size class 一个 central  ││
│  │  ┌──────────┐  ┌──────────┐                   ││
│  │  │ span 0   │  │ span 1   │  ...               ││
│  │  └──────────┘  └──────────┘                   ││
│  │  arenas []heapArena  (64MB 一个 arena)         ││
│  │  cenSpans []*mspan                              ││
│  └────────────────────────────────────────────────┘│
└──────────────────────────────────────────────────┘
         ↑ 从 mcentral 获取/归还 span
┌──────────────────────────────────────────────────┐
│                  mcache (每 P 一个)                │
│  ┌────────────────────────────────────────────────┐│
│  │  alloc [136]  tiny [0]                         ││
│  │  ┌──────┐ ┌──────┐ ┌──────┐                  ││
│  │  │span 0│ │span 1│ │span 2│ ...              ││
│  │  └──────┘ └──────┘ └──────┘                  ││
│  │  tiny allocator: 16 字节以下小对象              ││
│  └────────────────────────────────────────────────┘│
└──────────────────────────────────────────────────┘
         ↑ 无锁分配（mcache 私有）
         ↓ mcache 无空闲 span 时从 mcentral 获取
```

### 2.2 内存管理单元

#### Span（mspan）
```go
type mspan struct {
    next       *mspan        // 双向链表
    prev       *mspan
    startAddr  uintptr       // 起始页地址
    npages     uintptr       // 页数
    spanclass  spanClass     // size class
    state      mSpanStateBox // mSpanInUse / mSpanFree / mSpanManual
    nelems     uintptr       // 总对象数
    freeindex  uintptr       // 下一个空闲对象的索引
    allocBits  *gcBits       // 分配位图
    gcmarkBits *gcBits       // GC 标记位图
    allocCache uint64        // 分配缓存（加速小对象分配）
    central    *mcentral     // 所属的 central
}
```

**三种状态**：
- `mSpanInUse`：正在被使用
- `mSpanFree`：空闲，在 mheap 的 free list 中
- `mSpanManual`：手动管理（如栈内存）

#### Size Class 系统

Go 有 68 种 size class（`NumSizeClasses = 68`），覆盖 0 ~ 32KB：

```go
// src/internal/runtime/gc/sizeclasses.go (生成文件)
// class  bytes/obj  bytes/span  objects  tail waste  max waste  min align
//     1          8        8192     1024           0     87.50%          8
//     2         16        8192      512           0     43.75%         16
//     3         24        8192      341           8     29.24%          8
//     4         32        8192      256           0     21.88%         32
//     5         48        8192      170          32     31.52%         16
//   ...
//    44       4096        8192        2           0     15.60%       4096
//   ...
//    67      32768        8192        1           0      4.88%      32768
```

**规则**：
- 对象 ≤ 32KB：按 size class 分配
- 对象 > 32KB：直接分配多个页（`largeAlloc`）
- 对象 ≤ 16B：使用 tiny allocator

#### 页（Page）

- 每页 8KB（`_PageSize = 8192`）
- 多页组成一个 span
- 从 OS 申请的最小单位是 64KB（`_HeapAllocChunk = 64 << 10`）

### 2.3 分配流程

```go
// src/runtime/malloc.go
func mallocgc(size uintptr, typ *_type, needzero bool) unsafe.Pointer {
    // 1. 判断是否需要辅助 GC
    assistG := deductAssistCredit(size)

    // 2. 小对象分配
    if size <= maxSmallSize {
        if size <= maxTinySize {  // ≤ 16B
            // Tiny allocator
            return mallocgc_tiny(mcache, size)
        }
        // 按 size class 分配
        span := mcache.alloc[sizeclass]
        if span.freeindex == span.nelems {
            // mcache 无空闲，从 mcentral 获取
            span = mcache.refill(sizeclass)
        }
        return span.allocFromCache()
    }

    // 3. 大对象分配（> 32KB）
    span := mheap.allocLarge(npages)
    return span.base()
}
```

#### Tiny Allocator
- 处理 ≤ 16 字节的对象
- 将多个小对象合并到一个 16 字节槽中
- 减少了内存碎片和 GC 扫描压力
- 限制：不能包含指针（否则指针追踪会出问题）

```go
func mallocgc_tiny(mcache *mcache, size uintptr) unsafe.Pointer {
    tiny := mcache.tiny
    // 如果当前 tiny 块剩余空间够用
    if tiny.size+size <= maxTinySize {
        pos := tiny.base + tiny.size
        tiny.size += size
        memclrNoHeapPointers(pos, size)
        return pos
    }
    // 不够，分配新的 tiny 块
    span := mcache.alloc[tinySizeClass]
    tiny = span.allocFromCache()
    return tiny.base
}
```

### 2.4 内存释放与 Scavenger

- **Scavenger**（`runtime.(*scavengerState).run`）：后台 goroutine
- 将空闲 span 的物理内存归还给 OS
- 触发条件：`mheap_.scavengeGoal` 目标触发
- 使用 `madvise(MADV_DONTNEED)` 通知内核

### 2.5 内存分配器优化

| 优化 | 说明 |
|------|------|
| **无锁分配** | 小对象直接从 mcache 分配，无需锁 |
| **零拷贝分配** | 使用 `memclrNoHeapPointers` 快速清零 |
| **预分配** | mheap 从 OS 预申请 64KB 块 |
| **页缓存** | 每 P 的 pageCache 缓存 64 页 |
| **批量申请** | mcentral 一次申请多个 span |

---

## 三、垃圾回收（GC）

### 3.1 GC 概述

**GC 参数**：
- `GOGC=100`（默认）：堆增长 100% 触发 GC
- `GOMEMLIMIT`：Go 1.19+ 硬内存限制
- `gcPercent = 100`：触发阈值 = 上次存活堆大小 × (gcPercent / 100)

**GC 阶段**：
```
1. 标记准备（Mark Setup）     — STW，开启写屏障
2. 并发标记（Concurrent Mark）— 并发，三色标记
3. 标记终止（Mark Termination）— STW，关闭写屏障
4. 并发清扫（Concurrent Sweep）— 并发，回收白色对象
```

### 3.2 三色标记算法

```
白色：未标记的对象（候选清除）
灰色：已标记，但其引用的对象尚未扫描完
黑色：已标记，且所有引用对象都已扫描完
```

**算法流程**：
```
1. 初始状态：所有对象为白色
2. GC root 标记为灰色（根集合：全局变量、goroutine 栈、寄存器）
3. 从灰色集合取出对象，标记为黑色
4. 扫描黑色对象的所有引用，标记为灰色
5. 重复 3-4，直到灰色集合为空
6. 剩余白色对象为不可达，释放
```

**GC root 集合**：
- 全局变量（.data, .bss 段）
- goroutine 栈上的指针
- 寄存器中的指针
- 运行时数据结构中的指针

### 3.3 写屏障（Write Barrier）

**为什么需要写屏障？**
并发标记期间，程序可能修改对象的引用关系，导致把已标记为黑色的对象指向新分配的白色对象，造成对象被误回收。

**混合写屏障（Go 1.8+）**：
```
// 在指针写入时执行
writePointer(slot, ptr):
    // 1. 对旧值：置灰（删除屏障）
    shade(*slot)
    // 2. 对新值：置灰（插入屏障）
    shade(ptr)
    // 3. 实际写入
    *slot = ptr
```

**写屏障触发时机**：只在 GC 标记阶段开启（`gcphase == _GCmark`）

**写屏障的开销**：约 5-15% 的性能影响

### 3.4 GC 辅助机制（GC Assist）

```
mutator(应用) 分配内存 → 累计到一定量 → 必须协助 GC 完成标记工作
                    ↓
            mutator_assist() ← 扫灰色对象
                    ↓
                完成标记工作量
                    ↓
                继续分配
```

**GC 辅助设计**：让分配内存的应用 goroutine 也参与 GC 标记工作，确保"标记速度 ≥ 分配速度"。

```go
func deductAssistCredit(size uintptr) *g {
    // 如果没有足够的辅助额度
    if _g_.m.assistBytes < int64(size) {
        gcAssistAlloc(_g_)  // 强制协助
    }
    _g_.m.assistBytes -= int64(size)
    return _g_
}
```

### 3.5 GC 触发条件

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

**触发阈值计算**：
```go
// gcController.trigger() 使用目标堆增长率
// 目标堆大小 = 上次 GC 后的存活堆 + 存活堆 × GOGC/100
trigger, _ := gcController.trigger()
// 默认 GOGC=100，所以目标堆 ≈ 2 × 上次存活堆

// gcController.heapLive 是当前存活堆大小
```

### 3.6 GC 调优

```go
// 1. 调整 GOGC
GOGC=100  // 默认，堆增长 100% 触发
GOGC=200  // 减少 GC 频率，增加内存使用
GOGC=off  // 关闭 GC（不推荐生产使用）

// 2. 硬限制（Go 1.19+）
GOMEMLIMIT=4GiB  // 软限制，GC 尽力不超过此值

// 3. 设置内存限制（代码中）
debug.SetMemoryLimit(4 << 30)

// 4. 查看 GC 统计
var m runtime.MemStats
runtime.ReadMemStats(&m)
// m.NumGC, m.PauseTotalNs, m.LastGC, m.PauseNs
```

**GC 调优指标**：
| 指标 | 说明 | 理想值 |
|------|------|--------|
| GC 频率 | 每分钟 GC 次数 | 低频为好 |
| GC 暂停时间 | 每次 STW 时间 | < 100us |
| GC CPU 占比 | GC 占总 CPU 百分比 | < 5% |
| 堆增长率 | 每次 GC 后堆增长 | 接近 GOGC 设置 |

### 3.7 内存泄漏排查

**工具链**：
```bash
# 内存剖析
go tool pprof -http=:8080 http://localhost:6060/debug/pprof/heap
go tool pprof -http=:8080 http://localhost:6060/debug/pprof/heap?gc=1

# 实时追踪
go tool trace trace.out
```

**常见泄漏场景**：
1. **goroutine 泄漏**：channel 阻塞未退出，导致其栈上对象无法释放
2. **字符串截取**：`s[:5]` 保留整个底层数组的引用
3. **time.Ticker 未 Stop**：Ticker 的 channel 和 goroutine 无法回收
4. **finalizer 泄露**：`SetFinalizer` 导致对象延迟释放
5. **map 内存不释放**：map 的 bucket 不会自动缩容

---

## 四、Goroutine 栈管理

### 4.1 栈初始大小

```go
// src/runtime/stack.go
_StackMin = 2048 // 2KB（Go 1.4 之前是 4KB）
```

### 4.2 栈扩容

```go
// 栈空间不足时，在函数调用入口触发
func newstack() {
    // 1. 计算新栈大小 = 旧栈大小 × 2
    oldsize := gp.stack.hi - gp.stack.lo
    newsize := oldsize * 2

    // 2. 上限检查
    if newsize > _MaxStackSize { // 1GB
        throw("stack overflow")
    }

    // 3. 分配新栈
    newstack := stackalloc(uint32(newsize))

    // 4. 复制旧栈内容到新栈（含栈帧）
    gp.stack = newstack
    // 调整栈上的指针（调整所有栈帧中的指针）
    // 关键：copystack 负责调整所有栈上指针
    copystack(gp, newsize)
}
```

**栈扩容触发**：`stackguard0` 检查通过时，在 `morestack_noctxt` 汇编函数中调用 `newstack`。

### 4.3 栈收缩

```go
// GC 栈扫描时检查是否可收缩
func shrinkstack(gp *g) {
    oldsize := gp.stack.hi - gp.stack.lo
    // 如果栈使用率 < 25%，且栈大小 > _StackMin
    if used := gp.stack.hi - gp.stack.lo; used < oldsize/4 && oldsize > _StackMin {
        newsize := oldsize / 2
        if newsize < _StackMin {
            newsize = _StackMin
        }
        copystack(gp, newsize)
    }
}
```

### 4.4 栈复制细节

**关键问题**：栈复制后，goroutine 栈上的所有指针需要调整（因为栈地址变了）。

```go
func copystack(gp *g, newsize uintptr) {
    old := gp.stack
    // 1. 分配新栈
    new := stackalloc(uint32(newsize))

    // 2. 复制栈帧
    memmove(unsafe.Pointer(new.lo), unsafe.Pointer(old.lo), old.hi-old.lo)

    // 3. 调整栈上指针（核心）
    // 遍历所有栈帧，找到其中的指针，调整偏移量
    gentraceback(^uintptr(0), ^uintptr(0), 0, gp, 0, nil, 0x100)

    // 4. 更新 g 的栈指针
    gp.stack = new
    gp.stackguard0 = new.lo + _StackGuard
}
```

---

## 五、Channel 内部实现

### 5.1 hchan 结构体

```go
type hchan struct {
    qcount   uint           // 队列中元素数量
    dataqsiz uint           // 循环队列大小（缓冲区大小）
    buf      unsafe.Pointer // 指向缓冲区数组
    elemsize uint16         // 元素大小
    closed   uint32         // 是否已关闭
    elemtype *_type         // 元素类型
    sendx    uint           // 发送索引
    recvx    uint           // 接收索引
    recvq    waitq          // 接收等待队列（sudog 链表）
    sendq    waitq          // 发送等待队列（sudog 链表）
    lock     mutex          // 保护 hchan 的互斥锁
}
```

**sudog 结构**：
```go
type sudog struct {
    g    *g
    next *sudog
    prev *sudog
    elem unsafe.Pointer  // 数据元素指针
    isSelect bool
    c    *hchan  // 关联的 channel
}
```

### 5.2 发送操作

```go
// c <- x
func chansend(c *hchan, ep unsafe.Pointer, block bool, callerpc uintptr) bool {
    lock(&c.lock)

    // 1. 向 nil channel 发送 → 永久阻塞
    if c == nil {
        gopark(nil, nil, waitReasonChanSendNilChan, traceEvGoStop, 2)
        return false
    }

    // 2. 向已关闭的 channel 发送 → panic
    if c.closed != 0 {
        unlock(&c.lock)
        panic(plainError("send on closed channel"))
    }

    // 3. 尝试直接交给等待的接收者（recvq 不为空）
    if sg := c.recvq.dequeue(); sg != nil {
        // 直接发送，不经过缓冲区
        send(c, sg, ep, func() { unlock(&c.lock) }, 3)
        return true
    }

    // 4. 缓冲区有空位 → 放入缓冲区
    if c.qcount < c.dataqsiz {
        qp := chanbuf(c, c.sendx)
        typedmemmove(c.elemtype, qp, ep)
        c.sendx++
        if c.sendx == c.dataqsiz {
            c.sendx = 0
        }
        c.qcount++
        unlock(&c.lock)
        return true
    }

    // 5. 缓冲区满 → 阻塞发送者
    gp := getg()
    mysg := acquireSudog()
    mysg.elem = ep
    mysg.g = gp
    mysg.c = c
    c.sendq.enqueue(mysg)
    gopark(chanparkcommit, unsafe.Pointer(&c.lock),
        waitReasonChanSend, traceEvGoBlockSend, 2)
    // 唤醒后继续
    return true
}
```

### 5.3 接收操作

```go
// <- c
func chanrecv(c *hchan, ep unsafe.Pointer, block bool) (selected, received bool) {
    lock(&c.lock)

    // 1. 从 nil channel 接收 → 永久阻塞
    if c == nil {
        gopark(nil, nil, waitReasonChanReceiveNilChan, traceEvGoStop, 2)
        return
    }

    // 2. 从已关闭且空的 channel 接收 → 返回零值
    if c.closed != 0 && c.qcount == 0 {
        unlock(&c.lock)
        if ep != nil {
            typedmemclr(c.elemtype, ep)
        }
        return true, false
    }

    // 3. 尝试从等待的发送者直接接收
    if sg := c.sendq.dequeue(); sg != nil {
        recv(c, sg, ep, func() { unlock(&c.lock) }, 3)
        return true, true
    }

    // 4. 缓冲区有数据 → 从缓冲区取
    if c.qcount > 0 {
        qp := chanbuf(c, c.recvx)
        if ep != nil {
            typedmemmove(c.elemtype, ep, qp)
        }
        typedmemclr(c.elemtype, qp)
        c.recvx++
        if c.recvx == c.dataqsiz {
            c.recvx = 0
        }
        c.qcount--
        unlock(&c.lock)
        return true, true
    }

    // 5. 缓冲区空 → 阻塞接收者
    gp := getg()
    mysg := acquireSudog()
    mysg.elem = ep
    mysg.g = gp
    c.recvq.enqueue(mysg)
    gopark(chanparkcommit, unsafe.Pointer(&c.lock),
        waitReasonChanReceive, traceEvGoBlockRecv, 2)
    return true, true
}
```

### 5.4 select 的实现

**select 的五个阶段**：
1. **加锁所有 channel**（按地址顺序，避免死锁）
2. **轮询 case**：检查是否有 case 可执行
3. **注册到所有 channel 的等待队列**
4. **挂起 goroutine**
5. **被唤醒后，从等待队列中移除**，解锁所有 channel

```go
// src/runtime/select.go
func selectgo(cas0 *scase, order0 *uint16, pc0 *uintptr, ncases int, block bool) int {
    // 1. 生成随机遍历顺序和加锁顺序
    pollorder = order0[:ncases:ncases]
    lockorder = order0[ncases:][:ncases:ncases]

    // 2. 按地址排序 channel（加锁顺序）
    // 3. 加锁所有 channel
    // 4. 遍历 pollorder，检查是否有 case 可执行
    // 5. 如果没有，注册到所有 channel 的等待队列
    // 6. gopark 挂起
    // 7. 被唤醒后，解锁并返回
}
```

**select 的随机性**：`pollorder` 随机打乱，保证公平性。

---

## 六、Mutex 内部实现

### 6.1 mutex 结构

```go
// src/runtime/runtime2.go
type mutex struct {
    lockRankStruct
    key uintptr  // Futex 实现：uint32 key；Sema 实现：M* waitm
}
```

### 6.2 正常模式 vs 饥饿模式

| 模式 | 条件 | 行为 |
|------|------|------|
| **正常模式** | 默认 | 新等待者自旋 + 信号量等待，可能有公平性问题 |
| **饥饿模式** | 等待者等待 > 1ms | 直接交给队列头的等待者，禁止自旋 |

**自旋条件**：
```go
func sync_runtime_canSpin(i int) bool {
    // 1. 自旋次数 < 4
    // 2. 多核 CPU
    // 3. GOMAXPROCS > 1
    // 4. P 的本地队列为空
    return i < active_spin && ncpu > 1 && gomaxprocs > 1
         && sched.runqsize == 0 && runqempty(_g_.m.p.ptr())
}
```

### 6.3 Lock 实现（Go 1.26 基于 Futex/Sema）

Go 1.26 的 `mutex` 使用统一 `key` 字段：Futex 模式下为 `uint32` 计数器，Sema 模式下为 `M* waitm`。锁状态通过 `key` 的低位标记：第 1 位表示锁定，第 2 位表示有等待者，第 3 位表示饥饿模式。

```go
// src/runtime/lock_futex.go (Linux) / lock_sema.go (其他平台)
func (m *Mutex) Lock() {
    // 快速路径：CAS 尝试获取锁（key 从 0 → 1）
    if atomic.Cas(key32(&m.key), 0, 1) {
        return
    }
    // 慢路径：自旋 + 锁
    m.lockSlow()
}

func (m *Mutex) lockSlow() {
    var waitStartTime int64
    starving := false
    awoke := false
    iter := 0
    old := atomic.Load(key32(&m.key))

    for {
        // 1. 自旋：尝试获取已被锁定但未饥饿的锁
        if old&(mutexLocked|mutexStarving) == mutexLocked && canSpin(iter) {
            procyield(activeSpinCnt)
            iter++
            old = atomic.Load(key32(&m.key))
            continue
        }

        // 2. 设置新状态
        new := old
        if old&mutexStarving == 0 {
            new |= mutexLocked  // 尝试获取锁
        }
        if old&(mutexLocked|mutexStarving) != 0 {
            new += mutexWaiterShift  // 等待者计数 +1
        }

        // 3. CAS 更新
        if atomic.Cas(key32(&m.key), old, new) {
            if old&(mutexLocked|mutexStarving) == 0 {
                break  // 成功获取锁
            }
            // 4. 休眠等待
            m.lockUnlockSleep()
            starving = starving || nanotime()-waitStartTime > starvationThresholdNs
            old = atomic.Load(key32(&m.key))
            if old&mutexStarving != 0 {
                // 饥饿模式：直接获取
                delta := int32(mutexLocked - 1<<mutexWaiterShift)
                old := atomic.Xadd(key32(&m.key), delta)
                break
            }
            awoke = true
            iter = 0
        } else {
            old = atomic.Load(key32(&m.key))
        }
    }
}
```

---

## 七、sync.Map 内部实现

### 7.1 数据结构

```go
type Map struct {
    mu     Mutex
    read   atomic.Value  // readOnly 结构（无锁读）
    dirty  map[any]*entry // 脏数据（有锁）
    misses int            // 读 miss 计数
}

type readOnly struct {
    m       map[any]*entry
    amended bool  // read 是否落后于 dirty
}

type entry struct {
    p unsafe.Pointer // *interface{} 或 nil
}
```

### 7.2 读写分离策略

**读操作**：先读 read map（无锁），如果找到且 entry 未被删除则直接返回。如果 read 中没有且 amended=true，则加锁读 dirty。

**写操作**：如果 key 在 read 中且 entry 可用，CAS 更新（无锁）。否则加锁写 dirty，并将 dirty 提升为 read（当 miss 达到阈值时）。

**Dirty 提升**：当从 read 读 miss 的次数达到 dirty 的 key 数量时，dirty 提升为新的 read。

```go
func (m *Map) Load(key any) (value any, ok bool) {
    read, _ := m.read.Load().(readOnly)
    e, ok := read.m[key]
    if !ok && read.amended {
        m.mu.Lock()
        read, _ = m.read.Load().(readOnly)
        e, ok = read.m[key]
        if !ok && read.amended {
            e, ok = m.dirty[key]
            m.missLocked() // 递增 miss，可能触发 dirty 提升
        }
        m.mu.Unlock()
    }
    if !ok {
        return nil, false
    }
    return e.load()
}
```

---

> **总结**：Go 运行时通过精巧的 GMP 调度器实现了高效的并发模型，通过三色标记+混合写屏障实现了低延迟的 GC，通过分级内存分配器实现了高性能的内存管理。理解这些内部机制是成为 Go 高级开发者的关键。

---

## 面试题精选

### GMP 调度

**Q1: GMP 调度器中 G、M、P 分别是什么？它们之间如何协作？**

A: G（Goroutine）是轻量级协程，包含栈、PC、寄存器等上下文；M（Machine）是 OS 线程；P（Processor）是逻辑处理器，控制并发度（默认 GOMAXPROCS）。协作流程：G 被分配到 P 的本地队列，M 从 P 中获取 G 执行，G 阻塞时 M 寻找新 G，系统调用时 P 与 M 解绑。

**Q2: Work Stealing 机制是如何工作的？**

A: 当 P 的本地队列为空时，会随机从其他 P 的队列中偷取一半的 G 来执行。偷取顺序随机化，避免热点。

**Q3: Go 1.14 的抢占式调度是如何实现的？**

A: 后台监控线程 sysmon 检测到 G 运行超过 10ms，设置 preempt 标记，发送 SIGURG 信号，目标 M 在安全点被抢占。

**Q4: Goroutine 的栈初始大小是多少？如何扩容？**

A: 初始 2KB。当栈空间不足时，通过 `morestack` 触发扩容，新栈 = 旧栈 × 2，上限 1GB。通过 `copystack` 复制栈帧并调整指针。

### GC

**Q5: Go 的 GC 使用什么算法？**

A: 并发三色标记清除算法。白色（候选回收）、灰色（已标记未扫描）、黑色（已标记已扫描）。通过混合写屏障（插入屏障 + 删除屏障）保证并发安全。

**Q6: 混合写屏障是如何工作的？**

A: 在指针写入时，对旧值和新值都进行置灰操作。`writePointer(slot, ptr) { shade(*slot); shade(ptr); *slot = ptr }`

**Q7: GC 触发条件有哪些？**

A: 1）堆大小达到阈值 `gc_trigger = heapMarked + heapMarked × GOGC/100`；2）超过 2 分钟未 GC；3）手动调用 `runtime.GC()`

### 内存管理

**Q8: Go 的内存分配器是如何分级的？**

A: mcache（每 P 一个，无锁分配）→ mcentral（每个 size class 一个，需锁）→ mheap（全局堆，从 OS 申请大块内存）。小对象（≤32KB）按 size class 分配，大对象直接分配多页。

**Q9: Tiny Allocator 的作用是什么？**

A: 处理 ≤16 字节的对象，将多个小对象合并到一个 16 字节槽中，减少内存碎片和 GC 扫描压力。

### Channel

**Q10: Channel 的底层数据结构是什么？**

A: `hchan` 结构体，包含循环队列缓冲区、发送等待队列（sendq）、接收等待队列（recvq）、互斥锁。

**Q11: 向已关闭的 channel 发送数据会发生什么？**

A: 会 panic（`send on closed channel`）。从已关闭的 channel 接收数据不会 panic，会返回零值。

### Mutex

**Q12: Go 的 Mutex 有哪两种模式？**

A: 正常模式（自旋 + 信号量等待，可能有公平性问题）和饥饿模式（等待超过 1ms 后，直接交给队列头等待者，禁止自旋）。

### sync.Map

**Q13: sync.Map 适用于什么场景？**

A: 读多写少的场景（如配置管理、缓存）。写多读多的场景使用 `sync.RWMutex + map` 更好。

**Q14: sync.Map 的读操作如何实现无锁？**

A: 优先从 read map 中读取（无锁），如果 read 中没有且 dirty 有额外数据，则加锁从 dirty 读取。当 miss 次数达到 dirty 大小时，dirty 提升为新的 read。