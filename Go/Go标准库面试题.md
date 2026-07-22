# Go 标准库源码深度剖析：net/http、context、sync.Map、atomic

> 深入标准库核心组件的实现原理，基于 Go 1.26 源码

---

## 一、net/http 实现原理

### 1.1 Server 核心结构

```go
type Server struct {
    Addr         string
    Handler      Handler
    TLSConfig    *tls.Config
    ReadTimeout  time.Duration
    WriteTimeout time.Duration
    IdleTimeout  time.Duration
    MaxHeaderBytes int
    ConnContext  func(ctx context.Context, c net.Conn) context.Context

    mu         sync.Mutex
    listeners  map[*net.Listener]struct{}
    activeConn map[*conn]struct{}
    doneChan   chan struct{}
    onShutdown []func()
    inShutdown atomic.Bool
}
```

### 1.2 连接管理

**ListenAndServe** 循环：
```go
func (srv *Server) Serve(l net.Listener) error {
    // 1. 创建连接上下文
    baseCtx := context.Background()
    ctx := context.WithValue(baseCtx, ServerContextKey, srv)

    for {
        // 2. 接受 TCP 连接
        rw, err := l.Accept()

        // 3. 创建 http.conn
        c := srv.newConn(rw)
        c.setState(c.rwc, StateNew) // 跟踪连接状态

        // 4. 启动 goroutine 处理连接
        go c.serve(ctx)
    }
}
```

**conn.serve** — 连接处理循环（HTTP/1.1 keep-alive 的核心）：
```go
func (c *conn) serve(ctx context.Context) {
    // 1. 设置读超时
    c.r = &connReader{conn: c}
    // 2. 创建带客户端信息的 context
    ctx = context.WithValue(ctx, LocalAddrContextKey, c.rwc.LocalAddr())

    for {
        // 3. 读取请求
        w, err := c.readRequest(ctx)

        // 4. 调用 Handler
        serverHandler{c.server}.ServeHTTP(w, w.req)

        // 5. 清理并准备下一次请求
        w.cancelCtx()
        // HTTP/1.1 keep-alive：继续循环
        // Connection: close：退出循环
    }
}
```

### 1.3 ServeMux 路由实现

**ServeMux 结构**：
```go
type ServeMux struct {
    mu    sync.RWMutex
    m     map[string]muxEntry  // 精确匹配路径 → handler
    es    []muxEntry           // 模式匹配路径（按长度排序）
    hosts bool                 // 是否包含 host 匹配
}

type muxEntry struct {
    h       Handler
    pattern string
}
```

**路由匹配算法**：
```go
func (mux *ServeMux) Handler(r *http.Request) (h Handler, pattern string) {
    // 1. 优先精确匹配 m["/path"]
    // 2. 找不到时，尝试最长路径匹配（es 中的最长前缀）
    // 3. 如果都不匹配，返回 NotFoundHandler
}
```

**路由注册**：
```go
func (mux *ServeMux) Handle(pattern string, handler Handler) {
    mux.mu.Lock()
    defer mux.mu.Unlock()

    // 1. 检查是否已有冲突的 pattern
    // 2. 如果 pattern 结尾是 "/"，加入 es 列表（前缀匹配）
    // 3. 否则加入 m 字典（精确匹配）
    // 4. 按路径长度排序 es
}
```

### 1.4 HTTP/2 支持

Go 1.6+ 内置 HTTP/2 支持，通过 `h2c`（HTTP/2 Cleartext）或 TLS 协商。

**H2 升级**：
```go
// 在 TLS 握手时，通过 ALPN 协商
func (srv *Server) Serve(l net.Listener) error {
    // HTTP/2 通过 TLS ALPN 扩展协商
    if srv.TLSConfig != nil {
        // 配置 NextProto 包含 "h2"
    }
}
```

### 1.5 连接池（Transport）

```go
type Transport struct {
    idleMu       sync.Mutex
    idleConn     map[connectMethodKey][]*persistConn  // 空闲连接池
    idleConnWait map[connectMethodKey]chan struct{}    // 等待连接的 waiter

    // 参数
    MaxIdleConns        int
    MaxIdleConnsPerHost int
    IdleConnTimeout     time.Duration
    DisableKeepAlives   bool

    DialContext func(ctx context.Context, network, addr string) (net.Conn, error)
}
```

**连接复用流程**：
```go
func (t *Transport) roundTrip(req *Request) (*Response, error) {
    // 1. 尝试从 idleConn 获取已存在的连接
    // 2. 如果没有空闲连接，创建新连接
    // 3. 发送请求
    // 4. 读取响应
    // 5. 将连接放回 idleConn 池（如果 keep-alive）
}
```

### 1.6 ResponseWriter 实现

`http.ResponseWriter` 接口的默认实现是 `http.response`：

```go
type response struct {
    conn      *conn
    req       *Request
    wroteHeader bool
    written     int64    // 已写入字节数
    status      int      // 状态码
    contentLength int64
    handlerDone bool

    // 缓存的响应头
    header Header
    wroteContinue bool
}
```

**写入流程**：
```go
func (w *response) Write(data []byte) (int, error) {
    // 1. 如果未调用 WriteHeader，默认写 200
    // 2. 写入响应头（状态行 + header）
    // 3. 写入 body
    // 4. 记录已写入字节数
    return w.conn.bufw.Write(data)
}
```

---

## 二、context 实现原理

### 2.1 Context 接口

```go
type Context interface {
    Deadline() (deadline time.Time, ok bool)
    Done() <-chan struct{}
    Err() error
    Value(key any) any
}
```

### 2.2 emptyCtx

```go
type emptyCtx int

func (e *emptyCtx) Deadline() (deadline time.Time, ok bool) { return }
func (e *emptyCtx) Done() <-chan struct{}                   { return nil }
func (e *emptyCtx) Err() error                              { return nil }
func (e *emptyCtx) Value(key any) any                       { return nil }

var (
    background = new(emptyCtx)
    todo       = new(emptyCtx)
)
```

### 2.3 cancelCtx — 取消上下文

```go
type cancelCtx struct {
    Context

    mu       sync.Mutex            // 保护后续字段
    done     atomic.Value          // chan struct{}（懒创建）
    children map[canceler]struct{} // 子上下文
    err      atomic.Value          // 取消原因（原子操作，5x 快于 mutex）
    cause    error                 // 取消的根因
}

func (c *cancelCtx) Done() <-chan struct{} {
    d := c.done.Load()
    if d != nil {
        return d.(chan struct{})
    }
    // 懒创建：第一次调用时初始化
    c.mu.Lock()
    defer c.mu.Unlock()
    d = c.done.Load()
    if d == nil {
        d = make(chan struct{})
        c.done.Store(d)
    }
    return d.(chan struct{})
}

func (c *cancelCtx) Err() error {
    // 原子加载比 mutex 快约 5 倍
    if err := c.err.Load(); err != nil {
        <-c.Done()  // 确保 done channel 已关闭
        return err.(error)
    }
    return nil
}

**取消传播**：
```go
func (c *cancelCtx) cancel(removeFromParent bool, err, cause error) {
    if err == nil {
        panic("context: internal error: missing cancel error")
    }
    if cause == nil {
        cause = err
    }

    c.mu.Lock()

    // 1. 已经取消过，直接返回
    if c.err.Load() != nil {
        c.mu.Unlock()
        return
    }

    // 2. 设置错误（原子操作）
    c.err.Store(err)
    c.cause = cause

    // 3. 关闭 done channel
    d, _ := c.done.Load().(chan struct{})
    if d == nil {
        c.done.Store(closedchan)
    } else {
        close(d)
    }

    // 4. 级联取消所有子 context
    for child := range c.children {
        child.cancel(false, err, cause)
    }
    c.children = nil
    c.mu.Unlock()

    // 5. 从父 context 中移除自己
    if removeFromParent {
        removeChild(c.Context, c)
    }
}
```

### 2.4 timerCtx — 超时控制

```go
type timerCtx struct {
    cancelCtx
    timer    *time.Timer // 定时器
    deadline time.Time
}

func (c *timerCtx) Deadline() (deadline time.Time, ok bool) {
    return c.deadline, true
}

func WithTimeout(parent Context, timeout time.Duration) (Context, CancelFunc) {
    return WithDeadline(parent, time.Now().Add(timeout))
}

func WithDeadline(parent Context, d time.Time) (Context, CancelFunc) {
    // 1. 如果父 context 的 deadline 更早，直接使用 withCancel
    // 2. 创建 timerCtx
    // 3. 设置定时器：到期时自动取消
    // 4. 返回 c.timer 和 cancel 函数
}
```

### 2.5 valueCtx — 传值

```go
type valueCtx struct {
    Context
    key, val any
}

func (c *valueCtx) Value(key any) any {
    // 递归查找
    if c.key == key {
        return c.val
    }
    return c.Context.Value(key)  // 向上查找
}
```

**查找时间复杂度**：O(n)，n 为 valueCtx 链的长度。不要用 context 传递大量数据。

### 2.6 Context 最佳实践

```go
// 1. 作为第一个参数传递
func DoSomething(ctx context.Context, arg string) error { ... }

// 2. 不要传递 nil，用 context.Background() 或 context.TODO()
// 3. 只用 request-scoped 数据，不要传可选参数
// 4. 超时控制：使用 WithTimeout 或 WithDeadline
// 5. 并发控制：使用 WithCancel
```

---

## 三、atomic 操作实现

### 3.1 硬件级原子操作

```go
// 源码用汇编实现
//go:noescape
func Load(ptr *uint32) uint32

// amd64 实现（asm_amd64.s）
TEXT ·Load(SB), NOSPLIT, $0-12
    MOVQ ptr+0(FP), AX
    MOVL (AX), AX      // MOVL 本身就是原子操作（对齐的 32 位）
    MOVL AX, ret+8(FP)
    RET
```

### 3.2 CAS（Compare And Swap）

```go
// amd64 实现
TEXT ·Cas(SB), NOSPLIT, $0-17
    MOVQ ptr+0(FP), BX
    MOVL old+8(FP), AX
    MOVL new+12(FP), CX
    LOCK      // 锁总线
    CMPXCHGL CX, (BX)  // 比较并交换
    SETEQ    ret+16(FP)
    RET
```

### 3.3 atomic.Value

```go
type Value struct {
    v any
}

func (v *Value) Store(val any) {
    // 检查 val 是否为 nil
    // 检查类型一致性（第一次 Store 后，后续必须同类型）
    // 使用 runtime_procPin 防止写操作被打断
    v.v = val
}

func (v *Value) Load() any {
    return v.v
}
```

**atomic.Value 的约束**：
- 一旦 Store 了一个类型，后续必须 Store 同类型
- 第一次 Store 之前，Load 返回 nil
- 不能 Store nil

---

## 四、sync.Map 源码深度（Go 1.26 全新 HashTrieMap 实现）

> Go 1.26 的 `sync.Map` 已被完全重写。旧版 `read/dirty` 双 map 架构已移除，
> 新版使用 `internal/sync.HashTrieMap` — 基于**并发哈希字典树（hash-trie）**的数据结构。

### 4.1 新架构

```go
// src/sync/map.go — 对外暴露的 sync.Map
// 是 HashTrieMap 的薄封装
type Map struct {
    _ noCopy
    m isync.HashTrieMap[any, any]
}

// src/internal/sync/hashtriemap.go — 内部实现
type HashTrieMap[K comparable, V any] struct {
    inited   atomic.Uint32
    initMu   Mutex
    root     atomic.Pointer[indirect[K, V]]
    keyHash  hashFunc    // key 的哈希函数
    valEqual equalFunc   // value 的比较函数
    seed     uintptr     // 随机种子
}

type hashFunc func(unsafe.Pointer, uintptr) uintptr
type equalFunc func(unsafe.Pointer, unsafe.Pointer) bool
```

### 4.2 哈希字典树结构

```
HashTrieMap 使用哈希字典树（hash-trie）作为底层数据结构：

每个 key 通过哈希函数计算出一个哈希值
哈希值被分成多个片段（bits），每片段对应树的一层

root → [indirect node] → [indirect node] → [leaf node] → key-value
           ↓                    ↓
      [多个子节点]         [多个子节点]

- indirect node：内部节点，包含子节点数组（2^childBits 个槽）
- leaf node：叶子节点，存储 key-value 对
```

### 4.3 并发控制

```go
// 使用 CAS 操作实现无锁并发
// 读操作不需要加锁
// 写操作通过 CAS 原子地更新节点

// 删除使用墓碑标记（tombstone）
// 创建一个新的节点树替换旧节点
```

### 4.4 适用场景

- **读多写少**：配置管理、缓存（与旧版一致）
- **无锁读取**：读操作完全无锁，通过原子操作访问
- **写操作隔离**：写操作通过 CAS 原子替换节点，不影响并发读

---

## 五、sync.Pool 实现

### 5.1 结构

```go
type Pool struct {
    noCopy noCopy

    local     unsafe.Pointer // per-P 的 poolLocal 数组
    localSize uintptr

    victim     unsafe.Pointer // GC 后保留的上一轮对象
    victimSize uintptr

    New func() any
}

type poolLocal struct {
    poolLocalInternal
    pad [128 - unsafe.Sizeof(poolLocalInternal{})%128]byte // 缓存行填充
}

type poolLocalInternal struct {
    private any       // 仅当前 P 使用（无锁）
    shared  poolChain // 可被其他 P 偷取
}
```

### 5.2 Get/Put 操作

```go
func (p *Pool) Get() any {
    // 1. 获取当前 P 的 poolLocal
    l := p.pin()
    // 2. 先取 private（最快）
    x := l.private
    l.private = nil
    if x == nil {
        // 3. 从 shared 队列取
        x, _ = l.shared.popHead()
        if x == nil {
            // 4. 从其他 P 偷取
            x = p.getSlow()
        }
    }
    runtime_procUnpin()
    // 5. 如果都没找到，调用 New 创建
    if x == nil && p.New != nil {
        x = p.New()
    }
    return x
}

func (p *Pool) Put(x any) {
    l := p.pin()
    if l.private == nil {
        l.private = x
    } else {
        l.shared.pushHead(x)
    }
    runtime_procUnpin()
}
```

### 5.3 GC 影响

**Pool 在 GC 时会被清空**：
```go
// runtime/mgc.go
func poolCleanup() {
    // 1. 将当前 local 移为 victim
    // 2. 清空 local
    // 下次 Get 时，可取 victim 中的对象
}
```

**Victim 缓存**：GC 时不立即释放，保留一轮 GC，给 Pool 使用者缓冲时间。

---

> **总结**：标准库的每个组件都经过精心优化。net/http 通过 goroutine-per-connection 和连接池实现高并发；context 通过树形结构和级联取消实现优雅的请求生命周期管理；sync.Map 通过读写分离+无锁读实现高并发场景下的性能；sync.Pool 通过 per-P 缓存和 victim 机制减少 GC 压力。深入理解这些实现，能帮助我们在实际项目中做出更好的技术选型。

---

## 面试题精选

### net/http

**Q1: HTTP 服务的连接处理模型是怎样的？**

A: 每个 TCP 连接启动一个 goroutine 处理（`go c.serve(ctx)`），支持 HTTP/1.1 keep-alive 循环。

**Q2: DefaultServeMux 的路由规则是什么？**

A: 以 `/` 结尾的 pattern 是前缀匹配，否则是精确匹配。匹配时先精确匹配，再最长前缀匹配。

**Q3: Transport 的连接池是如何管理和复用的？**

A: 按 `connectMethodKey`（协议+地址）分类缓存空闲连接（`idleConn`），使用 `roundTrip` 时优先复用，超时未使用则关闭。

### context

**Q4: Context 的取消是如何传播的？**

A: 父 context 取消时，会遍历 `children` map 级联取消所有子 context。`cancelCtx.cancel()` 关闭 done channel → 遍历子节点 → 逐个子节点取消。

**Q5: WithTimeout 和 WithDeadline 的实现区别？**

A: WithTimeout 内部调用 WithDeadline（`time.Now().Add(timeout)`）。WithDeadline 创建 timerCtx，通过 time.Timer 到期自动取消。

**Q6: context.WithValue 的查找时间复杂度是多少？**

A: O(n)，n 为 valueCtx 链的长度。所以不要用 context 传递大量数据，只传 request-scoped 数据。

### sync.Map

**Q7: 为什么 Go 1.26 的 sync.Map 用 HashTrieMap 替代了旧版 read/dirty 架构？**

A: HashTrieMap 使用并发哈希字典树，读操作完全无锁（CAS 原子操作），写操作通过原子替换节点，不阻塞并发读，更适合高并发场景。

### sync.Pool

**Q8: sync.Pool 在 GC 时会发生什么？**

A: poolCleanup 将当前 local 移为 victim，清空 local。下次 Get 时优先从 victim 取，给使用者缓冲时间。

**Q9: Pool 中的对象什么时候会被回收？**

A: 每次 GC 时都会清空。所以 Pool 适合缓存临时对象（如缓冲区），不适合持久化对象。