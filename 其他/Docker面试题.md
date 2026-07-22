# Docker 深度剖析：命名空间、Cgroups、OverlayFS、容器运行时

> 面向高级开发者的 Docker 内核原理

---

## 一、Linux 命名空间（Namespaces）

### 1.1 命名空间类型

```c
// Linux 支持的 8 种命名空间
// 1. PID（进程 ID）：CLONE_NEWPID
// 2. NET（网络）：CLONE_NEWNET
// 3. MNT（挂载点）：CLONE_NEWNS
// 4. UTS（主机名）：CLONE_NEWUTS
// 5. IPC（进程间通信）：CLONE_NEWIPC
// 6. USER（用户）：CLONE_NEWUSER
// 7. CGROUP（控制组）：CLONE_NEWCGROUP
// 8. TIME（时间）：CLONE_NEWTIME（Linux 5.6+）

// 系统调用
// clone()：创建进程时指定命名空间
// unshare()：将进程移出原有命名空间
// setns()：加入已存在的命名空间
```

### 1.2 PID 命名空间

```c
// PID 命名空间实现
// 每个命名空间有自己的 PID 编号
// PID 1 在容器中是 init 进程
// 嵌套命名空间形成树形结构

// 关键点
// 1. 父命名空间可以看到子命名空间的进程
// 2. 子命名空间看不到父命名空间的进程
// 3. PID 隔离是单向的
```

### 1.3 网络命名空间

```c
// 网络命名空间实现
// 每个命名空间有独立的：
// 1. 网络设备（eth0, lo）
// 2. 路由表
// 3. iptables 规则
// 4. 网络栈

// veth pair（虚拟以太网对）
// 一对虚拟网卡，一端在容器，一端在宿主机
// 通过网桥连接不同命名空间

// 网络模式
// bridge：默认，通过 veth pair + 网桥
// host：共享宿主机网络
// none：无网络
// overlay：跨主机网络
```

---

## 二、Cgroups（控制组）

### 2.1 Cgroups 子系统

```c
// Linux cgroups v2（Linux 4.5+）
// 统一层级结构，不再区分控制器

// 主要子系统
// 1. cpu：CPU 时间片分配
// 2. memory：内存使用限制
// 3. io：磁盘 I/O 限制
// 4. pids：进程数限制
// 5. cpuset：CPU 亲和性
// 6. freezer：冻结/恢复进程

// cgroups 文件系统接口
// /sys/fs/cgroup/
// 每个组对应一个目录
// 通过写入文件设置限制
```

### 2.2 CPU 限制

```c
// CPU 限制实现
// cpu.weight：CPU 权重（相对值，范围 1-10000）
// cpu.max：CPU 绝对限制（quota/period）

// 示例
// 限制容器使用 1.5 个 CPU
// echo "150000 100000" > /sys/fs/cgroup/docker/cpu.max
// 150000us quota / 100000us period = 1.5 CPU
```

### 2.3 内存限制

```c
// 内存限制实现
// memory.max：硬限制（超过则 OOM）
// memory.high：软限制（超过则回收内存）
// memory.swap.max：swap 限制
// memory.oom.group：组内 OOM 时全部杀死

// 示例
// 限制容器内存 512MB
// echo "536870912" > /sys/fs/cgroup/docker/memory.max
```

---

## 三、OverlayFS（联合文件系统）

### 3.1 分层结构

```c
// OverlayFS 将多个目录合并为一个
// lowerdir：只读层（镜像层，可共享）
// upperdir：读写层（容器层，变更写入）
// workdir：工作目录（元数据操作）
// merged：合并后的视图

// 镜像分层示例
// docker pull ubuntu:22.04
// 每一层是一个 tar 包
// 解压到 overlay2 的 layer 目录
// 多个镜像共享相同的基础层
```

### 3.2 写时复制（Copy-on-Write）

```c
// 容器修改文件时
// 1. 检查文件是否在 upperdir 中
// 2. 如果在 upperdir 中 → 直接修改
// 3. 如果在 lowerdir 中 → 复制到 upperdir（copy_up）
// 4. 修改 upperdir 中的副本

// copy_up 问题
// 1. 大文件首次修改慢
// 2. 硬链接需要在同一个文件系统
```

---

## 四、容器运行时

### 4.1 OCI 标准

```c
// Open Container Initiative（OCI）标准
// 1. 镜像规范（Image Spec）
// 2. 运行时规范（Runtime Spec）

// 运行时规范定义
// 1. 容器配置（config.json）
// 2. 根文件系统（rootfs）
// 3. 挂载点
// 4. 进程信息
// 5. 网络配置
// 6. 资源限制
```

### 4.2 容器创建流程

```c
// 容器创建流程（以 runc 为例）
1. 解析 config.json
2. 创建命名空间（clone() 系统调用）
3. 设置 cgroups 限制
4. 挂载 rootfs（overlay2）
5. 设置网络（veth pair）
6. 运行 init 进程
7. 保持容器进程

// 关键点
// 1. 通过 clone() 创建子进程，指定命名空间
// 2. 子进程设置新的挂载命名空间
// 3. 通过 pivot_root 切换根文件系统
// 4. 执行容器中的 CMD
```

---

## 五、Docker 网络

### 5.1 CNM（Container Network Model）

```c
// Docker 使用 CNM 网络模型
// 三种网络驱动
// 1. bridge：单机网络（默认）
// 2. overlay：跨主机网络
// 3. macvlan：直接分配 MAC 地址

// Bridge 网络实现
// 1. 创建 Linux bridge（docker0）
// 2. 创建 veth pair
// 3. 一端连接到容器
// 4. 一端连接到 bridge
// 5. 配置 iptables NAT
// 6. 配置 DNS 和端口映射
```

---

## 六、Docker 安全

### 6.1 安全机制

```c
// 1. 命名空间隔离
// 2. cgroups 资源限制
// 3. Capabilities 限制
// 4. Seccomp 系统调用过滤
// 5. AppArmor/SELinux 强制访问控制
// 6. 只读根文件系统

// 安全最佳实践
// 1. 不要以 root 运行容器
// 2. 使用最小基础镜像
// 3. 限制容器 capabilities
// 4. 启用 seccomp 安全策略
// 5. 定期扫描镜像漏洞
```

---

> **总结**：Docker 通过命名空间实现隔离，通过 cgroups 实现资源限制，通过 OverlayFS 实现分层镜像，通过 OCI 运行时标准实现可移植性。理解这些底层机制是排查容器问题和优化容器性能的关键。

---

## 面试题精选

**Q1: Docker 容器和虚拟机的区别？**

A: 容器共享宿主机内核，进程级隔离，秒级启动，MB 级空间；虚拟机有独立内核，完全隔离，分钟级启动，GB 级空间。

**Q2: Docker 镜像分层原理是什么？**

A: 镜像由多个只读层组成，容器启动时添加可写层。写时复制（CoW）：修改文件时从只读层复制到可写层。

**Q3: CMD 和 ENTRYPOINT 的区别？**

A: CMD 可被 `docker run` 命令覆盖，ENTRYPOINT 不可被覆盖。常配合使用：ENTRYPOINT 固定入口，CMD 提供默认参数。

**Q4: 如何减小 Docker 镜像体积？**

A: 使用 Alpine 基础镜像（5MB）、多阶段构建、合并 RUN 指令、清理缓存、使用 `.dockerignore`。