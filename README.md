# storeSystem

基于 Linux C、非阻塞 Socket 与单线程 epoll Reactor 的内存 KV 服务。

当前发布版本为 `v0.2.0`，默认网络后端已经从 NtyCo 原型切换为非阻塞 Socket、
Level-Triggered epoll 与单线程 Reactor。NtyCo 仅作为历史/可选对照实现保留。

## 架构

```text
Client
  -> epoll Reactor（默认网络后端）
  -> 文本命令分发
  -> Array / RBTree / Hash 内存引擎
```

Reactor 负责监听、连接对象、每连接输入/输出缓冲、非阻塞收发、部分写和资源
回收。网络层通过请求回调调用服务层，不直接访问具体 KV 引擎。详细设计见
[`docs/reactor-v0.2.md`](docs/reactor-v0.2.md)。

## 环境与构建

验证目标环境：Ubuntu 22.04.5、GCC、GNU Make。

```bash
git clone --recurse-submodules https://github.com/xiaoshengka/storeSystem.git
cd storeSystem
make
```

默认构建生成：

- `kvstore`：epoll Reactor 服务端，监听 `0.0.0.0:9096`。
- `legacy_client`：保留的原型测试/压测客户端。
- `qps_client`：使用多条持久连接执行请求/响应校验的 QPS 基准客户端。

运行服务：

```bash
./kvstore
```

NtyCo 只作为历史对照后端，不参与默认构建：

```bash
git submodule update --init --recursive
make ntyco
```

该目标生成独立的 `kvstore-ntyco`，不会覆盖默认的 `kvstore` Reactor 服务端。

## 当前文本命令

v0.2.0 保持 v0.1.0 的空格分隔协议：

```text
SET key value     GET key     DEL key     MOD key value     COUNT
RSET key value    RGET key    RDEL key    RMOD key value    RCOUNT
HSET key value    HGET key    HDEL key    HMOD key value    HCOUNT
```

无效命令或参数数量错误返回以 `ERROR` 开头的响应，不应导致服务崩溃。

## 测试

```bash
make test
make integration-test
make asan
make valgrind
```

- 单元测试覆盖缓冲区增长/消费、非阻塞部分写、三种 KV 引擎 CRUD 和错误输入。
- 集成测试覆盖基础命令、多客户端并发、重复连接、异常断开和半关闭连接；
  `make valgrind` 也会在 Valgrind 下重复该集成测试。
- 性能测试必须记录 CPU/内存、编译选项、客户端和服务端位置、并发数、请求量与
  持续时间。仓库当前不提供未经 Ubuntu 实测的 QPS 或延迟数字。

## QPS 基准客户端

`bench/legacy_client.c` 是 v0.1.0 留下的混合测试程序，命令数量和测试流程硬编码，
主要用于兼容性对照。v0.2.0 使用独立的 `qps_client` 测量 Reactor：

```bash
make qps_client
./qps_client -s 127.0.0.1 -p 9096 -c 32 -n 1000000 -w 1000
```

- `-c`：持久 TCP 连接数，同时也是客户端工作线程数。
- `-n`：所有连接合计的实测请求数量，必须不少于连接数。
- `-w`：每条连接在计时前执行的预热请求数。
- `-s/-p`：服务端 IPv4 地址和端口。

客户端先为每条连接写入独立 Hash key，再统一起跑并循环执行命中 `HGET`。每条连接
保持一个在途请求，收到并校验完整响应后才发送下一条；建连、数据准备、预热和清理
不计入 QPS。输出包含客户端主机名、并发数、请求数、持续时间和 QPS。

该测试不使用 Pipeline，因为 v0.2.0 尚无可靠的响应帧边界。正式性能报告还必须在
结果旁记录服务端和客户端是否同机、CPU/内存、Ubuntu 版本、GCC 与优化选项；不要
将短时本机 smoke test 当成正式性能数据。

### v0.2.0 实测基线

以下结果来自 VMware Ubuntu 22.04.5（8 核 CPU、12 GB 内存），客户端与服务端同机，
使用 Makefile 默认编译选项 `-O2 -g -Wall -Wextra -Wpedantic`：

```text
workload: HGET hit, one request/response per connection
connections: 32
warmup_per_connection: 1000
requests_completed: 1000000 / 1000000
duration_seconds: 13.745987
qps: 72748.50
tcp_nodelay: on
```

这是指定环境和负载下的一次可复现实测基线，不代表其他机器、网络拓扑或请求分布下
的峰值性能。

## v0.2.0 已知限制

- 一次可读批次临时视为一条完整文本命令。
- 尚不支持 RESP、增量协议解析、粘包、半包或 Pipeline；这些属于 v0.3.0。
- 尚不支持 TTL/LRU、AOF 或 MySQL Cache-Aside。
- 默认使用单线程、Level-Triggered epoll；不包含 ET、多线程 Reactor 或 io_uring。
