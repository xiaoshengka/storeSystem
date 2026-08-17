# storeSystem

基于 Linux C、非阻塞 Socket 与单线程 epoll Reactor 的内存 KV 服务。

当前发布基线为 `v0.1.0`。`feature/reactor` 正在实现 `v0.2.0`；在 Ubuntu
验收、合并和发布完成前，不将仓库描述为已发布 v0.2.0。

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

## v0.2.0 已知限制

- 一次可读批次临时视为一条完整文本命令。
- 尚不支持 RESP、增量协议解析、粘包、半包或 Pipeline；这些属于 v0.3.0。
- 尚不支持 TTL/LRU、AOF 或 MySQL Cache-Aside。
- 默认使用单线程、Level-Triggered epoll；不包含 ET、多线程 Reactor 或 io_uring。
