# storeSystem

基于 Linux C、非阻塞 Socket 与单线程 epoll Reactor 的内存 KV 服务。

当前发布版本为 `v0.3.0`：epoll 主线已支持 RESP2 增量解析、可靠的粘包/半包
处理、Pipeline，以及统一的 `SET/GET/DEL/PING` 命令。NtyCo 仅作为历史/可选
对照实现保留。

## 架构

```text
Client
  -> epoll Reactor
  -> RESP2 增量解析/编码
  -> 统一命令服务
  -> Hash 或 RBTree 内存引擎
```

Reactor 负责连接、输入/输出缓冲、非阻塞收发、Pipeline 背压和资源回收；协议层
只处理 RESP 字节帧，服务层负责命令语义，引擎层不依赖网络或协议。v0.2.0 Reactor
设计见 [`docs/reactor-v0.2.md`](docs/reactor-v0.2.md)，v0.3.0 协议设计见
[`docs/resp-v0.3.md`](docs/resp-v0.3.md)，发布说明见
[`docs/releases/v0.3.0.md`](docs/releases/v0.3.0.md)。

## 环境与构建

验证目标环境：Ubuntu 22.04.5、GCC、GNU Make。

```bash
git clone --recurse-submodules https://github.com/xiaoshengka/storeSystem.git
cd storeSystem
make
```

默认构建生成：

- `kvstore`：RESP2 epoll Reactor 服务端，监听 `0.0.0.0:9096`。
- `qps_client`：支持多连接和可配置 Pipeline 的 RESP2 GET 基准客户端。
- `legacy_client`：v0.1.0 文本协议历史客户端，不用于 v0.3.0 epoll 主服务。

运行服务时可选择统一命令背后的引擎，默认使用 Hash：

```bash
./kvstore
./kvstore --engine hash
./kvstore --engine rbtree
```

NtyCo 只作为历史对照后端，不参与默认主线：

```bash
git submodule update --init --recursive
make ntyco
```

## RESP2 协议与命令

epoll 主服务只接受 RESP2 `Array of Bulk Strings` 请求，不接受 inline 文本命令。
例如 `SET key value` 的字节帧为：

```text
*3\r\n
$3\r\nSET\r\n
$3\r\nkey\r\n
$5\r\nvalue\r\n
```

支持的命令和响应：

| 命令 | 语义 | 响应 |
| --- | --- | --- |
| `SET key value` | 新增或覆盖 | `+OK` |
| `GET key` | 查询 | Bulk String 或 Null Bulk |
| `DEL key` | 删除 | Integer `1` 或 `0` |
| `PING` | 探活 | `+PONG` |
| `PING message` | 原样回显 | Bulk String |

命令名大小写不敏感。key/value 和 PING message 均按长度处理，支持空数据与嵌入
`NUL`。未知命令、参数数量错误和内部错误返回 RESP Error；非法、超限或 EOF 时仍
不完整的协议帧返回 `-ERR Protocol error\r\n`，发送后关闭连接。

当前协议限制：单帧最大 64 KiB、最多 128 个参数，只接受顶层 Array 和非 Null
Bulk String。输出缓冲达到 1 MiB 高水位时 Reactor 暂停读取该连接，排空响应后再
继续处理已缓存 Pipeline。

## 测试

```bash
make test
make integration-test
make asan
make valgrind
```

- 单元测试覆盖动态网络缓冲、旧引擎接口、RESP 增量解析/编码，以及 Hash/RBTree
  二进制安全的统一命令语义。
- 集成测试会分别启动 Hash 和 RBTree，覆盖逐字节半包、粘连和跨发送 Pipeline、
  超过 1 MiB 的响应背压、并发连接、二进制数据、half-close 和协议错误关闭。
- `make asan` 使用 ASan/UBSan 重复单元与双引擎集成测试；`make valgrind` 对相同
  主路径执行内存与资源检查。

## QPS 基准客户端

```bash
make qps_client
./qps_client -s 127.0.0.1 -p 9096 -c 32 -n 1000000 -w 1000 -P 1
./qps_client -s 127.0.0.1 -p 9096 -c 32 -n 1000000 -w 1000 -P 16
```

- `-c`：持久 TCP 连接数，同时也是客户端工作线程数。
- `-n`：所有连接合计的计时 GET 数量，必须不少于连接数。
- `-w`：每条连接在计时前执行的预热 GET 数量。
- `-P`：每批 Pipeline 深度，范围 `1..1024`，默认 `1`。
- `-s/-p`：服务端 IPv4 地址和端口。

每条连接先写入独立 key，再按 Pipeline 批次执行命中 GET，并逐个解析和校验 Bulk
响应；准备、预热和清理不计入 QPS。对比 Hash/RBTree 时只切换服务端
`--engine`，其余客户端参数、环境和数据必须保持相同。

短时 smoke test 的输出不作为正式性能结论。正式报告必须记录 Ubuntu/GCC/编译
选项、CPU/内存、客户端与服务端位置、引擎、并发数、Pipeline 深度、请求规模和
持续时间。

### v0.2.0 历史基线

以下数字仅是 v0.2.0 文本 `HGET`、无 Pipeline 的历史基线，不可直接作为 v0.3.0
RESP 或 Hash/RBTree 对比结果：

```text
environment: VMware Ubuntu 22.04.5, 8 CPU, 12 GB RAM, client/server same host
compiler flags: -O2 -g -Wall -Wextra -Wpedantic
workload: HGET hit, one request/response per connection
connections: 32
warmup_per_connection: 1000
requests_completed: 1000000 / 1000000
duration_seconds: 13.745987
qps: 72748.50
tcp_nodelay: on
```

## 当前范围与限制

- v0.3.0 仅统一 Hash/RBTree 主服务；Array 和旧前缀命令只保留在历史代码路径。
- 暂不支持 TTL/LRU、动态扩容、AOF、MySQL Cache-Aside、集群或复制。
- 默认仍是单线程、Level-Triggered epoll；不包含多线程 Reactor 或 io_uring。
- v0.4.0 计划中的动态扩容、TTL/LRU 与缓存统计尚未实现。
