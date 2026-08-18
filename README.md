# storeSystem

基于 Linux C、非阻塞 Socket 与单线程 epoll Reactor 的内存 KV 缓存服务。

当前发布版本为 `v0.4.0`：epoll 主服务已统一使用动态 Hash，并加入 TTL、
主动/惰性过期、精确 LRU、容量淘汰和缓存统计。NtyCo、Array 与 RBTree 仅作为
历史或算法对照保留。

## 架构

```text
Client
  -> epoll Reactor / timerfd 周期维护
  -> RESP2 增量解析与编码
  -> SET/GET/DEL/TTL 命令服务
  -> Cache（TTL 最小堆 + LRU 双向链表 + 统计）
  -> 动态 Hash（双表渐进式 rehash）
```

Reactor 负责连接、非阻塞收发、Pipeline 背压、周期回调和资源回收；协议层只处理
RESP 字节帧；服务层负责命令语义；Cache 不依赖网络或 RESP；Hash 只负责二进制
key 的索引。v0.4 设计见 [`docs/cache-v0.4.md`](docs/cache-v0.4.md)，阶段测试
工作流和结果见 [`docs/releases/v0.4.0.md`](docs/releases/v0.4.0.md)。

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
- `mixed_qps_client`：共享大 keyspace 的 90% GET / 10% SET、64B value
  混合基准客户端，可施加 TTL 和 LRU 容量压力。
- `legacy_client`：旧文本协议历史客户端，不用于 epoll 主服务。

## 启动与容量

```bash
./kvstore
./kvstore --maxmemory 64MiB
./kvstore --maxmemory 64MiB --maxkeys 100000
```

- `--maxmemory`：缓存条目的逻辑字节上限，接受字节数或大小写不敏感的
  `KiB/MiB/GiB` 后缀。
- `--maxkeys`：最大 key 数量，只接受无符号十进制整数。
- 两个上限都可以使用，任一超限都会触发 LRU；`0` 表示不限制。
- `--engine` 已移除，epoll 主服务固定使用 Hash。

`maxmemory` 统计 key/value 分配及缓存条目和 Hash 节点的固定元数据，不等同于
进程 RSS。Hash 桶数组和 TTL 堆的预留空间通过 `index_memory` 单独报告。

## RESP2 命令

服务只接受 RESP2 `Array of Bulk Strings`，命令名大小写不敏感，key/value 按长度
处理并支持空数据和嵌入 `NUL`。

| 命令 | 语义 | 响应 |
| --- | --- | --- |
| `SET key value` | 新增或覆盖，并清除旧 TTL | `+OK` |
| `SET key value EX seconds` | 写入并设置秒级 TTL | `+OK` |
| `SET key value PX milliseconds` | 写入并设置毫秒级 TTL | `+OK` |
| `GET key` | 查询并更新 LRU | Bulk String 或 Null Bulk |
| `DEL key` | 删除 | Integer `1` 或 `0` |
| `EXPIRE key seconds` | 设置秒级 TTL | Integer `1` 或 `0` |
| `PEXPIRE key milliseconds` | 设置毫秒级 TTL | Integer `1` 或 `0` |
| `TTL key` / `PTTL key` | 查询剩余 TTL | Integer |
| `PERSIST key` | 清除 TTL | Integer `1` 或 `0` |
| `INFO CACHE` | 查询缓存统计 | Bulk String |
| `PING [message]` | 探活或回显 | Simple/Bulk String |

`TTL/PTTL` 对缺失或已过期 key 返回 `-2`，对永久 key 返回 `-1`。非正数
`EXPIRE/PEXPIRE` 会立即删除现有 key；`SET EX/PX` 要求严格正整数。

`INFO CACHE` 返回以下稳定字段：

```text
keys used_memory index_memory maxmemory maxkeys
hits misses hit_rate expired_keys evicted_keys
hash_slots rehashing
```

只有 `GET` 参与 hit/miss 统计。惰性和主动删除都计入 `expired_keys`，容量驱逐才
计入 `evicted_keys`。

## 缓存行为

- Hash 从 16 桶开始，在负载因子达到 0.75 时扩容为两倍；请求操作和周期维护分批
  搬迁旧桶，避免一次性 O(N) rehash 阻塞 Reactor。
- 每个缓存条目位于精确 LRU 双向链表中，成功 `GET/SET` 移到头部，淘汰从尾部
  开始。
- TTL 使用绝对 Unix 毫秒截止时间和最小堆；访问时惰性过期，timerfd 每 100 ms
  触发一次主动过期，每次最多删除 64 个 key。
- 单个条目超过 `maxmemory` 时，`SET` 返回 `ERR cache capacity exceeded`，已有值
  保持不变。

## 测试

```bash
make test
make integration-test
make asan
make valgrind
```

- 单元测试覆盖缓冲区、RESP、旧引擎接口、动态 Hash、渐进 rehash、TTL 最小堆、
  精确 LRU、双容量限制、统计和服务命令。
- 集成测试覆盖半包/粘包、Pipeline、背压、并发连接、二进制数据、half-close、
  协议错误、真实定时过期和小容量 LRU。
- `make asan` 使用 ASan/UBSan；`make valgrind` 检查单元与集成主路径。

NtyCo 只作为历史对照后端：

```bash
git submodule update --init --recursive
make ntyco
```

## QPS 基准客户端

```bash
make qps_client
./qps_client -s 127.0.0.1 -p 9096 -c 32 -n 1000000 -w 1000 -P 16

make mixed_qps_client
./mixed_qps_client -s 127.0.0.1 -p 9096 \
  -c 32 -n 10000000 -w 1000 -P 16 -k 100000

# TTL 场景：预加载和计时 SET 均携带 PX 5000
./mixed_qps_client -c 32 -n 10000000 -w 1000 -P 16 \
  -k 100000 -T 5000
```

`-c` 是连接/客户端线程数，`-n` 是总计时请求数，`-w` 是每连接预热数，`-P`
是 Pipeline 深度（`1..1024`）。混合基准的 `-k` 是所有连接共享并在计时前完成
预加载的全局 keyspace，访问分布为确定性种子的均匀随机；`-T` 设置 SET 的毫秒
TTL，`0` 表示永久；`-S` 可复现实验随机序列，`-C` 保留测试键。准备、预热、
`INFO CACHE` 快照和清理均不计入 QPS。客户端报告 GET hit/miss，并通过计时前后
快照报告服务端 `expired_keys`、`evicted_keys`、内存和 Hash 状态增量。

v0.4 的阶段性能基线及解释记录在发布说明中；当前尚无延迟分位数或多轮中位数，
不得将单轮QPS作为生产或跨项目结论。性能报告必须记录Ubuntu/GCC/编译选项、
CPU/内存、客户端与服务端位置、容量配置、并发数、Pipeline深度、请求规模和持续时间。

## 当前限制

- AOF、MySQL Cache-Aside、配置文件、集群和复制尚未实现。
- Hash 只扩容、不缩容；当前仍使用已有的非加盐字节哈希函数。
- LRU 是精确实现，不是 Redis 的抽样近似算法；TTL 不支持成员级过期。
- RBTree 不再是 epoll 服务后端，计划在 v0.7 与 Skip List 一起用于有序集合模块。
