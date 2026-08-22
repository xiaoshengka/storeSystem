# storeSystem

基于 Linux C、非阻塞 Socket 与单线程 epoll Reactor 的内存 KV 缓存服务。

当前发布版本为 `v0.6.2`：新增项目原生 RDB、同步 `SAVE`、fork 子进程异步
`BGSAVE`、自动快照以及 RDB+AOF 检查点尾部恢复。网络仍是单线程 epoll Reactor；
后台执行单元只有 AOF writer 和 BGSAVE 子进程，不引入 io_uring。

## 架构

```text
Client
  -> epoll Reactor / timerfd 周期维护
  -> RESP2 增量解析与编码
  -> String / Hash / ZSet 命令服务
  -> Cache（顶层动态 Hash + TTL 最小堆 + LRU + 统计）
  -> 类型对象（Hash；成员 Hash + SkipList/RBTree ZSet）
  -> AOF（命令 transaction + chunk SPSC 队列 + writer writev/fdatasync + mmap 回放）
  -> RDB（KVRDB001 + CRC64 + fork BGSAVE + 原子替换）
```

Reactor 负责连接、非阻塞收发、Pipeline 背压、周期回调和资源回收；协议层只处理
RESP 字节帧；服务层负责命令语义；Cache 不依赖网络或 RESP；Hash 只负责二进制
key 的索引。v0.6.2 设计与验收口径见
[`docs/v0.6.2-rdb-performance.md`](docs/v0.6.2-rdb-performance.md)；v0.6.1 设计见
[`docs/v0.6.1-memory-aof.md`](docs/v0.6.1-memory-aof.md)；v0.6 集合设计见
[`docs/typed-collections-v0.6.md`](docs/typed-collections-v0.6.md)；v0.5.1 AOF 设计见
[`docs/aof-v0.5.1.md`](docs/aof-v0.5.1.md)；v0.4 Cache 设计见
[`docs/cache-v0.4.md`](docs/cache-v0.4.md)。

v0.6.1 的 64B Hash/ZSet mixed 长测、Redis 6.2.23 对照和发布门槛状态见
[`docs/v0.6.1-performance-report.md`](docs/v0.6.1-performance-report.md)。v0.6.2
的测试汇总与已知性能偏差记录在下文；本次发布接受 Hash P16 QPS 和部分 AOF
相对开销未达到原定严格门槛，不将这些偏差描述为已经通过。

## 环境与构建

验证目标环境：Ubuntu 22.04.5、GCC、GNU Make、pkg-config 和 jemalloc。

```bash
git clone --recurse-submodules https://github.com/xiaoshengka/storeSystem.git
cd storeSystem
sudo apt-get install build-essential pkg-config libjemalloc-dev
make
```

MySQL Cache-Aside 是可选构建；启用时还需安装客户端开发包并显式编译：

```bash
sudo apt-get install default-libmysqlclient-dev
make MYSQL=1
```

`MYSQL=0` 是默认值，不链接 `libmysqlclient`，保持 v0.6.2 请求路径。

默认构建要求 jemalloc 且缺失时直接失败。仅诊断 allocator 差异时可使用
`make ALLOCATOR=libc`；ASan/UBSan 和 Valgrind 目标自动使用 libc allocator。

默认构建生成：

- `kvstore`：RESP2 epoll Reactor 服务端，监听 `0.0.0.0:9096`。
- `qps_client`：支持多连接和可配置 Pipeline 的 RESP2 GET 基准客户端。
- `mixed_qps_client`：共享大 keyspace 的 90% GET / 10% SET、64B value
  混合基准客户端，可施加 TTL 和 LRU 容量压力。
- `collection_bench_client`：同一 RESP2 C 客户端覆盖 Hash/ZSet 写入、命中读、
  90% 读/10% 写和固定宽度 ZRANGE，可同时驱动本项目与 Redis。
- `legacy_client`：旧文本协议历史客户端，不用于 epoll 主服务。

## 启动、容量与持久化

```bash
./kvstore
./kvstore --maxmemory 64MiB
./kvstore --maxmemory 64MiB --maxkeys 100000
./kvstore --zset-engine rbtree
./kvstore --appendonly yes --appendfilename appendonly.aof \
  --appendfsync everysec
./kvstore --rdb yes --dbfilename dump.kvrdb
./kvstore --rdb yes --dbfilename dump.kvrdb \
  --save-seconds 60 --save-changes 10000
KVSTORE_MYSQL_PASSWORD='replace-me' ./kvstore \
  --appendonly yes --appendfilename appendonly.aof \
  --mysql yes --mysql-host 127.0.0.1 --mysql-user kvstore \
  --mysql-database kvstore
```

- `--maxmemory`：缓存条目的逻辑字节上限，接受字节数或大小写不敏感的
  `KiB/MiB/GiB` 后缀。
- `--maxkeys`：最大 key 数量，只接受无符号十进制整数。
- 两个上限都可以使用，任一超限都会触发 LRU；`0` 表示不限制。
- `--engine` 已移除，epoll 主服务固定使用 Hash。
- `--zset-engine skiplist|rbtree`：选择所有 ZSet 的有序索引，默认 `skiplist`；
  不改变顶层 keyspace 的 Hash 实现。
- `--appendonly yes|no`：是否启用 AOF，默认 `no`。
- `--appendfilename`：AOF 路径，默认 `appendonly.aof`。
- `--appendfsync always|everysec|no`：每条写命令同步、约每秒同步或交给操作系统，
  默认 `everysec`。该选项只在 AOF 开启时生效。
- `--rdb yes|no`：启用 RDB 保存/加载，默认 `no`。
- `--dbfilename`：RDB 路径，默认 `dump.kvrdb`。
- `--save-seconds N --save-changes M`：两个值必须同时为非零；经过至少 N 秒且累计
  至少 M 次成功写后自动触发一次 `BGSAVE`。自动保存不会调用阻塞式 `SAVE`。
- `--mysql yes|no`：启用 MySQL Cache-Aside，默认 `no`；启用时强制要求 AOF，密码
  只从 `KVSTORE_MYSQL_PASSWORD` 读取。
- `--mysql-host/port/user/database`：默认 `127.0.0.1:3306`、用户和库名均为
  `kvstore`。服务不执行 DDL，建表步骤见 `config/mysql-v0.7.0.sql`。
- `--mysql-read-workers` 默认 `8`；连接、读写超时默认 `2` 秒；读队列默认
  `4096`，有序写队列默认 `64MiB`，单 key 完整回源上限默认 `64MiB`。
- `--mysql-negative-ttl-ms/--mysql-negative-capacity`：空值缓存默认 `3000` 毫秒、
  `10000` 个 key，写命令会立即失效对应空值。

`maxmemory` 统计 key/value 分配及缓存条目和 Hash 节点的固定元数据，不等同于
进程 RSS。Hash 桶数组和 TTL 堆的预留空间通过 `index_memory` 单独报告。

启用 AOF 后，服务在监听端口前先回放文件。TTL 会以绝对 Unix 毫秒截止时间写入，
因此停机时间也计入 TTL；恢复时已经过期的写入不会重新获得完整 TTL。文件末尾因
崩溃留下的不完整 RESP 帧会截断到最后一条完整命令，中间损坏或未知命令会令启动
失败。写入或刷盘失败后，读命令继续可用，后续写命令返回
`ERR AOF persistence unavailable`。若批次末尾的合并 write 失败，服务会关闭当时
已有的客户端连接，避免发送尚未成功追加 AOF 的 `OK`；之后新连接仍可读取，写入
会返回上述错误。

同时启用 RDB 和 AOF 时，RDB 必须包含与当前 AOF 匹配的随机 `PING` 检查点及字节
偏移；启动先加载 RDB，再从该偏移回放 AOF 尾部。令牌、偏移、CRC 或格式不匹配会
严格拒绝启动，不会退化为全量 AOF 回放。只启用 RDB 时不要求 AOF 文件；只存在 AOF
而没有 RDB 时仍执行完整回放。本版本不做 AOF rewrite，因此检查点之前的历史仍保留。

### AOF 记录范围与缓冲

AOF 只记录实际改变 Cache 的写操作：

- 成功的 `SET`；带 TTL 时内部规范化为绝对时间的 `SET ... PXAT`。
- 成功的 `DEL` 和 `PERSIST`。
- 实际改变集合的 `HSET/HDEL/ZADD/ZREM`，保留批量参数。
- 成功的 `EXPIRE/PEXPIRE`，内部规范化为 `PEXPIREAT`；非正 TTL 导致的删除记录
  `DEL`。
- `maxmemory/maxkeys` 触发的 LRU 淘汰记录显式 `DEL`。

未实际生效的写命令、参数/容量错误、GET/TTL/INFO/PING 等只读命令和 TTL 自然过期
不重复写 AOF。

启用 AOF 时一次性分配 256 KiB 可复用编码缓冲区，命令直接追加到缓冲区，避免
逐命令 `malloc/free`。`no/everysec` 在缓冲达到 64 KiB、空间不足、Reactor 一轮
事件处理结束、周期维护或退出时合并 write；同一 Pipeline 的多条写命令可以进入
一次文件写入。`everysec` 的 `fdatasync` 由专用后台线程执行，不阻塞 Reactor 等待
同步完成；正常退出会等待最终同步。`always` 为保持逐命令持久性，仍对每条写命令
执行 write + fdatasync，但复用编码缓冲区。

### MySQL Cache-Aside

启用 MySQL 后，内存命中仍由单线程 Reactor 直接返回；String、Hash、ZSet 顶层 key
未命中时由读线程池完整加载对象，同 key 并发 miss 合并为一次查询。每个连接至多挂起
一个请求，完成结果通过 eventfd 回到 Reactor 线程并继续处理该连接剩余 Pipeline，
MySQL 工作线程不访问 Cache、AOF 或连接对象。

写成功的耐久边界仍是本地 Cache 与 AOF：每条实际生效的 AOF mutation 后紧邻一个
`PING KVMYSQL1:<uuid>:<seq>` 标记，后台单 writer 按序在同一 MySQL 事务内更新业务表、
change log 和水位。LRU 淘汰产生的无标记 `DEL` 只维护热快照，不删除最终数据。启动
时先扫描完整 AOF：MySQL 落后则补投，领先则按 change log 失效旧快照 key；首次启用
只接受空业务表，并把 RDB+AOF 恢复出的对象和绝对 TTL 灌入数据库。对账成功前不会
创建监听 socket。

运行期断库时，未过期的内存命中继续服务，miss 返回
`ERR MySQL backend unavailable`；写队列达到高水位后写命令返回 `TRYAGAIN`。脏 key
在 writer 水位追平前即使被 LRU 淘汰，后续读也会保持挂起而不会用数据库旧值回填。
服务最多等待 5 秒排空写队列，未完成 mutation 由 AOF 在下次启动补投。部署、恢复和
故障语义详见 `docs/v0.7.0-mysql-cache-aside.md`。

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
| `INFO PERSISTENCE` | 查询 AOF、RDB、BGSAVE、fork 与刷盘统计 | Bulk String |
| `INFO MYSQL` | 查询连接、队列、水位、回源、合并、负缓存及错误统计 | Bulk String |
| `DBSIZE` | 查询顶层 key 数，供恢复校验使用 | Integer |
| `PING [message]` | 探活或回显 | Simple/Bulk String |
| `HSET key field value [field value ...]` | 新增或更新 field | 新增 field 数 |
| `HGET key field` | 查询 field | Bulk String 或 Null Bulk |
| `HDEL key field [field ...]` | 删除 field | 删除 field 数 |
| `HLEN key` | field 数 | Integer |
| `HGETALL key` | 返回全部 field/value，顺序不保证 | Array of Bulk Strings |
| `ZADD key score member [score member ...]` | 新增或更新 member | 新增 member 数 |
| `ZREM key member [member ...]` | 删除 member | 删除 member 数 |
| `ZSCORE key member` | 查询 score | Bulk String 或 Null Bulk |
| `ZCARD key` | member 数 | Integer |
| `ZRANGE key start stop [WITHSCORES]` | 按闭区间 rank 查询，支持负数 | Array of Bulk Strings |
| `SAVE` | 在 Reactor 中同步写快照；完成前阻塞服务 | `+OK` 或 Error |
| `BGSAVE` | 一致性屏障后 fork 子进程写快照 | Simple String 或 Error |
| `LASTSAVE` | 最近一次成功快照的 Unix 秒，无则为 0 | Integer |

`TTL/PTTL` 对缺失或已过期 key 返回 `-2`，对永久 key 返回 `-1`。非正数
`EXPIRE/PEXPIRE` 会立即删除现有 key；`SET EX/PX` 要求严格正整数。
集合命令遇到其他类型返回 `WRONGTYPE`。HSET/ZADD 保留顶层 key 的已有 TTL；SET
可覆盖集合并清除 TTL。批量命令中的重复 field/member 以最后一次出现为准。

`INFO CACHE` 返回以下稳定字段：

```text
keys used_memory index_memory maxmemory maxkeys
hits misses hit_rate expired_keys evicted_keys
hash_slots rehashing string_keys hash_keys zset_keys
hash_fields zset_members zset_engine
```

对象查找命令参与 hit/miss 统计。惰性和主动删除都计入 `expired_keys`，容量驱逐
才计入 `evicted_keys`。

## 缓存行为

- Hash 从 16 桶开始，在负载因子达到 0.75 时扩容为两倍；请求操作和周期维护分批
  搬迁旧桶，避免一次性 O(N) rehash 阻塞 Reactor。
- 启用 `maxmemory` 或 `maxkeys` 后，每个缓存条目位于精确 LRU 双向链表中，成功
  `GET/SET` 移到头部，淘汰从尾部开始；两个限制均为 0 时不维护 LRU 链表。
- TTL 使用绝对 Unix 毫秒截止时间和最小堆；访问时惰性过期，timerfd 每 100 ms
  触发一次主动过期，每次最多删除 64 个 key。
- 单个条目超过 `maxmemory` 时，`SET` 返回 `ERR cache capacity exceeded`，已有值
  保持不变。
- `maxkeys` 只统计顶层 key；集合的内部节点、字符串、桶和有序索引计入
  `used_memory`。TTL、LRU 和淘汰作用于整个集合 key，不支持成员级 TTL。

## 测试

```bash
make test
make integration-test
make benchmark-test
KVSTORE_MYSQL_TEST_PASSWORD='replace-me' make mysql-integration-test
make asan
make valgrind
make helgrind
```

- 单元测试覆盖缓冲区、RESP、旧引擎接口、动态 Hash、渐进 rehash、TTL 最小堆、
  精确 LRU、双容量限制、统计、服务命令、AOF 缓冲/编解码、后台同步和尾部修复，
  以及 RDB 三种对象、二进制数据、无穷 score、CRC、版本、截断和长度溢出。
- 集成测试覆盖半包/粘包、Pipeline、背压、并发连接、二进制数据、half-close、
  协议错误、真实定时过期、小容量 LRU，以及 AOF 重启恢复、二进制 key/value 和
  绝对 TTL、Pipeline 批量 write、集合命令、两种 ZSet 后端交叉恢复，以及成功响应
  后 SIGKILL 的启动恢复；RDB 集成测试覆盖 SAVE/BGSAVE/LASTSAVE、自动触发、重复
  BGSAVE、子进程失败、Pipeline 持续读写、混合尾部恢复和 ZSet 交叉恢复。
- `make asan` 使用 ASan/UBSan；`make valgrind` 检查单元与集成主路径；
  `make helgrind` 专门覆盖 AOF writer 与 Reactor eventfd 协作。
- `make benchmark-test` 对 AOF 重放脚本和可选延迟采样做小规模 smoke test，不是
  性能基线。

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

# 启用逐响应延迟采样
./mixed_qps_client -s 127.0.0.1 -p 9096 \
  -c 32 -n 10000000 -w 1000 -P 16 -k 100000 -L
```

`-c` 是连接/客户端线程数，`-n` 是总计时请求数，`-w` 是每连接预热数，`-P`
是 Pipeline 深度（`1..1024`）。混合基准的 `-k` 是所有连接共享并在计时前完成
预加载的全局 keyspace，访问分布为确定性种子的均匀随机；`-T` 设置 SET 的毫秒
TTL，`0` 表示永久；`-S` 可复现实验随机序列，`-C` 保留测试键。准备、预热、
`INFO CACHE` 快照和清理均不计入 QPS。客户端报告 GET hit/miss，并通过计时前后
快照报告服务端 `expired_keys`、`evicted_keys`、内存和 Hash 状态增量。

`-L` 额外报告总体、GET 和 SET 的平均延迟、P50/P95/P99/P99.9、最大延迟以及
P99/P50 尾延迟放大倍数，单位为微秒。Pipeline 场景中的单请求延迟定义为“该批次
开始发送到对应响应解析完成”，包含客户端发送、服务端排队/执行、网络返回和同批
响应顺序；它不是服务端命令函数的纯执行时间。采样需要对每个响应调用
`clock_gettime` 并保存 8 字节样本；10,000,000 个请求的原始样本约 76.3 MiB，当前
聚合排序阶段连同 worker 样本的峰值约 152.6 MiB（不含其他客户端内存）。它也会
带来客户端开销，因此对比测试必须统一是否使用 `-L`。

P99 应和以下指标一起判断：

- P50 反映典型延迟，P95/P99/P99.9 反映不同程度的尾延迟，`max` 用于发现极端
  停顿，P99/P50 用于观察尾部放大。
- GET 与 SET 分位数用于区分普通 Cache 路径和 AOF 写路径；同时保留 QPS、完成数、
  错误数、命中率、过期/淘汰增量，避免用降低吞吐换取表面上的低延迟。
- P99 至少需要足够样本，并应执行多轮报告中位数和波动范围；当前客户端是闭环
  压测，会受到 coordinated omission 影响，不等价于固定到达率负载模型。

### v0.7.0 MySQL 全量冷 miss 实测

本轮在 VMware Ubuntu、客户端/服务/本机 MySQL 同机条件下，使用 32 个连接、
Pipeline 1、64B String value 和不重复 key。每轮均以新进程和空 AOF 启动，计时前
Cache key 数为 0；MySQL/InnoDB buffer pool 不清空，因此“冷”只表示 kvstore 内存
Cache 冷，数据库缓冲为热。测试脚本要求每轮全部 GET 返回预置 value、
`mysql_loads` 增量等于请求数、无合并/回源错误且读队列排空，否则拒绝汇总。

10,000 请求 smoke 为 29,024.98 QPS、P99 1.804 ms，校验通过。正式结果使用
100,000 个唯一 key，每轮 100,000 次 GET，5 轮全部校验通过：

| 轮次 | QPS | P99 |
| ---: | ---: | ---: |
| 1 | 26,491.81 | 1.965 ms |
| 2 | 27,525.39 | 1.850 ms |
| 3 | 26,987.51 | 1.973 ms |
| 4 | 26,868.13 | 1.901 ms |
| 5 | 23,343.86 | 2.179 ms |
| **中位数** | **26,868.13** | **1.965 ms** |

QPS 范围为 23,343.86–27,525.39，五轮总体 CV 为 5.67%；P99 范围为
1.850–2.179 ms。第 5 轮明显偏低，因此该数据作为完整披露的阶段实测中位数，不能
宣称为波动低于 5% 的稳定门禁。简历可表述为：“在 32 并发、64B value、全量冷 key
miss 条件下，MySQL 回源吞吐约 26.9K QPS，P99 延迟约 1.97 ms（5 轮中位数，
Pipeline=1）。”测试方法和未完成门禁见
[`docs/v0.7.0-mysql-cache-aside.md`](docs/v0.7.0-mysql-cache-aside.md)。

### Redis 6.2.23 Hash/ZSet 对照

先单独构建 Redis 6.2.23，并把 `redis-server` 路径传给脚本；仓库不包含 Redis
源码或二进制。脚本用同一个 C 客户端运行本项目与 Redis：本项目 Hash workload
只运行一次，ZSet workload 才分别运行 SkipList 和 RBTree；Redis 的 Hash/ZSet 各
运行一次。CSV 中本项目 Hash 的 `target` 为 `hash`，不会再把 Hash 结果误标为两种
ZSet 后端。Redis 禁用 RDB 和自动 AOF rewrite，并把 Hash/ZSet 紧凑编码阈值设为 0。
等价的 Redis 6.2.23 配置为：

```conf
save ""
hash-max-ziplist-entries 0
zset-max-ziplist-entries 0
auto-aof-rewrite-percentage 0
```

需要测试 AOF 时再设置 `appendonly yes` 与 `appendfsync no|everysec`。脚本直接通过
命令行传入这些选项，命令行配置会覆盖 redis.conf 中的同名配置。

`collection_bench_client -k/--keyspace` 表示读、混合和 ZRANGE workload 共享集合
中的 field/member 总数，与连接数无关。例如 `-c 32 -k 100000` 是 32 个连接共同
访问同一个包含 100,000 个 field/member 的 Hash/ZSet，并且只预加载一次。纯插入
workload 从空集合开始，最终基数等于 `--requests`；各连接使用不重叠的全局请求
编号，插入预热使用独立临时 key 并在计时前删除。客户端和 CSV 同时报告初始与预期
最终 cardinality，避免把 `keyspace` 错解为每连接容量。

`hash-mixed` 的 10% HSET 会保持 value 长度为 64B，但把前 16 字节改为该请求的
十六进制序号，并用下一字节区分预热和计时阶段，确保计时写是更新已有 field 的真实
数据修改，而不是把相同 value 重写一遍；这样本项目和 Redis 都承担对应的对象更新
与 AOF 记录工作。

先运行小规模 smoke：

```bash
make kvstore collection_bench_client
python3 bench/redis_collection_compare.py \
  --redis-server /usr/local/bin/redis-server \
  --aof-policies off --repeats 1 \
  --connections 32 --pipelines 8 \
  --requests 10000 --keyspace 1000 \
  --csv bench/results/redis-6.2.23-smoke.csv
```

确认本项目 Hash、两种 ZSet 后端和 Redis 的全部 workload 无错误后，再运行 v0.6.2
正式对照。每个单元固定 5 轮；P16/P64、100,000 keyspace、64B payload、
10,000,000 请求：

```bash
make kvstore collection_bench_client
python3 bench/redis_collection_compare.py \
  --redis-server /usr/local/bin/redis-server \
  --targets skiplist,rbtree,redis \
  --workloads string-mixed,hash-mixed,zset-mixed \
  --aof-policies off,everysec --scenarios normal,bgsave \
  --repeats 5 --connections 32 --pipelines 16,64 \
  --requests 10000000 --keyspace 100000 \
  --csv bench/results/v0.6.2-redis-6.2.23.csv

python3 bench/v062_release_gate.py \
  --summary-csv bench/results/v0.6.2-redis-6.2.23.csv.summary.csv \
  --json bench/results/v0.6.2-gate.json
```

`--pipelines` 接受单值或逗号列表，例如 `1,4,16,64`；P16/P64 是 v0.6.2 发布门槛，
P1/P4 只作诊断，各深度独立汇总而不混算中位数。
默认对 AOF off 运行全部 workload，并对含写 workload 额外运行 `appendfsync no` 和
`everysec`。CSV 包含 QPS、Mean/P50/P95/P99/P99.9/max、错误数、VmHWM、逻辑
内存、AOF 大小、共享数据 key、初始/预期/实际 cardinality 和 keyspace 范围；每轮
结束前会用 `HLEN/ZCARD` 校验实际 cardinality，不一致立即失败。脚本启动时校验 Redis 必须是
6.2.23，每轮打印 START/DONE、QPS、错误数和耗时，并在每轮结束后立即 flush CSV，
中断时已完成结果不会丢失。摘要计算 QPS 变异系数，超过 5% 的整组标记无效并以退出
码 2 要求重跑。BGSAVE 单元在共享数据预加载后、客户端仍运行时触发一次后台快照，
并记录快照持续时间、fork 暂停、父进程 VmHWM、子进程峰值 RSS/page faults 和 Redis
COW 字节。门禁脚本按四组 ZSet mixed 的几何平均选择后端；差异不超过 1% 时保留
SkipList，然后严格检查项目 QPS/P99 与 everysec 相对损失。客户端使用确定性 seed；正式报告还必须记录 OS、CPU、
内存、磁盘、编译选项、客户端/服务端位置和持续时间。本仓库不写入未经目标 Ubuntu
环境实测的集合性能数字。

### v0.6.2 验收结果与发布决定

本次正式汇总固定 32 连接、100,000 keyspace、64B payload、90% 读/10% 写、
10,000,000 请求，并对每个单元执行 5 轮取中位数。P16/P64 覆盖 AOF off、
everysec 和 everysec+BGSAVE。结果文件位于：

- [`bench/result/v0.6.2-redis-6.2.23.csv.summary.csv`](bench/result/v0.6.2-redis-6.2.23.csv.summary.csv)
- [`bench/result/v0.6.2-gate.json`](bench/result/v0.6.2-gate.json)
- [`bench/result/aof-latency.csv`](bench/result/aof-latency.csv)
- [`bench/result/aof-replay.csv.summary.csv`](bench/result/aof-replay.csv.summary.csv)

42 个 Redis 对照汇总组的 QPS CV 全部低于 5%，最高约 4.70%；错误总数为 0，
最终 cardinality 均为 100,000，BGSAVE 生成的 RDB 也都恢复到 100,000。
18 个胜出后端发布对照单元中 15 个通过：String 和 ZSet 的 P16/P64 全部不劣于
Redis，Hash P64 全部通过，三个失败单元均集中在 Hash P16 QPS。下表中的 P99
优势为项目相对 Redis 的降低比例，正值代表项目更低。

| 工作负载 | Pipeline | 策略/场景 | 项目 QPS | Redis QPS | QPS 差异 | 项目 P99 | Redis P99 | P99 优势 | 对照结果 |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| String | 16 | off | 808,490 | 747,081 | +8.22% | 959 μs | 1,275 μs | 24.75% | 通过 |
| String | 16 | everysec | 752,673 | 711,209 | +5.83% | 871 μs | 1,325 μs | 34.28% | 通过 |
| String | 16 | everysec+BGSAVE | 758,729 | 710,063 | +6.85% | 862 μs | 1,343 μs | 35.79% | 通过 |
| String | 64 | off | 1,888,875 | 1,543,316 | +22.39% | 1,642 μs | 2,403 μs | 31.65% | 通过 |
| String | 64 | everysec | 1,770,578 | 1,437,695 | +23.15% | 1,500 μs | 2,619 μs | 42.75% | 通过 |
| String | 64 | everysec+BGSAVE | 1,751,162 | 1,420,981 | +23.24% | 1,666 μs | 2,632 μs | 36.69% | 通过 |
| Hash | 16 | off | 833,188 | 852,135 | -2.22% | 1,011 μs | 1,031 μs | 1.89% | **QPS 未通过** |
| Hash | 16 | everysec | 737,511 | 757,458 | -2.63% | 1,099 μs | 1,190 μs | 7.61% | **QPS 未通过** |
| Hash | 16 | everysec+BGSAVE | 744,091 | 794,301 | -6.32% | 1,064 μs | 1,089 μs | 2.31% | **QPS 未通过** |
| Hash | 64 | off | 1,471,940 | 1,218,598 | +20.79% | 2,380 μs | 3,275 μs | 27.32% | 通过 |
| Hash | 64 | everysec | 1,337,242 | 1,194,017 | +12.00% | 2,570 μs | 3,520 μs | 26.99% | 通过 |
| Hash | 64 | everysec+BGSAVE | 1,383,256 | 1,219,927 | +13.39% | 2,484 μs | 3,559 μs | 30.20% | 通过 |
| ZSet | 16 | off | 760,678 | 658,041 | +15.60% | 996 μs | 1,236 μs | 19.39% | 通过 |
| ZSet | 16 | everysec | 734,461 | 643,565 | +14.12% | 1,260 μs | 1,271 μs | 0.90% | 通过 |
| ZSet | 16 | everysec+BGSAVE | 735,473 | 641,986 | +14.56% | 1,260 μs | 1,267 μs | 0.55% | 通过 |
| ZSet | 64 | off | 1,575,172 | 989,565 | +59.18% | 2,283 μs | 3,473 μs | 34.27% | 通过 |
| ZSet | 64 | everysec | 1,532,505 | 967,685 | +58.37% | 2,492 μs | 3,529 μs | 29.37% | 通过 |
| ZSet | 64 | everysec+BGSAVE | 1,510,698 | 961,113 | +57.18% | 2,547 μs | 3,568 μs | 28.62% | 通过 |

ZSet 的四组 P16/P64、off/everysec 几何平均 QPS 为 SkipList 1,077,643、RBTree
1,074,634，SkipList 领先约 0.28%。差异不超过 1%，因此按预定规则继续使用
SkipList 作为默认后端。

严格门禁 JSON 的最终状态仍为 `passed: false`。除三个 Hash P16 Redis 对照外，
项目自身 everysec 相对 off 还有以下五项偏差：

| 内部门槛偏差 | 实测 | 原门槛 |
| --- | ---: | ---: |
| String P16 everysec QPS 损失 | 6.90% | ≤ 5% |
| String P64 everysec QPS 损失 | 6.26% | ≤ 5% |
| Hash P16 everysec QPS 损失 | 11.48% | ≤ 5% |
| Hash P64 everysec QPS 损失 | 9.15% | ≤ 5% |
| ZSet P16 everysec P99 增长 | 26.46% | ≤ 10% |

这些偏差被明确接受为 v0.6.2 的已知限制：版本按功能完整性、正确性、资源检查、
15/18 Redis 对照通过以及 BGSAVE/恢复结果发布，不把 `passed: false` 改写为通过；
Hash P16 和 AOF 尾延迟继续作为后续性能债务。

#### BGSAVE 影响

相对普通 everysec，BGSAVE 期间的项目 QPS 变化为 +3.44% 到 -1.42%，P99 大多在
±3.4% 内；String P64 P99 增长 11.10%。fork 暂停为 2.5–9.3 ms，后台快照持续
338–605 ms，所有单元无客户端错误、无 major fault 且恢复基数正确。

| 类型 | Pipeline | BGSAVE QPS 变化 | P99 变化 | fork 暂停 | 快照持续时间 |
| --- | ---: | ---: | ---: | ---: | ---: |
| String | 16 | +0.80% | -0.97% | 2.500 ms | 605 ms |
| String | 64 | -1.10% | +11.10% | 3.209 ms | 565 ms |
| Hash | 16 | +0.89% | -3.21% | 9.305 ms | 479 ms |
| Hash | 64 | +3.44% | -3.34% | 8.397 ms | 522 ms |
| ZSet SkipList | 16 | +0.14% | +0.02% | 4.818 ms | 338 ms |
| ZSet SkipList | 64 | -1.42% | +2.19% | 4.037 ms | 382 ms |

#### AOF 在线吞吐与延迟

AOF 在线测试同样使用 32 连接、Pipeline 16、100,000 keyspace、64B value、
10,000,000 请求和 5 轮。1.5 亿计时请求全部完成，GET 命中率为 100%，SET 错误
为 0；`no` 与 `everysec` 每轮生成相同的 134,366,493-byte AOF。

| AOF 策略 | QPS 中位数 | QPS CV | 相对 off | P50 | P99 | P99.9 | 单轮最大延迟中位数 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| off | 802,657 | 0.49% | — | 612 μs | 965 μs | 1,074 μs | 3.99 ms |
| no | 762,516 | 0.57% | -5.00% | 660 μs | 816 μs | 1,004 μs | 3.85 ms |
| everysec | 761,931 | 0.49% | -5.07% | 664 μs | 815 μs | 1,116 μs | 13.85 ms |

everysec 相对 no 只再损失约 0.08% QPS，主要固定成本来自 AOF 编码、writer 往返
和响应屏障。everysec 的最大延迟中位数约为 off 的 3.5 倍，且五轮最大值均落在
13.6–14.5 ms，说明周期同步仍会形成极端长尾。P99 低于 off 不代表持久化降低了
服务时间，因为当前 Pipeline 延迟包含批次排队和响应顺序；应结合 P50、P99.9、
max 与 `INFO PERSISTENCE` 的 fdatasync 统计判断。

#### AOF 回放

回放摘要把第一轮单列为 `first`，后四轮列为 `subsequent`。项目在全部 String、
Hash、ZSet 和 10k/100k/1m 规模上都快于 Redis；以下为 1,000,000 命令后四轮
中位数。

| 类型/后端 | 项目回放 | Redis 回放 | 相对 Redis |
| --- | ---: | ---: | ---: |
| String | 0.403 s | 0.726 s | 1.80× |
| Hash | 0.357 s | 0.900 s | 2.52× |
| ZSet SkipList | 0.527 s | 1.045 s | 1.98× |
| ZSet RBTree | 0.608 s | 1.045 s | 1.72× |

各规模项目相对 Redis 的回放结果均更快。ZSet 首轮 10k 时 RBTree
更快，但 100k/1m 以及后续轮次总体由 SkipList 占优。部分后四轮 CV 超过 5%，
因此回放数据用于阶段结论，不作为 v0.6.2 严格门禁。

结果文件本身没有嵌入完整 CPU、内存、磁盘/文件系统、Redis 二进制校验值和提交号；
复现实验时仍必须补齐这些环境元数据。v0.6.2 的发布决定是在保留这一可复现性缺口
和上述性能债务说明的前提下作出。

`--targets skiplist,rbtree` 可用于不启动 Redis 的本项目 smoke；Hash 仍只运行一次，
ZSet 分别运行两个后端。正式三方对照保持默认
`--targets skiplist,rbtree,redis`，并必须提供 `--redis-server`。

可以用脚本自动按相同参数比较 AOF 策略。脚本会为每轮使用独立 AOF，服务端与
客户端均在本机，并输出每轮结果、多轮 QPS/P99 摘要和可选 CSV：

```bash
make kvstore mixed_qps_client
python3 bench/aof_latency_bench.py \
  --policies off,no,everysec --repeats 5 \
  --connections 32 --requests 10000000 --warmup 1000 \
  --pipeline 16 --keyspace 100000 --maxmemory 64MiB \
  --build-label "gcc-O2" --csv bench/results/aof-latency.csv
```

默认不测试 `always`，因为逐命令 fdatasync 在大请求量下会持续较久；需要时可显式
加入 `--policies off,no,everysec,always`。若指定 `--aof-dir`，脚本会覆盖其中命名为
`<policy>-<run>.aof` 的本轮测试文件，不要指向存放业务 AOF 的目录。

### v0.5.1 在线吞吐与尾延迟实测

本轮使用 GCC `-O2`，客户端与服务端通过 `127.0.0.1` 同机运行；32 条连接、
Pipeline 16、共享 100,000 key、90% GET / 10% SET、64B value、无 TTL、每连接
预热 1,000 次，每轮计时 10,000,000 请求并重复 5 轮。以下均为五轮中位数，范围
列为五轮最小值到最大值；本轮 CSV 未附 CPU、内存、磁盘和 Ubuntu 版本，因此属于
阶段实测，不作为跨环境结论。

| AOF 策略 | QPS 中位数 | QPS 范围 | 相对 off | Mean | P50 | P99 中位数 | P99 范围 | P99.9 | P99/P50 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `off` | 763,921.95 | 754,841.02–764,430.76 | — | 659.150 μs | 648.118 μs | 1,004.092 μs | 998.557–1,008.982 μs | 1,089.858 μs | 1.543 |
| `appendfsync no` | 730,769.71 | 718,050.12–749,737.19 | -4.34% | 689.600 μs | 672.381 μs | 1,033.979 μs | 1,019.637–1,060.179 μs | 1,179.229 μs | 1.538 |
| `appendfsync everysec` | 725,574.85 | 718,079.70–728,318.14 | -5.02% | 695.959 μs | 685.759 μs | 1,051.311 μs | 1,045.843–1,060.424 μs | 1,143.447 μs | 1.535 |

相对关闭 AOF，`no/everysec` 的 P99 分别增加约 2.98% 和 4.70%，但 P99/P50 都
保持在约 1.54，主要表现为整体延迟平移，而不是 P99 尾部明显展开。每轮请求全部
完成，GET 命中率均为 100%，SET 错误为 0。GET/SET P99 几乎相同，是因为当前
Pipeline 指标从整批发送开始计时，两类请求共同承担批次排队和响应顺序，不能据此
认定单条 SET 与 GET 的服务端执行时间相同。

两种 AOF 策略每轮最终文件均为 133,166,493 bytes（约 127.0 MiB），包含计时外的
预加载 SET 和清理 DEL，不能直接除以计时时间作为在线 AOF 写带宽。单次最大延迟
为 2.26–12.82 ms，但 P99.9 仍在约 1.07–1.28 ms；`max` 只代表极少数离群点，需
结合延迟直方图的 `>2/>5/>10 ms` 次数继续定位。

## AOF 启动重放基准

先构建并停止占用 9096 端口的服务，再运行：

```bash
make kvstore
python3 bench/aof_replay_bench.py \
  --redis-server /usr/local/bin/redis-server \
  --targets project-skiplist,project-rbtree,redis \
  --workloads string,hash,zset \
  --sizes 10000,100000,1000000 --repeats 5 \
  --csv bench/results/aof-replay.csv
```

脚本生成 Redis 6.2.23 与本项目都可加载的确定性 RESP2 AOF：唯一 key 的 64B
`SET`、同一 key 下唯一 field 的 64B `HSET`、同一 key 下唯一 member 的 `ZADD`。
ZSet 分别由本项目 SkipList/RBTree 和 Redis 加载；String/Hash 本项目只运行一次。
加载后用 `DBSIZE/GET`、`HLEN/HGET`、`ZCARD/ZSCORE` 校验。脚本记录 AOF 字节数、
纯 replay 耗时、CPU 时间、进程
启动到端口可连接耗时、每秒
回放命令数、MiB/s 和进程 `VmHWM` 峰值 RSS。多轮摘要输出 replay 的
min/median/P95/max/CV；逐轮 CSV 标记首轮与后续轮次，另生成 `.summary.csv`。
本项目纯回放耗时不包含打开文件、Reactor
初始化和监听，`startup_ready_seconds` 则包含完整可用路径。

重复启动通常会受到 Linux page cache 影响。正式报告应区分冷缓存与热缓存，记录
CPU、内存、磁盘/文件系统、Ubuntu/GCC/编译选项、AOF 大小、命令数和值长度，并且
不要在未获权限时通过清理系统 page cache 干扰同机其他进程。
全 SET/唯一 key 是标准化基线；覆盖写、DEL、TTL 等不同命令组合及最终存活 key 数
会改变回放成本，正式容量评估还应增加贴近真实 AOF 命令分布的数据集。

### v0.5.1 启动重放实测

本轮生成唯一 key 的 RESP2 `SET` AOF，value 固定 64B，每个规模重复 5 次。重放
中位数和完整启动中位数取五轮中值，重放范围为最小值到最大值；CV 是纯回放耗时的
变异系数。

| 命令数 | AOF 大小 | 重放中位数 | 重放范围 | 完整启动中位数 | 中位命令吞吐 | 中位字节吞吐 | 峰值 RSS 中位数 | CV |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10,000 | 0.97 MiB | 5.035 ms | 4.889–5.263 ms | 10.785 ms | 1.986M cmd/s | 192.96 MiB/s | 4.29 MiB | 2.84% |
| 100,000 | 9.81 MiB | 54.360 ms | 53.098–57.670 ms | 57.532 ms | 1.840M cmd/s | 180.50 MiB/s | 26.21 MiB | 3.00% |
| 1,000,000 | 99.08 MiB | 780.884 ms | 553.039–997.459 ms | 786.548 ms | 1.281M cmd/s | 126.88 MiB/s | 242.72 MiB | 22.40% |

10,000 和 100,000 命令的波动约 3%，基本接近线性；1,000,000 命令的 CV 达
22.40%，第 1 轮约 997 ms、第 4/5 轮约 565/553 ms，明显受到 page cache、VM
调度、CPU 频率、内存缺页或宿主机负载影响，不能只取最快一轮。100 万命令时纯
回放约占完整启动时间的 99.28%，后续启动优化应优先分析 RESP 解析、Cache 插入、
Hash 扩容和内存分配。由于每条命令都创建一个存活 key，该规模的峰值 RSS 约
255 bytes/key；覆盖写或 DEL 较多时不能按命令数直接推算 RSS。

v0.4 的阶段性能基线及解释记录在发布说明中；以上 v0.5.1 数据已经包含多轮中位数
和延迟分位数，但缺少完整机器与存储环境信息，仍不得作为生产或跨项目结论。正式
性能报告必须记录 Ubuntu/GCC/编译选项、CPU/内存、磁盘/文件系统、客户端与服务端
位置、容量配置、并发数、Pipeline 深度、请求规模和持续时间。

## 当前限制

- v0.7.0 已完成 MySQL Cache-Aside 功能和全量冷 String miss 实测，但 100% 内存命中、
  热写回、热点合并/断库性能及当前版本完整 sanitizer/Valgrind/Helgrind 发布矩阵尚未
  闭环，因此当前 feature 分支不应创建正式 `v0.7.0` tag/Release。
- v0.6.2 在明确接受严格门禁 `passed: false` 的前提下发布：Hash P16 的三个 Redis
  对照单元 QPS 未达标，且 String/Hash 的部分 everysec QPS 损失与 ZSet P16
  everysec P99 增幅超过原门槛；详见上文验收表和原始结果文件。
- AOF rewrite、外部 SQL 写入/CDC、配置文件、集群和复制尚未实现；RDB 不是 Redis
  RDB 格式，检查点前 AOF 历史不会清理。
- `SAVE` 按 Redis 语义同步阻塞 Reactor，只用于人工维护、诊断和确定性测试，不进入
  QPS 发布结果；生产快照使用 `BGSAVE`。`always` 按定义逐条等待 fdatasync。
- Hash 只扩容、不缩容；当前仍使用已有的非加盐字节哈希函数。
- LRU 是精确实现，不是 Redis 的抽样近似算法；TTL 不支持成员级过期。
- io_uring、高维向量检索、负载均衡、分片、复制和集群不属于 v0.7.0。
