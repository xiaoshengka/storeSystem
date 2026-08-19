# storeSystem

基于 Linux C、非阻塞 Socket 与单线程 epoll Reactor 的内存 KV 缓存服务。

当前发布版本为 `v0.5.1`：在 v0.5.0 AOF 功能基础上加入可复用缓冲区、Pipeline
批量 write 和 everysec 后台 fdatasync。NtyCo、Array 与 RBTree 仅作为历史或算法
对照保留；AOF rewrite 不属于本阶段。

## 架构

```text
Client
  -> epoll Reactor / timerfd 周期维护
  -> RESP2 增量解析与编码
  -> SET/GET/DEL/TTL 命令服务
  -> Cache（TTL 最小堆 + LRU 双向链表 + 统计）
  -> 动态 Hash（双表渐进式 rehash）
  -> AOF（RESP2 写命令 + 批量 write + 后台 everysec fsync + 启动回放）
```

Reactor 负责连接、非阻塞收发、Pipeline 背压、周期回调和资源回收；协议层只处理
RESP 字节帧；服务层负责命令语义；Cache 不依赖网络或 RESP；Hash 只负责二进制
key 的索引。v0.5.1 AOF 设计见
[`docs/aof-v0.5.1.md`](docs/aof-v0.5.1.md)；v0.4 Cache 设计见
[`docs/cache-v0.4.md`](docs/cache-v0.4.md)。

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

## 启动、容量与 AOF

```bash
./kvstore
./kvstore --maxmemory 64MiB
./kvstore --maxmemory 64MiB --maxkeys 100000
./kvstore --appendonly yes --appendfilename appendonly.aof \
  --appendfsync everysec
```

- `--maxmemory`：缓存条目的逻辑字节上限，接受字节数或大小写不敏感的
  `KiB/MiB/GiB` 后缀。
- `--maxkeys`：最大 key 数量，只接受无符号十进制整数。
- 两个上限都可以使用，任一超限都会触发 LRU；`0` 表示不限制。
- `--engine` 已移除，epoll 主服务固定使用 Hash。
- `--appendonly yes|no`：是否启用 AOF，默认 `no`。
- `--appendfilename`：AOF 路径，默认 `appendonly.aof`。
- `--appendfsync always|everysec|no`：每条写命令同步、约每秒同步或交给操作系统，
  默认 `everysec`。该选项只在 AOF 开启时生效。

`maxmemory` 统计 key/value 分配及缓存条目和 Hash 节点的固定元数据，不等同于
进程 RSS。Hash 桶数组和 TTL 堆的预留空间通过 `index_memory` 单独报告。

启用 AOF 后，服务在监听端口前先回放文件。TTL 会以绝对 Unix 毫秒截止时间写入，
因此停机时间也计入 TTL；恢复时已经过期的写入不会重新获得完整 TTL。文件末尾因
崩溃留下的不完整 RESP 帧会截断到最后一条完整命令，中间损坏或未知命令会令启动
失败。写入或刷盘失败后，读命令继续可用，后续写命令返回
`ERR AOF persistence unavailable`。若批次末尾的合并 write 失败，服务会关闭当时
已有的客户端连接，避免发送尚未成功追加 AOF 的 `OK`；之后新连接仍可读取，写入
会返回上述错误。

### AOF 记录范围与缓冲

AOF 只记录实际改变 Cache 的写操作：

- 成功的 `SET`；带 TTL 时内部规范化为绝对时间的 `SET ... PXAT`。
- 成功的 `DEL` 和 `PERSIST`。
- 成功的 `EXPIRE/PEXPIRE`，内部规范化为 `PEXPIREAT`；非正 TTL 导致的删除记录
  `DEL`。
- `maxmemory/maxkeys` 触发的 LRU 淘汰记录显式 `DEL`。

未命中的写命令、参数/容量错误、GET/TTL/INFO/PING 等只读命令和 TTL 自然过期
不重复写 AOF。

启用 AOF 时一次性分配 256 KiB 可复用编码缓冲区，命令直接追加到缓冲区，避免
逐命令 `malloc/free`。`no/everysec` 在缓冲达到 64 KiB、空间不足、Reactor 一轮
事件处理结束、周期维护或退出时合并 write；同一 Pipeline 的多条写命令可以进入
一次文件写入。`everysec` 的 `fdatasync` 由专用后台线程执行，不阻塞 Reactor 等待
同步完成；正常退出会等待最终同步。`always` 为保持逐命令持久性，仍对每条写命令
执行 write + fdatasync，但复用编码缓冲区。

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
make benchmark-test
make asan
make valgrind
```

- 单元测试覆盖缓冲区、RESP、旧引擎接口、动态 Hash、渐进 rehash、TTL 最小堆、
  精确 LRU、双容量限制、统计、服务命令、AOF 缓冲/编解码、后台同步和尾部修复。
- 集成测试覆盖半包/粘包、Pipeline、背压、并发连接、二进制数据、half-close、
  协议错误、真实定时过期、小容量 LRU，以及 AOF 重启恢复、二进制 key/value 和
  绝对 TTL、Pipeline 批量 write，以及成功响应后 SIGKILL 的启动恢复。
- `make asan` 使用 ASan/UBSan；`make valgrind` 检查单元与集成主路径。
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
  --sizes 10000,100000,1000000 --repeats 5 --value-size 64 \
  --csv bench/results/aof-replay.csv
```

脚本直接生成 key 唯一、值长度固定的确定性 RESP2 `SET` AOF，逐个启动真实
`kvstore`，校验实际回放命令数，并记录：AOF 字节数、纯 `aof_replay` 耗时、进程
启动到端口可连接耗时、每秒
回放命令数、MiB/s 和进程 `VmHWM` 峰值 RSS。多轮摘要输出 replay 的
min/P50/P95/max；原始每轮结果可写入 CSV。纯回放耗时不包含打开文件、Reactor
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

- AOF rewrite、MySQL Cache-Aside、配置文件、集群和复制尚未实现。
- AOF 普通文件 write 仍由 Reactor 执行；脏页限流或文件系统异常仍可能影响尾
  延迟。`always` 按定义逐条等待 fdatasync，吞吐显著低于另外两种策略。
- Hash 只扩容、不缩容；当前仍使用已有的非加盐字节哈希函数。
- LRU 是精确实现，不是 Redis 的抽样近似算法；TTL 不支持成员级过期。
- RBTree 不再是 epoll 服务后端，计划在 v0.7 与 Skip List 一起用于有序集合模块。
