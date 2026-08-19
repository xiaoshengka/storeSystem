# v0.5.1 AOF 设计

## 范围与模块边界

v0.5.0 在现有单线程 Reactor 和 Hash Cache 上增加 AOF；v0.5.1 优化追加与刷盘
路径，不改变网络模型，也不引入 rewrite、MySQL 或复制。网络、协议、命令和
Cache 仍由单线程 Reactor 执行；仅 `everysec` 的 `fdatasync` 使用一个专用后台
线程。

```text
service -> cache
        -> persistence/aof reusable buffer -> batched write -> append-only file
reactor event batch end -> AOF buffer flush
reactor timerfd -> service maintenance -> cache maintenance + fsync request
background fsync worker -> fdatasync
startup -> AOF RESP2 replay -> service replay API -> cache
```

`src/persistence/aof.c` 只负责 RESP2 记录编码、缓冲、文件追加、刷盘和流式回放，
不解析业务语义；服务层决定哪些成功写操作需要持久化。AOF 默认关闭，避免改变
未启用持久化时的性能和磁盘行为。

## 记录格式与写入语义

AOF 是连续 RESP2 `Array of Bulk Strings`，因此 key/value 保持二进制安全。服务只在
写操作实际改变数据后记录：

- `SET key value` 保存永久值。
- 带 TTL 的 SET 规范化为内部记录 `SET key value PXAT unix_ms`。
- `EXPIRE/PEXPIRE` 规范化为 `PEXPIREAT key unix_ms`；非正 TTL 的成功删除记录
  `DEL`。
- 成功的 `DEL` 和 `PERSIST` 记录同名命令。
- LRU 容量淘汰通过 Cache 的通用淘汰通知记录显式 `DEL`，恢复结果不依赖未写入
  AOF 的历史 GET/LRU 顺序。
- 惰性或主动 TTL 过期无需记录 DEL，因为绝对截止时间会在回放时重建或跳过。

以下情况不写 AOF：

- `GET`、`TTL/PTTL`、`INFO CACHE` 和 `PING` 等只读命令。
- 未命中的 `DEL/EXPIRE/PEXPIRE/PERSIST`，以及对永久 key 执行的无效果 PERSIST。
- 参数错误、容量错误、内部错误或其他未成功修改 Cache 的命令。
- TTL 自然到期；其绝对截止时间已经包含在先前的 PXAT/PEXPIREAT 记录中。

公开 RESP 命令仍只接受 v0.4 的 `SET EX|PX` 与 `EXPIRE/PEXPIRE`；`PXAT` 和
`PEXPIREAT` 是 AOF 回放的内部规范格式，不对客户端开放。

内存修改成功后才会追加相应用户写记录。在线请求把记录加入 AOF buffer；Reactor
在可能发送响应前以及每轮事件批次结束时执行合并 write。若编码或 write 失败，
对应请求返回 `ERR AOF persistence unavailable`；若事件批次末尾的合并 write 才
失败，Reactor 会关闭当时的客户端连接，避免发送已经排队的成功响应。若后台 fsync
异步失败，最迟在下一次 Reactor 刷新或 100 ms 周期维护时发现并将 AOF 标记为
失败。故障只触发一次连接清理，之后新连接仍可执行读命令，写命令返回 AOF 错误。
容量淘汰通知发生在 SET 完成前，其 DEL 会先于 SET 进入同一缓冲区，与 Cache 的
实际变更顺序一致。

## 追加缓冲与合并 write

AOF 打开时一次性分配 256 KiB 编码缓冲区，之后每条命令直接编码到未使用区域，
不再为每条记录执行 `malloc/free`。缓冲区在以下时机写入文件：

- 累积数据达到 64 KiB 阈值。
- 当前空间不足以容纳下一条完整记录。
- 一轮 Reactor 事件处理结束；若同一连接的 Pipeline 或同一轮多个连接产生多条
  写命令，它们合并为一次 write。
- 100 ms 周期维护、正常退出或显式 AOF flush。

单条 AOF 记录不会拆开编码；底层 write 仍处理 `EINTR` 和部分写。`no/everysec`
允许一个 Reactor 批次的数据暂存在用户态缓冲区，但 Reactor 会在发送该批响应前
刷新，SIGKILL 集成测试验证收到成功响应的 Pipeline 可以从文件恢复。

`always` 为保持每条成功响应前已经 `fdatasync` 的语义，不跨命令批量刷盘；它仍
复用同一编码缓冲区，从而去除逐命令内存分配，但每条写命令仍执行一次 write 和
一次 fdatasync。

## 刷盘策略

| 策略 | 行为 | 主要权衡 |
| --- | --- | --- |
| `always` | 每条记录执行 write + fdatasync，成功后才响应 | 最小已确认数据窗口，写延迟最高 |
| `everysec` | Reactor 合并 write；周期回调约每秒向后台线程提交 fdatasync | 正常情况下最多约 1 秒未同步数据，不在 Reactor 中直接等待 fsync |
| `no` | Reactor 合并 write，不主动 fdatasync | 吞吐优先，持久性由操作系统决定 |

后台线程只执行 `fdatasync`，不读取或修改 Cache，也不处理网络事件。写入代次由
mutex 保护；若同步期间 Reactor 又完成新 write，新代次会在下一次同步中覆盖。
`everysec` 正常退出时提交最终同步并 `pthread_join` 等待完成；`no` 只刷新用户态
buffer 并关闭文件，不主动同步。

## 启动回放与异常文件

启动顺序为 Cache 初始化、AOF 打开与回放、AOF 绑定到在线写路径、Reactor 监听。
回放使用固定上限缓冲区增量读取，不把整个 AOF 载入内存。只接受本版本生成的
`SET`、`DEL`、`PERSIST` 和 `PEXPIREAT` 记录：

- TTL 使用绝对 Unix 毫秒时间；截止时间已到的 SET 会保证该 key 不存在，已到期的
  PEXPIREAT 等价于删除。
- 文件中间的非法 RESP、未知命令、错误参数或超限记录会令启动失败。
- EOF 处的不完整记录视为崩溃尾帧，自动 `ftruncate` 到最后一条完整记录并给出
  warning；已经完整但业务非法的尾帧不会静默丢弃。
- 回放受当前 `maxmemory/maxkeys` 配置约束；配置小于历史数据需要时可能启动失败或
  按已记录的淘汰顺序恢复。

## 回放与尾延迟基准

服务端使用 `CLOCK_MONOTONIC` 包围 `aof_replay` 调用，并输出
`aof_replay_commands` 与 `aof_replay_duration_seconds`。该区间只包含 RESP2 解析和
Cache 重建，不包含 AOF open、Reactor 初始化及 listen。`bench/aof_replay_bench.py`
另外从创建进程计时到 9096 端口可连接，得到完整的 `startup_ready_seconds`，并
报告命令/s、MiB/s 和 `/proc/<pid>/status` 的 `VmHWM`。

`mixed_qps_client -L` 在计时阶段为每个响应保存纳秒样本。Pipeline 中的延迟起点是
整批开始发送，终点是对应响应解析完成，因此包含发送、服务端处理、返回和同批响应
顺序。输出总体及 GET/SET 的 mean、P50、P95、P99、P99.9、max 和 P99/P50；QPS、
完成/错误数、命中率、过期与淘汰增量继续同时输出。`-L` 会引入时间戳和样本内存
开销，只能与同样启用 `-L` 的测试对比。

`bench/aof_latency_bench.py` 负责按 `off/no/everysec`（可选 `always`）多轮启停
服务、调用 `mixed_qps_client -L`、隔离每轮 AOF，并输出原始 CSV 及各策略的 QPS
中位数、范围、P99 中位数和 P99.9 中位数。

正式测试需要多轮运行，并同时记录 AOF 策略、AOF 大小/命令数、Pipeline、连接数、
CPU/内存、磁盘/文件系统和冷/热 page cache 状态。当前客户端为闭环负载，P99 仍会
受到 coordinated omission 影响；若需要固定到达率的服务等级结论，应另行实现
open-loop 基准，不在本阶段扩展范围内。

## 已知限制

- v0.5 不实现 AOF rewrite，文件会随成功写命令持续增长。
- 普通文件 write 仍由 Reactor 执行；虽然已经批量化，但脏页限流、文件系统或磁盘
  异常仍可能使 write 阻塞。后台 fdatasync 与 write 的内核竞争也可能影响尾延迟。
- `always` 按定义仍在 Reactor 中逐条等待 fdatasync，不适合追求高吞吐的场景。
- 没有目录级 fsync、RDB、复制或多进程共同写同一 AOF 的支持。
- AOF 文件应由单个服务实例独占；不要在运行时手工编辑。
