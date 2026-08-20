# v0.6.0 Hash 与 ZSet 设计

## 对象和所有权

顶层 keyspace 仍是从 16 桶开始、负载因子 0.75、双表渐进 rehash 的动态 Hash。
每个顶层 entry 独占一个 `kv_object_t`，类型为 String、Hash 或 ZSet。Cache 统一
析构对象，并把对象节点、字符串、桶和 ZSet 有序索引计入 `used_memory`；
`maxkeys` 只统计顶层 key。TTL、LRU、主动/惰性过期和淘汰也只作用于顶层 key。

`SET` 以 String 替换任意旧类型并清除 TTL。`HSET`、`ZADD` 修改已有集合时保留
TTL；最后一个 field/member 被删除时，整个顶层 key 同步删除。类型不匹配返回
Redis 风格 `WRONGTYPE`。

## Hash

Hash field/value 均按显式长度处理，可包含 NUL。内部复用动态 Hash 引擎，因此具有
渐进 rehash 行为。`HGETALL` 通过同时遍历新旧表输出，顺序不稳定，调用方不应依赖
排列。

## ZSet 双索引

所有 ZSet 都有成员 Hash 字典，字典节点保存 score 和有序节点地址，保证 member
唯一并使 `ZSCORE` 为均摊 O(1)。进程级 `--zset-engine skiplist|rbtree` 选择有序
索引，默认 SkipList：

- SkipList 最大 32 层，逐层晋升概率 0.25，并维护 span 以支持 rank 定位。
- RBTree 按 `(score, member)` 排序，节点维护子树大小以支持 rank 定位。
- score 使用 `double`，拒绝 NaN，允许正负无穷；同分 member 按二进制字典序排序。
- `ZADD/ZREM` 为 O(log N)，`ZSCORE` 均摊 O(1)，`ZRANGE` 为 O(log N + M)。

成员 score 更新先准备新有序节点，再从旧索引移除并插入新索引。批量 HSET/ZADD
在可能触发容量限制时操作对象副本，通过容量检查后一次替换，避免部分提交。

## RESP、AOF 和限制

集合读命令使用 RESP2 Bulk、Integer、Null Bulk 或 Array of Bulk Strings。Reactor
复用 1 MiB 编码缓冲；完整响应超过上限时返回明确错误，不发送半帧。

AOF 只追加实际改变数据的 HSET/HDEL/ZADD/ZREM。批量命令保留为批量 RESP2 帧，
无变化命令不写入；过期和淘汰仍以顶层 DEL/绝对截止时间表达。AOF 不绑定 ZSet
有序后端，可由 SkipList 写入并用 RBTree 恢复，反之亦然。

本版本不实现 Redis Set/SADD、成员级 TTL、ZADD NX/XX/GT/LT/CH/INCR、按 score
范围查询或全局 keyspace 引擎切换。

## Redis 对照数据集

集合对照客户端的 `keyspace` 是读、混合和 ZRANGE workload 的全局 field/member
数。所有并发连接共享同一个 Hash key 或 ZSet key，并由单独的准备连接只预加载
一次。纯插入 workload 从空集合开始，以全局请求编号生成不重复 field/member，
最终基数等于计时请求数；每连接预热写入独立临时 key，并在正式计时前删除。因此
连接数只改变并发度，不会把数据集复制成每连接一份。

测试矩阵把本项目 Hash 作为独立 target 只运行一次；`--zset-engine` 只用于分别运行
SkipList/RBTree ZSet workload。混合 Hash 写保持 64B value 长度，但嵌入请求序号和
预热/计时阶段标记，保证每次计时 HSET 都实际更新已有 field。Redis 对照关闭 RDB、紧凑编码和自动 AOF
rewrite（`auto-aof-rewrite-percentage 0`），避免后台 rewrite 干扰 AOF 大小、吞吐和
尾延迟比较。
