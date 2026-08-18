# v0.4.0 Cache TTL/LRU 设计

## 范围与边界

v0.4.0 将 epoll 主服务统一为 Hash 缓存，完成动态扩容、TTL、主动/惰性过期、
精确 LRU、容量限制和统计。RESP 增量解析、Pipeline 和 Reactor 连接状态机保持
v0.3 语义；AOF、MySQL、配置文件、Skip List 和有序集合不属于本阶段。

```text
service -> cache -> hash index
reactor -> generic periodic callback -> cache maintenance
```

Cache 条目持有 value、过期截止时间、LRU 指针、最小堆下标和 Hash 节点句柄。
Hash 节点持有二进制 key；两个模块不会重复保存 key。

## 动态 Hash

- 初始表为 16 桶，拉链法解决冲突。
- 插入导致负载因子超过 0.75 时分配两倍大小的新表并开始 rehash。
- rehash 期间新 key 进入新表，查询和删除检查新旧两表。
- 普通缓存操作扫描一个旧桶；100 ms 周期维护最多扫描 16 个旧桶。
- 所有旧桶迁移完成后释放旧表。v0.4 不执行缩容。

该实现保留原有二进制安全 Hash 接口供历史代码测试，但 epoll 服务通过 Cache API
使用新的通用 payload 索引。

## TTL

条目使用 `expire_at_ms == 0` 表示永久，否则保存 Unix epoch 毫秒截止时间。生产
时钟使用 `CLOCK_REALTIME`，测试可注入假时钟。

- 惰性过期：任何定位 key 的缓存操作发现 `now >= expire_at_ms` 时立即删除。
- 主动过期：TTL 条目进入最小堆；timerfd 每 100 ms 唤醒通用 Reactor 周期回调，
  每轮最多删除 64 个过期 key。
- 修改、删除和容量淘汰都通过堆下标以 O(log N) 从任意位置移除 TTL 条目。
- `SET` 不带 EX/PX 时清除旧 TTL；EX/PX 截止时间溢出时拒绝写入。

## LRU 与容量

所有条目同时位于双向链表。成功 GET/SET 移到头部；DEL、TTL 查询、EXPIRE 和
PERSIST 不改变顺序。任一容量上限超出时从尾部删除，直到两个条件都满足。

- `maxkeys` 限制 Hash 条目数。
- `maxmemory` 是可确定的逻辑条目费用：key/value 分配、Cache 条目和 Hash 节点
  固定大小；不代表 malloc RSS。
- `index_memory` 单独报告 Hash 桶数组与 TTL 堆预留数组。
- 新条目自身超过 maxmemory 时直接拒绝；更新先分配替换 value 并计算最终容量，
  然后驱逐其他条目并提交，失败时保留原值。

## 命令与统计

公开命令为 `SET [EX|PX]`、`GET`、`DEL`、`EXPIRE`、`PEXPIRE`、`TTL`、`PTTL`、
`PERSIST`、`INFO CACHE` 和 `PING`。整数按有符号 64 位严格解析并检查单位换算与
截止时间溢出。

只有 GET 更新 `hits/misses`。过期删除更新 `expired_keys`；容量删除更新
`evicted_keys`。`INFO CACHE` 还报告容量、使用量、key 数、Hash 桶数及 rehash
状态。

## 已知限制

- 主动过期有每轮预算，因此大量 key 同时到期时，物理回收可能分多轮完成；逻辑
  读取仍通过惰性过期保证看不到失效值。
- Hash 不缩容，且本阶段未替换为带随机种子的抗碰撞哈希函数。
- v0.4 没有持久化；v0.5 AOF 必须保存或重建绝对过期语义。
