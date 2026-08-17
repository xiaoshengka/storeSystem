# v0.3.0 RESP2、增量解析与 Pipeline 设计

## 1. 范围

本阶段只为 epoll 主服务增加 RESP2 子集、统一命令和 Hash/RBTree 可选后端。Array、
NtyCo 文本命令、TTL/LRU、持久化和数据库不进入本阶段。

```text
Socket bytes
  -> Reactor connection input buffer
  -> RESP incremental parser
  -> SET / GET / DEL / PING service
  -> selected Hash or RBTree engine
  -> RESP encoder
  -> Reactor connection output buffer
```

协议模块不访问引擎；网络模块只根据回调返回的“完整/不完整/错误、消费字节数、
响应和关闭标志”管理连接。

## 2. 请求语法

请求必须是顶层 RESP2 Array，数组元素必须是非 Null Bulk String：

```text
request    = "*" argc CRLF bulk-string{argc}
bulk-string = "$" length CRLF payload[length] CRLF
```

- Array 至少包含一个参数，最多 128 个参数。
- 完整请求帧（含头部与 CRLF）最大 64 KiB。
- 数字字段只接受非负十进制，所有累加和边界计算检查溢出。
- 不接受 inline、Simple String 请求、嵌套 Array、Null Array 或 Null Bulk。
- Bulk payload 是二进制数据，允许长度为零以及嵌入 `NUL`。

解析器不修改输入、不分配 payload，通过 `pointer + length` 返回零拷贝参数切片。
数据不足时消费量为零；完整时只返回当前第一帧的长度，后续粘连帧留给下一轮解析。

## 3. 命令与回复

- `SET key value`：Hash/RBTree 均执行 upsert，返回 `+OK\r\n`。
- `GET key`：命中返回 `$length\r\npayload\r\n`，未命中返回 `$-1\r\n`。
- `DEL key`：删除返回 `:1\r\n`，不存在返回 `:0\r\n`。
- `PING`：返回 `+PONG\r\n`；带一个参数时以 Bulk String 原样返回。
- 命令名使用 ASCII 大小写不敏感比较；其他命令返回
  `-ERR unknown command\r\n`。
- 参数数量不正确返回 `-ERR wrong number of arguments\r\n`；引擎失败返回
  `-ERR internal error\r\n`。

非法、超限或 EOF 时仍不完整的帧返回 `-ERR Protocol error\r\n`。连接进入
close-after-write 状态，不再读取或解析后续字节，错误响应排空后统一回收。

## 4. 二进制安全引擎接口

Hash 和 RBTree 节点分别保存 key/value 指针与长度。为兼容历史 NtyCo 和旧单元测试，
现有字符串 API 继续以 `strlen` 调用内部逻辑并保留 insert-only 等旧返回语义；RESP
服务使用新的字节接口：

- Hash 对全部 key 字节计算哈希，并用长度加 `memcmp` 判断相等。
- RBTree 用公共前缀 `memcmp`，相等时以长度决定顺序。
- upsert 在 key 已存在时先成功分配新 value，再替换旧 value，避免分配失败破坏数据。
- GET 返回借用指针；单线程服务会在当前响应编码完成后才执行下一条修改命令。

## 5. Pipeline 与背压

每次收到数据后，Reactor 循环调用协议/服务回调：

1. 完整帧：消费该帧，将响应按顺序追加到连接输出缓冲。
2. 不完整帧：保留所有输入字节并等待下一次 `EPOLLIN`。
3. 协议错误：追加错误响应，标记发送后关闭。
4. 输出缓冲达到 1 MiB：暂停该连接的 `EPOLLIN`，只保留 `EPOLLOUT`。
5. 输出排空：先继续处理输入缓冲中的完整帧，再恢复读取。

因此响应顺序与请求顺序一致，部分写继续依赖 `net_buffer_t` 的 read/write offset，
慢客户端不能使单连接待发送数据无限增长。

## 6. 启动、测试与压测

```bash
./kvstore --engine hash
./kvstore --engine rbtree

make test
make integration-test
make asan
make valgrind

./qps_client -c 32 -n 1000000 -w 1000 -P 1
./qps_client -c 32 -n 1000000 -w 1000 -P 16
```

集成测试对两种引擎运行完全相同的 RESP 用例。`qps_client` 的 `-P` 范围为
`1..1024`；Hash/RBTree 性能对比必须保持服务端环境和所有客户端参数一致，并单独
记录引擎与 Pipeline 深度。仓库不把功能性 smoke test 的瞬时 QPS 写成正式结论。
