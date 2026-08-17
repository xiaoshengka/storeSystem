# v0.2.0 epoll Reactor 设计

## 边界

`src/net` 只负责网络事件和连接生命周期。请求处理函数在初始化 Reactor 时注入，
因此网络代码不依赖 Array、RBTree、Hash 或具体命令。

本阶段不定义流式协议帧。每次从非阻塞 Socket 读取到 `EAGAIN` 后，将本批数据作为
一条文本请求交给服务层；RESP 增量解析和 Pipeline 留到 v0.3.0。

## 对外接口

`include/net/reactor.h` 提供：

- `reactor_init`：创建 epoll、单个监听 Socket 和用于停止循环的 eventfd。
- `reactor_run`：运行单线程 Level-Triggered 事件循环。
- `reactor_stop`：设置停止状态并唤醒阻塞的 `epoll_wait`。
- `reactor_destroy`：注销事件、关闭 fd，并释放所有活动连接及缓冲区。
- `reactor_add/modify/remove`：对 epoll 注册操作的最小封装。

## 连接状态

每个客户端由动态分配的连接对象表示，`epoll_event.data.ptr` 直接指向该对象。对象
包含 fd、当前事件掩码、输入/输出缓冲和读写位置，不再按 fd 建立百万项静态数组。

```text
accept
  -> EPOLLIN：读取至 EAGAIN，生成一条响应
  -> EPOLLOUT：发送至完成或 EAGAIN
  -> EPOLLIN：响应发送完成，等待下一条请求
  -> close：EOF、致命错误或完整响应后的 peer half-close
```

仅当输出缓冲区存在待发送数据时注册 `EPOLLOUT`，避免可写事件造成忙轮询。部分写
通过输出缓冲的 `read_pos` 保留进度。`EPOLLERR`、`EPOLLHUP`、`EPOLLRDHUP`、
`EINTR`、`EAGAIN` 和 SIGPIPE 均有明确处理路径。

## 验收

在 Ubuntu 22.04.5 的干净工作区依次执行：

```bash
make clean && make
make test
make integration-test
make asan
make valgrind
```

`make valgrind` 会在 Valgrind 下运行单元测试及完整 Reactor 集成测试；也可以通过
`KVSTORE_SERVER_COMMAND` 为集成脚本提供自定义的服务端启动命令。只有上述验证
通过后，才合并到 `develop/main` 并发布 `v0.2.0`。
