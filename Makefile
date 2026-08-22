CC ?= gcc
NETWORK_BACKEND ?= epoll
MYSQL ?= 0
BUILD_DIR ?= build/$(NETWORK_BACKEND)-mysql$(MYSQL)

CPPFLAGS ?= -Iinclude
CFLAGS ?= -std=c11 -O2 -g -Wall -Wextra -Wpedantic
DEPFLAGS ?= -MMD -MP
LDFLAGS ?=
LDLIBS ?=
MYSQL_COLD_MISS_ARGS ?=
MYSQL_PERFORMANCE_GATE_ARGS ?=

ifeq ($(MYSQL),1)
MYSQL_CFLAGS := $(shell pkg-config --cflags mysqlclient 2>/dev/null)
MYSQL_LIBS := $(shell pkg-config --libs mysqlclient 2>/dev/null)
ifeq ($(strip $(MYSQL_LIBS)),)
MYSQL_CFLAGS := $(shell mysql_config --cflags 2>/dev/null)
MYSQL_LIBS := $(shell mysql_config --libs 2>/dev/null)
endif
ifeq ($(strip $(MYSQL_LIBS)),)
$(error MySQL client development files are required for MYSQL=1)
endif
CPPFLAGS += $(MYSQL_CFLAGS) -DKVSTORE_WITH_MYSQL
LDLIBS += $(MYSQL_LIBS)
endif

ifneq ($(filter clean asan valgrind valgrind-run helgrind,$(MAKECMDGOALS)),)
ALLOCATOR ?= libc
else
ALLOCATOR ?= jemalloc
endif

ifeq ($(ALLOCATOR),jemalloc)
JEMALLOC_CFLAGS := $(shell pkg-config --cflags jemalloc 2>/dev/null)
JEMALLOC_LIBS := $(shell pkg-config --libs jemalloc 2>/dev/null)
ifeq ($(strip $(JEMALLOC_LIBS)),)
$(error jemalloc is required for the default build; install libjemalloc-dev or use ALLOCATOR=libc for diagnostics)
endif
CPPFLAGS += $(JEMALLOC_CFLAGS) -DKVSTORE_ALLOCATOR_JEMALLOC
LDLIBS += $(JEMALLOC_LIBS)
else ifeq ($(ALLOCATOR),libc)
CPPFLAGS += -DKVSTORE_ALLOCATOR_LIBC
else
$(error Unsupported ALLOCATOR '$(ALLOCATOR)'; use jemalloc or libc)
endif

LEGACY_ENGINE_SRCS := \
	src/kvstore_array.c \
	src/kvstore_rbtree.c \
	src/kvstore_hash.c
HASH_ENGINE_SRCS := src/kvstore_hash.c
OBJECT_ENGINE_SRCS := src/engine/object.c
CACHE_SRCS := src/cache/cache.c
AOF_SRCS := src/persistence/aof.c
RDB_SRCS := src/persistence/rdb.c
PERSISTENCE_SRCS := $(AOF_SRCS) $(RDB_SRCS)
LEGACY_SERVICE_SRCS := src/kvstore.c
RESP_SERVICE_SRCS := src/service/kvstore_resp_service.c
PROTOCOL_SRCS := src/protocol/resp.c
APP_SRCS := src/app/main.c
STORAGE_SRCS := src/storage/mysql_store.c

ifeq ($(NETWORK_BACKEND),ntyco)
CPPFLAGS += -DNETWORK_BACKEND_NTYCO -INtyCo/include -INtyCo/core
LDFLAGS += -LNtyCo
LDLIBS += -lntyco
NET_SRCS := src/ntyco_entry.c
SERVER_SERVICE_SRCS := $(LEGACY_SERVICE_SRCS)
SERVER_PROTOCOL_SRCS :=
SERVER_ENGINE_SRCS := $(LEGACY_ENGINE_SRCS)
SERVER_TARGET := kvstore-ntyco
else ifeq ($(NETWORK_BACKEND),epoll)
CPPFLAGS += -pthread
LDLIBS += -pthread
NET_SRCS := src/net/buffer.c src/net/reactor.c
SERVER_SERVICE_SRCS := $(RESP_SERVICE_SRCS) $(CACHE_SRCS) $(PERSISTENCE_SRCS) $(STORAGE_SRCS)
SERVER_PROTOCOL_SRCS := $(PROTOCOL_SRCS)
SERVER_ENGINE_SRCS := $(HASH_ENGINE_SRCS) $(OBJECT_ENGINE_SRCS)
SERVER_TARGET := kvstore
else
$(error Unsupported NETWORK_BACKEND '$(NETWORK_BACKEND)'; use epoll or ntyco)
endif

SERVER_SRCS := $(APP_SRCS) $(SERVER_SERVICE_SRCS) $(SERVER_PROTOCOL_SRCS) $(SERVER_ENGINE_SRCS) $(NET_SRCS)
SERVER_OBJS := $(SERVER_SRCS:%.c=$(BUILD_DIR)/%.o)

BUFFER_TEST_SRCS := tests/test_buffer.c src/net/buffer.c
BUFFER_TEST_OBJS := $(BUFFER_TEST_SRCS:%.c=$(BUILD_DIR)/%.o)
KV_TEST_SRCS := tests/test_kvstore.c $(LEGACY_SERVICE_SRCS) $(LEGACY_ENGINE_SRCS)
KV_TEST_OBJS := $(KV_TEST_SRCS:%.c=$(BUILD_DIR)/%.o)
RESP_TEST_SRCS := tests/test_resp.c $(PROTOCOL_SRCS)
RESP_TEST_OBJS := $(RESP_TEST_SRCS:%.c=$(BUILD_DIR)/%.o)
HASH_TEST_SRCS := tests/test_hash.c $(HASH_ENGINE_SRCS)
HASH_TEST_OBJS := $(HASH_TEST_SRCS:%.c=$(BUILD_DIR)/%.o)
OBJECT_TEST_SRCS := tests/test_object.c $(OBJECT_ENGINE_SRCS) $(HASH_ENGINE_SRCS)
OBJECT_TEST_OBJS := $(OBJECT_TEST_SRCS:%.c=$(BUILD_DIR)/%.o)
CACHE_TEST_SRCS := tests/test_cache.c $(CACHE_SRCS) $(OBJECT_ENGINE_SRCS) $(HASH_ENGINE_SRCS)
CACHE_TEST_OBJS := $(CACHE_TEST_SRCS:%.c=$(BUILD_DIR)/%.o)
RESP_SERVICE_TEST_SRCS := tests/test_resp_service.c $(RESP_SERVICE_SRCS) $(CACHE_SRCS) $(PERSISTENCE_SRCS) $(STORAGE_SRCS) $(PROTOCOL_SRCS) $(OBJECT_ENGINE_SRCS) $(HASH_ENGINE_SRCS)
RESP_SERVICE_TEST_OBJS := $(RESP_SERVICE_TEST_SRCS:%.c=$(BUILD_DIR)/%.o)
AOF_TEST_SRCS := tests/test_aof.c $(AOF_SRCS) $(PROTOCOL_SRCS)
AOF_TEST_OBJS := $(AOF_TEST_SRCS:%.c=$(BUILD_DIR)/%.o)
RDB_TEST_SRCS := tests/test_rdb.c $(RDB_SRCS) $(CACHE_SRCS) $(OBJECT_ENGINE_SRCS) $(HASH_ENGINE_SRCS)
RDB_TEST_OBJS := $(RDB_TEST_SRCS:%.c=$(BUILD_DIR)/%.o)
REACTOR_ASYNC_TEST_SRCS := tests/test_reactor_async.c src/net/reactor.c src/net/buffer.c
REACTOR_ASYNC_TEST_OBJS := $(REACTOR_ASYNC_TEST_SRCS:%.c=$(BUILD_DIR)/%.o)
LEGACY_CLIENT_OBJ := $(BUILD_DIR)/bench/legacy_client.o
QPS_CLIENT_OBJ := $(BUILD_DIR)/bench/qps_client.o
MIXED_QPS_CLIENT_OBJ := $(BUILD_DIR)/bench/mixed_qps_client.o
COLLECTION_BENCH_CLIENT_OBJ := $(BUILD_DIR)/bench/collection_bench_client.o
ALL_OBJS := $(SERVER_OBJS) $(BUFFER_TEST_OBJS) $(KV_TEST_OBJS) \
	$(RESP_TEST_OBJS) $(HASH_TEST_OBJS) $(CACHE_TEST_OBJS) \
	$(OBJECT_TEST_OBJS) $(RESP_SERVICE_TEST_OBJS) $(AOF_TEST_OBJS) $(RDB_TEST_OBJS) $(LEGACY_CLIENT_OBJ) \
	$(REACTOR_ASYNC_TEST_OBJS) \
	$(QPS_CLIENT_OBJ) $(MIXED_QPS_CLIENT_OBJ) $(COLLECTION_BENCH_CLIENT_OBJ)

LINK_TARGETS := $(SERVER_TARGET) legacy_client qps_client mixed_qps_client \
	collection_bench_client test_buffer test_kvstore test_resp test_hash \
	test_object test_cache test_resp_service test_aof test_rdb \
	test_reactor_async

.PHONY: all clean test integration-test mysql-integration-test mysql-cold-miss-bench mysql-performance-gate mysql-asan mysql-valgrind mysql-helgrind benchmark-test asan valgrind valgrind-run helgrind ntyco
.PHONY: $(LINK_TARGETS)

all: $(SERVER_TARGET) legacy_client qps_client mixed_qps_client collection_bench_client

$(SERVER_TARGET): $(SERVER_OBJS)
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS)

legacy_client: $(LEGACY_CLIENT_OBJ)
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS)

qps_client: $(QPS_CLIENT_OBJ)
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS) -pthread

$(QPS_CLIENT_OBJ): CFLAGS += -pthread

mixed_qps_client: $(MIXED_QPS_CLIENT_OBJ)
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS) -pthread

$(MIXED_QPS_CLIENT_OBJ): CFLAGS += -pthread

collection_bench_client: $(COLLECTION_BENCH_CLIENT_OBJ)
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS) -pthread

$(COLLECTION_BENCH_CLIENT_OBJ): CFLAGS += -pthread

test_buffer: $(BUFFER_TEST_OBJS)
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS)

test_kvstore: $(KV_TEST_OBJS)
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS)

test_resp: $(RESP_TEST_OBJS)
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS)

test_hash: $(HASH_TEST_OBJS)
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS)

test_object: $(OBJECT_TEST_OBJS)
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS)

test_cache: $(CACHE_TEST_OBJS)
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS)

test_resp_service: $(RESP_SERVICE_TEST_OBJS)
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS)

test_aof: $(AOF_TEST_OBJS)
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS)

test_rdb: $(RDB_TEST_OBJS)
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS)

test_reactor_async: $(REACTOR_ASYNC_TEST_OBJS)
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS)

test: test_buffer test_kvstore test_resp test_hash test_object test_cache test_resp_service test_aof test_rdb test_reactor_async
	./test_buffer
	./test_kvstore
	./test_resp
	./test_hash
	./test_object
	./test_cache
	./test_resp_service
	./test_aof
	./test_rdb
	./test_reactor_async

integration-test: kvstore
	KVSTORE_SERVER_COMMAND="./kvstore --maxmemory 4MiB --maxkeys 4096" python3 tests/reactor_integration.py
	KVSTORE_CACHE_SERVER_COMMAND="./kvstore --maxmemory 1MiB --maxkeys 2" python3 tests/cache_integration.py
	python3 tests/aof_integration.py
	python3 tests/rdb_integration.py

mysql-integration-test:
	$(MAKE) MYSQL=1 kvstore
	python3 tests/mysql_integration.py

mysql-cold-miss-bench:
	$(MAKE) MYSQL=1 kvstore mixed_qps_client
	python3 bench/mysql_cold_miss_bench.py $(MYSQL_COLD_MISS_ARGS)

mysql-performance-gate:
	$(MAKE) MYSQL=1 kvstore mixed_qps_client
	python3 bench/mysql_performance_gate.py $(MYSQL_PERFORMANCE_GATE_ARGS)

mysql-asan:
	$(MAKE) MYSQL=1 ALLOCATOR=libc BUILD_DIR=build/asan-mysql \
		CFLAGS="$(CFLAGS) -fsanitize=address,undefined -fno-omit-frame-pointer" \
		LDFLAGS="$(LDFLAGS) -fsanitize=address,undefined" test kvstore
	ASAN_OPTIONS=detect_leaks=1 python3 tests/mysql_integration.py

mysql-valgrind:
	$(MAKE) MYSQL=1 ALLOCATOR=libc BUILD_DIR=build/valgrind-mysql kvstore
	KVSTORE_MYSQL_SERVER_PREFIX="valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1" \
		python3 tests/mysql_integration.py

mysql-helgrind:
	$(MAKE) MYSQL=1 ALLOCATOR=libc BUILD_DIR=build/helgrind-mysql \
		CFLAGS="$(CFLAGS) -DKVSTORE_HELGRIND" kvstore
	KVSTORE_MYSQL_SERVER_PREFIX="valgrind --tool=helgrind --error-exitcode=1" \
		python3 tests/mysql_integration.py

benchmark-test: kvstore mixed_qps_client collection_bench_client
	python3 tests/benchmark_smoke.py

asan:
	$(MAKE) ALLOCATOR=libc clean
	$(MAKE) ALLOCATOR=libc BUILD_DIR=build/asan \
		CFLAGS="$(CFLAGS) -fsanitize=address,undefined -fno-omit-frame-pointer" \
		LDFLAGS="$(LDFLAGS) -fsanitize=address,undefined" test
	$(MAKE) ALLOCATOR=libc BUILD_DIR=build/asan \
		CFLAGS="$(CFLAGS) -fsanitize=address,undefined -fno-omit-frame-pointer" \
		LDFLAGS="$(LDFLAGS) -fsanitize=address,undefined" integration-test
	$(MAKE) ALLOCATOR=libc BUILD_DIR=build/asan \
		CFLAGS="$(CFLAGS) -fsanitize=address,undefined -fno-omit-frame-pointer" \
		LDFLAGS="$(LDFLAGS) -fsanitize=address,undefined" benchmark-test

valgrind:
	$(MAKE) ALLOCATOR=libc clean
	$(MAKE) ALLOCATOR=libc valgrind-run

valgrind-run: test_buffer test_kvstore test_resp test_hash test_object test_cache test_resp_service test_aof test_rdb test_reactor_async kvstore
	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./test_buffer
	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./test_kvstore
	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./test_resp
	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./test_hash
	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./test_object
	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./test_cache
	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./test_resp_service
	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./test_aof
	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./test_rdb
	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./test_reactor_async
	KVSTORE_SERVER_COMMAND="valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./kvstore --maxmemory 4MiB --maxkeys 4096" \
		KVSTORE_SHOW_SERVER_LOGS=1 python3 tests/reactor_integration.py
	KVSTORE_CACHE_SERVER_COMMAND="valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./kvstore --maxmemory 1MiB --maxkeys 2" \
		KVSTORE_SHOW_SERVER_LOGS=1 python3 tests/cache_integration.py
	KVSTORE_AOF_SERVER_PREFIX="valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1" \
		KVSTORE_SHOW_SERVER_LOGS=1 python3 tests/aof_integration.py
	KVSTORE_RDB_SERVER_PREFIX="valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1" \
		KVSTORE_SHOW_SERVER_LOGS=1 python3 tests/rdb_integration.py

helgrind:
	$(MAKE) ALLOCATOR=libc clean
	$(MAKE) ALLOCATOR=libc CPPFLAGS="$(CPPFLAGS) -DKVSTORE_HELGRIND" \
		test_aof test_reactor_async kvstore
	valgrind --tool=helgrind --error-exitcode=1 ./test_aof
	valgrind --tool=helgrind --error-exitcode=1 ./test_reactor_async
	KVSTORE_AOF_SERVER_PREFIX="valgrind --tool=helgrind --error-exitcode=1" \
		python3 tests/aof_integration.py
	KVSTORE_RDB_SERVER_PREFIX="valgrind --tool=helgrind --error-exitcode=1" \
		python3 tests/rdb_integration.py

ntyco:
	@test -f NtyCo/Makefile || { \
		echo "NtyCo is not initialized; run: git submodule update --init --recursive"; \
		exit 1; \
	}
	$(MAKE) -C NtyCo
	$(MAKE) NETWORK_BACKEND=ntyco kvstore-ntyco

$(BUILD_DIR)/%.o: %.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $(DEPFLAGS) -c $< -o $@

-include $(ALL_OBJS:.o=.d)

clean:
	rm -rf build kvstore kvstore-ntyco legacy_client qps_client mixed_qps_client collection_bench_client test_buffer test_kvstore test_resp test_hash test_object test_cache test_resp_service test_aof test_rdb test_reactor_async
