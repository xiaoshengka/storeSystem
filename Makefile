CC ?= gcc
NETWORK_BACKEND ?= epoll
BUILD_DIR ?= build/$(NETWORK_BACKEND)

CPPFLAGS ?= -Iinclude
CFLAGS ?= -std=c11 -O2 -g -Wall -Wextra -Wpedantic
LDFLAGS ?=
LDLIBS ?=

ENGINE_SRCS := \
	src/kvstore_array.c \
	src/kvstore_rbtree.c \
	src/kvstore_hash.c
SERVICE_SRCS := src/kvstore.c
APP_SRCS := src/app/main.c

ifeq ($(NETWORK_BACKEND),ntyco)
CPPFLAGS += -DNETWORK_BACKEND_NTYCO -INtyCo/include -INtyCo/core
LDFLAGS += -LNtyCo
LDLIBS += -lntyco
NET_SRCS := src/ntyco_entry.c
SERVER_TARGET := kvstore-ntyco
else ifeq ($(NETWORK_BACKEND),epoll)
NET_SRCS := src/net/buffer.c src/net/reactor.c
SERVER_TARGET := kvstore
else
$(error Unsupported NETWORK_BACKEND '$(NETWORK_BACKEND)'; use epoll or ntyco)
endif

SERVER_SRCS := $(APP_SRCS) $(SERVICE_SRCS) $(ENGINE_SRCS) $(NET_SRCS)
SERVER_OBJS := $(SERVER_SRCS:%.c=$(BUILD_DIR)/%.o)

BUFFER_TEST_SRCS := tests/test_buffer.c src/net/buffer.c
BUFFER_TEST_OBJS := $(BUFFER_TEST_SRCS:%.c=$(BUILD_DIR)/%.o)
KV_TEST_SRCS := tests/test_kvstore.c $(SERVICE_SRCS) $(ENGINE_SRCS)
KV_TEST_OBJS := $(KV_TEST_SRCS:%.c=$(BUILD_DIR)/%.o)
LEGACY_CLIENT_OBJ := $(BUILD_DIR)/bench/legacy_client.o
QPS_CLIENT_OBJ := $(BUILD_DIR)/bench/qps_client.o

.PHONY: all clean test integration-test asan valgrind valgrind-run ntyco

all: $(SERVER_TARGET) legacy_client qps_client

$(SERVER_TARGET): $(SERVER_OBJS)
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS)

legacy_client: $(LEGACY_CLIENT_OBJ)
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS)

qps_client: $(QPS_CLIENT_OBJ)
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS) -pthread

$(QPS_CLIENT_OBJ): CFLAGS += -pthread

test_buffer: $(BUFFER_TEST_OBJS)
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS)

test_kvstore: $(KV_TEST_OBJS)
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS)

test: test_buffer test_kvstore
	./test_buffer
	./test_kvstore

integration-test: kvstore
	python3 tests/reactor_integration.py

asan:
	$(MAKE) clean
	$(MAKE) BUILD_DIR=build/asan \
		CFLAGS="$(CFLAGS) -fsanitize=address,undefined -fno-omit-frame-pointer" \
		LDFLAGS="$(LDFLAGS) -fsanitize=address,undefined" test integration-test

valgrind:
	$(MAKE) clean
	$(MAKE) valgrind-run

valgrind-run: test_buffer test_kvstore kvstore
	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./test_buffer
	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./test_kvstore
	KVSTORE_SERVER_COMMAND="valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./kvstore" \
		KVSTORE_SHOW_SERVER_LOGS=1 python3 tests/reactor_integration.py

ntyco:
	@test -f NtyCo/Makefile || { \
		echo "NtyCo is not initialized; run: git submodule update --init --recursive"; \
		exit 1; \
	}
	$(MAKE) -C NtyCo
	$(MAKE) NETWORK_BACKEND=ntyco kvstore-ntyco

$(BUILD_DIR)/%.o: %.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

clean:
	rm -rf build kvstore kvstore-ntyco legacy_client qps_client test_buffer test_kvstore
