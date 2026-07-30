
CC = gcc
FLAGS = -I ./NtyCo/core/ -L ./NtyCo/ -lntyco
FLAGS += -Isrc -INtyCo/include     # -Isrc 让编译器去 src 找头文件
SRCS = $(wildcard src/*.c)
TESTCASE_SRCS = testcase.c
TARGET = kvstore
SUBDIR = ./NtyCo/
TESTCASE = testcase

OBJS = $(SRCS:src/%.c=%.o)

all: $(SUBDIR) $(TARGET) $(TESTCASE)

$(SUBDIR): ECHO
	make -C $@

ECHO:
	@echo $(SUBDIR)

$(TARGET): $(OBJS)
	$(CC) -o $@ $^ $(FLAGS)

$(TESTCASE): $(TESTCASE_SRCS)
	$(CC) -o $@ $^

%.o: src/%.c
	$(CC) $(FLAGS) -c $^ -o $@


clean:
	rm -rf $(OBJS) $(TARGET) $(TESTCASE)

