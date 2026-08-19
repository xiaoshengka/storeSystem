#define _POSIX_C_SOURCE 200809L

#include "persistence/aof.h"

#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

typedef struct replay_capture {
    size_t count;
} replay_capture_t;

static aof_argument_t argument(const void *data, size_t length)
{
    aof_argument_t result;

    result.data = data;
    result.length = length;
    return result;
}

static int capture_command(const aof_argument_t *arguments,
                           size_t argument_count,
                           void *context)
{
    static const unsigned char binary_key[] = {'k', 0, 'y'};
    replay_capture_t *capture = context;

    if (capture->count == 0) {
        assert(argument_count == 3U);
        assert(arguments[0].length == 3U);
        assert(memcmp(arguments[0].data, "SET", 3U) == 0);
        assert(arguments[1].length == sizeof(binary_key));
        assert(memcmp(arguments[1].data, binary_key, sizeof(binary_key)) == 0);
    } else {
        assert(capture->count == 1U);
        assert(argument_count == 2U);
        assert(arguments[0].length == 3U);
        assert(memcmp(arguments[0].data, "DEL", 3U) == 0);
    }
    capture->count++;
    return 0;
}

static void write_complete(int fd, const void *data, size_t length)
{
    const unsigned char *bytes = data;
    size_t position = 0;

    while (position < length) {
        ssize_t written = write(fd, bytes + position, length - position);

        assert(written > 0);
        position += (size_t)written;
    }
}

int main(void)
{
    static const unsigned char binary_key[] = {'k', 0, 'y'};
    static const unsigned char binary_value[] = {'v', 0, '1'};
    static const unsigned char partial[] = "*2\r\n$3\r\nDEL\r\n$10\r\nabc";
    char path[] = "/tmp/storeSystem-aof-XXXXXX";
    aof_argument_t set_arguments[3];
    aof_argument_t del_arguments[2];
    aof_replay_stats_t stats;
    replay_capture_t capture = {0};
    struct stat before;
    struct stat after;
    aof_t *aof;
    int fd;

    fd = mkstemp(path);
    assert(fd >= 0);
    assert(close(fd) == 0);

    assert(aof_open(&aof, path, AOF_FSYNC_ALWAYS) == 0);
    set_arguments[0] = argument("SET", 3U);
    set_arguments[1] = argument(binary_key, sizeof(binary_key));
    set_arguments[2] = argument(binary_value, sizeof(binary_value));
    assert(aof_append(aof, set_arguments, 3U) == 0);
    del_arguments[0] = argument("DEL", 3U);
    del_arguments[1] = argument(binary_key, sizeof(binary_key));
    assert(aof_append(aof, del_arguments, 2U) == 0);
    assert(!aof_is_failed(aof));
    assert(aof_close(aof) == 0);

    assert(stat(path, &before) == 0);
    fd = open(path, O_WRONLY | O_APPEND);
    assert(fd >= 0);
    write_complete(fd, partial, sizeof(partial) - 1U);
    assert(close(fd) == 0);

    assert(aof_open(&aof, path, AOF_FSYNC_EVERYSEC) == 0);
    assert(aof_replay(aof, capture_command, &capture, &stats) == 0);
    assert(capture.count == 2U);
    assert(stats.commands_loaded == 2U);
    assert(stats.truncated_tail_repaired == 1);
    assert(stat(path, &after) == 0);
    assert(after.st_size == before.st_size);
    assert(aof_maintain(aof) == 0);
    assert(aof_close(aof) == 0);

    assert(aof_open(&aof, path, AOF_FSYNC_EVERYSEC) == 0);
    assert(aof_append(aof, del_arguments, 2U) == 0);
    {
        struct timespec delay = {1, 100000000L};

        assert(nanosleep(&delay, NULL) == 0);
    }
    assert(aof_maintain(aof) == 0);
    assert(aof_close(aof) == 0);

    fd = open(path, O_WRONLY | O_TRUNC);
    assert(fd >= 0);
    assert(close(fd) == 0);
    assert(stat(path, &after) == 0 && after.st_size == 0);
    assert(aof_open(&aof, path, AOF_FSYNC_NO) == 0);
    assert(aof_append(aof, set_arguments, 3U) == 0);
    assert(aof_append(aof, del_arguments, 2U) == 0);
    assert(stat(path, &after) == 0 && after.st_size == 0);
    assert(aof_flush(aof) == 0);
    assert(stat(path, &after) == 0 && after.st_size > 0);
    assert(aof_close(aof) == 0);
    assert(stat(path, &after) == 0 && after.st_size > 0);

    assert(unlink(path) == 0);
    puts("test_aof: PASS");
    return 0;
}
