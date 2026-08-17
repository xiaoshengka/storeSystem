#include "net/buffer.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static void set_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    assert(flags >= 0);
    assert(fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0);
}

static size_t drain_socket(int fd)
{
    char chunk[4096];
    size_t total = 0;

    for (;;) {
        ssize_t received = recv(fd, chunk, sizeof(chunk), 0);
        size_t index;

        if (received > 0) {
            for (index = 0; index < (size_t)received; ++index) {
                assert(chunk[index] == 'x');
            }
            total += (size_t)received;
            continue;
        }
        if (received < 0 && errno == EINTR) {
            continue;
        }
        if (received < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return total;
        }
        assert(received != 0);
        assert(0 && "unexpected recv failure");
    }
}

static void test_buffer_operations(void)
{
    net_buffer_t buffer;
    const char first[] = "abcdef";
    const char second[] = "ghijklmnopqrstuvwxyz";

    assert(net_buffer_init(&buffer, 8) == 0);
    assert(net_buffer_append(&buffer, first, sizeof(first) - 1U) == 0);
    assert(net_buffer_readable(&buffer) == 6U);
    net_buffer_consume(&buffer, 4U);
    assert(net_buffer_append(&buffer, second, sizeof(second) - 1U) == 0);
    assert(net_buffer_readable(&buffer) == 22U);
    assert(memcmp(buffer.data + buffer.read_pos, "efghijklmnopqrstuvwxyz", 22U) == 0);
    net_buffer_consume(&buffer, 22U);
    assert(net_buffer_readable(&buffer) == 0U);
    net_buffer_destroy(&buffer);
}

static void test_partial_nonblocking_flush(void)
{
    enum { PAYLOAD_SIZE = 1024 * 1024 };
    net_buffer_t buffer;
    char *payload;
    int sockets[2];
    int send_buffer_size = 1024;
    int result;
    size_t received = 0;

    payload = malloc(PAYLOAD_SIZE);
    assert(payload != NULL);
    memset(payload, 'x', PAYLOAD_SIZE);
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
    set_nonblocking(sockets[0]);
    set_nonblocking(sockets[1]);
    assert(setsockopt(sockets[0], SOL_SOCKET, SO_SNDBUF,
                      &send_buffer_size, sizeof(send_buffer_size)) == 0);

    assert(net_buffer_init(&buffer, 512) == 0);
    assert(net_buffer_append(&buffer, payload, PAYLOAD_SIZE) == 0);
    result = net_buffer_flush_fd(sockets[0], &buffer);
    assert(result == 0);
    assert(net_buffer_readable(&buffer) > 0U);

    while (net_buffer_readable(&buffer) > 0U) {
        received += drain_socket(sockets[1]);
        result = net_buffer_flush_fd(sockets[0], &buffer);
        assert(result >= 0);
    }
    received += drain_socket(sockets[1]);
    assert(received == PAYLOAD_SIZE);

    net_buffer_destroy(&buffer);
    close(sockets[0]);
    close(sockets[1]);
    free(payload);
}

int main(void)
{
    test_buffer_operations();
    test_partial_nonblocking_flush();
    puts("test_buffer: PASS");
    return 0;
}
