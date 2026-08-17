#include "net/buffer.h"

#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>

static int net_buffer_compact(net_buffer_t *buffer)
{
    size_t readable;

    if (buffer == NULL || buffer->read_pos == 0) {
        return 0;
    }

    readable = net_buffer_readable(buffer);
    if (readable > 0) {
        memmove(buffer->data, buffer->data + buffer->read_pos, readable);
    }
    buffer->read_pos = 0;
    buffer->write_pos = readable;
    return 0;
}

int net_buffer_init(net_buffer_t *buffer, size_t initial_capacity)
{
    if (buffer == NULL) {
        return -1;
    }

    memset(buffer, 0, sizeof(*buffer));
    if (initial_capacity == 0) {
        return 0;
    }

    buffer->data = malloc(initial_capacity);
    if (buffer->data == NULL) {
        return -1;
    }
    buffer->capacity = initial_capacity;
    return 0;
}

void net_buffer_destroy(net_buffer_t *buffer)
{
    if (buffer == NULL) {
        return;
    }

    free(buffer->data);
    memset(buffer, 0, sizeof(*buffer));
}

void net_buffer_reset(net_buffer_t *buffer)
{
    if (buffer == NULL) {
        return;
    }
    buffer->read_pos = 0;
    buffer->write_pos = 0;
}

size_t net_buffer_readable(const net_buffer_t *buffer)
{
    if (buffer == NULL || buffer->write_pos < buffer->read_pos) {
        return 0;
    }
    return buffer->write_pos - buffer->read_pos;
}

int net_buffer_reserve(net_buffer_t *buffer, size_t additional)
{
    size_t required;
    size_t capacity;
    char *new_data;

    if (buffer == NULL || additional > SIZE_MAX - net_buffer_readable(buffer)) {
        return -1;
    }

    if (buffer->capacity - buffer->write_pos >= additional) {
        return 0;
    }

    net_buffer_compact(buffer);
    if (buffer->capacity - buffer->write_pos >= additional) {
        return 0;
    }

    required = buffer->write_pos + additional;
    capacity = buffer->capacity == 0 ? 512U : buffer->capacity;
    while (capacity < required) {
        if (capacity > SIZE_MAX / 2U) {
            capacity = required;
            break;
        }
        capacity *= 2U;
    }

    new_data = realloc(buffer->data, capacity);
    if (new_data == NULL) {
        return -1;
    }
    buffer->data = new_data;
    buffer->capacity = capacity;
    return 0;
}

int net_buffer_append(net_buffer_t *buffer, const void *data, size_t length)
{
    if (buffer == NULL || (data == NULL && length != 0)) {
        return -1;
    }
    if (net_buffer_reserve(buffer, length) != 0) {
        return -1;
    }
    if (length > 0) {
        memcpy(buffer->data + buffer->write_pos, data, length);
        buffer->write_pos += length;
    }
    return 0;
}

void net_buffer_consume(net_buffer_t *buffer, size_t length)
{
    size_t readable;

    if (buffer == NULL) {
        return;
    }
    readable = net_buffer_readable(buffer);
    if (length >= readable) {
        net_buffer_reset(buffer);
        return;
    }
    buffer->read_pos += length;
}

int net_buffer_flush_fd(int fd, net_buffer_t *buffer)
{
    while (net_buffer_readable(buffer) > 0) {
        ssize_t written = send(fd,
                               buffer->data + buffer->read_pos,
                               net_buffer_readable(buffer),
                               MSG_NOSIGNAL);
        if (written > 0) {
            net_buffer_consume(buffer, (size_t)written);
            continue;
        }
        if (written < 0 && errno == EINTR) {
            continue;
        }
        if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return 0;
        }
        return -1;
    }
    return 1;
}
