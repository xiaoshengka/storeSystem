#ifndef STORE_SYSTEM_NET_BUFFER_H
#define STORE_SYSTEM_NET_BUFFER_H

#include <stddef.h>

typedef struct net_buffer {
    char *data;
    size_t capacity;
    size_t read_pos;
    size_t write_pos;
} net_buffer_t;

int net_buffer_init(net_buffer_t *buffer, size_t initial_capacity);
void net_buffer_destroy(net_buffer_t *buffer);
void net_buffer_reset(net_buffer_t *buffer);
size_t net_buffer_readable(const net_buffer_t *buffer);
int net_buffer_reserve(net_buffer_t *buffer, size_t additional);
void *net_buffer_write_pointer(net_buffer_t *buffer,
                               size_t additional,
                               size_t *capacity);
int net_buffer_commit(net_buffer_t *buffer, size_t length);
int net_buffer_append(net_buffer_t *buffer, const void *data, size_t length);
void net_buffer_consume(net_buffer_t *buffer, size_t length);

/*
 * Flush buffered bytes to a non-blocking socket.
 * Returns 1 when drained, 0 on EAGAIN with bytes pending, and -1 on error.
 */
int net_buffer_flush_fd(int fd, net_buffer_t *buffer);

#endif
