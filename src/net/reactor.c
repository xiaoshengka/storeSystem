#define _GNU_SOURCE

#include "net/reactor.h"

#include "net/buffer.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#define REACTOR_MAX_EVENTS 256
#define REACTOR_IO_CHUNK 4096U
#define REACTOR_INITIAL_BUFFER 512U
#define REACTOR_MAX_REQUEST (64U * 1024U)

enum reactor_source_kind {
    REACTOR_SOURCE_LISTENER,
    REACTOR_SOURCE_WAKE,
    REACTOR_SOURCE_CLIENT
};

typedef struct reactor_connection reactor_connection_t;

struct reactor_connection {
    enum reactor_source_kind kind;
    struct reactor *owner;
    int fd;
    uint32_t events;
    int peer_eof;
    net_buffer_t input;
    net_buffer_t output;
    reactor_connection_t *previous;
    reactor_connection_t *next;
};

struct reactor {
    int epoll_fd;
    volatile sig_atomic_t stopping;
    uint16_t port;
    reactor_request_handler handler;
    void *handler_context;
    reactor_connection_t *listener;
    reactor_connection_t *wake_source;
    reactor_connection_t *clients;
};

static int set_nonblocking(int fd)
{
    int flags;

    do {
        flags = fcntl(fd, F_GETFL, 0);
    } while (flags < 0 && errno == EINTR);
    if (flags < 0) {
        return -1;
    }

    if ((flags & O_NONBLOCK) != 0) {
        return 0;
    }
    do {
        flags = fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    } while (flags < 0 && errno == EINTR);
    return flags < 0 ? -1 : 0;
}

int reactor_add(reactor_t *reactor, int fd, uint32_t events, void *event_data)
{
    struct epoll_event event;

    if (reactor == NULL || reactor->epoll_fd < 0 || fd < 0 || event_data == NULL) {
        errno = EINVAL;
        return -1;
    }
    memset(&event, 0, sizeof(event));
    event.events = events;
    event.data.ptr = event_data;
    return epoll_ctl(reactor->epoll_fd, EPOLL_CTL_ADD, fd, &event);
}

int reactor_modify(reactor_t *reactor, int fd, uint32_t events, void *event_data)
{
    struct epoll_event event;

    if (reactor == NULL || reactor->epoll_fd < 0 || fd < 0 || event_data == NULL) {
        errno = EINVAL;
        return -1;
    }
    memset(&event, 0, sizeof(event));
    event.events = events;
    event.data.ptr = event_data;
    return epoll_ctl(reactor->epoll_fd, EPOLL_CTL_MOD, fd, &event);
}

int reactor_remove(reactor_t *reactor, int fd)
{
    if (reactor == NULL || reactor->epoll_fd < 0 || fd < 0) {
        errno = EINVAL;
        return -1;
    }
    return epoll_ctl(reactor->epoll_fd, EPOLL_CTL_DEL, fd, NULL);
}

static void client_link(reactor_t *reactor, reactor_connection_t *connection)
{
    connection->next = reactor->clients;
    if (reactor->clients != NULL) {
        reactor->clients->previous = connection;
    }
    reactor->clients = connection;
}

static void client_unlink(reactor_t *reactor, reactor_connection_t *connection)
{
    if (connection->previous != NULL) {
        connection->previous->next = connection->next;
    } else {
        reactor->clients = connection->next;
    }
    if (connection->next != NULL) {
        connection->next->previous = connection->previous;
    }
    connection->previous = NULL;
    connection->next = NULL;
}

static void connection_destroy(reactor_connection_t *connection)
{
    reactor_t *reactor;

    if (connection == NULL) {
        return;
    }
    reactor = connection->owner;
    if (connection->kind == REACTOR_SOURCE_CLIENT && reactor != NULL) {
        client_unlink(reactor, connection);
    }
    if (connection->fd >= 0) {
        if (reactor != NULL && reactor->epoll_fd >= 0) {
            if (reactor_remove(reactor, connection->fd) != 0 && errno != ENOENT && errno != EBADF) {
                perror("epoll_ctl DEL");
            }
        }
        close(connection->fd);
        connection->fd = -1;
    }
    net_buffer_destroy(&connection->input);
    net_buffer_destroy(&connection->output);
    free(connection);
}

static reactor_connection_t *source_create(reactor_t *reactor,
                                           enum reactor_source_kind kind,
                                           int fd)
{
    reactor_connection_t *source = calloc(1, sizeof(*source));

    if (source == NULL) {
        return NULL;
    }
    source->kind = kind;
    source->owner = reactor;
    source->fd = fd;
    return source;
}

static reactor_connection_t *client_create(reactor_t *reactor, int fd)
{
    reactor_connection_t *connection = source_create(reactor, REACTOR_SOURCE_CLIENT, fd);

    if (connection == NULL) {
        return NULL;
    }
    if (net_buffer_init(&connection->input, REACTOR_INITIAL_BUFFER) != 0 ||
        net_buffer_init(&connection->output, REACTOR_INITIAL_BUFFER) != 0) {
        net_buffer_destroy(&connection->input);
        net_buffer_destroy(&connection->output);
        free(connection);
        return NULL;
    }
    return connection;
}

static int listener_create(uint16_t port)
{
    struct sockaddr_in address;
    int enabled = 1;
    int fd;

    fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        return -1;
    }
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled)) != 0) {
        close(fd);
        return -1;
    }

    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(port);
    if (bind(fd, (struct sockaddr *)&address, sizeof(address)) != 0 ||
        listen(fd, SOMAXCONN) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int connection_set_events(reactor_connection_t *connection, uint32_t events)
{
    if (reactor_modify(connection->owner, connection->fd, events, connection) != 0) {
        return -1;
    }
    connection->events = events;
    return 0;
}

static int handle_accept(reactor_t *reactor)
{
    for (;;) {
        int client_fd = accept4(reactor->listener->fd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
        reactor_connection_t *connection;

        if (client_fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return 0;
            }
            return -1;
        }
        if (set_nonblocking(client_fd) != 0) {
            close(client_fd);
            continue;
        }

        connection = client_create(reactor, client_fd);
        if (connection == NULL) {
            close(client_fd);
            continue;
        }
        connection->events = EPOLLIN | EPOLLRDHUP;
        if (reactor_add(reactor, client_fd, connection->events, connection) != 0) {
            connection->fd = -1;
            close(client_fd);
            net_buffer_destroy(&connection->input);
            net_buffer_destroy(&connection->output);
            free(connection);
            continue;
        }
        client_link(reactor, connection);
    }
}

static int prepare_response(reactor_connection_t *connection)
{
    char response[REACTOR_MAX_REQUEST];
    size_t response_length = 0;
    int result;

    if (net_buffer_readable(&connection->input) == 0) {
        return connection->peer_eof ? -1 : 0;
    }

    result = connection->owner->handler(
        connection->input.data + connection->input.read_pos,
        net_buffer_readable(&connection->input),
        response,
        sizeof(response),
        &response_length,
        connection->owner->handler_context);
    net_buffer_reset(&connection->input);

    if (response_length > sizeof(response)) {
        errno = EOVERFLOW;
        return -1;
    }
    if (result != 0 && response_length == 0) {
        static const char fallback[] = "ERROR";
        memcpy(response, fallback, sizeof(fallback) - 1U);
        response_length = sizeof(fallback) - 1U;
    }
    if (net_buffer_append(&connection->output, response, response_length) != 0) {
        return -1;
    }
    return connection_set_events(connection, EPOLLOUT | EPOLLRDHUP);
}

static int handle_read(reactor_connection_t *connection)
{
    char chunk[REACTOR_IO_CHUNK];

    for (;;) {
        ssize_t received = recv(connection->fd, chunk, sizeof(chunk), 0);

        if (received > 0) {
            if (net_buffer_readable(&connection->input) + (size_t)received > REACTOR_MAX_REQUEST ||
                net_buffer_append(&connection->input, chunk, (size_t)received) != 0) {
                errno = EMSGSIZE;
                return -1;
            }
            continue;
        }
        if (received == 0) {
            connection->peer_eof = 1;
            break;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            break;
        }
        return -1;
    }
    return prepare_response(connection);
}

static int handle_write(reactor_connection_t *connection)
{
    int result = net_buffer_flush_fd(connection->fd, &connection->output);

    if (result < 0) {
        return -1;
    }
    if (result == 0) {
        return 0;
    }
    if (connection->peer_eof) {
        return -1;
    }
    return connection_set_events(connection, EPOLLIN | EPOLLRDHUP);
}

static void drain_wake_fd(reactor_t *reactor)
{
    uint64_t value;

    while (read(reactor->wake_source->fd, &value, sizeof(value)) < 0 && errno == EINTR) {
    }
}

int reactor_init(reactor_t **out_reactor,
                 uint16_t port,
                 reactor_request_handler handler,
                 void *handler_context)
{
    reactor_t *reactor;
    int listener_fd;
    int wake_fd;

    if (out_reactor == NULL || handler == NULL || port == 0) {
        errno = EINVAL;
        return -1;
    }
    *out_reactor = NULL;

    reactor = calloc(1, sizeof(*reactor));
    if (reactor == NULL) {
        return -1;
    }
    reactor->epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    reactor->port = port;
    reactor->handler = handler;
    reactor->handler_context = handler_context;
    if (reactor->epoll_fd < 0) {
        free(reactor);
        return -1;
    }

    listener_fd = listener_create(port);
    if (listener_fd < 0) {
        reactor_destroy(reactor);
        return -1;
    }
    reactor->listener = source_create(reactor, REACTOR_SOURCE_LISTENER, listener_fd);
    if (reactor->listener == NULL ||
        reactor_add(reactor, listener_fd, EPOLLIN, reactor->listener) != 0) {
        if (reactor->listener == NULL) {
            close(listener_fd);
        }
        reactor_destroy(reactor);
        return -1;
    }
    reactor->listener->events = EPOLLIN;

    wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wake_fd < 0) {
        reactor_destroy(reactor);
        return -1;
    }
    reactor->wake_source = source_create(reactor, REACTOR_SOURCE_WAKE, wake_fd);
    if (reactor->wake_source == NULL ||
        reactor_add(reactor, wake_fd, EPOLLIN, reactor->wake_source) != 0) {
        if (reactor->wake_source == NULL) {
            close(wake_fd);
        }
        reactor_destroy(reactor);
        return -1;
    }
    reactor->wake_source->events = EPOLLIN;

    *out_reactor = reactor;
    return 0;
}

int reactor_run(reactor_t *reactor)
{
    struct epoll_event events[REACTOR_MAX_EVENTS];

    if (reactor == NULL) {
        errno = EINVAL;
        return -1;
    }
    while (!reactor->stopping) {
        int ready = epoll_wait(reactor->epoll_fd, events, REACTOR_MAX_EVENTS, -1);
        int index;

        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        for (index = 0; index < ready; ++index) {
            reactor_connection_t *source = events[index].data.ptr;
            uint32_t active = events[index].events;
            int result = 0;

            if (source == NULL) {
                continue;
            }
            if (source->kind == REACTOR_SOURCE_WAKE) {
                drain_wake_fd(reactor);
                continue;
            }
            if (source->kind == REACTOR_SOURCE_LISTENER) {
                if ((active & (EPOLLERR | EPOLLHUP)) != 0) {
                    errno = EIO;
                    return -1;
                }
                if ((active & EPOLLIN) != 0 && handle_accept(reactor) != 0) {
                    return -1;
                }
                continue;
            }

            if ((active & (EPOLLERR | EPOLLHUP)) != 0) {
                connection_destroy(source);
                continue;
            }
            if ((active & EPOLLRDHUP) != 0) {
                source->peer_eof = 1;
            }
            if ((active & EPOLLIN) != 0) {
                result = handle_read(source);
            }
            if (result == 0 && (active & EPOLLOUT) != 0) {
                result = handle_write(source);
            }
            if (result != 0 || (source->peer_eof && net_buffer_readable(&source->output) == 0)) {
                connection_destroy(source);
            }
        }
    }
    return 0;
}

void reactor_stop(reactor_t *reactor)
{
    uint64_t value = 1;

    if (reactor == NULL) {
        return;
    }
    reactor->stopping = 1;
    if (reactor->wake_source != NULL && reactor->wake_source->fd >= 0) {
        ssize_t ignored = write(reactor->wake_source->fd, &value, sizeof(value));
        (void)ignored;
    }
}

void reactor_destroy(reactor_t *reactor)
{
    if (reactor == NULL) {
        return;
    }
    while (reactor->clients != NULL) {
        connection_destroy(reactor->clients);
    }
    connection_destroy(reactor->listener);
    reactor->listener = NULL;
    connection_destroy(reactor->wake_source);
    reactor->wake_source = NULL;
    if (reactor->epoll_fd >= 0) {
        close(reactor->epoll_fd);
        reactor->epoll_fd = -1;
    }
    free(reactor);
}
