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
#include <sys/timerfd.h>
#include <unistd.h>

#define REACTOR_MAX_EVENTS 256
#define REACTOR_IO_CHUNK 4096U
#define REACTOR_INITIAL_BUFFER 512U
#define REACTOR_MAX_REQUEST (64U * 1024U)
#define REACTOR_MAX_RESPONSE (1024U * 1024U)
#define REACTOR_OUTPUT_HIGH_WATER (1024U * 1024U)

enum reactor_source_kind {
    REACTOR_SOURCE_LISTENER,
    REACTOR_SOURCE_WAKE,
    REACTOR_SOURCE_TIMER,
    REACTOR_SOURCE_CLIENT
};

typedef struct reactor_connection reactor_connection_t;

struct reactor_connection {
    enum reactor_source_kind kind;
    struct reactor *owner;
    int fd;
    uint32_t events;
    int peer_eof;
    int close_after_write;
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
    reactor_connection_t *timer_source;
    reactor_periodic_handler periodic_handler;
    void *periodic_context;
    reactor_flush_handler flush_handler;
    void *flush_context;
    reactor_connection_t *clients;
    unsigned char *response_scratch;
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
    if (connection->events == events) {
        return 0;
    }
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

static int refresh_client_events(reactor_connection_t *connection)
{
    uint32_t events = EPOLLRDHUP;

    if (net_buffer_readable(&connection->output) > 0) {
        events |= EPOLLOUT;
    }
    if (!connection->peer_eof && !connection->close_after_write &&
        net_buffer_readable(&connection->output) < REACTOR_OUTPUT_HIGH_WATER) {
        events |= EPOLLIN;
    }
    return connection_set_events(connection, events);
}

static int process_input(reactor_connection_t *connection)
{
    unsigned char *response = connection->owner->response_scratch;

    while (net_buffer_readable(&connection->input) > 0 &&
           net_buffer_readable(&connection->output) < REACTOR_OUTPUT_HIGH_WATER &&
           !connection->close_after_write) {
        size_t available = net_buffer_readable(&connection->input);
        size_t consumed = 0;
        size_t response_length = 0;
        int close_after_response = 0;
        int result = connection->owner->handler(
            (const unsigned char *)connection->input.data + connection->input.read_pos,
            available,
            connection->peer_eof,
            response,
            REACTOR_MAX_RESPONSE,
            &consumed,
            &response_length,
            &close_after_response,
            connection->owner->handler_context);

        if (result == REACTOR_HANDLER_INCOMPLETE) {
            if (available > REACTOR_MAX_REQUEST) {
                errno = EMSGSIZE;
                return -1;
            }
            break;
        }
        if (result != REACTOR_HANDLER_COMPLETE || consumed == 0 ||
            consumed > available || response_length > REACTOR_MAX_RESPONSE) {
            errno = EPROTO;
            return -1;
        }
        net_buffer_consume(&connection->input, consumed);
        if (response_length > 0 &&
            net_buffer_append(&connection->output, response, response_length) != 0) {
            return -1;
        }
        if (close_after_response) {
            connection->close_after_write = 1;
            net_buffer_reset(&connection->input);
        }
    }
    return 0;
}

static int handle_read(reactor_connection_t *connection)
{
    char chunk[REACTOR_IO_CHUNK];

    for (;;) {
        if (connection->close_after_write ||
            net_buffer_readable(&connection->output) >= REACTOR_OUTPUT_HIGH_WATER) {
            break;
        }
        ssize_t received = recv(connection->fd, chunk, sizeof(chunk), 0);

        if (received > 0) {
            if (net_buffer_append(&connection->input, chunk, (size_t)received) != 0 ||
                process_input(connection) != 0) {
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
    if (process_input(connection) != 0) {
        return -1;
    }
    return refresh_client_events(connection);
}

static int handle_write(reactor_connection_t *connection)
{
    int result = net_buffer_flush_fd(connection->fd, &connection->output);

    if (result < 0) {
        return -1;
    }
    if (result == 0) {
        return refresh_client_events(connection);
    }
    if (connection->close_after_write) {
        return -1;
    }
    if (process_input(connection) != 0) {
        return -1;
    }
    if (connection->peer_eof && net_buffer_readable(&connection->output) == 0) {
        return -1;
    }
    return refresh_client_events(connection);
}

static void drain_wake_fd(reactor_t *reactor)
{
    uint64_t value;

    while (read(reactor->wake_source->fd, &value, sizeof(value)) < 0 && errno == EINTR) {
    }
}

static int drain_timer_fd(reactor_t *reactor)
{
    uint64_t expirations;
    ssize_t result;

    do {
        result = read(reactor->timer_source->fd,
                      &expirations,
                      sizeof(expirations));
    } while (result < 0 && errno == EINTR);
    if (result < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
        return -1;
    }
    return reactor->periodic_handler(reactor->periodic_context);
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
    reactor->response_scratch = malloc(REACTOR_MAX_RESPONSE);
    if (reactor->response_scratch == NULL) {
        free(reactor);
        return -1;
    }
    reactor->epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    reactor->port = port;
    reactor->handler = handler;
    reactor->handler_context = handler_context;
    if (reactor->epoll_fd < 0) {
        free(reactor->response_scratch);
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
        int flush_failed = 0;

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
            if (source->kind == REACTOR_SOURCE_TIMER) {
                if ((active & (EPOLLERR | EPOLLHUP)) != 0 ||
                    drain_timer_fd(reactor) != 0) {
                    return -1;
                }
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
                if (reactor->flush_handler != NULL &&
                    reactor->flush_handler(reactor->flush_context) != 0) {
                    flush_failed = 1;
                    break;
                }
                result = handle_write(source);
            }
            if (result != 0 || (source->peer_eof && net_buffer_readable(&source->output) == 0)) {
                connection_destroy(source);
            }
        }
        if (flush_failed) {
            while (reactor->clients != NULL) {
                connection_destroy(reactor->clients);
            }
            continue;
        }
        if (reactor->flush_handler != NULL &&
            reactor->flush_handler(reactor->flush_context) != 0) {
            while (reactor->clients != NULL) {
                connection_destroy(reactor->clients);
            }
        }
    }
    return 0;
}

int reactor_set_flush_handler(reactor_t *reactor,
                              reactor_flush_handler handler,
                              void *handler_context)
{
    if (reactor == NULL || handler == NULL || reactor->flush_handler != NULL) {
        errno = EINVAL;
        return -1;
    }
    reactor->flush_handler = handler;
    reactor->flush_context = handler_context;
    return 0;
}

int reactor_set_periodic(reactor_t *reactor,
                         uint64_t interval_ms,
                         reactor_periodic_handler handler,
                         void *handler_context)
{
    struct itimerspec schedule;
    reactor_connection_t *source;
    int timer_fd;

    if (reactor == NULL || interval_ms == 0 || handler == NULL ||
        reactor->timer_source != NULL) {
        errno = EINVAL;
        return -1;
    }
    timer_fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (timer_fd < 0) {
        return -1;
    }
    memset(&schedule, 0, sizeof(schedule));
    schedule.it_value.tv_sec = (time_t)(interval_ms / 1000U);
    schedule.it_value.tv_nsec = (long)((interval_ms % 1000U) * 1000000U);
    schedule.it_interval = schedule.it_value;
    if (timerfd_settime(timer_fd, 0, &schedule, NULL) != 0) {
        close(timer_fd);
        return -1;
    }
    source = source_create(reactor, REACTOR_SOURCE_TIMER, timer_fd);
    if (source == NULL || reactor_add(reactor, timer_fd, EPOLLIN, source) != 0) {
        if (source == NULL) {
            close(timer_fd);
        } else {
            connection_destroy(source);
        }
        return -1;
    }
    source->events = EPOLLIN;
    reactor->timer_source = source;
    reactor->periodic_handler = handler;
    reactor->periodic_context = handler_context;
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
    connection_destroy(reactor->timer_source);
    reactor->timer_source = NULL;
    if (reactor->epoll_fd >= 0) {
        close(reactor->epoll_fd);
        reactor->epoll_fd = -1;
    }
    free(reactor->response_scratch);
    free(reactor);
}
