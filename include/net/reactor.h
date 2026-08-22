#ifndef STORE_SYSTEM_NET_REACTOR_H
#define STORE_SYSTEM_NET_REACTOR_H

#include <stddef.h>
#include <stdint.h>

#include "net/buffer.h"

typedef struct reactor reactor_t;

typedef struct reactor_request_token {
    uint64_t connection_id;
} reactor_request_token_t;

enum reactor_handler_result {
    REACTOR_HANDLER_ERROR = -1,
    REACTOR_HANDLER_INCOMPLETE = 0,
    REACTOR_HANDLER_COMPLETE = 1,
    REACTOR_HANDLER_DEFERRED = 2
};

typedef int (*reactor_request_handler)(const unsigned char *input,
                                       size_t input_length,
                                       int end_of_stream,
                                       net_buffer_t *response,
                                       size_t *consumed,
                                       int *close_after_response,
                                       uint64_t *response_barrier,
                                       reactor_request_token_t request_token,
                                       void *context);
typedef int (*reactor_periodic_handler)(void *context);
typedef int (*reactor_flush_handler)(void *context);
typedef int (*reactor_barrier_ready_handler)(uint64_t sequence, void *context);
typedef int (*reactor_async_handler)(void *context);

int reactor_init(reactor_t **out_reactor,
                 uint16_t port,
                 reactor_request_handler handler,
                 void *handler_context);
int reactor_run(reactor_t *reactor);
int reactor_set_periodic(reactor_t *reactor,
                         uint64_t interval_ms,
                         reactor_periodic_handler handler,
                         void *handler_context);
int reactor_set_flush_handler(reactor_t *reactor,
                              reactor_flush_handler handler,
                              void *handler_context);
int reactor_set_response_barrier(reactor_t *reactor,
                                 reactor_barrier_ready_handler handler,
                                 void *handler_context);
int reactor_set_async_handler(reactor_t *reactor,
                              reactor_async_handler handler,
                              void *handler_context);
int reactor_complete_response(reactor_t *reactor,
                              reactor_request_token_t request_token,
                              const void *response,
                              size_t response_length,
                              int close_after_response,
                              uint64_t response_barrier);
int reactor_wake_fd(reactor_t *reactor);
void reactor_stop(reactor_t *reactor);
void reactor_destroy(reactor_t *reactor);
void reactor_close_in_child(reactor_t *reactor);

int reactor_add(reactor_t *reactor, int fd, uint32_t events, void *event_data);
int reactor_modify(reactor_t *reactor, int fd, uint32_t events, void *event_data);
int reactor_remove(reactor_t *reactor, int fd);

#endif
