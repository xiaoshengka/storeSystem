#ifndef STORE_SYSTEM_NET_REACTOR_H
#define STORE_SYSTEM_NET_REACTOR_H

#include <stddef.h>
#include <stdint.h>

typedef struct reactor reactor_t;

typedef int (*reactor_request_handler)(const char *request,
                                       size_t request_length,
                                       char *response,
                                       size_t response_capacity,
                                       size_t *response_length,
                                       void *context);

int reactor_init(reactor_t **out_reactor,
                 uint16_t port,
                 reactor_request_handler handler,
                 void *handler_context);
int reactor_run(reactor_t *reactor);
void reactor_stop(reactor_t *reactor);
void reactor_destroy(reactor_t *reactor);

int reactor_add(reactor_t *reactor, int fd, uint32_t events, void *event_data);
int reactor_modify(reactor_t *reactor, int fd, uint32_t events, void *event_data);
int reactor_remove(reactor_t *reactor, int fd);

#endif
