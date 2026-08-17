#define _POSIX_C_SOURCE 200809L

#include "net/reactor.h"
#include "service/kvstore_service.h"

#include <errno.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#ifndef NETWORK_BACKEND_NTYCO
#define DEFAULT_PORT 9096U

static reactor_t *active_reactor;

static void handle_stop_signal(int signal_number)
{
    (void)signal_number;
    reactor_stop(active_reactor);
}

static int install_signal_handlers(void)
{
    struct sigaction action;

    memset(&action, 0, sizeof(action));
    action.sa_handler = handle_stop_signal;
    sigemptyset(&action.sa_mask);
    if (sigaction(SIGINT, &action, NULL) != 0 ||
        sigaction(SIGTERM, &action, NULL) != 0) {
        return -1;
    }
    return 0;
}

static int dispatch_request(const char *request,
                            size_t request_length,
                            char *response,
                            size_t response_capacity,
                            size_t *response_length,
                            void *context)
{
    (void)context;
    return kvstore_execute_request(request,
                                   request_length,
                                   response,
                                   response_capacity,
                                   response_length);
}

int main(void)
{
    reactor_t *reactor = NULL;
    int result = 1;

    if (kvstore_engine_init() != 0) {
        fprintf(stderr, "failed to initialize KV engines\n");
        return 1;
    }
    if (install_signal_handlers() != 0) {
        perror("sigaction");
        goto cleanup_engines;
    }
    if (reactor_init(&reactor, (uint16_t)DEFAULT_PORT, dispatch_request, NULL) != 0) {
        perror("reactor_init");
        goto cleanup_engines;
    }

    active_reactor = reactor;
    printf("storeSystem v0.2.0-dev reactor listening on port %u\n", DEFAULT_PORT);
    if (reactor_run(reactor) != 0 && errno != EINTR) {
        perror("reactor_run");
        goto cleanup_reactor;
    }
    result = 0;

cleanup_reactor:
    active_reactor = NULL;
    reactor_destroy(reactor);
cleanup_engines:
    kvstore_engine_destroy();
    return result;
}
#else
#include "kvstore.h"

int main(void)
{
    int result;

    if (kvstore_engine_init() != 0) {
        fprintf(stderr, "failed to initialize KV engines\n");
        return 1;
    }
    result = ntyco_entry();
    kvstore_engine_destroy();
    return result;
}
#endif
