#define _POSIX_C_SOURCE 200809L

#include "net/reactor.h"

#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

typedef struct async_test_context {
    reactor_t *reactor;
    reactor_request_token_t token;
    pthread_mutex_t mutex;
    pthread_cond_t condition;
    int pending;
    int completion_done;
    int completion_result;
    int stop_requested;
} async_test_context_t;

static int test_handler(const unsigned char *input, size_t input_length,
                        int end_of_stream, net_buffer_t *response,
                        size_t *consumed, int *close_after_response,
                        uint64_t *response_barrier,
                        reactor_request_token_t token, void *context)
{
    async_test_context_t *test = context;

    (void)end_of_stream;
    *consumed = 0;
    *close_after_response = 0;
    *response_barrier = 0;
    if (input_length == 0) return REACTOR_HANDLER_INCOMPLETE;
    *consumed = 1U;
    if (input[0] == 'D') {
        pthread_mutex_lock(&test->mutex);
        test->token = token;
        test->pending = 1;
        pthread_cond_broadcast(&test->condition);
        pthread_mutex_unlock(&test->mutex);
        return REACTOR_HANDLER_DEFERRED;
    }
    assert(net_buffer_append(response, input, 1U) == 0);
    return REACTOR_HANDLER_COMPLETE;
}

static int complete_deferred(void *context)
{
    async_test_context_t *test = context;
    reactor_request_token_t token;
    int result;

    pthread_mutex_lock(&test->mutex);
    if (test->stop_requested) {
        pthread_mutex_unlock(&test->mutex);
        reactor_stop(test->reactor);
        return 0;
    }
    if (!test->pending) {
        pthread_mutex_unlock(&test->mutex);
        return 0;
    }
    test->pending = 0;
    token = test->token;
    pthread_mutex_unlock(&test->mutex);
    result = reactor_complete_response(test->reactor, token, "D", 1U,
                                       0, 0);
    pthread_mutex_lock(&test->mutex);
    test->completion_result = result;
    test->completion_done = 1;
    pthread_cond_broadcast(&test->condition);
    pthread_mutex_unlock(&test->mutex);
    return result < 0 ? -1 : 0;
}

static void *run_reactor(void *context)
{
    async_test_context_t *test = context;
    assert(reactor_run(test->reactor) == 0);
    return NULL;
}

static int connect_client(void)
{
    struct sockaddr_in address;
    int fd;
    int attempt;

    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_port = htons(9096U);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    for (attempt = 0; attempt < 100; ++attempt) {
        fd = socket(AF_INET, SOCK_STREAM, 0);
        assert(fd >= 0);
        if (connect(fd, (struct sockaddr *)&address, sizeof(address)) == 0)
            return fd;
        close(fd);
        {
            struct timespec delay = {0, 10000000L};
            nanosleep(&delay, NULL);
        }
    }
    return -1;
}

static void wait_state(async_test_context_t *test, int completion)
{
    struct timespec deadline;
    int *value = completion ? &test->completion_done : &test->pending;

    assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
    deadline.tv_sec += 2;
    pthread_mutex_lock(&test->mutex);
    while (!*value) {
        int result = pthread_cond_timedwait(&test->condition, &test->mutex,
                                            &deadline);
        assert(result == 0);
    }
    pthread_mutex_unlock(&test->mutex);
}

static void wake_reactor(async_test_context_t *test)
{
    uint64_t value = 1;
    ssize_t written;
    do {
        written = write(reactor_wake_fd(test->reactor), &value, sizeof(value));
    } while (written < 0 && errno == EINTR);
    assert(written == (ssize_t)sizeof(value));
}

int main(void)
{
    async_test_context_t test;
    pthread_t thread;
    unsigned char response[3];
    size_t received = 0;
    int client;

    memset(&test, 0, sizeof(test));
    assert(pthread_mutex_init(&test.mutex, NULL) == 0);
    assert(pthread_cond_init(&test.condition, NULL) == 0);
    assert(reactor_init(&test.reactor, 9096U, test_handler, &test) == 0);
    assert(reactor_set_async_handler(test.reactor, complete_deferred, &test) == 0);
    assert(pthread_create(&thread, NULL, run_reactor, &test) == 0);

    client = connect_client();
    assert(client >= 0);
    assert(send(client, "DAB", 3U, 0) == 3);
    wait_state(&test, 0);
    wake_reactor(&test);
    while (received < sizeof(response)) {
        ssize_t amount = recv(client, response + received,
                              sizeof(response) - received, 0);
        assert(amount > 0);
        received += (size_t)amount;
    }
    assert(memcmp(response, "DAB", sizeof(response)) == 0);
    wait_state(&test, 1);
    pthread_mutex_lock(&test.mutex);
    assert(test.completion_result == 1);
    test.completion_done = 0;
    pthread_mutex_unlock(&test.mutex);
    close(client);

    client = connect_client();
    assert(client >= 0);
    assert(send(client, "D", 1U, 0) == 1);
    wait_state(&test, 0);
    close(client);
    {
        struct timespec delay = {0, 50000000L};
        nanosleep(&delay, NULL);
    }
    wake_reactor(&test);
    wait_state(&test, 1);
    pthread_mutex_lock(&test.mutex);
    assert(test.completion_result == 0);
    test.stop_requested = 1;
    pthread_mutex_unlock(&test.mutex);

    wake_reactor(&test);
    assert(pthread_join(thread, NULL) == 0);
    reactor_destroy(test.reactor);
    pthread_cond_destroy(&test.condition);
    pthread_mutex_destroy(&test.mutex);
    puts("test_reactor_async: PASS");
    return 0;
}
