#define _POSIX_C_SOURCE 200809L

#include "net/reactor.h"
#include "protocol/resp.h"
#include "service/kvstore_service.h"

#include <errno.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#ifndef NETWORK_BACKEND_NTYCO
#define DEFAULT_PORT 9096U
#define CACHE_MAINTENANCE_INTERVAL_MS 100U

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

static int encode_reply(const kvstore_reply_t *reply,
                        unsigned char *response,
                        size_t response_capacity,
                        size_t *response_length)
{
    switch (reply->type) {
    case KVSTORE_REPLY_SIMPLE:
        return resp_encode_simple_string(response,
                                         response_capacity,
                                         (const char *)reply->data,
                                         response_length);
    case KVSTORE_REPLY_ERROR:
        return resp_encode_error(response,
                                 response_capacity,
                                 (const char *)reply->data,
                                 response_length);
    case KVSTORE_REPLY_INTEGER:
        return resp_encode_integer(response,
                                   response_capacity,
                                   reply->integer,
                                   response_length);
    case KVSTORE_REPLY_BULK:
        return resp_encode_bulk_string(response,
                                       response_capacity,
                                       reply->data,
                                       reply->length,
                                       response_length);
    case KVSTORE_REPLY_NULL_BULK:
        return resp_encode_null_bulk_string(response,
                                            response_capacity,
                                            response_length);
    default:
        return -1;
    }
}

static int dispatch_request(const unsigned char *input,
                            size_t input_length,
                            int end_of_stream,
                            unsigned char *response,
                            size_t response_capacity,
                            size_t *consumed,
                            size_t *response_length,
                            int *close_after_response,
                            void *context)
{
    kvstore_service_t *service = context;
    resp_request_t request;
    kvstore_argument_t arguments[RESP_MAX_ARGUMENTS];
    kvstore_reply_t reply;
    size_t index;
    int parse_result;

    if (consumed == NULL || response_length == NULL ||
        close_after_response == NULL) {
        return REACTOR_HANDLER_ERROR;
    }
    *consumed = 0;
    *response_length = 0;
    *close_after_response = 0;
    parse_result = resp_parse_request(input,
                                      input_length,
                                      end_of_stream,
                                      &request,
                                      consumed);
    if (parse_result == RESP_PARSE_INCOMPLETE) {
        return REACTOR_HANDLER_INCOMPLETE;
    }
    if (parse_result == RESP_PARSE_ERROR) {
        static const char protocol_error[] = "ERR Protocol error";

        if (resp_encode_error(response,
                              response_capacity,
                              protocol_error,
                              response_length) != 0) {
            return REACTOR_HANDLER_ERROR;
        }
        *consumed = input_length;
        *close_after_response = 1;
        return REACTOR_HANDLER_COMPLETE;
    }

    for (index = 0; index < request.argument_count; ++index) {
        arguments[index].data = request.arguments[index].data;
        arguments[index].length = request.arguments[index].length;
    }
    if (kvstore_service_execute(service,
                                arguments,
                                request.argument_count,
                                &reply) != 0 ||
        encode_reply(&reply,
                     response,
                     response_capacity,
                     response_length) != 0) {
        return REACTOR_HANDLER_ERROR;
    }
    return REACTOR_HANDLER_COMPLETE;
}

static int maintain_cache(void *context)
{
    return kvstore_service_maintain(context);
}

static int suffix_equals(const char *value, const char *expected)
{
    while (*value != '\0' && *expected != '\0') {
        unsigned char left = (unsigned char)*value;
        unsigned char right = (unsigned char)*expected;

        if (left >= 'A' && left <= 'Z') {
            left = (unsigned char)(left - 'A' + 'a');
        }
        if (left != right) {
            return 0;
        }
        value++;
        expected++;
    }
    return *value == '\0' && *expected == '\0';
}

static int parse_size(const char *text, int allow_units, size_t *result)
{
    size_t value = 0;
    size_t index = 0;
    size_t multiplier = 1U;

    if (text == NULL || result == NULL || text[0] < '0' || text[0] > '9') {
        return -1;
    }
    while (text[index] >= '0' && text[index] <= '9') {
        unsigned int digit = (unsigned int)(text[index] - '0');

        if (value > (SIZE_MAX - digit) / 10U) {
            return -1;
        }
        value = value * 10U + digit;
        index++;
    }
    if (text[index] != '\0') {
        if (!allow_units) {
            return -1;
        }
        if (suffix_equals(text + index, "kib")) {
            multiplier = 1024U;
        } else if (suffix_equals(text + index, "mib")) {
            multiplier = 1024U * 1024U;
        } else if (suffix_equals(text + index, "gib")) {
            multiplier = 1024U * 1024U * 1024U;
        } else {
            return -1;
        }
    }
    if (value > SIZE_MAX / multiplier) {
        return -1;
    }
    *result = value * multiplier;
    return 0;
}

static void print_usage(const char *program)
{
    fprintf(stderr,
            "Usage: %s [--maxmemory SIZE] [--maxkeys COUNT]\n"
            "  SIZE accepts bytes or KiB/MiB/GiB suffixes; 0 means unlimited.\n"
            "  COUNT is an unsigned decimal integer; 0 means unlimited.\n",
            program);
}

static int parse_options(int argc, char **argv, cache_config_t *config)
{
    int saw_maxmemory = 0;
    int saw_maxkeys = 0;
    int index;

    memset(config, 0, sizeof(*config));
    if (argc == 2 && strcmp(argv[1], "--help") == 0) {
        print_usage(argv[0]);
        return 1;
    }
    for (index = 1; index < argc; index += 2) {
        if (index + 1 >= argc) {
            print_usage(argv[0]);
            return -1;
        }
        if (strcmp(argv[index], "--maxmemory") == 0 && !saw_maxmemory) {
            if (parse_size(argv[index + 1], 1, &config->max_memory) != 0) {
                print_usage(argv[0]);
                return -1;
            }
            saw_maxmemory = 1;
        } else if (strcmp(argv[index], "--maxkeys") == 0 && !saw_maxkeys) {
            if (parse_size(argv[index + 1], 0, &config->max_keys) != 0) {
                print_usage(argv[0]);
                return -1;
            }
            saw_maxkeys = 1;
        } else {
            print_usage(argv[0]);
            return -1;
        }
    }
    return 0;
}

int main(int argc, char **argv)
{
    cache_config_t cache_config;
    kvstore_service_t service;
    reactor_t *reactor = NULL;
    int parse_result;
    int result = 1;

    parse_result = parse_options(argc, argv, &cache_config);
    if (parse_result != 0) {
        return parse_result > 0 ? 0 : 2;
    }
    if (kvstore_service_init(&service, &cache_config) != 0) {
        fprintf(stderr, "failed to initialize Hash cache\n");
        return 1;
    }
    if (install_signal_handlers() != 0) {
        perror("sigaction");
        goto cleanup_service;
    }
    if (reactor_init(&reactor,
                     (uint16_t)DEFAULT_PORT,
                     dispatch_request,
                     &service) != 0) {
        perror("reactor_init");
        goto cleanup_service;
    }
    if (reactor_set_periodic(reactor,
                             CACHE_MAINTENANCE_INTERVAL_MS,
                             maintain_cache,
                             &service) != 0) {
        perror("reactor_set_periodic");
        goto cleanup_reactor;
    }

    active_reactor = reactor;
    printf("storeSystem v0.4.0-dev RESP reactor listening on port %u "
           "(engine=hash, maxmemory=%zu, maxkeys=%zu)\n",
           DEFAULT_PORT,
           cache_config.max_memory,
           cache_config.max_keys);
    if (reactor_run(reactor) != 0 && errno != EINTR) {
        perror("reactor_run");
        goto cleanup_reactor;
    }
    result = 0;

cleanup_reactor:
    active_reactor = NULL;
    reactor_destroy(reactor);
cleanup_service:
    kvstore_service_destroy(&service);
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
