#define _POSIX_C_SOURCE 200809L

#include "net/reactor.h"
#include "persistence/aof.h"
#include "protocol/resp.h"
#include "service/kvstore_service.h"

#include <errno.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#ifndef NETWORK_BACKEND_NTYCO
#define DEFAULT_PORT 9096U
#define CACHE_MAINTENANCE_INTERVAL_MS 100U

static reactor_t *active_reactor;

static double elapsed_seconds(const struct timespec *start,
                              const struct timespec *end)
{
    time_t seconds = end->tv_sec - start->tv_sec;
    long nanoseconds = end->tv_nsec - start->tv_nsec;

    return (double)seconds + (double)nanoseconds / 1000000000.0;
}

typedef struct app_options {
    cache_config_t cache;
    kv_zset_engine_t zset_engine;
    int appendonly;
    const char *appendfilename;
    aof_fsync_policy_t appendfsync;
} app_options_t;

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
    case KVSTORE_REPLY_ARRAY:
        {
            int header_length;
            size_t position;
            size_t index;

            header_length = snprintf((char *)response,
                                     response_capacity,
                                     "*%zu\r\n",
                                     reply->element_count);
            if (header_length < 0 ||
                (size_t)header_length >= response_capacity) return -1;
            position = (size_t)header_length;
            for (index = 0; index < reply->element_count; ++index) {
                size_t encoded = 0;

                if (resp_encode_bulk_string(response + position,
                                            response_capacity - position,
                                            reply->elements[index].data,
                                            reply->elements[index].length,
                                            &encoded) != 0) return -1;
                position += encoded;
            }
            *response_length = position;
            return 0;
        }
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
                            uint64_t *response_barrier,
                            void *context)
{
    kvstore_service_t *service = context;
    resp_request_t request;
    kvstore_argument_t arguments[RESP_MAX_ARGUMENTS];
    kvstore_reply_t reply;
    size_t index;
    int parse_result;

    if (consumed == NULL || response_length == NULL ||
        close_after_response == NULL || response_barrier == NULL) {
        return REACTOR_HANDLER_ERROR;
    }
    *consumed = 0;
    *response_length = 0;
    *close_after_response = 0;
    *response_barrier = 0;
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
    if (kvstore_service_execute_with_barrier(service,
                                             arguments,
                                             request.argument_count,
                                             &reply,
                                             response_barrier) != 0 ||
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

static int flush_service(void *context)
{
    return kvstore_service_flush(context);
}

static int aof_barrier_ready(uint64_t sequence, void *context)
{
    kvstore_service_t *service = context;

    return aof_sequence_ready(service->aof, sequence);
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
            "          [--appendonly yes|no] [--appendfilename PATH]\n"
            "          [--appendfsync always|everysec|no]\n"
            "          [--zset-engine skiplist|rbtree]\n"
            "  SIZE accepts bytes or KiB/MiB/GiB suffixes; 0 means unlimited.\n"
            "  COUNT is an unsigned decimal integer; 0 means unlimited.\n"
            "  AOF defaults: appendonly=no, appendfilename=appendonly.aof,\n"
            "                appendfsync=everysec.\n"
            "  ZSet defaults: zset-engine=skiplist.\n",
            program);
}

static int parse_options(int argc, char **argv, app_options_t *options)
{
    int saw_maxmemory = 0;
    int saw_maxkeys = 0;
    int saw_appendonly = 0;
    int saw_appendfilename = 0;
    int saw_appendfsync = 0;
    int saw_zset_engine = 0;
    int index;

    memset(options, 0, sizeof(*options));
    options->appendfilename = "appendonly.aof";
    options->appendfsync = AOF_FSYNC_EVERYSEC;
    options->zset_engine = KV_ZSET_SKIPLIST;
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
            if (parse_size(argv[index + 1],
                           1,
                           &options->cache.max_memory) != 0) {
                print_usage(argv[0]);
                return -1;
            }
            saw_maxmemory = 1;
        } else if (strcmp(argv[index], "--maxkeys") == 0 && !saw_maxkeys) {
            if (parse_size(argv[index + 1],
                           0,
                           &options->cache.max_keys) != 0) {
                print_usage(argv[0]);
                return -1;
            }
            saw_maxkeys = 1;
        } else if (strcmp(argv[index], "--appendonly") == 0 &&
                   !saw_appendonly) {
            if (suffix_equals(argv[index + 1], "yes")) {
                options->appendonly = 1;
            } else if (suffix_equals(argv[index + 1], "no")) {
                options->appendonly = 0;
            } else {
                print_usage(argv[0]);
                return -1;
            }
            saw_appendonly = 1;
        } else if (strcmp(argv[index], "--appendfilename") == 0 &&
                   !saw_appendfilename && argv[index + 1][0] != '\0') {
            options->appendfilename = argv[index + 1];
            saw_appendfilename = 1;
        } else if (strcmp(argv[index], "--appendfsync") == 0 &&
                   !saw_appendfsync) {
            if (suffix_equals(argv[index + 1], "always")) {
                options->appendfsync = AOF_FSYNC_ALWAYS;
            } else if (suffix_equals(argv[index + 1], "everysec")) {
                options->appendfsync = AOF_FSYNC_EVERYSEC;
            } else if (suffix_equals(argv[index + 1], "no")) {
                options->appendfsync = AOF_FSYNC_NO;
            } else {
                print_usage(argv[0]);
                return -1;
            }
            saw_appendfsync = 1;
        } else if (strcmp(argv[index], "--zset-engine") == 0 &&
                   !saw_zset_engine) {
            if (suffix_equals(argv[index + 1], "skiplist")) {
                options->zset_engine = KV_ZSET_SKIPLIST;
            } else if (suffix_equals(argv[index + 1], "rbtree")) {
                options->zset_engine = KV_ZSET_RBTREE;
            } else {
                print_usage(argv[0]);
                return -1;
            }
            saw_zset_engine = 1;
        } else {
            print_usage(argv[0]);
            return -1;
        }
    }
    return 0;
}

int main(int argc, char **argv)
{
    app_options_t options;
    kvstore_service_config_t service_config;
    kvstore_service_t service;
    reactor_t *reactor = NULL;
    aof_t *aof = NULL;
    aof_replay_stats_t replay_stats;
    double replay_duration_seconds = 0.0;
    int parse_result;
    int result = 1;

    parse_result = parse_options(argc, argv, &options);
    if (parse_result != 0) {
        return parse_result > 0 ? 0 : 2;
    }
    memset(&service_config, 0, sizeof(service_config));
    service_config.cache = options.cache;
    service_config.zset_engine = options.zset_engine;
    if (kvstore_service_init_with_config(&service, &service_config) != 0) {
        fprintf(stderr, "failed to initialize typed cache\n");
        return 1;
    }
    memset(&replay_stats, 0, sizeof(replay_stats));
    if (options.appendonly) {
        struct timespec replay_start;
        struct timespec replay_end;

        if (aof_open(&aof,
                     options.appendfilename,
                     options.appendfsync) != 0) {
            perror("aof_open");
            goto cleanup_service;
        }
        if (clock_gettime(CLOCK_MONOTONIC, &replay_start) != 0) {
            perror("clock_gettime");
            goto cleanup_aof;
        }
        if (aof_replay(aof,
                       kvstore_service_replay_aof,
                       &service,
                       &replay_stats) != 0) {
            perror("aof_replay");
            goto cleanup_aof;
        }
        if (clock_gettime(CLOCK_MONOTONIC, &replay_end) != 0) {
            perror("clock_gettime");
            goto cleanup_aof;
        }
        replay_duration_seconds = elapsed_seconds(&replay_start, &replay_end);
        fprintf(stderr,
                "aof_replay_commands: %zu\n"
                "aof_replay_duration_seconds: %.9f\n",
                replay_stats.commands_loaded,
                replay_duration_seconds);
        if (replay_stats.truncated_tail_repaired) {
            fprintf(stderr,
                    "warning: repaired an incomplete AOF tail in %s\n",
                    options.appendfilename);
        }
        kvstore_service_attach_aof(&service, aof);
    }
    if (install_signal_handlers() != 0) {
        perror("sigaction");
        goto cleanup_aof;
    }
    if (reactor_init(&reactor,
                     (uint16_t)DEFAULT_PORT,
                     dispatch_request,
                     &service) != 0) {
        perror("reactor_init");
        goto cleanup_aof;
    }
    if (reactor_set_periodic(reactor,
                             CACHE_MAINTENANCE_INTERVAL_MS,
                             maintain_cache,
                             &service) != 0) {
        perror("reactor_set_periodic");
        goto cleanup_reactor;
    }
    if (reactor_set_flush_handler(reactor, flush_service, &service) != 0) {
        perror("reactor_set_flush_handler");
        goto cleanup_reactor;
    }
    if (aof != NULL &&
        (aof_set_notify_fd(aof, reactor_wake_fd(reactor)) != 0 ||
         reactor_set_response_barrier(reactor,
                                      aof_barrier_ready,
                                      &service) != 0)) {
        perror("aof response barrier");
        goto cleanup_reactor;
    }

    active_reactor = reactor;
    printf("storeSystem v0.6.1 RESP reactor listening on port %u "
           "(keyspace=hash, zset=%s, maxmemory=%zu, maxkeys=%zu, "
           "aof=%s, loaded=%zu)\n",
           DEFAULT_PORT,
           kv_zset_engine_name(options.zset_engine),
           options.cache.max_memory,
           options.cache.max_keys,
           options.appendonly ? options.appendfilename : "off",
           replay_stats.commands_loaded);
    if (reactor_run(reactor) != 0 && errno != EINTR) {
        perror("reactor_run");
        goto cleanup_reactor;
    }
    result = 0;

cleanup_reactor:
    active_reactor = NULL;
    reactor_destroy(reactor);
cleanup_aof:
    kvstore_service_attach_aof(&service, NULL);
    if (aof_close(aof) != 0) {
        perror("aof_close");
        result = 1;
    }
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
