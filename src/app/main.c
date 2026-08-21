#define _GNU_SOURCE

#include "net/reactor.h"
#include "persistence/aof.h"
#include "persistence/rdb.h"
#include "protocol/resp.h"
#include "service/kvstore_service.h"

#include <errno.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>

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
    int rdb_enabled;
    const char *dbfilename;
    uint64_t save_seconds;
    uint64_t save_changes;
} app_options_t;

typedef struct persistence_manager {
    kvstore_service_t *service;
    aof_t *aof;
    reactor_t *reactor;
    const char *path;
    int enabled;
    uint64_t save_seconds;
    uint64_t save_changes;
    pid_t child_pid;
    uint64_t child_covered_changes;
    uint64_t child_snapshot_time;
    struct timespec child_started;
    struct timespec automatic_epoch;
    uint64_t last_save_time;
    uint64_t last_save_duration_us;
    uint64_t last_fork_pause_us;
    uint64_t last_child_peak_rss_kb;
    uint64_t last_child_minor_faults;
    uint64_t last_child_major_faults;
    uint64_t checkpoint_offset;
    int last_save_status;
} persistence_manager_t;

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
                        net_buffer_t *response)
{
    unsigned char *destination;
    size_t capacity;
    size_t encoded = 0;
    size_t required = 32U;
    size_t index;

    if (reply->type == KVSTORE_REPLY_SIMPLE ||
        reply->type == KVSTORE_REPLY_ERROR) {
        if (reply->length > SIZE_MAX - 3U) return -1;
        required = reply->length + 3U;
    } else if (reply->type == KVSTORE_REPLY_BULK) {
        if (reply->length > SIZE_MAX - 64U) return -1;
        required = reply->length + 64U;
    } else if (reply->type == KVSTORE_REPLY_NULL_BULK) {
        required = 5U;
    } else if (reply->type == KVSTORE_REPLY_ARRAY) {
        required = 64U;
        for (index = 0; index < reply->element_count; ++index) {
            if (required > SIZE_MAX - 64U ||
                reply->elements[index].length > SIZE_MAX - required - 64U)
                return -1;
            required += reply->elements[index].length + 64U;
        }
    }
    destination = net_buffer_write_pointer(response, required, &capacity);
    if (destination == NULL) return -1;
    switch (reply->type) {
    case KVSTORE_REPLY_SIMPLE:
        if (resp_encode_simple_string(destination, capacity,
                                      (const char *)reply->data,
                                      &encoded) != 0) return -1;
        break;
    case KVSTORE_REPLY_ERROR:
        if (resp_encode_error(destination, capacity,
                              (const char *)reply->data,
                              &encoded) != 0) return -1;
        break;
    case KVSTORE_REPLY_INTEGER:
        if (resp_encode_integer(destination, capacity, reply->integer,
                                &encoded) != 0) return -1;
        break;
    case KVSTORE_REPLY_BULK:
        if (resp_encode_bulk_string(destination, capacity, reply->data,
                                    reply->length, &encoded) != 0) return -1;
        break;
    case KVSTORE_REPLY_NULL_BULK:
        if (resp_encode_null_bulk_string(destination, capacity,
                                         &encoded) != 0) return -1;
        break;
    case KVSTORE_REPLY_ARRAY:
        {
            int header_length;
            size_t position;

            header_length = snprintf((char *)destination,
                                     capacity,
                                     "*%zu\r\n",
                                     reply->element_count);
            if (header_length < 0 ||
                (size_t)header_length >= capacity) return -1;
            position = (size_t)header_length;
            for (index = 0; index < reply->element_count; ++index) {
                size_t element_length = 0;

                if (resp_encode_bulk_string(destination + position,
                                            capacity - position,
                                            reply->elements[index].data,
                                            reply->elements[index].length,
                                            &element_length) != 0) return -1;
                position += element_length;
            }
            encoded = position;
            break;
        }
    default:
        return -1;
    }
    return net_buffer_commit(response, encoded);
}

static int dispatch_request(const unsigned char *input,
                            size_t input_length,
                            int end_of_stream,
                            net_buffer_t *response,
                            size_t *consumed,
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

    if (consumed == NULL || response == NULL ||
        close_after_response == NULL || response_barrier == NULL) {
        return REACTOR_HANDLER_ERROR;
    }
    *consumed = 0;
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

        unsigned char *destination;
        size_t capacity;
        size_t response_length;

        destination = net_buffer_write_pointer(response, 64U, &capacity);
        if (destination == NULL ||
            resp_encode_error(destination, capacity, protocol_error,
                              &response_length) != 0 ||
            net_buffer_commit(response, response_length) != 0) {
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
        encode_reply(&reply, response) != 0) {
        return REACTOR_HANDLER_ERROR;
    }
    return REACTOR_HANDLER_COMPLETE;
}

static uint64_t monotonic_elapsed_us(const struct timespec *start,
                                     const struct timespec *end)
{
    time_t seconds = end->tv_sec - start->tv_sec;
    long nanoseconds = end->tv_nsec - start->tv_nsec;

    if (nanoseconds < 0) {
        seconds--;
        nanoseconds += 1000000000L;
    }
    return seconds < 0 ? 0 : (uint64_t)seconds * UINT64_C(1000000) +
                               (uint64_t)nanoseconds / 1000U;
}

static int persistence_checkpoint(persistence_manager_t *manager,
                                  rdb_checkpoint_t *checkpoint)
{
    memset(checkpoint, 0, sizeof(*checkpoint));
    return manager->aof == NULL
               ? 0 : aof_create_checkpoint(manager->aof, checkpoint);
}

static void persistence_finish_child(persistence_manager_t *manager,
                                     int status,
                                     const struct timespec *finished)
{
    manager->last_save_duration_us =
        monotonic_elapsed_us(&manager->child_started, finished);
    if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
        manager->last_save_status = 0;
        manager->last_save_time = manager->child_snapshot_time;
        kvstore_service_snapshot_committed(manager->service,
                                            manager->child_covered_changes);
    } else {
        manager->last_save_status = -1;
    }
    manager->child_pid = 0;
    manager->child_covered_changes = 0;
    manager->child_snapshot_time = 0;
    manager->automatic_epoch = *finished;
}

static int persistence_reap(persistence_manager_t *manager, int wait)
{
    int status;
    pid_t result;
    struct timespec finished;
    struct rusage usage;

    if (manager->child_pid <= 0) return 0;
    do {
        memset(&usage, 0, sizeof(usage));
        result = wait4(manager->child_pid, &status, wait ? 0 : WNOHANG,
                       &usage);
    } while (result < 0 && errno == EINTR);
    if (result == 0) return 0;
    if (result < 0) return -1;
    manager->last_child_peak_rss_kb = (uint64_t)usage.ru_maxrss;
    manager->last_child_minor_faults = (uint64_t)usage.ru_minflt;
    manager->last_child_major_faults = (uint64_t)usage.ru_majflt;
    if (clock_gettime(CLOCK_MONOTONIC, &finished) != 0) return -1;
    persistence_finish_child(manager, status, &finished);
    return 0;
}

static int persistence_save(void *context, int background)
{
    persistence_manager_t *manager = context;
    rdb_checkpoint_t checkpoint;
    rdb_stats_t stats;
    struct timespec started;
    struct timespec finished;
    struct timespec wall;
    uint64_t covered_changes;

    if (manager == NULL || !manager->enabled) {
        errno = ENOTSUP;
        return -1;
    }
    if (persistence_reap(manager, 0) != 0) return -1;
    if (manager->child_pid > 0) {
        errno = EBUSY;
        return -1;
    }
    if (clock_gettime(CLOCK_MONOTONIC, &started) != 0 ||
        persistence_checkpoint(manager, &checkpoint) != 0) {
        manager->last_save_status = -1;
        return -1;
    }
    covered_changes = kvstore_service_dirty_changes(manager->service);
    manager->checkpoint_offset = checkpoint.valid
                                     ? checkpoint.aof_offset : 0;
    if (!background) {
        int result = rdb_save(manager->path,
                              manager->service->cache,
                              &checkpoint,
                              &stats);

        if (clock_gettime(CLOCK_MONOTONIC, &finished) == 0) {
            manager->last_save_duration_us =
                monotonic_elapsed_us(&started, &finished);
            manager->automatic_epoch = finished;
        }
        if (result != 0) {
            manager->last_save_status = -1;
            return -1;
        }
        manager->last_save_status = 0;
        manager->last_save_time = stats.snapshot_time_ms / 1000U;
        kvstore_service_snapshot_committed(manager->service, covered_changes);
        return 0;
    }

    if (clock_gettime(CLOCK_REALTIME, &wall) != 0 ||
        aof_pause_for_fork(manager->aof) != 0) return -1;
    manager->child_pid = fork();
    if (manager->child_pid == 0) {
        reactor_close_in_child(manager->reactor);
        aof_close_in_child(manager->aof);
        int child_result = rdb_save(manager->path,
                                    manager->service->cache,
                                    &checkpoint,
                                    &stats);

        _exit(child_result == 0 ? 0 : 1);
    }
    aof_resume_after_fork(manager->aof);
    if (manager->child_pid < 0) {
        manager->child_pid = 0;
        manager->last_save_status = -1;
        return -1;
    }
    if (clock_gettime(CLOCK_MONOTONIC, &finished) == 0) {
        manager->last_fork_pause_us = monotonic_elapsed_us(&started, &finished);
    }
    manager->child_started = started;
    manager->child_covered_changes = covered_changes;
    manager->child_snapshot_time = (uint64_t)wall.tv_sec;
    return 0;
}

static uint64_t persistence_lastsave(void *context)
{
    persistence_manager_t *manager = context;

    return manager == NULL ? 0 : manager->last_save_time;
}

static void persistence_get_info(void *context,
                                 kvstore_persistence_info_t *info)
{
    persistence_manager_t *manager = context;

    if (info == NULL) return;
    memset(info, 0, sizeof(*info));
    if (manager == NULL) return;
    info->rdb_enabled = manager->enabled;
    info->bgsave_in_progress = manager->child_pid > 0;
    info->dirty_changes = kvstore_service_dirty_changes(manager->service);
    info->last_save_time = manager->last_save_time;
    info->last_save_duration_us = manager->last_save_duration_us;
    info->last_fork_pause_us = manager->last_fork_pause_us;
    info->last_child_peak_rss_kb = manager->last_child_peak_rss_kb;
    info->last_child_minor_faults = manager->last_child_minor_faults;
    info->last_child_major_faults = manager->last_child_major_faults;
    info->last_save_status = manager->last_save_status;
    info->checkpoint_offset = manager->checkpoint_offset;
}

static int persistence_maintain(persistence_manager_t *manager)
{
    struct timespec now;

    if (persistence_reap(manager, 0) != 0) return -1;
    if (!manager->enabled || manager->child_pid > 0 ||
        manager->save_seconds == 0 || manager->save_changes == 0 ||
        kvstore_service_dirty_changes(manager->service) <
            manager->save_changes) return 0;
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) return -1;
    if ((uint64_t)(now.tv_sec - manager->automatic_epoch.tv_sec) <
        manager->save_seconds) return 0;
    return persistence_save(manager, 1);
}

static int maintain_cache(void *context)
{
    persistence_manager_t *manager = context;

    return kvstore_service_maintain(manager->service) != 0 ||
           persistence_maintain(manager) != 0
               ? -1 : 0;
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
            "          [--rdb yes|no] [--dbfilename PATH]\n"
            "          [--save-seconds SECONDS] [--save-changes COUNT]\n"
            "          [--zset-engine skiplist|rbtree]\n"
            "  SIZE accepts bytes or KiB/MiB/GiB suffixes; 0 means unlimited.\n"
            "  COUNT is an unsigned decimal integer; 0 means unlimited.\n"
            "  AOF defaults: appendonly=no, appendfilename=appendonly.aof,\n"
            "                appendfsync=everysec.\n"
            "  RDB defaults: rdb=no, dbfilename=dump.kvrdb, automatic save off.\n"
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
    int saw_rdb = 0;
    int saw_dbfilename = 0;
    int saw_save_seconds = 0;
    int saw_save_changes = 0;
    int index;

    memset(options, 0, sizeof(*options));
    options->appendfilename = "appendonly.aof";
    options->appendfsync = AOF_FSYNC_EVERYSEC;
    options->zset_engine = KV_ZSET_SKIPLIST;
    options->dbfilename = "dump.kvrdb";
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
        } else if (strcmp(argv[index], "--rdb") == 0 && !saw_rdb) {
            if (suffix_equals(argv[index + 1], "yes")) {
                options->rdb_enabled = 1;
            } else if (suffix_equals(argv[index + 1], "no")) {
                options->rdb_enabled = 0;
            } else {
                print_usage(argv[0]);
                return -1;
            }
            saw_rdb = 1;
        } else if (strcmp(argv[index], "--dbfilename") == 0 &&
                   !saw_dbfilename && argv[index + 1][0] != '\0') {
            options->dbfilename = argv[index + 1];
            saw_dbfilename = 1;
        } else if (strcmp(argv[index], "--save-seconds") == 0 &&
                   !saw_save_seconds) {
            size_t parsed;

            if (parse_size(argv[index + 1], 0, &parsed) != 0) {
                print_usage(argv[0]);
                return -1;
            }
            options->save_seconds = (uint64_t)parsed;
            saw_save_seconds = 1;
        } else if (strcmp(argv[index], "--save-changes") == 0 &&
                   !saw_save_changes) {
            size_t parsed;

            if (parse_size(argv[index + 1], 0, &parsed) != 0) {
                print_usage(argv[0]);
                return -1;
            }
            options->save_changes = (uint64_t)parsed;
            saw_save_changes = 1;
        } else {
            print_usage(argv[0]);
            return -1;
        }
    }
    if ((options->save_seconds == 0) != (options->save_changes == 0) ||
        (!options->rdb_enabled &&
         (options->save_seconds != 0 || options->save_changes != 0))) {
        print_usage(argv[0]);
        return -1;
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
    rdb_checkpoint_t rdb_checkpoint;
    rdb_stats_t rdb_stats;
    persistence_manager_t persistence;
    kvstore_persistence_admin_t persistence_admin;
    uint64_t replay_offset = 0;
    int rdb_loaded = 0;
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
    memset(&persistence, 0, sizeof(persistence));
    persistence.service = &service;
    persistence.enabled = options.rdb_enabled;
    persistence.path = options.dbfilename;
    persistence.save_seconds = options.save_seconds;
    persistence.save_changes = options.save_changes;
    persistence.last_save_status = 1;
    if (clock_gettime(CLOCK_MONOTONIC, &persistence.automatic_epoch) != 0) {
        perror("clock_gettime");
        goto cleanup_service;
    }
    memset(&replay_stats, 0, sizeof(replay_stats));
    memset(&rdb_checkpoint, 0, sizeof(rdb_checkpoint));
    memset(&rdb_stats, 0, sizeof(rdb_stats));
    if (options.appendonly) {
        if (aof_open(&aof,
                     options.appendfilename,
                     options.appendfsync) != 0) {
            perror("aof_open");
            goto cleanup_service;
        }
        persistence.aof = aof;
    }
    if (options.rdb_enabled) {
        if (access(options.dbfilename, F_OK) == 0) {
            if (rdb_load(options.dbfilename,
                         service.cache,
                         options.zset_engine,
                         &rdb_checkpoint,
                         &rdb_stats) != 0) {
                perror("rdb_load");
                goto cleanup_aof;
            }
            rdb_loaded = 1;
            persistence.last_save_time = rdb_stats.snapshot_time_ms / 1000U;
            persistence.last_save_status = 0;
            persistence.checkpoint_offset = rdb_checkpoint.valid
                                                ? rdb_checkpoint.aof_offset : 0;
            if (aof != NULL) {
                if (!rdb_checkpoint.valid ||
                    aof_validate_checkpoint(aof, &rdb_checkpoint) != 0) {
                    errno = EINVAL;
                    perror("rdb/aof checkpoint");
                    goto cleanup_aof;
                }
                replay_offset = rdb_checkpoint.aof_offset;
            }
        } else if (errno != ENOENT) {
            perror("access rdb");
            goto cleanup_aof;
        }
    }
    if (options.appendonly) {
        struct timespec replay_start;
        struct timespec replay_end;

        if (clock_gettime(CLOCK_MONOTONIC, &replay_start) != 0) {
            perror("clock_gettime");
            goto cleanup_aof;
        }
        if (aof_replay_from(aof,
                            replay_offset,
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
    kvstore_service_snapshot_committed(&service, UINT64_MAX);
    memset(&persistence_admin, 0, sizeof(persistence_admin));
    persistence_admin.save = persistence_save;
    persistence_admin.lastsave = persistence_lastsave;
    persistence_admin.get_info = persistence_get_info;
    kvstore_service_set_persistence_admin(&service,
                                          &persistence_admin,
                                          &persistence);
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
    persistence.reactor = reactor;
    if (reactor_set_periodic(reactor,
                             CACHE_MAINTENANCE_INTERVAL_MS,
                             maintain_cache,
                             &persistence) != 0) {
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
    printf("storeSystem v0.6.2 RESP reactor listening on port %u "
           "(keyspace=hash, zset=%s, maxmemory=%zu, maxkeys=%zu, "
           "aof=%s, rdb=%s, loaded=%zu, rdb_loaded=%d)\n",
           DEFAULT_PORT,
           kv_zset_engine_name(options.zset_engine),
           options.cache.max_memory,
           options.cache.max_keys,
           options.appendonly ? options.appendfilename : "off",
           options.rdb_enabled ? options.dbfilename : "off",
           replay_stats.commands_loaded,
           rdb_loaded);
    if (reactor_run(reactor) != 0 && errno != EINTR) {
        perror("reactor_run");
        goto cleanup_reactor;
    }
    result = 0;

cleanup_reactor:
    active_reactor = NULL;
    if (persistence.child_pid > 0 &&
        persistence_reap(&persistence, 1) != 0) {
        perror("waitpid bgsave");
        result = 1;
    }
    reactor_destroy(reactor);
cleanup_aof:
    kvstore_service_set_persistence_admin(&service, NULL, NULL);
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
