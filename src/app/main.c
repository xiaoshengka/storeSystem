#define _GNU_SOURCE

#include "net/reactor.h"
#include "engine/hash.h"
#include "persistence/aof.h"
#include "persistence/rdb.h"
#include "protocol/resp.h"
#include "service/kvstore_service.h"
#include "storage/mysql_store.h"

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <signal.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <strings.h>
#include <string.h>
#include <time.h>
#include <sys/resource.h>
#include <sys/random.h>
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
    int mysql_enabled;
    const char *mysql_host;
    unsigned int mysql_port;
    const char *mysql_user;
    const char *mysql_database;
    size_t mysql_read_workers;
    unsigned int mysql_connect_timeout;
    unsigned int mysql_io_timeout;
    size_t mysql_read_queue_limit;
    size_t mysql_write_queue_bytes;
    size_t mysql_load_max_bytes;
    uint64_t mysql_negative_ttl_ms;
    size_t mysql_negative_capacity;
} app_options_t;

typedef struct mysql_pending_request {
    struct app_context *app;
    reactor_request_token_t token;
    kvstore_argument_t arguments[RESP_MAX_ARGUMENTS];
    size_t argument_count;
    unsigned char *payload;
    struct mysql_pending_request *dirty_next;
    uint64_t load_generation;
    int load_registered;
} mysql_pending_request_t;

typedef struct app_context {
    kvstore_service_t *service;
    mysql_store_t *mysql;
    reactor_t *reactor;
    hashtable_t *negative_cache;
    hashtable_t *dirty_keys;
    hashtable_t *load_states;
    mysql_pending_request_t *dirty_wait_head;
    mysql_pending_request_t *dirty_wait_tail;
    uint64_t negative_ttl_ms;
    size_t negative_capacity;
} app_context_t;

typedef struct mysql_negative_entry {
    uint64_t expire_at_ms;
} mysql_negative_entry_t;

typedef struct mysql_load_state {
    uint64_t generation;
    size_t waiters;
} mysql_load_state_t;

typedef struct persistence_manager {
    kvstore_service_t *service;
    app_context_t *app;
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

static void negative_entry_destroy(void *payload)
{
    free(payload);
}

static void negative_cache_invalidate(app_context_t *app,
                                      const void *key, size_t key_length)
{
    mysql_negative_entry_t *entry;

    if (app->negative_cache == NULL) return;
    (void)kv_hash_rehash_step(app->negative_cache, 1U);
    entry = kv_hash_remove(app->negative_cache, key, key_length);
    free(entry);
}

static int negative_cache_contains(app_context_t *app,
                                   const void *key, size_t key_length)
{
    kv_hash_node_t *node;
    mysql_negative_entry_t *entry;
    uint64_t now;

    if (app->negative_cache == NULL) return 0;
    (void)kv_hash_rehash_step(app->negative_cache, 1U);
    node = kv_hash_find(app->negative_cache, key, key_length);
    if (node == NULL) return 0;
    entry = kv_hash_node_payload(node);
    now = cache_current_time_ms(app->service->cache);
    if (entry->expire_at_ms > now) return 1;
    entry = kv_hash_remove_node(app->negative_cache, node);
    free(entry);
    return 0;
}

static int negative_cache_add(app_context_t *app,
                              const void *key, size_t key_length)
{
    kv_hash_node_t *node;
    mysql_negative_entry_t *entry;
    uint64_t now;

    if (app->negative_cache == NULL || app->negative_capacity == 0 ||
        app->negative_ttl_ms == 0) return 0;
    (void)kv_hash_rehash_step(app->negative_cache, 1U);
    now = cache_current_time_ms(app->service->cache);
    node = kv_hash_find(app->negative_cache, key, key_length);
    if (node != NULL) {
        entry = kv_hash_node_payload(node);
        entry->expire_at_ms = now > UINT64_MAX - app->negative_ttl_ms
                                  ? UINT64_MAX : now + app->negative_ttl_ms;
        return 0;
    }
    if (kv_hash_count(app->negative_cache) >= app->negative_capacity) {
        kv_hash_iterator_t iterator;
        kv_hash_node_t *victim;
        kv_hash_iterator_begin(app->negative_cache, &iterator);
        victim = kv_hash_iterator_next(&iterator);
        if (victim != NULL) {
            entry = kv_hash_remove_node(app->negative_cache, victim);
            free(entry);
        }
    }
    entry = malloc(sizeof(*entry));
    if (entry == NULL) return -1;
    entry->expire_at_ms = now > UINT64_MAX - app->negative_ttl_ms
                              ? UINT64_MAX : now + app->negative_ttl_ms;
    if (kv_hash_insert(app->negative_cache, key, key_length, entry, NULL) != 0) {
        free(entry);
        return -1;
    }
    return 0;
}

static void bump_inflight_load_generation(app_context_t *app,
                                          const void *key,
                                          size_t key_length)
{
    kv_hash_node_t *node;
    mysql_load_state_t *state;

    if (app->load_states == NULL) return;
    node = kv_hash_find(app->load_states, key, key_length);
    if (node == NULL) return;
    state = kv_hash_node_payload(node);
    state->generation++;
}

static int register_pending_load(app_context_t *app,
                                 mysql_pending_request_t *pending)
{
    const void *key = pending->arguments[1].data;
    size_t key_length = pending->arguments[1].length;
    kv_hash_node_t *node;
    mysql_load_state_t *state;

    (void)kv_hash_rehash_step(app->load_states, 1U);
    node = kv_hash_find(app->load_states, key, key_length);
    if (node == NULL) {
        state = calloc(1, sizeof(*state));
        if (state == NULL ||
            kv_hash_insert(app->load_states, key, key_length,
                           state, &node) != 0) {
            free(state);
            return -1;
        }
    } else {
        state = kv_hash_node_payload(node);
    }
    if (state->waiters == SIZE_MAX) return -1;
    pending->load_generation = state->generation;
    pending->load_registered = 1;
    state->waiters++;
    return 0;
}

static int pending_load_is_stale(mysql_pending_request_t *pending)
{
    kv_hash_node_t *node;
    mysql_load_state_t *state;

    if (!pending->load_registered || pending->app->load_states == NULL)
        return 0;
    node = kv_hash_find(pending->app->load_states,
                        pending->arguments[1].data,
                        pending->arguments[1].length);
    if (node == NULL) return 1;
    state = kv_hash_node_payload(node);
    return state->generation != pending->load_generation;
}

static void unregister_pending_load(mysql_pending_request_t *pending)
{
    kv_hash_node_t *node;
    mysql_load_state_t *state;

    if (!pending->load_registered || pending->app->load_states == NULL) return;
    node = kv_hash_find(pending->app->load_states,
                        pending->arguments[1].data,
                        pending->arguments[1].length);
    pending->load_registered = 0;
    if (node == NULL) return;
    state = kv_hash_node_payload(node);
    if (state->waiters > 0) state->waiters--;
    if (state->waiters == 0) {
        state = kv_hash_remove_node(pending->app->load_states, node);
        free(state);
    }
}

static mysql_pending_request_t *pending_request_copy(
    app_context_t *app,
    reactor_request_token_t token,
    const kvstore_argument_t *arguments,
    size_t argument_count)
{
    mysql_pending_request_t *pending;
    size_t payload_size = 0;
    size_t payload_offset = 0;
    size_t index;

    if (argument_count > RESP_MAX_ARGUMENTS) return NULL;
    for (index = 0; index < argument_count; ++index) {
        if (arguments[index].length > SIZE_MAX - payload_size) return NULL;
        payload_size += arguments[index].length;
    }
    pending = calloc(1, sizeof(*pending));
    if (pending != NULL)
        pending->payload = malloc(payload_size == 0 ? 1U : payload_size);
    if (pending == NULL || pending->payload == NULL) {
        if (pending != NULL) free(pending->payload);
        free(pending);
        return NULL;
    }
    pending->token = token;
    pending->app = app;
    pending->argument_count = argument_count;
    for (index = 0; index < argument_count; ++index) {
        pending->arguments[index].data = pending->payload + payload_offset;
        pending->arguments[index].length = arguments[index].length;
        memcpy(pending->payload + payload_offset, arguments[index].data,
               arguments[index].length);
        payload_offset += arguments[index].length;
    }
    return pending;
}

static uint64_t dirty_key_sequence(app_context_t *app,
                                   const void *key, size_t key_length)
{
    kv_hash_node_t *node;
    uint64_t *sequence;
    mysql_store_stats_t stats;

    if (app->dirty_keys == NULL) return 0;
    (void)kv_hash_rehash_step(app->dirty_keys, 1U);
    node = kv_hash_find(app->dirty_keys, key, key_length);
    if (node == NULL) return 0;
    sequence = kv_hash_node_payload(node);
    mysql_store_get_stats(app->mysql, &stats);
    if (*sequence <= stats.applied_sequence) {
        sequence = kv_hash_remove_node(app->dirty_keys, node);
        free(sequence);
        return 0;
    }
    return *sequence;
}

static int mark_dirty_key(app_context_t *app,
                          const void *key, size_t key_length,
                          uint64_t sequence)
{
    kv_hash_node_t *node;
    uint64_t *stored;

    if (app->dirty_keys == NULL || sequence == 0) return 0;
    bump_inflight_load_generation(app, key, key_length);
    (void)kv_hash_rehash_step(app->dirty_keys, 1U);
    node = kv_hash_find(app->dirty_keys, key, key_length);
    if (node != NULL) {
        stored = kv_hash_node_payload(node);
        if (sequence > *stored) *stored = sequence;
        return 0;
    }
    stored = malloc(sizeof(*stored));
    if (stored == NULL) return -1;
    *stored = sequence;
    if (kv_hash_insert(app->dirty_keys, key, key_length, stored, NULL) != 0) {
        free(stored);
        return -1;
    }
    return 0;
}

static void queue_dirty_waiter(app_context_t *app,
                               mysql_pending_request_t *pending)
{
    if (app->dirty_wait_tail == NULL) app->dirty_wait_head = pending;
    else app->dirty_wait_tail->dirty_next = pending;
    app->dirty_wait_tail = pending;
}

static void purge_applied_dirty_keys(app_context_t *app, size_t budget)
{
    mysql_store_stats_t stats;
    kv_hash_iterator_t iterator;
    kv_hash_node_t *node;

    if (app == NULL || app->mysql == NULL || app->dirty_keys == NULL) return;
    mysql_store_get_stats(app->mysql, &stats);
    (void)kv_hash_rehash_step(app->dirty_keys, budget == 0 ? 1U : budget);
    kv_hash_iterator_begin(app->dirty_keys, &iterator);
    while (budget > 0 && (node = kv_hash_iterator_next(&iterator)) != NULL) {
        uint64_t *sequence = kv_hash_node_payload(node);
        budget--;
        if (*sequence <= stats.applied_sequence) {
            sequence = kv_hash_remove_node(app->dirty_keys, node);
            free(sequence);
        }
    }
}

static int dispatch_request(const unsigned char *input,
                            size_t input_length,
                            int end_of_stream,
                            net_buffer_t *response,
                            size_t *consumed,
                            int *close_after_response,
                            uint64_t *response_barrier,
                            reactor_request_token_t request_token,
                            void *context)
{
    app_context_t *app = context;
    kvstore_service_t *service = app->service;
    resp_request_t request;
    kvstore_argument_t arguments[RESP_MAX_ARGUMENTS];
    kvstore_reply_t reply;
    size_t index;
    int parse_result;
    int mysql_key_write = 0;
    uint64_t mysql_sequence_before = service->mysql_next_sequence;


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
    if (app->mysql != NULL && request.argument_count >= 2U) {
        const kvstore_argument_t *command = &arguments[0];
        int key_command = 0;
        int set_command = 0;
        int write_command = 0;
        int negative_hit = 0;

#define COMMAND_IS(literal) \
        (command->length == sizeof(literal) - 1U && \
         strncasecmp((const char *)command->data, literal, \
                     sizeof(literal) - 1U) == 0)
        set_command = COMMAND_IS("SET");
        write_command = set_command || COMMAND_IS("DEL") ||
                        COMMAND_IS("EXPIRE") || COMMAND_IS("PEXPIRE") ||
                        COMMAND_IS("PERSIST") || COMMAND_IS("HSET") ||
                        COMMAND_IS("HDEL") || COMMAND_IS("ZADD") ||
                        COMMAND_IS("ZREM");
        mysql_key_write = write_command;
        key_command = set_command || COMMAND_IS("GET") || COMMAND_IS("DEL") ||
                      COMMAND_IS("EXPIRE") || COMMAND_IS("PEXPIRE") ||
                      COMMAND_IS("TTL") || COMMAND_IS("PTTL") ||
                      COMMAND_IS("PERSIST") || COMMAND_IS("HSET") ||
                      COMMAND_IS("HGET") || COMMAND_IS("HDEL") ||
                      COMMAND_IS("HLEN") || COMMAND_IS("HGETALL") ||
                      COMMAND_IS("ZADD") || COMMAND_IS("ZREM") ||
                      COMMAND_IS("ZSCORE") || COMMAND_IS("ZCARD") ||
                      COMMAND_IS("ZRANGE");
#undef COMMAND_IS
        negative_hit = key_command &&
                       negative_cache_contains(app, arguments[1].data,
                                               arguments[1].length);
        if (write_command)
            negative_cache_invalidate(app, arguments[1].data,
                                      arguments[1].length);
        if (negative_hit) mysql_store_note_negative_hit(app->mysql);
        if (key_command && !set_command &&
            !negative_hit &&
            cache_get_object(service->cache,
                             arguments[1].data,
                             arguments[1].length,
                             0) == NULL) {
            mysql_pending_request_t *pending = pending_request_copy(
                app, request_token, arguments, request.argument_count);
            if (pending == NULL) return REACTOR_HANDLER_ERROR;
            if (dirty_key_sequence(app, arguments[1].data,
                                   arguments[1].length) != 0) {
                queue_dirty_waiter(app, pending);
                return REACTOR_HANDLER_DEFERRED;
            }
            if (register_pending_load(app, pending) != 0 ||
                mysql_store_submit_load(app->mysql,
                                        arguments[1].data,
                                        arguments[1].length,
                                        pending) < 0) {
                static const unsigned char busy[] =
                    "TRYAGAIN MySQL read queue is full";
                unregister_pending_load(pending);
                free(pending->payload);
                free(pending);
                memset(&reply, 0, sizeof(reply));
                reply.type = KVSTORE_REPLY_ERROR;
                reply.data = busy;
                reply.length = sizeof(busy) - 1U;
                if (encode_reply(&reply, response) != 0)
                    return REACTOR_HANDLER_ERROR;
                return REACTOR_HANDLER_COMPLETE;
            }
            return REACTOR_HANDLER_DEFERRED;
        }
    }
    if (kvstore_service_execute_with_barrier(service,
                                             arguments,
                                             request.argument_count,
                                             &reply,
                                             response_barrier) != 0) {
        return REACTOR_HANDLER_ERROR;
    }
    if (mysql_key_write && service->mysql_next_sequence > mysql_sequence_before &&
        mark_dirty_key(app, arguments[1].data, arguments[1].length,
                       service->mysql_next_sequence) != 0)
        return REACTOR_HANDLER_ERROR;
    if (encode_reply(&reply, response) != 0) return REACTOR_HANDLER_ERROR;
    return REACTOR_HANDLER_COMPLETE;
}

static int fill_random_bytes(unsigned char *data, size_t length)
{
    size_t offset = 0;

    while (offset < length) {
        ssize_t received = getrandom(data + offset, length - offset, 0);
        if (received > 0) {
            offset += (size_t)received;
            continue;
        }
        if (received < 0 && errno == EINTR) continue;
        return -1;
    }
    return 0;
}

typedef struct mysql_aof_scan {
    mysql_store_t *store;
    mysql_store_argument_t previous[RESP_MAX_ARGUMENTS];
    size_t previous_count;
    unsigned char writer_uuid[16];
    uint64_t maximum_sequence;
    uint64_t database_sequence;
    uint64_t recovered_mutations;
    int has_writer_uuid;
    int recover;
} mysql_aof_scan_t;

static unsigned char app_ascii_upper(unsigned char value)
{
    return value >= 'a' && value <= 'z'
               ? (unsigned char)(value - ('a' - 'A')) : value;
}

static int app_argument_equals(const void *data, size_t length,
                               const char *literal)
{
    size_t expected = strlen(literal);
    size_t index;

    if (length != expected) return 0;
    for (index = 0; index < length; ++index) {
        if (app_ascii_upper(((const unsigned char *)data)[index]) !=
            (unsigned char)literal[index]) return 0;
    }
    return 1;
}

static int mysql_aof_mutation(const aof_argument_t *arguments,
                              size_t argument_count)
{
    if (argument_count < 2U) return 0;
    return app_argument_equals(arguments[0].data, arguments[0].length, "SET") ||
           app_argument_equals(arguments[0].data, arguments[0].length, "DEL") ||
           app_argument_equals(arguments[0].data, arguments[0].length, "HSET") ||
           app_argument_equals(arguments[0].data, arguments[0].length, "HDEL") ||
           app_argument_equals(arguments[0].data, arguments[0].length, "ZADD") ||
           app_argument_equals(arguments[0].data, arguments[0].length, "ZREM") ||
           app_argument_equals(arguments[0].data, arguments[0].length, "PEXPIREAT") ||
           app_argument_equals(arguments[0].data, arguments[0].length, "PERSIST");
}

static int pending_request_is_write(const mysql_pending_request_t *pending)
{
    const kvstore_argument_t *command;

    if (pending == NULL || pending->argument_count < 2U) return 0;
    command = &pending->arguments[0];
    return app_argument_equals(command->data, command->length, "SET") ||
           app_argument_equals(command->data, command->length, "DEL") ||
           app_argument_equals(command->data, command->length, "EXPIRE") ||
           app_argument_equals(command->data, command->length, "PEXPIRE") ||
           app_argument_equals(command->data, command->length, "PERSIST") ||
           app_argument_equals(command->data, command->length, "HSET") ||
           app_argument_equals(command->data, command->length, "HDEL") ||
           app_argument_equals(command->data, command->length, "ZADD") ||
           app_argument_equals(command->data, command->length, "ZREM");
}

static int decode_hex_digit(unsigned char value)
{
    if (value >= '0' && value <= '9') return (int)(value - '0');
    if (value >= 'a' && value <= 'f') return (int)(value - 'a') + 10;
    if (value >= 'A' && value <= 'F') return (int)(value - 'A') + 10;
    return -1;
}

static int parse_mysql_marker(const aof_argument_t *arguments,
                              size_t argument_count,
                              unsigned char uuid[16],
                              uint64_t *sequence)
{
    const unsigned char *text;
    size_t length;
    size_t position;
    uint64_t value = 0;

    if (argument_count != 2U ||
        !app_argument_equals(arguments[0].data, arguments[0].length, "PING"))
        return 0;
    text = arguments[1].data;
    length = arguments[1].length;
    if (length < 43U || memcmp(text, "KVMYSQL1:", 9U) != 0 ||
        text[41] != ':') return 0;
    for (position = 0; position < 16U; ++position) {
        int high = decode_hex_digit(text[9U + position * 2U]);
        int low = decode_hex_digit(text[10U + position * 2U]);
        if (high < 0 || low < 0) return -1;
        uuid[position] = (unsigned char)((high << 4) | low);
    }
    for (position = 42U; position < length; ++position) {
        unsigned int digit;
        if (text[position] < '0' || text[position] > '9') return -1;
        digit = (unsigned int)(text[position] - '0');
        if (value > (UINT64_MAX - digit) / 10U) return -1;
        value = value * 10U + digit;
    }
    if (value == 0) return -1;
    *sequence = value;
    return 1;
}

static int scan_mysql_aof(const aof_argument_t *arguments,
                          size_t argument_count,
                          void *context)
{
    mysql_aof_scan_t *scan = context;
    unsigned char uuid[16];
    uint64_t sequence;
    int marker = parse_mysql_marker(arguments, argument_count, uuid, &sequence);

    if (marker < 0) return -1;
    if (marker > 0) {
        if (scan->previous_count == 0 ||
            (scan->has_writer_uuid &&
             memcmp(scan->writer_uuid, uuid, sizeof(uuid)) != 0) ||
            sequence <= scan->maximum_sequence) return -1;
        if (!scan->has_writer_uuid) {
            memcpy(scan->writer_uuid, uuid, sizeof(uuid));
            scan->has_writer_uuid = 1;
        }
        scan->maximum_sequence = sequence;
        if (scan->recover && sequence > scan->database_sequence) {
            if (mysql_store_recover_mutation(scan->store, sequence,
                                             scan->previous,
                                             scan->previous_count) != 0)
                return -1;
            scan->database_sequence = sequence;
            scan->recovered_mutations++;
        }
        scan->previous_count = 0;
        return 0;
    }
    scan->previous_count = 0;
    if (mysql_aof_mutation(arguments, argument_count)) {
        if (argument_count > RESP_MAX_ARGUMENTS) return -1;
        for (size_t index = 0; index < argument_count; ++index) {
            scan->previous[index].data = arguments[index].data;
            scan->previous[index].length = arguments[index].length;
        }
        scan->previous_count = argument_count;
    }
    return 0;
}

typedef struct mysql_bootstrap_context {
    mysql_store_t *store;
    uint64_t objects;
} mysql_bootstrap_context_t;

static int bootstrap_mysql_object(const void *key, size_t key_length,
                                  kv_object_t *object,
                                  uint64_t expire_at_ms, void *context)
{
    mysql_bootstrap_context_t *bootstrap = context;
    if (mysql_store_bootstrap_object(bootstrap->store, key, key_length,
                                     object, expire_at_ms) != 0) return -1;
    bootstrap->objects++;
    return 0;
}

static int invalidate_snapshot_key(const void *key, size_t key_length,
                                   void *context)
{
    cache_t *cache = context;
    return cache_delete(cache, key, key_length) < 0 ? -1 : 0;
}

static void pending_request_destroy(mysql_pending_request_t *pending)
{
    if (pending == NULL) return;
    unregister_pending_load(pending);
    free(pending->payload);
    free(pending);
}

static void pending_context_destroy(void *context)
{
    pending_request_destroy(context);
}

static int complete_pending_error(app_context_t *app,
                                  mysql_pending_request_t *pending,
                                  const unsigned char *message,
                                  size_t message_length)
{
    kvstore_reply_t reply;
    net_buffer_t encoded;
    int result;

    memset(&reply, 0, sizeof(reply));
    reply.type = KVSTORE_REPLY_ERROR;
    reply.data = message;
    reply.length = message_length;
    if (net_buffer_init(&encoded, 128U) != 0) return -1;
    if (encode_reply(&reply, &encoded) != 0) {
        net_buffer_destroy(&encoded);
        return -1;
    }
    result = reactor_complete_response(app->reactor,
                                       pending->token,
                                       encoded.data + encoded.read_pos,
                                       net_buffer_readable(&encoded),
                                       0,
                                       0);
    net_buffer_destroy(&encoded);
    return result < 0 ? -1 : 0;
}

static int complete_pending_request(app_context_t *app,
                                    mysql_pending_request_t *pending)
{
    kvstore_reply_t reply;
    net_buffer_t encoded;
    uint64_t barrier = 0;
    uint64_t sequence_before = app->service->mysql_next_sequence;
    int result;

    if (pending_request_is_write(pending)) {
        negative_cache_invalidate(app,
                                  pending->arguments[1].data,
                                  pending->arguments[1].length);
    }
    if (net_buffer_init(&encoded, 256U) != 0) return -1;
    if (kvstore_service_execute_with_barrier(app->service,
                                             pending->arguments,
                                             pending->argument_count,
                                             &reply,
                                             &barrier) != 0) {
        net_buffer_destroy(&encoded);
        return -1;
    }
    if (pending->argument_count >= 2U &&
        app->service->mysql_next_sequence > sequence_before &&
        mark_dirty_key(app, pending->arguments[1].data,
                       pending->arguments[1].length,
                       app->service->mysql_next_sequence) != 0) {
        net_buffer_destroy(&encoded);
        return -1;
    }
    if (encode_reply(&reply, &encoded) != 0) {
        net_buffer_destroy(&encoded);
        return -1;
    }
    result = reactor_complete_response(app->reactor,
                                       pending->token,
                                       encoded.data + encoded.read_pos,
                                       net_buffer_readable(&encoded),
                                       0,
                                       barrier);
    net_buffer_destroy(&encoded);
    return result < 0 ? -1 : 0;
}

static int drain_dirty_waiters(app_context_t *app)
{
    static const unsigned char busy[] = "TRYAGAIN MySQL read queue is full";
    mysql_pending_request_t **cursor = &app->dirty_wait_head;
    mysql_pending_request_t *last = NULL;

    while (*cursor != NULL) {
        mysql_pending_request_t *pending = *cursor;
        const void *key = pending->arguments[1].data;
        size_t key_length = pending->arguments[1].length;
        int result;

        if (dirty_key_sequence(app, key, key_length) != 0) {
            last = pending;
            cursor = &pending->dirty_next;
            continue;
        }
        *cursor = pending->dirty_next;
        pending->dirty_next = NULL;
        if (cache_get_object(app->service->cache, key, key_length, 0) != NULL) {
            result = complete_pending_request(app, pending);
            pending_request_destroy(pending);
        } else if (register_pending_load(app, pending) != 0 ||
                   mysql_store_submit_load(app->mysql, key, key_length,
                                           pending) < 0) {
            unregister_pending_load(pending);
            result = complete_pending_error(app, pending, busy,
                                            sizeof(busy) - 1U);
            pending_request_destroy(pending);
        } else {
            result = 0;
        }
        if (result != 0) return -1;
    }
    app->dirty_wait_tail = last;
    return 0;
}

static int drain_mysql_completions(void *context)
{
    static const unsigned char unavailable[] = "ERR MySQL backend unavailable";
    static const unsigned char too_large[] = "ERR MySQL object exceeds load limit";
    static const unsigned char internal[] = "ERR MySQL backend data error";
    static const unsigned char busy[] = "TRYAGAIN MySQL read queue is full";
    app_context_t *app = context;
    mysql_store_load_result_t *results;
    mysql_store_load_result_t *result;

    if (drain_dirty_waiters(app) != 0) return -1;
    purge_applied_dirty_keys(app, 1024U);
    results = mysql_store_take_load_results(app->mysql);

    for (result = results; result != NULL; result = result->next) {
        mysql_store_waiter_t *waiter;
        int store_result = CACHE_SET_OK;
        int stale = 0;

        for (waiter = result->waiters; waiter != NULL; waiter = waiter->next) {
            if (pending_load_is_stale(waiter->context)) {
                stale = 1;
                break;
            }
        }
        if (stale) {
            for (waiter = result->waiters; waiter != NULL; waiter = waiter->next) {
                mysql_pending_request_t *pending = waiter->context;
                const void *key = pending->arguments[1].data;
                size_t key_length = pending->arguments[1].length;
                int completion_result = 0;

                unregister_pending_load(pending);
                if (cache_get_object(app->service->cache, key, key_length, 0) != NULL) {
                    completion_result = complete_pending_request(app, pending);
                    pending_request_destroy(pending);
                } else if (dirty_key_sequence(app, key, key_length) != 0) {
                    queue_dirty_waiter(app, pending);
                } else if (register_pending_load(app, pending) != 0 ||
                           mysql_store_submit_load(app->mysql, key, key_length,
                                                   pending) < 0) {
                    unregister_pending_load(pending);
                    completion_result = complete_pending_error(
                        app, pending, busy, sizeof(busy) - 1U);
                    pending_request_destroy(pending);
                }
                if (completion_result != 0) {
                    mysql_store_release_load_result(results);
                    return -1;
                }
            }
            continue;
        }

        if (result->status == MYSQL_STORE_OK && result->found) {
            mysql_pending_request_t *first = result->waiters == NULL
                                                 ? NULL
                                                 : result->waiters->context;
            if (first == NULL) {
                store_result = CACHE_SET_ERROR;
            } else {
                store_result = cache_store_object(app->service->cache,
                    first->arguments[1].data, first->arguments[1].length,
                    result->object, result->expire_at_ms, 0);
                if (store_result == CACHE_SET_OK) result->object = NULL;
            }
        } else if (result->status == MYSQL_STORE_OK && !result->found &&
                   result->waiters != NULL) {
            mysql_pending_request_t *first = result->waiters->context;
            if (first != NULL)
                (void)negative_cache_add(app, first->arguments[1].data,
                                         first->arguments[1].length);
        }
        for (waiter = result->waiters; waiter != NULL; waiter = waiter->next) {
            mysql_pending_request_t *pending = waiter->context;
            int completion_result;

            unregister_pending_load(pending);

            if (result->status == MYSQL_STORE_TOO_LARGE ||
                store_result == CACHE_SET_CAPACITY) {
                completion_result = complete_pending_error(
                    app, pending, too_large, sizeof(too_large) - 1U);
            } else if (result->status == MYSQL_STORE_UNAVAILABLE) {
                completion_result = complete_pending_error(
                    app, pending, unavailable, sizeof(unavailable) - 1U);
            } else if (result->status != MYSQL_STORE_OK ||
                       store_result != CACHE_SET_OK) {
                completion_result = complete_pending_error(
                    app, pending, internal, sizeof(internal) - 1U);
            } else {
                completion_result = complete_pending_request(app, pending);
            }
            pending_request_destroy(pending);
            if (completion_result != 0) {
                mysql_store_release_load_result(results);
                return -1;
            }
        }
    }
    mysql_store_release_load_result(results);
    return 0;
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

    purge_applied_dirty_keys(manager->app, 1024U);
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
            "          [--mysql yes|no] [--mysql-host HOST] [--mysql-port PORT]\n"
            "          [--mysql-user USER] [--mysql-database DATABASE]\n"
            "          [--mysql-read-workers COUNT]\n"
            "          [--mysql-connect-timeout SECONDS]\n"
            "          [--mysql-io-timeout SECONDS]\n"
            "          [--mysql-read-queue COUNT]\n"
            "          [--mysql-write-queue SIZE] [--mysql-load-max SIZE]\n"
            "          [--mysql-negative-ttl-ms MILLISECONDS]\n"
            "          [--mysql-negative-capacity COUNT]\n"
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
    options->mysql_host = "127.0.0.1";
    options->mysql_port = 3306U;
    options->mysql_user = "kvstore";
    options->mysql_database = "kvstore";
    options->mysql_read_workers = 8U;
    options->mysql_connect_timeout = 2U;
    options->mysql_io_timeout = 2U;
    options->mysql_read_queue_limit = 4096U;
    options->mysql_write_queue_bytes = 64U * 1024U * 1024U;
    options->mysql_load_max_bytes = 64U * 1024U * 1024U;
    options->mysql_negative_ttl_ms = 3000U;
    options->mysql_negative_capacity = 10000U;
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
        } else if (strcmp(argv[index], "--mysql") == 0) {
            if (suffix_equals(argv[index + 1], "yes"))
                options->mysql_enabled = 1;
            else if (suffix_equals(argv[index + 1], "no"))
                options->mysql_enabled = 0;
            else {
                print_usage(argv[0]);
                return -1;
            }
        } else if (strcmp(argv[index], "--mysql-host") == 0 &&
                   argv[index + 1][0] != '\0') {
            options->mysql_host = argv[index + 1];
        } else if (strcmp(argv[index], "--mysql-user") == 0 &&
                   argv[index + 1][0] != '\0') {
            options->mysql_user = argv[index + 1];
        } else if (strcmp(argv[index], "--mysql-database") == 0 &&
                   argv[index + 1][0] != '\0') {
            options->mysql_database = argv[index + 1];
        } else if (strcmp(argv[index], "--mysql-port") == 0) {
            size_t parsed;
            if (parse_size(argv[index + 1], 0, &parsed) != 0 ||
                parsed == 0 || parsed > 65535U) {
                print_usage(argv[0]);
                return -1;
            }
            options->mysql_port = (unsigned int)parsed;
        } else if (strcmp(argv[index], "--mysql-read-workers") == 0) {
            if (parse_size(argv[index + 1], 0,
                           &options->mysql_read_workers) != 0 ||
                options->mysql_read_workers == 0 ||
                options->mysql_read_workers > 64U) {
                print_usage(argv[0]);
                return -1;
            }
        } else if (strcmp(argv[index], "--mysql-connect-timeout") == 0) {
            size_t parsed;
            if (parse_size(argv[index + 1], 0, &parsed) != 0 ||
                parsed == 0 || parsed > UINT_MAX) {
                print_usage(argv[0]);
                return -1;
            }
            options->mysql_connect_timeout = (unsigned int)parsed;
        } else if (strcmp(argv[index], "--mysql-io-timeout") == 0) {
            size_t parsed;
            if (parse_size(argv[index + 1], 0, &parsed) != 0 ||
                parsed == 0 || parsed > UINT_MAX) {
                print_usage(argv[0]);
                return -1;
            }
            options->mysql_io_timeout = (unsigned int)parsed;
        } else if (strcmp(argv[index], "--mysql-read-queue") == 0) {
            if (parse_size(argv[index + 1], 0,
                           &options->mysql_read_queue_limit) != 0 ||
                options->mysql_read_queue_limit == 0) {
                print_usage(argv[0]);
                return -1;
            }
        } else if (strcmp(argv[index], "--mysql-write-queue") == 0) {
            if (parse_size(argv[index + 1], 1,
                           &options->mysql_write_queue_bytes) != 0 ||
                options->mysql_write_queue_bytes == 0) {
                print_usage(argv[0]);
                return -1;
            }
        } else if (strcmp(argv[index], "--mysql-load-max") == 0) {
            if (parse_size(argv[index + 1], 1,
                           &options->mysql_load_max_bytes) != 0 ||
                options->mysql_load_max_bytes == 0) {
                print_usage(argv[0]);
                return -1;
            }
        } else if (strcmp(argv[index], "--mysql-negative-ttl-ms") == 0) {
            size_t parsed;
            if (parse_size(argv[index + 1], 0, &parsed) != 0) {
                print_usage(argv[0]);
                return -1;
            }
            options->mysql_negative_ttl_ms = (uint64_t)parsed;
        } else if (strcmp(argv[index], "--mysql-negative-capacity") == 0) {
            if (parse_size(argv[index + 1], 0,
                           &options->mysql_negative_capacity) != 0) {
                print_usage(argv[0]);
                return -1;
            }
        } else {
            print_usage(argv[0]);
            return -1;
        }
    }
    if ((options->save_seconds == 0) != (options->save_changes == 0) ||
        (!options->rdb_enabled &&
         (options->save_seconds != 0 || options->save_changes != 0)) ||
        (options->mysql_enabled && !options->appendonly)) {
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
    mysql_store_config_t mysql_config;
    mysql_store_t *mysql_store = NULL;
    unsigned char mysql_writer_uuid[16];
    unsigned char mysql_active_writer_uuid[16];
    mysql_aof_scan_t mysql_scan;
    mysql_store_bootstrap_state_t mysql_bootstrap_state;
    uint64_t mysql_database_sequence = 0;
    uint64_t mysql_start_sequence = 0;
    int mysql_has_active_writer = 0;
    app_context_t app;
    uint64_t replay_offset = 0;
    int rdb_loaded = 0;
    double replay_duration_seconds = 0.0;
    int parse_result;
    int result = 1;
    int mysql_library_initialized = 0;

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
    memset(&app, 0, sizeof(app));
    app.service = &service;
    memset(&persistence, 0, sizeof(persistence));
    persistence.service = &service;
    persistence.app = &app;
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
    memset(&mysql_scan, 0, sizeof(mysql_scan));
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
    if (options.mysql_enabled) {
        const char *password = getenv("KVSTORE_MYSQL_PASSWORD");
        aof_replay_stats_t mysql_scan_stats;

        if (password == NULL) {
            fprintf(stderr, "KVSTORE_MYSQL_PASSWORD is required when MySQL is enabled\n");
            goto cleanup_aof;
        }
        memset(&mysql_scan_stats, 0, sizeof(mysql_scan_stats));
        if (aof_replay(aof, scan_mysql_aof, &mysql_scan,
                       &mysql_scan_stats) != 0) {
            perror("scan MySQL AOF outbox");
            goto cleanup_aof;
        }
        if (mysql_scan.has_writer_uuid) {
            memcpy(mysql_writer_uuid, mysql_scan.writer_uuid,
                   sizeof(mysql_writer_uuid));
        } else if (fill_random_bytes(mysql_writer_uuid,
                                     sizeof(mysql_writer_uuid)) != 0) {
            perror("getrandom mysql writer uuid");
            goto cleanup_aof;
        }
        if (mysql_store_library_init() != 0) {
            perror("mysql library initialization");
            goto cleanup_aof;
        }
        mysql_library_initialized = 1;
        memset(&mysql_config, 0, sizeof(mysql_config));
        mysql_config.host = options.mysql_host;
        mysql_config.port = options.mysql_port;
        mysql_config.user = options.mysql_user;
        mysql_config.password = password;
        mysql_config.database = options.mysql_database;
        mysql_config.read_workers = options.mysql_read_workers;
        mysql_config.connect_timeout_seconds = options.mysql_connect_timeout;
        mysql_config.io_timeout_seconds = options.mysql_io_timeout;
        mysql_config.read_queue_limit = options.mysql_read_queue_limit;
        mysql_config.write_queue_max_bytes = options.mysql_write_queue_bytes;
        mysql_config.load_max_bytes = options.mysql_load_max_bytes;
        mysql_config.zset_engine = options.zset_engine;
        mysql_config.notify_fd = -1;
        mysql_config.destroy_waiter_context = pending_context_destroy;
        memcpy(mysql_config.writer_uuid, mysql_writer_uuid,
               sizeof(mysql_config.writer_uuid));
        if (mysql_store_open(&mysql_store, &mysql_config) != 0 ||
            mysql_store_check_schema(mysql_store) != 0 ||
            mysql_store_check_permissions(mysql_store) != 0 ||
            mysql_store_get_startup_state(mysql_store,
                                          &mysql_bootstrap_state,
                                          mysql_active_writer_uuid,
                                          &mysql_has_active_writer,
                                          &mysql_database_sequence) != 0) {
            perror("mysql backend initialization");
            goto cleanup_mysql;
        }
        if (mysql_has_active_writer) {
            if (mysql_scan.has_writer_uuid &&
                memcmp(mysql_writer_uuid, mysql_active_writer_uuid,
                       sizeof(mysql_writer_uuid)) != 0) {
                errno = EPROTO;
                perror("MySQL writer UUID does not match AOF outbox");
                goto cleanup_mysql;
            }
            if (!mysql_scan.has_writer_uuid) {
                memcpy(mysql_writer_uuid, mysql_active_writer_uuid,
                       sizeof(mysql_writer_uuid));
                memcpy(mysql_scan.writer_uuid, mysql_writer_uuid,
                       sizeof(mysql_writer_uuid));
                mysql_scan.has_writer_uuid = 1;
                if (mysql_store_set_writer_uuid(mysql_store,
                                                mysql_writer_uuid) != 0) {
                    perror("adopt MySQL writer UUID");
                    goto cleanup_mysql;
                }
            }
        }
        if (mysql_bootstrap_state == MYSQL_STORE_BOOTSTRAP_EMPTY ||
            mysql_bootstrap_state == MYSQL_STORE_BOOTSTRAP_IN_PROGRESS) {
            mysql_bootstrap_context_t bootstrap;

            memset(&bootstrap, 0, sizeof(bootstrap));
            bootstrap.store = mysql_store;
            if (mysql_store_bootstrap_begin(mysql_store) != 0 ||
                cache_visit(service.cache, bootstrap_mysql_object,
                            &bootstrap) != 0 ||
                mysql_store_bootstrap_finish(mysql_store,
                                             mysql_scan.maximum_sequence) != 0) {
                perror("bootstrap MySQL from RDB/AOF snapshot");
                goto cleanup_mysql;
            }
            mysql_database_sequence = mysql_scan.maximum_sequence;
            fprintf(stderr, "mysql_bootstrap_objects: %" PRIu64 "\n",
                    bootstrap.objects);
        } else {
            if (!mysql_has_active_writer) {
                errno = EPROTO;
                perror("READY MySQL schema has no active writer UUID");
                goto cleanup_mysql;
            }
            if (mysql_database_sequence < mysql_scan.maximum_sequence) {
                aof_replay_stats_t recovery_stats;

                memset(&recovery_stats, 0, sizeof(recovery_stats));
                mysql_scan.store = mysql_store;
                mysql_scan.database_sequence = mysql_database_sequence;
                mysql_scan.maximum_sequence = 0;
                mysql_scan.previous_count = 0;
                mysql_scan.recover = 1;
                if (aof_replay(aof, scan_mysql_aof, &mysql_scan,
                               &recovery_stats) != 0) {
                    perror("recover MySQL mutations from AOF");
                    goto cleanup_mysql;
                }
                mysql_database_sequence = mysql_scan.database_sequence;
                fprintf(stderr, "mysql_recovered_mutations: %" PRIu64 "\n",
                        mysql_scan.recovered_mutations);
            } else if (mysql_database_sequence > mysql_scan.maximum_sequence &&
                       mysql_store_visit_changes_after(
                           mysql_store, mysql_scan.maximum_sequence,
                           invalidate_snapshot_key, service.cache) != 0) {
                perror("invalidate snapshot keys from MySQL change log");
                goto cleanup_mysql;
            }
        }
        mysql_start_sequence = mysql_database_sequence > mysql_scan.maximum_sequence
                                   ? mysql_database_sequence
                                   : mysql_scan.maximum_sequence;
        if (kv_hash_create(&app.negative_cache) != 0 ||
            kv_hash_create(&app.dirty_keys) != 0 ||
            kv_hash_create(&app.load_states) != 0) {
            perror("create MySQL key state indexes");
            goto cleanup_mysql;
        }
        app.negative_ttl_ms = options.mysql_negative_ttl_ms;
        app.negative_capacity = options.mysql_negative_capacity;
        kvstore_service_attach_mysql(&service, mysql_store,
                                     mysql_writer_uuid, mysql_start_sequence);
        app.mysql = mysql_store;
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
        goto cleanup_mysql;
    }
    if (reactor_init(&reactor,
                     (uint16_t)DEFAULT_PORT,
                     dispatch_request,
                     &app) != 0) {
        perror("reactor_init");
        goto cleanup_mysql;
    }
    persistence.reactor = reactor;
    app.reactor = reactor;
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
    if (mysql_store != NULL &&
        (mysql_store_set_notify_fd(mysql_store,
                                   reactor_wake_fd(reactor)) != 0 ||
         reactor_set_async_handler(reactor,
                                   drain_mysql_completions,
                                   &app) != 0)) {
        perror("mysql reactor completion handler");
        goto cleanup_reactor;
    }

    active_reactor = reactor;
    printf("storeSystem v0.7.0 RESP reactor listening on port %u "
           "(keyspace=hash, zset=%s, maxmemory=%zu, maxkeys=%zu, "
           "aof=%s, rdb=%s, mysql=%s, loaded=%zu, rdb_loaded=%d)\n",
           DEFAULT_PORT,
           kv_zset_engine_name(options.zset_engine),
           options.cache.max_memory,
           options.cache.max_keys,
           options.appendonly ? options.appendfilename : "off",
           options.rdb_enabled ? options.dbfilename : "off",
           options.mysql_enabled ? options.mysql_database : "off",
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
cleanup_mysql:
    kvstore_service_attach_mysql(&service, NULL, NULL, 0);
    app.mysql = NULL;
    if (mysql_store_close(mysql_store, 5000U) != 0) {
        perror("mysql_store_close");
        result = 1;
    }
    mysql_store = NULL;
    if (mysql_library_initialized) {
        mysql_store_library_end();
        mysql_library_initialized = 0;
    }
    kv_hash_release(app.negative_cache, negative_entry_destroy);
    app.negative_cache = NULL;
    kv_hash_release(app.dirty_keys, negative_entry_destroy);
    app.dirty_keys = NULL;
    while (app.dirty_wait_head != NULL) {
        mysql_pending_request_t *pending = app.dirty_wait_head;
        app.dirty_wait_head = pending->dirty_next;
        pending_request_destroy(pending);
    }
    app.dirty_wait_tail = NULL;
    kv_hash_release(app.load_states, negative_entry_destroy);
    app.load_states = NULL;
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
