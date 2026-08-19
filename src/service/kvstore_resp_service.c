#include "service/kvstore_service.h"

#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>

static const unsigned char reply_ok[] = "OK";
static const unsigned char reply_pong[] = "PONG";
static const unsigned char error_unknown[] = "ERR unknown command";
static const unsigned char error_arity[] = "ERR wrong number of arguments";
static const unsigned char error_internal[] = "ERR internal error";
static const unsigned char error_syntax[] = "ERR syntax error";
static const unsigned char error_integer[] =
    "ERR value is not an integer or out of range";
static const unsigned char error_expire[] = "ERR invalid expire time in SET";
static const unsigned char error_capacity[] = "ERR cache capacity exceeded";
static const unsigned char error_aof[] = "ERR AOF persistence unavailable";

enum service_command {
    SERVICE_COMMAND_SET,
    SERVICE_COMMAND_GET,
    SERVICE_COMMAND_DEL,
    SERVICE_COMMAND_PING,
    SERVICE_COMMAND_EXPIRE,
    SERVICE_COMMAND_PEXPIRE,
    SERVICE_COMMAND_TTL,
    SERVICE_COMMAND_PTTL,
    SERVICE_COMMAND_PERSIST,
    SERVICE_COMMAND_INFO,
    SERVICE_COMMAND_UNKNOWN
};

static unsigned char ascii_upper(unsigned char value)
{
    if (value >= 'a' && value <= 'z') {
        return (unsigned char)(value - ('a' - 'A'));
    }
    return value;
}

static int argument_equals(const kvstore_argument_t *argument, const char *name)
{
    size_t length = strlen(name);
    size_t index;

    if (argument->length != length) {
        return 0;
    }
    for (index = 0; index < length; ++index) {
        if (ascii_upper(argument->data[index]) != (unsigned char)name[index]) {
            return 0;
        }
    }
    return 1;
}

static enum service_command find_command(const kvstore_argument_t *argument)
{
    if (argument_equals(argument, "SET")) return SERVICE_COMMAND_SET;
    if (argument_equals(argument, "GET")) return SERVICE_COMMAND_GET;
    if (argument_equals(argument, "DEL")) return SERVICE_COMMAND_DEL;
    if (argument_equals(argument, "PING")) return SERVICE_COMMAND_PING;
    if (argument_equals(argument, "EXPIRE")) return SERVICE_COMMAND_EXPIRE;
    if (argument_equals(argument, "PEXPIRE")) return SERVICE_COMMAND_PEXPIRE;
    if (argument_equals(argument, "TTL")) return SERVICE_COMMAND_TTL;
    if (argument_equals(argument, "PTTL")) return SERVICE_COMMAND_PTTL;
    if (argument_equals(argument, "PERSIST")) return SERVICE_COMMAND_PERSIST;
    if (argument_equals(argument, "INFO")) return SERVICE_COMMAND_INFO;
    return SERVICE_COMMAND_UNKNOWN;
}

static int is_write_command(enum service_command command)
{
    return command == SERVICE_COMMAND_SET || command == SERVICE_COMMAND_DEL ||
           command == SERVICE_COMMAND_EXPIRE ||
           command == SERVICE_COMMAND_PEXPIRE ||
           command == SERVICE_COMMAND_PERSIST;
}

static void set_data_reply(kvstore_reply_t *reply,
                           kvstore_reply_type_t type,
                           const unsigned char *data,
                           size_t length)
{
    reply->type = type;
    reply->data = data;
    reply->length = length;
    reply->integer = 0;
}

static void set_error(kvstore_reply_t *reply,
                      const unsigned char *message,
                      size_t length)
{
    set_data_reply(reply, KVSTORE_REPLY_ERROR, message, length);
}

static void set_aof_error(kvstore_reply_t *reply)
{
    set_error(reply, error_aof, sizeof(error_aof) - 1U);
}

static void set_integer(kvstore_reply_t *reply, int64_t value)
{
    reply->type = KVSTORE_REPLY_INTEGER;
    reply->data = NULL;
    reply->length = 0;
    reply->integer = value;
}

static int parse_int64(const kvstore_argument_t *argument, int64_t *result)
{
    uint64_t magnitude = 0;
    uint64_t limit;
    size_t index = 0;
    int negative = 0;

    if (argument == NULL || result == NULL || argument->length == 0) {
        return -1;
    }
    if (argument->data[index] == '-') {
        negative = 1;
        index++;
        if (index == argument->length) {
            return -1;
        }
    }
    limit = negative ? (uint64_t)INT64_MAX + 1U : (uint64_t)INT64_MAX;
    for (; index < argument->length; ++index) {
        unsigned int digit;

        if (argument->data[index] < '0' || argument->data[index] > '9') {
            return -1;
        }
        digit = (unsigned int)(argument->data[index] - '0');
        if (magnitude > (limit - digit) / 10U) {
            return -1;
        }
        magnitude = magnitude * 10U + digit;
    }
    if (negative) {
        *result = magnitude == (uint64_t)INT64_MAX + 1U
                      ? INT64_MIN
                      : -(int64_t)magnitude;
    } else {
        *result = (int64_t)magnitude;
    }
    return 0;
}

static int parse_uint64_bytes(const unsigned char *data,
                              size_t length,
                              uint64_t *result)
{
    uint64_t value = 0;
    size_t index;

    if (data == NULL || result == NULL || length == 0) {
        return -1;
    }
    for (index = 0; index < length; ++index) {
        unsigned int digit;

        if (data[index] < '0' || data[index] > '9') {
            return -1;
        }
        digit = (unsigned int)(data[index] - '0');
        if (value > (UINT64_MAX - digit) / 10U) {
            return -1;
        }
        value = value * 10U + digit;
    }
    *result = value;
    return 0;
}

static aof_argument_t aof_argument(const void *data, size_t length)
{
    aof_argument_t result;

    result.data = data;
    result.length = length;
    return result;
}

static int append_key_command(kvstore_service_t *service,
                              const char *command,
                              const kvstore_argument_t *key)
{
    aof_argument_t arguments[2];

    if (service->aof == NULL) {
        return 0;
    }
    arguments[0] = aof_argument(command, strlen(command));
    arguments[1] = aof_argument(key->data, key->length);
    return aof_append(service->aof, arguments, 2U);
}

static int append_deadline_command(kvstore_service_t *service,
                                   const char *command,
                                   const kvstore_argument_t *key,
                                   uint64_t deadline)
{
    aof_argument_t arguments[3];
    char deadline_text[32];
    int length;

    if (service->aof == NULL) {
        return 0;
    }
    length = snprintf(deadline_text,
                      sizeof(deadline_text),
                      "%" PRIu64,
                      deadline);
    if (length < 0 || (size_t)length >= sizeof(deadline_text)) {
        return -1;
    }
    arguments[0] = aof_argument(command, strlen(command));
    arguments[1] = aof_argument(key->data, key->length);
    arguments[2] = aof_argument(deadline_text, (size_t)length);
    return aof_append(service->aof, arguments, 3U);
}

static int append_set_command(kvstore_service_t *service,
                              const kvstore_argument_t *arguments,
                              uint64_t deadline)
{
    aof_argument_t record[5];
    char deadline_text[32];
    size_t count = 3U;
    int length;

    if (service->aof == NULL) {
        return 0;
    }
    record[0] = aof_argument("SET", 3U);
    record[1] = aof_argument(arguments[1].data, arguments[1].length);
    record[2] = aof_argument(arguments[2].data, arguments[2].length);
    if (deadline != 0) {
        length = snprintf(deadline_text,
                          sizeof(deadline_text),
                          "%" PRIu64,
                          deadline);
        if (length < 0 || (size_t)length >= sizeof(deadline_text)) {
            return -1;
        }
        record[3] = aof_argument("PXAT", 4U);
        record[4] = aof_argument(deadline_text, (size_t)length);
        count = 5U;
    }
    return aof_append(service->aof, record, count);
}

static void record_eviction(const void *key,
                            size_t key_length,
                            void *context)
{
    kvstore_service_t *service = context;
    kvstore_argument_t argument;

    argument.data = key;
    argument.length = key_length;
    (void)append_key_command(service, "DEL", &argument);
}

static int duration_ms(const kvstore_argument_t *argument,
                       uint64_t multiplier,
                       int require_positive,
                       int64_t *parsed,
                       uint64_t *milliseconds)
{
    int64_t value;

    if (parse_int64(argument, &value) != 0) {
        return -1;
    }
    *parsed = value;
    if (value <= 0) {
        return require_positive ? -2 : 0;
    }
    if ((uint64_t)value > UINT64_MAX / multiplier) {
        return -1;
    }
    *milliseconds = (uint64_t)value * multiplier;
    return 0;
}

int kvstore_service_init(kvstore_service_t *service,
                         const cache_config_t *cache_config)
{
    if (service == NULL) {
        return -1;
    }
    memset(service, 0, sizeof(*service));
    if (cache_create(&service->cache, cache_config) != 0) {
        return -1;
    }
    cache_set_eviction_callback(service->cache, record_eviction, service);
    service->initialized = 1;
    return 0;
}

void kvstore_service_destroy(kvstore_service_t *service)
{
    if (service == NULL || !service->initialized) {
        return;
    }
    cache_destroy(service->cache);
    memset(service, 0, sizeof(*service));
}

int kvstore_service_maintain(kvstore_service_t *service)
{
    if (service == NULL || !service->initialized) {
        return -1;
    }
    (void)cache_maintain(service->cache, 64U, 16U);
    (void)aof_maintain(service->aof);
    return 0;
}

int kvstore_service_flush(kvstore_service_t *service)
{
    if (service == NULL || !service->initialized) {
        return -1;
    }
    if (aof_flush(service->aof) != 0 &&
        !service->aof_flush_failure_reported) {
        service->aof_flush_failure_reported = 1;
        return -1;
    }
    return 0;
}

void kvstore_service_attach_aof(kvstore_service_t *service, aof_t *aof)
{
    if (service != NULL && service->initialized) {
        service->aof = aof;
    }
}

static int execute_set(kvstore_service_t *service,
                       const kvstore_argument_t *arguments,
                       size_t argument_count,
                       kvstore_reply_t *reply)
{
    uint64_t ttl_ms = 0;
    uint64_t expire_at_ms = 0;
    uint64_t now;
    int result;

    if (argument_count == 5U) {
        uint64_t multiplier;
        int64_t parsed;
        int parse_result;

        if (argument_equals(&arguments[3], "EX")) {
            multiplier = 1000U;
        } else if (argument_equals(&arguments[3], "PX")) {
            multiplier = 1U;
        } else {
            set_error(reply, error_syntax, sizeof(error_syntax) - 1U);
            return 0;
        }
        parse_result = duration_ms(&arguments[4],
                                   multiplier,
                                   1,
                                   &parsed,
                                   &ttl_ms);
        if (parse_result == -2) {
            set_error(reply, error_expire, sizeof(error_expire) - 1U);
            return 0;
        }
        if (parse_result != 0) {
            set_error(reply, error_integer, sizeof(error_integer) - 1U);
            return 0;
        }
    }
    now = cache_current_time_ms(service->cache);
    if (ttl_ms != 0) {
        if (ttl_ms > UINT64_MAX - now) {
            set_error(reply, error_integer, sizeof(error_integer) - 1U);
            return 0;
        }
        expire_at_ms = now + ttl_ms;
    }
    result = cache_set_expire_at(service->cache,
                                 arguments[1].data,
                                 arguments[1].length,
                                 arguments[2].data,
                                 arguments[2].length,
                                 expire_at_ms);
    if (result == CACHE_SET_CAPACITY) {
        set_error(reply, error_capacity, sizeof(error_capacity) - 1U);
    } else if (result == CACHE_SET_RANGE) {
        set_error(reply, error_integer, sizeof(error_integer) - 1U);
    } else if (result != CACHE_SET_OK) {
        set_error(reply, error_internal, sizeof(error_internal) - 1U);
    } else if (append_set_command(service, arguments, expire_at_ms) != 0) {
        set_aof_error(reply);
    } else {
        set_data_reply(reply,
                       KVSTORE_REPLY_SIMPLE,
                       reply_ok,
                       sizeof(reply_ok) - 1U);
    }
    return 0;
}

static int execute_expire(kvstore_service_t *service,
                          const kvstore_argument_t *arguments,
                          uint64_t multiplier,
                          kvstore_reply_t *reply)
{
    int64_t parsed;
    uint64_t ttl_ms = 0;
    uint64_t deadline = 0;
    int parse_result;
    int result;

    parse_result = duration_ms(&arguments[2],
                               multiplier,
                               0,
                               &parsed,
                               &ttl_ms);
    if (parse_result != 0) {
        set_error(reply, error_integer, sizeof(error_integer) - 1U);
        return 0;
    }
    if (parsed <= 0) {
        result = cache_delete(service->cache,
                              arguments[1].data,
                              arguments[1].length);
    } else {
        uint64_t now = cache_current_time_ms(service->cache);

        if (ttl_ms > UINT64_MAX - now) {
            set_error(reply, error_integer, sizeof(error_integer) - 1U);
            return 0;
        }
        deadline = now + ttl_ms;
        result = cache_expire_at(service->cache,
                                 arguments[1].data,
                                 arguments[1].length,
                                 deadline);
    }
    if (result == -2) {
        set_error(reply, error_integer, sizeof(error_integer) - 1U);
    } else if (result < 0) {
        set_error(reply, error_internal, sizeof(error_internal) - 1U);
    } else if (result > 0 &&
               (parsed <= 0
                    ? append_key_command(service, "DEL", &arguments[1])
                    : append_deadline_command(service,
                                              "PEXPIREAT",
                                              &arguments[1],
                                              deadline)) != 0) {
        set_aof_error(reply);
    } else {
        set_integer(reply, result);
    }
    return 0;
}

static int execute_info(kvstore_service_t *service, kvstore_reply_t *reply)
{
    cache_stats_t stats;
    uint64_t requests;
    double hit_rate;
    int written;

    cache_get_stats(service->cache, &stats);
    requests = stats.hits + stats.misses;
    hit_rate = requests == 0 ? 0.0
                             : (double)stats.hits * 100.0 / (double)requests;
    written = snprintf((char *)service->info_buffer,
                       sizeof(service->info_buffer),
                       "keys:%zu\r\n"
                       "used_memory:%zu\r\n"
                       "index_memory:%zu\r\n"
                       "maxmemory:%zu\r\n"
                       "maxkeys:%zu\r\n"
                       "hits:%" PRIu64 "\r\n"
                       "misses:%" PRIu64 "\r\n"
                       "hit_rate:%.2f%%\r\n"
                       "expired_keys:%" PRIu64 "\r\n"
                       "evicted_keys:%" PRIu64 "\r\n"
                       "hash_slots:%zu\r\n"
                       "rehashing:%d\r\n",
                       stats.keys,
                       stats.used_memory,
                       stats.index_memory,
                       stats.max_memory,
                       stats.max_keys,
                       stats.hits,
                       stats.misses,
                       hit_rate,
                       stats.expired_keys,
                       stats.evicted_keys,
                       stats.hash_slots,
                       stats.rehashing);
    if (written < 0 || (size_t)written >= sizeof(service->info_buffer)) {
        set_error(reply, error_internal, sizeof(error_internal) - 1U);
    } else {
        set_data_reply(reply,
                       KVSTORE_REPLY_BULK,
                       service->info_buffer,
                       (size_t)written);
    }
    return 0;
}

int kvstore_service_execute(kvstore_service_t *service,
                            const kvstore_argument_t *arguments,
                            size_t argument_count,
                            kvstore_reply_t *reply)
{
    enum service_command command;

    if (service == NULL || !service->initialized || arguments == NULL ||
        argument_count == 0 || reply == NULL) {
        return -1;
    }
    memset(reply, 0, sizeof(*reply));
    command = find_command(&arguments[0]);
    if (command == SERVICE_COMMAND_UNKNOWN) {
        set_error(reply, error_unknown, sizeof(error_unknown) - 1U);
        return 0;
    }

    if ((command == SERVICE_COMMAND_SET &&
         argument_count != 3U && argument_count != 5U) ||
        ((command == SERVICE_COMMAND_GET || command == SERVICE_COMMAND_DEL ||
          command == SERVICE_COMMAND_TTL || command == SERVICE_COMMAND_PTTL ||
          command == SERVICE_COMMAND_PERSIST) &&
         argument_count != 2U) ||
        ((command == SERVICE_COMMAND_EXPIRE ||
          command == SERVICE_COMMAND_PEXPIRE) &&
         argument_count != 3U) ||
        (command == SERVICE_COMMAND_PING &&
         argument_count != 1U && argument_count != 2U) ||
        (command == SERVICE_COMMAND_INFO && argument_count != 2U)) {
        set_error(reply, error_arity, sizeof(error_arity) - 1U);
        return 0;
    }
    if (is_write_command(command) && aof_is_failed(service->aof)) {
        set_aof_error(reply);
        return 0;
    }

    switch (command) {
    case SERVICE_COMMAND_SET:
        return execute_set(service, arguments, argument_count, reply);
    case SERVICE_COMMAND_GET:
        reply->data = cache_get(service->cache,
                                arguments[1].data,
                                arguments[1].length,
                                &reply->length);
        reply->type = reply->data != NULL ? KVSTORE_REPLY_BULK
                                          : KVSTORE_REPLY_NULL_BULK;
        return 0;
    case SERVICE_COMMAND_DEL:
        {
            int deleted = cache_delete(service->cache,
                                       arguments[1].data,
                                       arguments[1].length);

            if (deleted < 0) {
                set_error(reply, error_internal, sizeof(error_internal) - 1U);
            } else if (deleted > 0 &&
                       append_key_command(service,
                                          "DEL",
                                          &arguments[1]) != 0) {
                set_aof_error(reply);
            } else {
                set_integer(reply, deleted);
            }
        }
        return 0;
    case SERVICE_COMMAND_PING:
        if (argument_count == 2U) {
            set_data_reply(reply,
                           KVSTORE_REPLY_BULK,
                           arguments[1].data,
                           arguments[1].length);
        } else {
            set_data_reply(reply,
                           KVSTORE_REPLY_SIMPLE,
                           reply_pong,
                           sizeof(reply_pong) - 1U);
        }
        return 0;
    case SERVICE_COMMAND_EXPIRE:
        return execute_expire(service, arguments, 1000U, reply);
    case SERVICE_COMMAND_PEXPIRE:
        return execute_expire(service, arguments, 1U, reply);
    case SERVICE_COMMAND_TTL:
    case SERVICE_COMMAND_PTTL:
        {
            int64_t ttl_ms;

            if (cache_ttl_ms(service->cache,
                             arguments[1].data,
                             arguments[1].length,
                             &ttl_ms) != 0) {
                set_error(reply, error_internal, sizeof(error_internal) - 1U);
            } else if (command == SERVICE_COMMAND_TTL && ttl_ms >= 0) {
                set_integer(reply, ttl_ms / 1000);
            } else {
                set_integer(reply, ttl_ms);
            }
        }
        return 0;
    case SERVICE_COMMAND_PERSIST:
        {
            int persisted = cache_persist(service->cache,
                                          arguments[1].data,
                                          arguments[1].length);

            if (persisted < 0) {
                set_error(reply, error_internal, sizeof(error_internal) - 1U);
            } else if (persisted > 0 &&
                       append_key_command(service,
                                          "PERSIST",
                                          &arguments[1]) != 0) {
                set_aof_error(reply);
            } else {
                set_integer(reply, persisted);
            }
        }
        return 0;
    case SERVICE_COMMAND_INFO:
        if (!argument_equals(&arguments[1], "CACHE")) {
            set_error(reply, error_syntax, sizeof(error_syntax) - 1U);
            return 0;
        }
        return execute_info(service, reply);
    default:
        return -1;
    }
}

static int replay_argument_equals(const aof_argument_t *argument,
                                  const char *name)
{
    kvstore_argument_t service_argument;

    service_argument.data = argument->data;
    service_argument.length = argument->length;
    return argument_equals(&service_argument, name);
}

int kvstore_service_replay_aof(const aof_argument_t *arguments,
                               size_t argument_count,
                               void *context)
{
    kvstore_service_t *service = context;
    uint64_t now;
    int result;

    if (service == NULL || !service->initialized || arguments == NULL ||
        argument_count == 0) {
        return -1;
    }
    now = cache_current_time_ms(service->cache);
    if (replay_argument_equals(&arguments[0], "SET")) {
        uint64_t deadline = 0;

        if (argument_count != 3U && argument_count != 5U) {
            return -1;
        }
        if (argument_count == 5U) {
            if (!replay_argument_equals(&arguments[3], "PXAT") ||
                parse_uint64_bytes(arguments[4].data,
                                   arguments[4].length,
                                   &deadline) != 0 ||
                deadline == 0) {
                return -1;
            }
        }
        if (deadline != 0 && deadline <= now) {
            result = cache_delete(service->cache,
                                  arguments[1].data,
                                  arguments[1].length);
            return result < 0 ? -1 : 0;
        }
        result = cache_set_expire_at(service->cache,
                                     arguments[1].data,
                                     arguments[1].length,
                                     arguments[2].data,
                                     arguments[2].length,
                                     deadline);
        return result == CACHE_SET_OK ? 0 : -1;
    }
    if (replay_argument_equals(&arguments[0], "DEL") &&
        argument_count == 2U) {
        result = cache_delete(service->cache,
                              arguments[1].data,
                              arguments[1].length);
        return result < 0 ? -1 : 0;
    }
    if (replay_argument_equals(&arguments[0], "PERSIST") &&
        argument_count == 2U) {
        result = cache_persist(service->cache,
                               arguments[1].data,
                               arguments[1].length);
        return result < 0 ? -1 : 0;
    }
    if (replay_argument_equals(&arguments[0], "PEXPIREAT") &&
        argument_count == 3U) {
        uint64_t deadline;

        if (parse_uint64_bytes(arguments[2].data,
                               arguments[2].length,
                               &deadline) != 0 ||
            deadline == 0) {
            return -1;
        }
        if (deadline <= now) {
            result = cache_delete(service->cache,
                                  arguments[1].data,
                                  arguments[1].length);
        } else {
            result = cache_expire_at(service->cache,
                                     arguments[1].data,
                                     arguments[1].length,
                                     deadline);
        }
        return result < 0 ? -1 : 0;
    }
    return -1;
}
