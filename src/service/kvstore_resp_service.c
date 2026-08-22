#include "service/kvstore_service.h"

#include "protocol/resp.h"

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
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
static const unsigned char error_mysql_queue[] =
    "TRYAGAIN MySQL writer queue is above the high-water mark";
static const unsigned char error_wrongtype[] =
    "WRONGTYPE Operation against a key holding the wrong kind of value";
static const unsigned char error_float[] = "ERR value is not a valid float";
static const unsigned char error_response_too_large[] =
    "ERR response exceeds 1 MiB limit";
static const unsigned char error_rdb_disabled[] = "ERR RDB persistence disabled";
static const unsigned char error_save_busy[] =
    "ERR Background save already in progress";
static const unsigned char reply_bgsave_started[] = "Background saving started";

#define SERVICE_MAX_RESPONSE (1024U * 1024U)
#define SERVICE_MAX_ARRAY_ELEMENTS 65536U

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
    SERVICE_COMMAND_DBSIZE,
    SERVICE_COMMAND_HSET,
    SERVICE_COMMAND_HGET,
    SERVICE_COMMAND_HDEL,
    SERVICE_COMMAND_HLEN,
    SERVICE_COMMAND_HGETALL,
    SERVICE_COMMAND_ZADD,
    SERVICE_COMMAND_ZREM,
    SERVICE_COMMAND_ZSCORE,
    SERVICE_COMMAND_ZCARD,
    SERVICE_COMMAND_ZRANGE,
    SERVICE_COMMAND_SAVE,
    SERVICE_COMMAND_BGSAVE,
    SERVICE_COMMAND_LASTSAVE,
    SERVICE_COMMAND_UNKNOWN
};

static unsigned char ascii_upper(unsigned char value)
{
    if (value >= 'a' && value <= 'z') {
        return (unsigned char)(value - ('a' - 'A'));
    }
    return value;
}

static int argument_equals_length(const kvstore_argument_t *argument,
                                  const char *name,
                                  size_t length)
{
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

#define argument_equals(argument, literal) \
    argument_equals_length((argument), (literal), sizeof(literal) - 1U)

static enum service_command find_command(const kvstore_argument_t *argument)
{
    switch (argument->length) {
    case 3U:
        if (argument_equals(argument, "SET")) return SERVICE_COMMAND_SET;
        if (argument_equals(argument, "GET")) return SERVICE_COMMAND_GET;
        if (argument_equals(argument, "DEL")) return SERVICE_COMMAND_DEL;
        if (argument_equals(argument, "TTL")) return SERVICE_COMMAND_TTL;
        break;
    case 4U:
        if (argument_equals(argument, "PING")) return SERVICE_COMMAND_PING;
        if (argument_equals(argument, "PTTL")) return SERVICE_COMMAND_PTTL;
        if (argument_equals(argument, "INFO")) return SERVICE_COMMAND_INFO;
        if (argument_equals(argument, "HSET")) return SERVICE_COMMAND_HSET;
        if (argument_equals(argument, "HGET")) return SERVICE_COMMAND_HGET;
        if (argument_equals(argument, "HDEL")) return SERVICE_COMMAND_HDEL;
        if (argument_equals(argument, "HLEN")) return SERVICE_COMMAND_HLEN;
        if (argument_equals(argument, "ZADD")) return SERVICE_COMMAND_ZADD;
        if (argument_equals(argument, "ZREM")) return SERVICE_COMMAND_ZREM;
        if (argument_equals(argument, "SAVE")) return SERVICE_COMMAND_SAVE;
        break;
    case 5U:
        if (argument_equals(argument, "ZCARD")) return SERVICE_COMMAND_ZCARD;
        break;
    case 6U:
        if (argument_equals(argument, "EXPIRE")) return SERVICE_COMMAND_EXPIRE;
        if (argument_equals(argument, "DBSIZE")) return SERVICE_COMMAND_DBSIZE;
        if (argument_equals(argument, "ZSCORE")) return SERVICE_COMMAND_ZSCORE;
        if (argument_equals(argument, "ZRANGE")) return SERVICE_COMMAND_ZRANGE;
        if (argument_equals(argument, "BGSAVE")) return SERVICE_COMMAND_BGSAVE;
        break;
    case 7U:
        if (argument_equals(argument, "PEXPIRE")) return SERVICE_COMMAND_PEXPIRE;
        if (argument_equals(argument, "PERSIST")) return SERVICE_COMMAND_PERSIST;
        if (argument_equals(argument, "HGETALL")) return SERVICE_COMMAND_HGETALL;
        break;
    case 8U:
        if (argument_equals(argument, "LASTSAVE")) return SERVICE_COMMAND_LASTSAVE;
        break;
    default:
        break;
    }
    return SERVICE_COMMAND_UNKNOWN;
}

static int is_write_command(enum service_command command)
{
    return command == SERVICE_COMMAND_SET || command == SERVICE_COMMAND_DEL ||
           command == SERVICE_COMMAND_EXPIRE ||
           command == SERVICE_COMMAND_PEXPIRE ||
           command == SERVICE_COMMAND_PERSIST ||
           command == SERVICE_COMMAND_HSET || command == SERVICE_COMMAND_HDEL ||
           command == SERVICE_COMMAND_ZADD || command == SERVICE_COMMAND_ZREM;
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

static void set_array(kvstore_reply_t *reply,
                      const kvstore_argument_t *elements,
                      size_t element_count)
{
    reply->type = KVSTORE_REPLY_ARRAY;
    reply->data = NULL;
    reply->length = 0;
    reply->integer = 0;
    reply->elements = elements;
    reply->element_count = element_count;
}

static int reserve_reply_elements(kvstore_service_t *service, size_t count)
{
    kvstore_argument_t *replacement;
    size_t capacity;

    if (count > SERVICE_MAX_ARRAY_ELEMENTS) return -2;
    if (count <= service->reply_element_capacity) return 0;
    capacity = service->reply_element_capacity == 0 ? 16U
                                                    : service->reply_element_capacity;
    while (capacity < count) {
        if (capacity > SERVICE_MAX_ARRAY_ELEMENTS / 2U) {
            capacity = SERVICE_MAX_ARRAY_ELEMENTS;
            break;
        }
        capacity *= 2U;
    }
    replacement = realloc(service->reply_elements,
                          capacity * sizeof(*replacement));
    if (replacement == NULL) return -1;
    service->reply_elements = replacement;
    service->reply_element_capacity = capacity;
    return 0;
}

static int reserve_score_buffer(kvstore_service_t *service, size_t bytes)
{
    unsigned char *replacement;
    size_t capacity;

    if (bytes <= service->reply_score_capacity) return 0;
    capacity = service->reply_score_capacity == 0 ? 256U
                                                  : service->reply_score_capacity;
    while (capacity < bytes) {
        if (capacity > SERVICE_MAX_RESPONSE / 2U) {
            capacity = SERVICE_MAX_RESPONSE;
            break;
        }
        capacity *= 2U;
    }
    if (capacity < bytes) return -2;
    replacement = realloc(service->reply_score_buffer, capacity);
    if (replacement == NULL) return -1;
    service->reply_score_buffer = replacement;
    service->reply_score_capacity = capacity;
    return 0;
}

static size_t decimal_digits(size_t value)
{
    size_t digits = 1U;

    while (value >= 10U) {
        value /= 10U;
        digits++;
    }
    return digits;
}

static size_t bulk_encoded_size(size_t length)
{
    if (length > SIZE_MAX - decimal_digits(length) - 5U) return SIZE_MAX;
    return length + decimal_digits(length) + 5U;
}

static int format_score(unsigned char *output, size_t capacity, double score)
{
    double scaled;

    if (output == NULL || capacity == 0) return -1;
    scaled = score * 10.0;
    if (isfinite(score) && !(score == 0.0 && signbit(score)) &&
        scaled >= (double)INT64_MIN && scaled <= (double)INT64_MAX) {
        int64_t tenths = (int64_t)scaled;

        if (scaled == (double)tenths && (double)tenths / 10.0 == score) {
            unsigned char reverse[32];
            uint64_t magnitude = tenths < 0
                                     ? (uint64_t)(-(tenths + 1)) + 1U
                                     : (uint64_t)tenths;
            unsigned int fraction = (unsigned int)(magnitude % 10U);
            uint64_t whole = magnitude / 10U;
            size_t digits = 0;
            size_t position = 0;
            size_t index;
            size_t needed;

            do {
                reverse[digits++] = (unsigned char)('0' + whole % 10U);
                whole /= 10U;
            } while (whole != 0);
            needed = digits + (tenths < 0 ? 1U : 0U) +
                     (fraction != 0 ? 2U : 0U);
            if (needed > capacity) return -1;
            if (tenths < 0) output[position++] = '-';
            for (index = 0; index < digits; ++index) {
                output[position++] = reverse[digits - index - 1U];
            }
            if (fraction != 0) {
                output[position++] = '.';
                output[position++] = (unsigned char)('0' + fraction);
            }
            return (int)position;
        }
    }
    return snprintf((char *)output, capacity, "%.17g", score);
}

static int parse_double_argument(const kvstore_argument_t *argument,
                                 double *result)
{
    char stack[128];
    char *text = stack;
    char *end;
    double value;

    if (argument == NULL || result == NULL || argument->length == 0 ||
        argument->length == SIZE_MAX) return -1;
    if (argument->length + 1U > sizeof(stack)) {
        text = malloc(argument->length + 1U);
        if (text == NULL) return -2;
    }
    memcpy(text, argument->data, argument->length);
    text[argument->length] = '\0';
    errno = 0;
    value = strtod(text, &end);
    if (text != stack && (end == NULL || (size_t)(end - text) != argument->length)) {
        free(text);
        return -1;
    }
    if (text == stack &&
        (end == NULL || (size_t)(end - text) != argument->length)) return -1;
    if (text != stack) free(text);
    if (errno == ERANGE || isnan(value)) return -1;
    *result = value;
    return 0;
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

static int capture_mysql_mutation(kvstore_service_t *service,
                                  const aof_argument_t *arguments,
                                  size_t argument_count)
{
    size_t required = 0;
    size_t capacity;
    unsigned char *replacement;

    if (!service->mysql_recording_command || service->mysql_store == NULL)
        return 0;
    if (argument_count == 0 || argument_count > 128U) return -1;
    for (size_t index = 0; index < argument_count; ++index) {
        if (arguments[index].length > SIZE_MAX - required) return -1;
        required += arguments[index].length;
    }
    if (required > service->mysql_mutation_buffer_capacity) {
        capacity = service->mysql_mutation_buffer_capacity == 0
                       ? 512U : service->mysql_mutation_buffer_capacity;
        while (capacity < required) {
            if (capacity > SIZE_MAX / 2U) {
                capacity = required;
                break;
            }
            capacity *= 2U;
        }
        replacement = realloc(service->mysql_mutation_buffer, capacity);
        if (replacement == NULL) return -1;
        service->mysql_mutation_buffer = replacement;
        service->mysql_mutation_buffer_capacity = capacity;
    }
    service->mysql_mutation_buffer_used = 0;
    service->mysql_mutation_count = argument_count;
    for (size_t index = 0; index < argument_count; ++index) {
        unsigned char *destination = service->mysql_mutation_buffer +
                                     service->mysql_mutation_buffer_used;
        memcpy(destination, arguments[index].data, arguments[index].length);
        service->mysql_mutation_arguments[index].data = destination;
        service->mysql_mutation_arguments[index].length = arguments[index].length;
        service->mysql_mutation_buffer_used += arguments[index].length;
    }
    return 0;
}

static int append_aof_record(kvstore_service_t *service,
                             const aof_argument_t *arguments,
                             size_t argument_count)
{
    if (aof_append(service->aof, arguments, argument_count) != 0) return -1;
    return capture_mysql_mutation(service, arguments, argument_count);
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
    return append_aof_record(service, arguments, 2U);
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
    return append_aof_record(service, arguments, 3U);
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
    return append_aof_record(service, record, count);
}

static int append_original_command(kvstore_service_t *service,
                                   const kvstore_argument_t *arguments,
                                   size_t argument_count)
{
    aof_argument_t record[RESP_MAX_ARGUMENTS];
    size_t index;

    if (service->aof == NULL) return 0;
    if (argument_count == 0 || argument_count > RESP_MAX_ARGUMENTS) return -1;
    for (index = 0; index < argument_count; ++index) {
        record[index] = aof_argument(arguments[index].data,
                                     arguments[index].length);
    }
    return append_aof_record(service, record, argument_count);
}

static int append_mysql_marker(kvstore_service_t *service, uint64_t sequence)
{
    static const char hexadecimal[] = "0123456789abcdef";
    aof_argument_t marker[2];
    unsigned char text[64];
    size_t position = 0;
    int written;

    memcpy(text, "KVMYSQL1:", 9U);
    position = 9U;
    for (size_t index = 0; index < sizeof(service->mysql_writer_uuid); ++index) {
        text[position++] = (unsigned char)hexadecimal[
            service->mysql_writer_uuid[index] >> 4U];
        text[position++] = (unsigned char)hexadecimal[
            service->mysql_writer_uuid[index] & 0x0fU];
    }
    text[position++] = ':';
    written = snprintf((char *)text + position,
                       sizeof(text) - position,
                       "%" PRIu64, sequence);
    if (written < 0 || (size_t)written >= sizeof(text) - position) return -1;
    position += (size_t)written;
    marker[0] = aof_argument("PING", 4U);
    marker[1] = aof_argument(text, position);
    return aof_append(service->aof, marker, 2U);
}

static size_t mysql_write_estimate(const kvstore_argument_t *arguments,
                                   size_t argument_count)
{
    size_t amount = 128U;

    for (size_t index = 0; index < argument_count; ++index) {
        if (arguments[index].length > SIZE_MAX - amount) return SIZE_MAX;
        amount += arguments[index].length;
    }
    return amount;
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
    kvstore_service_config_t config;

    memset(&config, 0, sizeof(config));
    if (cache_config != NULL) config.cache = *cache_config;
    config.zset_engine = KV_ZSET_SKIPLIST;
    return kvstore_service_init_with_config(service, &config);
}

int kvstore_service_init_with_config(kvstore_service_t *service,
                                     const kvstore_service_config_t *config)
{
    if (service == NULL) {
        return -1;
    }
    memset(service, 0, sizeof(*service));
    service->zset_engine = config == NULL ? KV_ZSET_SKIPLIST
                                         : config->zset_engine;
    if (service->zset_engine != KV_ZSET_SKIPLIST &&
        service->zset_engine != KV_ZSET_RBTREE) return -1;
    if (cache_create(&service->cache,
                     config == NULL ? NULL : &config->cache) != 0) {
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
    free(service->reply_elements);
    free(service->reply_score_buffer);
    free(service->mysql_mutation_buffer);
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

void kvstore_service_attach_mysql(kvstore_service_t *service,
                                  mysql_store_t *store,
                                  const unsigned char writer_uuid[16],
                                  uint64_t next_sequence)
{
    if (service == NULL || !service->initialized) return;
    service->mysql_store = store;
    service->mysql_next_sequence = next_sequence;
    memset(service->mysql_writer_uuid, 0, sizeof(service->mysql_writer_uuid));
    if (store != NULL && writer_uuid != NULL)
        memcpy(service->mysql_writer_uuid, writer_uuid,
               sizeof(service->mysql_writer_uuid));
}

void kvstore_service_set_persistence_admin(
    kvstore_service_t *service,
    const kvstore_persistence_admin_t *admin,
    void *context)
{
    if (service == NULL || !service->initialized) return;
    memset(&service->persistence_admin, 0,
           sizeof(service->persistence_admin));
    if (admin != NULL) service->persistence_admin = *admin;
    service->persistence_context = context;
}

uint64_t kvstore_service_dirty_changes(const kvstore_service_t *service)
{
    return service == NULL ? 0 : service->dirty_changes;
}

void kvstore_service_snapshot_committed(kvstore_service_t *service,
                                        uint64_t covered_changes)
{
    if (service == NULL) return;
    service->dirty_changes = covered_changes >= service->dirty_changes
                                 ? 0
                                 : service->dirty_changes - covered_changes;
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

static void set_wrongtype(kvstore_reply_t *reply)
{
    set_error(reply, error_wrongtype, sizeof(error_wrongtype) - 1U);
}

static void set_object_error(kvstore_reply_t *reply, int result)
{
    if (result == CACHE_SET_CAPACITY) {
        set_error(reply, error_capacity, sizeof(error_capacity) - 1U);
    } else {
        set_error(reply, error_internal, sizeof(error_internal) - 1U);
    }
}

static int execute_hset(kvstore_service_t *service,
                        const kvstore_argument_t *arguments,
                        size_t argument_count,
                        kvstore_reply_t *reply)
{
    size_t pair_count = (argument_count - 2U) / 2U;
    cache_entry_ref_t *entry_ref = NULL;
    kv_object_t *original = cache_get_hash_object_ref(
        service->cache, arguments[1].data, arguments[1].length, 0, &entry_ref);
    kv_object_t *working = original;
    size_t index;
    int added_total = 0;
    int changed_any = 0;
    int copied = 0;
    int result;

    if (original != NULL && kv_object_type(original) != KV_OBJECT_HASH) {
        set_wrongtype(reply);
        return 0;
    }
    if (original == NULL) {
        if (kv_object_create_hash(&working) != 0) {
            set_error(reply, error_internal, sizeof(error_internal) - 1U);
            return 0;
        }
        copied = 1;
    } else if (pair_count > 1U || cache_max_memory(service->cache) != 0) {
        if (kv_object_clone(original, &working) != 0) {
            set_error(reply, error_internal, sizeof(error_internal) - 1U);
            return 0;
        }
        copied = 1;
    }
    for (index = 0; index < pair_count; ++index) {
        int added = 0;
        int changed = 0;
        size_t position = 2U + index * 2U;

        if (kv_object_hash_set(working,
                               arguments[position].data,
                               arguments[position].length,
                               arguments[position + 1U].data,
                               arguments[position + 1U].length,
                               &added,
                               &changed) != 0) {
            if (copied) kv_object_destroy(working);
            set_error(reply, error_internal, sizeof(error_internal) - 1U);
            return 0;
        }
        added_total += added;
        changed_any |= changed;
    }
    if (changed_any) {
        if (copied) {
            result = cache_store_object(service->cache,
                                        arguments[1].data,
                                        arguments[1].length,
                                        working,
                                        0,
                                        1);
            if (result != CACHE_SET_OK) {
                kv_object_destroy(working);
                set_object_error(reply, result);
                return 0;
            }
        } else {
            result = cache_recharge_ref(service->cache, entry_ref);
            if (result != CACHE_SET_OK) {
                set_object_error(reply, result);
                return 0;
            }
        }
        if (append_original_command(service, arguments, argument_count) != 0) {
            set_aof_error(reply);
            return 0;
        }
    } else if (copied) {
        kv_object_destroy(working);
    }
    set_integer(reply, added_total);
    return 0;
}

static int execute_hdel(kvstore_service_t *service,
                        const kvstore_argument_t *arguments,
                        size_t argument_count,
                        kvstore_reply_t *reply)
{
    cache_entry_ref_t *entry_ref = NULL;
    kv_object_t *object = cache_get_hash_object_ref(
        service->cache, arguments[1].data, arguments[1].length, 0, &entry_ref);
    size_t index;
    int removed = 0;

    if (object == NULL) {
        set_integer(reply, 0);
        return 0;
    }
    if (kv_object_type(object) != KV_OBJECT_HASH) {
        set_wrongtype(reply);
        return 0;
    }
    for (index = 2U; index < argument_count; ++index) {
        int result = kv_object_hash_delete(object,
                                           arguments[index].data,
                                           arguments[index].length);

        if (result < 0) {
            set_error(reply, error_internal, sizeof(error_internal) - 1U);
            return 0;
        }
        removed += result;
    }
    if (removed > 0) {
        if (kv_object_hash_length(object) == 0) {
            if (cache_delete(service->cache,
                             arguments[1].data,
                             arguments[1].length) < 0) {
                set_error(reply, error_internal, sizeof(error_internal) - 1U);
                return 0;
            }
        } else if (cache_recharge_ref(service->cache,
                                      entry_ref) != CACHE_SET_OK) {
            set_error(reply, error_internal, sizeof(error_internal) - 1U);
            return 0;
        }
        if (append_original_command(service, arguments, argument_count) != 0) {
            set_aof_error(reply);
            return 0;
        }
    }
    set_integer(reply, removed);
    return 0;
}

typedef struct hash_array_context {
    kvstore_service_t *service;
    size_t index;
    size_t encoded_size;
    int too_large;
} hash_array_context_t;

static int collect_hash_pair(const void *field,
                             size_t field_length,
                             const void *value,
                             size_t value_length,
                             void *context)
{
    hash_array_context_t *collection = context;
    size_t field_size = bulk_encoded_size(field_length);
    size_t value_size = bulk_encoded_size(value_length);

    if (field_size == SIZE_MAX || value_size == SIZE_MAX ||
        collection->encoded_size > SERVICE_MAX_RESPONSE - field_size ||
        collection->encoded_size + field_size >
            SERVICE_MAX_RESPONSE - value_size) {
        collection->too_large = 1;
        return -1;
    }
    collection->encoded_size += field_size + value_size;
    collection->service->reply_elements[collection->index].data = field;
    collection->service->reply_elements[collection->index++].length = field_length;
    collection->service->reply_elements[collection->index].data = value;
    collection->service->reply_elements[collection->index++].length = value_length;
    return 0;
}

static int execute_hgetall(kvstore_service_t *service,
                           const kvstore_argument_t *arguments,
                           kvstore_reply_t *reply)
{
    kv_object_t *object = cache_get_hash_object_ref(
        service->cache, arguments[1].data, arguments[1].length, 1, NULL);
    hash_array_context_t context;
    size_t fields;
    int reserve_result;

    if (object == NULL) {
        set_array(reply, NULL, 0);
        return 0;
    }
    if (kv_object_type(object) != KV_OBJECT_HASH) {
        set_wrongtype(reply);
        return 0;
    }
    fields = kv_object_hash_length(object);
    if (fields > SERVICE_MAX_ARRAY_ELEMENTS / 2U) {
        set_error(reply,
                  error_response_too_large,
                  sizeof(error_response_too_large) - 1U);
        return 0;
    }
    reserve_result = reserve_reply_elements(service, fields * 2U);
    if (reserve_result != 0) {
        set_error(reply,
                  reserve_result == -2 ? error_response_too_large : error_internal,
                  reserve_result == -2 ? sizeof(error_response_too_large) - 1U
                                       : sizeof(error_internal) - 1U);
        return 0;
    }
    context.service = service;
    context.index = 0;
    context.encoded_size = decimal_digits(fields * 2U) + 3U;
    context.too_large = 0;
    if (kv_object_hash_visit(object, collect_hash_pair, &context) != 0) {
        set_error(reply,
                  context.too_large ? error_response_too_large : error_internal,
                  context.too_large ? sizeof(error_response_too_large) - 1U
                                    : sizeof(error_internal) - 1U);
        return 0;
    }
    set_array(reply, service->reply_elements, context.index);
    return 0;
}

static int execute_zadd(kvstore_service_t *service,
                        const kvstore_argument_t *arguments,
                        size_t argument_count,
                        kvstore_reply_t *reply)
{
    size_t pair_count = (argument_count - 2U) / 2U;
    double *scores = service->zadd_scores;
    cache_entry_ref_t *entry_ref = NULL;
    kv_object_t *original;
    kv_object_t *working;
    size_t index;
    int copied = 0;
    int added_total = 0;
    int changed_any = 0;
    int result;

    if (pair_count > sizeof(service->zadd_scores) /
                         sizeof(service->zadd_scores[0])) {
        set_error(reply, error_internal, sizeof(error_internal) - 1U);
        return 0;
    }
    for (index = 0; index < pair_count; ++index) {
        result = parse_double_argument(&arguments[2U + index * 2U],
                                       &scores[index]);
        if (result != 0) {
            set_error(reply,
                      result == -2 ? error_internal : error_float,
                      result == -2 ? sizeof(error_internal) - 1U
                                   : sizeof(error_float) - 1U);
            return 0;
        }
    }
    original = cache_get_object_ref(service->cache,
                                    arguments[1].data,
                                    arguments[1].length,
                                    0,
                                    &entry_ref);
    working = original;
    if (original != NULL && kv_object_type(original) != KV_OBJECT_ZSET) {
        set_wrongtype(reply);
        return 0;
    }
    if (original == NULL) {
        if (kv_object_create_zset(&working, service->zset_engine) != 0) {
            set_error(reply, error_internal, sizeof(error_internal) - 1U);
            return 0;
        }
        copied = 1;
    } else if (pair_count > 1U || cache_max_memory(service->cache) != 0) {
        if (kv_object_clone(original, &working) != 0) {
            set_error(reply, error_internal, sizeof(error_internal) - 1U);
            return 0;
        }
        copied = 1;
    }
    for (index = 0; index < pair_count; ++index) {
        size_t position = 3U + index * 2U;
        int added = 0;
        int changed = 0;

        if (kv_object_zset_add(working,
                               scores[index],
                               arguments[position].data,
                               arguments[position].length,
                               &added,
                               &changed) != 0) {
            if (copied) kv_object_destroy(working);
            set_error(reply, error_internal, sizeof(error_internal) - 1U);
            return 0;
        }
        added_total += added;
        changed_any |= changed;
    }
    if (changed_any) {
        if (copied) {
            result = cache_store_object(service->cache,
                                        arguments[1].data,
                                        arguments[1].length,
                                        working,
                                        0,
                                        1);
            if (result != CACHE_SET_OK) {
                kv_object_destroy(working);
                set_object_error(reply, result);
                return 0;
            }
        } else {
            result = cache_recharge_ref(service->cache, entry_ref);
            if (result != CACHE_SET_OK) {
                set_object_error(reply, result);
                return 0;
            }
        }
        if (append_original_command(service, arguments, argument_count) != 0) {
            set_aof_error(reply);
            return 0;
        }
    } else if (copied) {
        kv_object_destroy(working);
    }
    set_integer(reply, added_total);
    return 0;
}

static int execute_zrem(kvstore_service_t *service,
                        const kvstore_argument_t *arguments,
                        size_t argument_count,
                        kvstore_reply_t *reply)
{
    cache_entry_ref_t *entry_ref = NULL;
    kv_object_t *object = cache_get_object_ref(service->cache,
                                               arguments[1].data,
                                               arguments[1].length,
                                               0,
                                               &entry_ref);
    size_t index;
    int removed = 0;

    if (object == NULL) {
        set_integer(reply, 0);
        return 0;
    }
    if (kv_object_type(object) != KV_OBJECT_ZSET) {
        set_wrongtype(reply);
        return 0;
    }
    for (index = 2U; index < argument_count; ++index) {
        int result = kv_object_zset_remove(object,
                                           arguments[index].data,
                                           arguments[index].length);

        if (result < 0) {
            set_error(reply, error_internal, sizeof(error_internal) - 1U);
            return 0;
        }
        removed += result;
    }
    if (removed > 0) {
        if (kv_object_zset_length(object) == 0) {
            if (cache_delete(service->cache,
                             arguments[1].data,
                             arguments[1].length) < 0) {
                set_error(reply, error_internal, sizeof(error_internal) - 1U);
                return 0;
            }
        } else if (cache_recharge_ref(service->cache,
                                      entry_ref) != CACHE_SET_OK) {
            set_error(reply, error_internal, sizeof(error_internal) - 1U);
            return 0;
        }
        if (append_original_command(service, arguments, argument_count) != 0) {
            set_aof_error(reply);
            return 0;
        }
    }
    set_integer(reply, removed);
    return 0;
}

typedef struct zrange_count_context {
    size_t count;
} zrange_count_context_t;

static int count_zset_member(const void *member,
                             size_t member_length,
                             double score,
                             void *context)
{
    zrange_count_context_t *count = context;

    (void)member;
    (void)member_length;
    (void)score;
    count->count++;
    return 0;
}

typedef struct zrange_array_context {
    kvstore_service_t *service;
    size_t index;
    size_t score_index;
    size_t encoded_size;
    int with_scores;
    int too_large;
} zrange_array_context_t;

static int collect_zset_member(const void *member,
                               size_t member_length,
                               double score,
                               void *context)
{
    zrange_array_context_t *collection = context;
    size_t member_size = bulk_encoded_size(member_length);

    if (member_size == SIZE_MAX ||
        collection->encoded_size > SERVICE_MAX_RESPONSE - member_size) {
        collection->too_large = 1;
        return -1;
    }
    collection->encoded_size += member_size;
    collection->service->reply_elements[collection->index].data = member;
    collection->service->reply_elements[collection->index++].length = member_length;
    if (collection->with_scores) {
        unsigned char *destination = collection->service->reply_score_buffer +
                                     collection->score_index * 32U;
        int written = format_score(destination, 32U, score);
        size_t score_size;

        if (written < 0 || written >= 32) return -1;
        score_size = bulk_encoded_size((size_t)written);
        if (score_size == SIZE_MAX ||
            collection->encoded_size > SERVICE_MAX_RESPONSE - score_size) {
            collection->too_large = 1;
            return -1;
        }
        collection->encoded_size += score_size;
        collection->service->reply_elements[collection->index].data = destination;
        collection->service->reply_elements[collection->index++].length =
            (size_t)written;
        collection->score_index++;
    }
    return 0;
}

static int execute_zrange(kvstore_service_t *service,
                          const kvstore_argument_t *arguments,
                          size_t argument_count,
                          kvstore_reply_t *reply)
{
    int64_t start;
    int64_t stop;
    int with_scores = argument_count == 5U;
    kv_object_t *object;
    zrange_count_context_t count = {0};
    zrange_array_context_t collection;
    size_t visited;
    size_t element_count;
    int reserve_result;

    if (parse_int64(&arguments[2], &start) != 0 ||
        parse_int64(&arguments[3], &stop) != 0) {
        set_error(reply, error_integer, sizeof(error_integer) - 1U);
        return 0;
    }
    if (with_scores && !argument_equals(&arguments[4], "WITHSCORES")) {
        set_error(reply, error_syntax, sizeof(error_syntax) - 1U);
        return 0;
    }
    object = cache_get_object(service->cache,
                              arguments[1].data,
                              arguments[1].length,
                              1);
    if (object == NULL) {
        set_array(reply, NULL, 0);
        return 0;
    }
    if (kv_object_type(object) != KV_OBJECT_ZSET) {
        set_wrongtype(reply);
        return 0;
    }
    if (kv_object_zset_range(object,
                             start,
                             stop,
                             count_zset_member,
                             &count,
                             &visited) != 0) {
        set_error(reply, error_internal, sizeof(error_internal) - 1U);
        return 0;
    }
    if (count.count > SERVICE_MAX_ARRAY_ELEMENTS / (with_scores ? 2U : 1U)) {
        set_error(reply,
                  error_response_too_large,
                  sizeof(error_response_too_large) - 1U);
        return 0;
    }
    element_count = count.count * (with_scores ? 2U : 1U);
    reserve_result = reserve_reply_elements(service, element_count);
    if (reserve_result == 0 && with_scores) {
        reserve_result = reserve_score_buffer(service, count.count * 32U);
    }
    if (reserve_result != 0) {
        set_error(reply,
                  reserve_result == -2 ? error_response_too_large : error_internal,
                  reserve_result == -2 ? sizeof(error_response_too_large) - 1U
                                       : sizeof(error_internal) - 1U);
        return 0;
    }
    collection.service = service;
    collection.index = 0;
    collection.score_index = 0;
    collection.encoded_size = decimal_digits(element_count) + 3U;
    collection.with_scores = with_scores;
    collection.too_large = 0;
    if (kv_object_zset_range(object,
                             start,
                             stop,
                             collect_zset_member,
                             &collection,
                             &visited) != 0) {
        set_error(reply,
                  collection.too_large ? error_response_too_large : error_internal,
                  collection.too_large ? sizeof(error_response_too_large) - 1U
                                       : sizeof(error_internal) - 1U);
        return 0;
    }
    set_array(reply, service->reply_elements, collection.index);
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
                        "rehashing:%d\r\n"
                        "string_keys:%zu\r\n"
                        "hash_keys:%zu\r\n"
                        "zset_keys:%zu\r\n"
                        "hash_fields:%zu\r\n"
                        "zset_members:%zu\r\n"
                        "zset_engine:%s\r\n",
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
                        stats.rehashing,
                        stats.string_keys,
                        stats.hash_keys,
                        stats.zset_keys,
                        stats.hash_fields,
                        stats.zset_members,
                        kv_zset_engine_name(service->zset_engine));
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

static int command_arity_valid(enum service_command command,
                               size_t argument_count)
{
    switch (command) {
    case SERVICE_COMMAND_SET:
        return argument_count == 3U || argument_count == 5U;
    case SERVICE_COMMAND_GET:
    case SERVICE_COMMAND_DEL:
    case SERVICE_COMMAND_TTL:
    case SERVICE_COMMAND_PTTL:
    case SERVICE_COMMAND_PERSIST:
    case SERVICE_COMMAND_HLEN:
    case SERVICE_COMMAND_HGETALL:
    case SERVICE_COMMAND_ZCARD:
        return argument_count == 2U;
    case SERVICE_COMMAND_EXPIRE:
    case SERVICE_COMMAND_PEXPIRE:
    case SERVICE_COMMAND_HGET:
    case SERVICE_COMMAND_ZSCORE:
        return argument_count == 3U;
    case SERVICE_COMMAND_PING:
        return argument_count == 1U || argument_count == 2U;
    case SERVICE_COMMAND_DBSIZE:
    case SERVICE_COMMAND_SAVE:
    case SERVICE_COMMAND_BGSAVE:
    case SERVICE_COMMAND_LASTSAVE:
        return argument_count == 1U;
    case SERVICE_COMMAND_INFO:
        return argument_count == 2U;
    case SERVICE_COMMAND_HSET:
    case SERVICE_COMMAND_ZADD:
        return argument_count >= 4U && argument_count % 2U == 0;
    case SERVICE_COMMAND_HDEL:
    case SERVICE_COMMAND_ZREM:
        return argument_count >= 3U;
    case SERVICE_COMMAND_ZRANGE:
        return argument_count == 4U || argument_count == 5U;
    default:
        return 0;
    }
}

static int execute_persistence_info(kvstore_service_t *service,
                                    kvstore_reply_t *reply);
static int execute_mysql_info(kvstore_service_t *service,
                              kvstore_reply_t *reply);

static int execute_save_command(kvstore_service_t *service,
                                int background,
                                kvstore_reply_t *reply)
{
    int result;

    if (service->persistence_admin.save == NULL) {
        set_error(reply, error_rdb_disabled,
                  sizeof(error_rdb_disabled) - 1U);
        return 0;
    }
    result = service->persistence_admin.save(service->persistence_context,
                                             background);
    if (result != 0) {
        if (errno == EBUSY) {
            set_error(reply, error_save_busy, sizeof(error_save_busy) - 1U);
        } else if (errno == ENOTSUP) {
            set_error(reply, error_rdb_disabled,
                      sizeof(error_rdb_disabled) - 1U);
        } else {
            set_error(reply, error_internal, sizeof(error_internal) - 1U);
        }
    } else if (background) {
        set_data_reply(reply, KVSTORE_REPLY_SIMPLE,
                       reply_bgsave_started,
                       sizeof(reply_bgsave_started) - 1U);
    } else {
        set_data_reply(reply, KVSTORE_REPLY_SIMPLE,
                       reply_ok, sizeof(reply_ok) - 1U);
    }
    return 0;
}

static int execute_command(kvstore_service_t *service,
                           enum service_command command,
                           const kvstore_argument_t *arguments,
                           size_t argument_count,
                           kvstore_reply_t *reply)
{
    if (service == NULL || !service->initialized || arguments == NULL ||
        argument_count == 0 || reply == NULL) {
        return -1;
    }
    memset(reply, 0, sizeof(*reply));
    if (command == SERVICE_COMMAND_UNKNOWN) {
        set_error(reply, error_unknown, sizeof(error_unknown) - 1U);
        return 0;
    }

    if (!command_arity_valid(command, argument_count)) {
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
        {
            kv_object_t *object = cache_get_object(service->cache,
                                                   arguments[1].data,
                                                   arguments[1].length,
                                                   1);

            if (object == NULL) {
                reply->type = KVSTORE_REPLY_NULL_BULK;
            } else if (kv_object_type(object) != KV_OBJECT_STRING) {
                set_wrongtype(reply);
            } else {
                reply->data = kv_object_string_value(object, &reply->length);
                reply->type = KVSTORE_REPLY_BULK;
            }
        }
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
        if (argument_equals(&arguments[1], "PERSISTENCE")) {
            return execute_persistence_info(service, reply);
        }
        if (argument_equals(&arguments[1], "MYSQL")) {
            return execute_mysql_info(service, reply);
        }
        if (!argument_equals(&arguments[1], "CACHE")) {
            set_error(reply, error_syntax, sizeof(error_syntax) - 1U);
            return 0;
        }
        return execute_info(service, reply);
    case SERVICE_COMMAND_DBSIZE:
        {
            cache_stats_t stats;

            cache_get_stats(service->cache, &stats);
            set_integer(reply, (int64_t)stats.keys);
        }
        return 0;
    case SERVICE_COMMAND_SAVE:
        return execute_save_command(service, 0, reply);
    case SERVICE_COMMAND_BGSAVE:
        return execute_save_command(service, 1, reply);
    case SERVICE_COMMAND_LASTSAVE:
        set_integer(reply,
                    service->persistence_admin.lastsave == NULL
                        ? 0
                        : (int64_t)service->persistence_admin.lastsave(
                              service->persistence_context));
        return 0;
    case SERVICE_COMMAND_HSET:
        return execute_hset(service, arguments, argument_count, reply);
    case SERVICE_COMMAND_HGET:
        {
            kv_object_t *object = cache_get_hash_object_ref(
                service->cache, arguments[1].data, arguments[1].length, 1, NULL);

            if (object == NULL) {
                reply->type = KVSTORE_REPLY_NULL_BULK;
            } else if (kv_object_type(object) != KV_OBJECT_HASH) {
                set_wrongtype(reply);
            } else {
                reply->data = kv_object_hash_get(object,
                                                 arguments[2].data,
                                                 arguments[2].length,
                                                 &reply->length);
                reply->type = reply->data == NULL ? KVSTORE_REPLY_NULL_BULK
                                                  : KVSTORE_REPLY_BULK;
            }
        }
        return 0;
    case SERVICE_COMMAND_HDEL:
        return execute_hdel(service, arguments, argument_count, reply);
    case SERVICE_COMMAND_HLEN:
        {
            kv_object_t *object = cache_get_hash_object_ref(
                service->cache, arguments[1].data, arguments[1].length, 1, NULL);

            if (object == NULL) set_integer(reply, 0);
            else if (kv_object_type(object) != KV_OBJECT_HASH) set_wrongtype(reply);
            else set_integer(reply, (int64_t)kv_object_hash_length(object));
        }
        return 0;
    case SERVICE_COMMAND_HGETALL:
        return execute_hgetall(service, arguments, reply);
    case SERVICE_COMMAND_ZADD:
        return execute_zadd(service, arguments, argument_count, reply);
    case SERVICE_COMMAND_ZREM:
        return execute_zrem(service, arguments, argument_count, reply);
    case SERVICE_COMMAND_ZSCORE:
        {
            kv_object_t *object = cache_get_object(service->cache,
                                                   arguments[1].data,
                                                   arguments[1].length,
                                                   1);
            double score;
            int found;

            if (object == NULL) {
                reply->type = KVSTORE_REPLY_NULL_BULK;
                return 0;
            }
            if (kv_object_type(object) != KV_OBJECT_ZSET) {
                set_wrongtype(reply);
                return 0;
            }
            found = kv_object_zset_score(object,
                                         arguments[2].data,
                                         arguments[2].length,
                                         &score);
            if (found < 0) {
                set_error(reply, error_internal, sizeof(error_internal) - 1U);
            } else if (found == 0) {
                reply->type = KVSTORE_REPLY_NULL_BULK;
            } else {
                int written;

                if (reserve_score_buffer(service, 32U) != 0) {
                    set_error(reply, error_internal, sizeof(error_internal) - 1U);
                    return 0;
                }
                written = format_score(service->reply_score_buffer,
                                       32U,
                                       score);
                if (written < 0 || written >= 32) {
                    set_error(reply, error_internal, sizeof(error_internal) - 1U);
                } else {
                    set_data_reply(reply,
                                   KVSTORE_REPLY_BULK,
                                   service->reply_score_buffer,
                                   (size_t)written);
                }
            }
        }
        return 0;
    case SERVICE_COMMAND_ZCARD:
        {
            kv_object_t *object = cache_get_object(service->cache,
                                                   arguments[1].data,
                                                   arguments[1].length,
                                                   1);

            if (object == NULL) set_integer(reply, 0);
            else if (kv_object_type(object) != KV_OBJECT_ZSET) set_wrongtype(reply);
            else set_integer(reply, (int64_t)kv_object_zset_length(object));
        }
        return 0;
    case SERVICE_COMMAND_ZRANGE:
        return execute_zrange(service,
                              arguments,
                              argument_count,
                              reply);
    default:
        return -1;
    }
}

int kvstore_service_execute_with_barrier(kvstore_service_t *service,
                                         const kvstore_argument_t *arguments,
                                         size_t argument_count,
                                         kvstore_reply_t *reply,
                                         uint64_t *response_barrier)
{
    enum service_command command;
    int logical_write;
    int write_command;
    int mysql_write;
    size_t mysql_bytes = 0;
    int result;

    if (response_barrier != NULL) *response_barrier = 0;
    if (service == NULL || arguments == NULL || argument_count == 0 ||
        reply == NULL) return -1;
    command = find_command(&arguments[0]);
    logical_write = command != SERVICE_COMMAND_UNKNOWN &&
                    command_arity_valid(command, argument_count) &&
                    is_write_command(command);
    write_command = logical_write && service->aof != NULL;
    mysql_write = logical_write && service->mysql_store != NULL;
    if (mysql_write) {
        mysql_bytes = mysql_write_estimate(arguments, argument_count);
        if (service->aof == NULL) {
            memset(reply, 0, sizeof(*reply));
            set_aof_error(reply);
            return 0;
        }
        if (mysql_bytes == SIZE_MAX ||
            !mysql_store_can_accept_write(service->mysql_store, mysql_bytes)) {
            memset(reply, 0, sizeof(*reply));
            set_error(reply, error_mysql_queue, sizeof(error_mysql_queue) - 1U);
            return 0;
        }
        service->mysql_candidate_sequence = service->mysql_next_sequence + 1U;
        if (service->mysql_candidate_sequence == 0) {
            memset(reply, 0, sizeof(*reply));
            set_error(reply, error_internal, sizeof(error_internal) - 1U);
            return 0;
        }
    }
    if (write_command && aof_transaction_begin(service->aof) != 0) {
        static const unsigned char busy[] =
            "TRYAGAIN AOF writer queue is above the high-water mark";

        memset(reply, 0, sizeof(*reply));
        set_error(reply, busy, sizeof(busy) - 1U);
        return 0;
    }
    service->mysql_mutation_count = 0;
    service->mysql_mutation_buffer_used = 0;
    service->mysql_recording_command = mysql_write;
    result = execute_command(service, command, arguments, argument_count, reply);
    service->mysql_recording_command = 0;
    if (result == 0 && logical_write && reply->type != KVSTORE_REPLY_ERROR) {
        service->dirty_changes++;
    }
    if (!write_command) return result;
    if (result != 0) {
        aof_transaction_rollback(service->aof);
        return result;
    }
    if (mysql_write && service->mysql_mutation_count > 0 &&
        append_mysql_marker(service, service->mysql_candidate_sequence) != 0) {
        aof_transaction_rollback(service->aof);
        set_aof_error(reply);
        return 0;
    }
    if (aof_transaction_commit(service->aof, response_barrier) != 0) {
        aof_transaction_rollback(service->aof);
        set_aof_error(reply);
    } else if (mysql_write && service->mysql_mutation_count > 0) {
        service->mysql_next_sequence = service->mysql_candidate_sequence;
        if (mysql_store_submit_mutation(
                service->mysql_store,
                service->mysql_candidate_sequence,
                service->mysql_mutation_arguments,
                service->mysql_mutation_count,
                mysql_bytes) != 0) {
            set_error(reply, error_mysql_queue, sizeof(error_mysql_queue) - 1U);
        }
    }
    return 0;
}

static int execute_persistence_info(kvstore_service_t *service,
                                    kvstore_reply_t *reply)
{
    aof_info_t info;
    kvstore_persistence_info_t persistence;
    int written;

    aof_get_info(service->aof, &info);
    memset(&persistence, 0, sizeof(persistence));
    persistence.dirty_changes = service->dirty_changes;
    if (service->persistence_admin.get_info != NULL) {
        service->persistence_admin.get_info(service->persistence_context,
                                            &persistence);
    }
    written = snprintf((char *)service->info_buffer,
                       sizeof(service->info_buffer),
                       "aof_enabled:%d\r\n"
                       "aof_queue_bytes:%zu\r\n"
                       "aof_queue_high_water:%zu\r\n"
                       "aof_queue_low_water:%zu\r\n"
                       "aof_enqueued_sequence:%" PRIu64 "\r\n"
                       "aof_written_sequence:%" PRIu64 "\r\n"
                       "aof_synced_sequence:%" PRIu64 "\r\n"
                       "aof_written_bytes:%" PRIu64 "\r\n"
                       "aof_backpressure_events:%" PRIu64 "\r\n"
                       "aof_fsync_count:%" PRIu64 "\r\n"
                       "aof_fsync_total_us:%" PRIu64 "\r\n"
                       "aof_fsync_max_us:%" PRIu64 "\r\n"
                       "aof_backpressured:%d\r\n"
                       "aof_failed:%d\r\n"
                       "aof_last_error:%d\r\n"
                       "rdb_enabled:%d\r\n"
                       "rdb_bgsave_in_progress:%d\r\n"
                       "rdb_dirty_changes:%" PRIu64 "\r\n"
                       "rdb_last_save_time:%" PRIu64 "\r\n"
                        "rdb_last_save_duration_us:%" PRIu64 "\r\n"
                        "rdb_last_fork_pause_us:%" PRIu64 "\r\n"
                        "rdb_last_child_peak_rss_kb:%" PRIu64 "\r\n"
                        "rdb_last_child_minor_faults:%" PRIu64 "\r\n"
                        "rdb_last_child_major_faults:%" PRIu64 "\r\n"
                        "rdb_last_save_status:%d\r\n"
                       "rdb_checkpoint_offset:%" PRIu64 "\r\n",
                       service->aof != NULL,
                       info.queue_bytes,
                       info.queue_high_water,
                       info.queue_low_water,
                       info.enqueued_sequence,
                       info.written_sequence,
                       info.synced_sequence,
                       info.written_bytes,
                       info.backpressure_events,
                       info.fsync_count,
                       info.fsync_total_us,
                       info.fsync_max_us,
                       info.backpressured,
                       info.failed,
                       info.last_error,
                       persistence.rdb_enabled,
                       persistence.bgsave_in_progress,
                       persistence.dirty_changes,
                       persistence.last_save_time,
                        persistence.last_save_duration_us,
                        persistence.last_fork_pause_us,
                        persistence.last_child_peak_rss_kb,
                        persistence.last_child_minor_faults,
                        persistence.last_child_major_faults,
                        persistence.last_save_status,
                       persistence.checkpoint_offset);
    if (written < 0 || (size_t)written >= sizeof(service->info_buffer)) {
        set_error(reply, error_internal, sizeof(error_internal) - 1U);
    } else {
        set_data_reply(reply, KVSTORE_REPLY_BULK, service->info_buffer,
                       (size_t)written);
    }
    return 0;
}

static int execute_mysql_info(kvstore_service_t *service,
                              kvstore_reply_t *reply)
{
    mysql_store_stats_t stats;
    int written;

    mysql_store_get_stats(service->mysql_store, &stats);
    written = snprintf((char *)service->info_buffer,
                       sizeof(service->info_buffer),
                       "mysql_enabled:%d\r\n"
                       "mysql_read_workers:%zu\r\n"
                       "mysql_connected_readers:%zu\r\n"
                       "mysql_connected_writer:%d\r\n"
                       "mysql_pending_reads:%zu\r\n"
                       "mysql_read_queue_limit:%zu\r\n"
                       "mysql_pending_write_bytes:%zu\r\n"
                       "mysql_write_queue_max_bytes:%zu\r\n"
                       "mysql_load_max_bytes:%zu\r\n"
                       "mysql_submitted_sequence:%" PRIu64 "\r\n"
                       "mysql_applied_sequence:%" PRIu64 "\r\n"
                       "mysql_loads:%" PRIu64 "\r\n"
                       "mysql_coalesced_loads:%" PRIu64 "\r\n"
                       "mysql_negative_cache_hits:%" PRIu64 "\r\n"
                       "mysql_load_errors:%" PRIu64 "\r\n"
                       "mysql_write_errors:%" PRIu64 "\r\n"
                       "mysql_reconnects:%" PRIu64 "\r\n"
                       "mysql_last_error:%u\r\n",
                       service->mysql_store != NULL,
                       stats.read_workers,
                       stats.connected_readers,
                       stats.connected_writer,
                       stats.pending_reads,
                       stats.read_queue_limit,
                       stats.pending_write_bytes,
                       stats.write_queue_max_bytes,
                       stats.load_max_bytes,
                       stats.submitted_sequence,
                       stats.applied_sequence,
                       stats.loads,
                       stats.coalesced_loads,
                       stats.negative_cache_hits,
                       stats.load_errors,
                       stats.write_errors,
                       stats.reconnects,
                       stats.last_error);
    if (written < 0 || (size_t)written >= sizeof(service->info_buffer)) {
        set_error(reply, error_internal, sizeof(error_internal) - 1U);
    } else {
        set_data_reply(reply, KVSTORE_REPLY_BULK, service->info_buffer,
                       (size_t)written);
    }
    return 0;
}

int kvstore_service_execute(kvstore_service_t *service,
                            const kvstore_argument_t *arguments,
                            size_t argument_count,
                            kvstore_reply_t *reply)
{
    return kvstore_service_execute_with_barrier(service,
                                                arguments,
                                                argument_count,
                                                reply,
                                                NULL);
}

static int replay_argument_equals(const aof_argument_t *argument,
                                  const char *name)
{
    kvstore_argument_t service_argument;

    service_argument.data = argument->data;
    service_argument.length = argument->length;
    return argument_equals_length(&service_argument, name, strlen(name));
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
    if (replay_argument_equals(&arguments[0], "PING") &&
        (argument_count == 1U || argument_count == 2U)) {
        return 0;
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
    if (replay_argument_equals(&arguments[0], "HSET") ||
        replay_argument_equals(&arguments[0], "HDEL") ||
        replay_argument_equals(&arguments[0], "ZADD") ||
        replay_argument_equals(&arguments[0], "ZREM")) {
        kvstore_argument_t service_arguments[RESP_MAX_ARGUMENTS];
        kvstore_reply_t reply;
        size_t index;

        if (argument_count > RESP_MAX_ARGUMENTS) return -1;
        for (index = 0; index < argument_count; ++index) {
            service_arguments[index].data = arguments[index].data;
            service_arguments[index].length = arguments[index].length;
        }
        if (kvstore_service_execute(service,
                                    service_arguments,
                                    argument_count,
                                    &reply) != 0 ||
            reply.type == KVSTORE_REPLY_ERROR) return -1;
        return 0;
    }
    return -1;
}
