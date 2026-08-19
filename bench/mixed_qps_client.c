#define _POSIX_C_SOURCE 200809L

#include <arpa/inet.h>
#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#define DEFAULT_SERVER "127.0.0.1"
#define DEFAULT_PORT 9096U
#define DEFAULT_CONNECTIONS 16U
#define DEFAULT_REQUESTS 100000U
#define DEFAULT_WARMUP 100U
#define DEFAULT_PIPELINE 1U
#define DEFAULT_KEYSPACE 100000U
#define DEFAULT_SEED 1U
#define MAX_CONNECTIONS 1024U
#define MAX_PIPELINE 1024U
#define COMMAND_CAPACITY 256U
#define RECEIVE_BUFFER_SIZE 65536U
#define VALUE_SIZE 64U
#define IO_TIMEOUT_SECONDS 10
#define CONNECT_RETRIES 50U
#define CONNECT_RETRY_NS 20000000L

static const char VALUE_64[] =
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

typedef enum operation_type {
    OPERATION_GET,
    OPERATION_SET,
    OPERATION_DEL
} operation_type_t;

typedef enum batch_phase {
    PHASE_PRELOAD,
    PHASE_WARMUP,
    PHASE_MEASURED,
    PHASE_CLEANUP
} batch_phase_t;

typedef struct benchmark_control {
    pthread_mutex_t mutex;
    pthread_cond_t condition;
    unsigned int ready_workers;
    int start;
    int abort;
} benchmark_control_t;

typedef struct benchmark_options {
    char server[INET_ADDRSTRLEN];
    uint16_t port;
    unsigned int connections;
    uint64_t requests;
    uint64_t warmup;
    unsigned int pipeline;
    uint64_t keyspace;
    uint64_t ttl_ms;
    uint64_t seed;
    int cleanup;
    int latency;
} benchmark_options_t;

typedef struct cache_snapshot {
    uint64_t keys;
    uint64_t used_memory;
    uint64_t index_memory;
    uint64_t hits;
    uint64_t misses;
    uint64_t expired_keys;
    uint64_t evicted_keys;
    uint64_t hash_slots;
    uint64_t rehashing;
} cache_snapshot_t;

typedef struct worker {
    unsigned int id;
    int fd;
    uint64_t run_id;
    uint64_t request_start;
    uint64_t request_count;
    uint64_t preload_start;
    uint64_t preload_count;
    uint64_t keyspace;
    uint64_t warmup_count;
    uint64_t ttl_ms;
    uint64_t random_state;
    uint64_t schedule_seed;
    uint64_t get_completed;
    uint64_t get_hits;
    uint64_t get_misses;
    uint64_t set_completed;
    uint64_t set_errors;
    uint64_t *get_latencies_ns;
    uint64_t *set_latencies_ns;
    uint64_t get_latency_capacity;
    uint64_t set_latency_capacity;
    uint64_t get_latency_count;
    uint64_t set_latency_count;
    int latency_enabled;
    unsigned int pipeline_depth;
    char *command_buffer;
    size_t command_buffer_capacity;
    operation_type_t *pipeline_operations;
    char *receive_buffer;
    size_t receive_start;
    size_t receive_end;
    int error_number;
    benchmark_control_t *control;
} worker_t;

static void print_usage(const char *program)
{
    fprintf(stderr,
            "Usage: %s [-s IPv4] [-p port] [-c connections] "
            "[-n total_requests] [-w warmup_gets_per_connection] "
            "[-P pipeline_depth] [-k keyspace] [-T ttl_ms] [-S seed] [-C] [-L]\n"
            "  -k  Total shared keyspace; all keys are preloaded before timing.\n"
            "  -T  Apply SET PX ttl_ms during preload and measured writes; 0 disables TTL.\n"
            "  -C  Keep benchmark keys instead of deleting them after the run.\n"
            "  -L  Measure per-response latency percentiles (adds client overhead).\n"
            "total_requests must be a multiple of 10.\n"
            "Defaults: -s %s -p %u -c %u -n %u -w %u -P %u "
            "-k %u -T 0 -S %u, cleanup enabled\n",
            program,
            DEFAULT_SERVER,
            DEFAULT_PORT,
            DEFAULT_CONNECTIONS,
            DEFAULT_REQUESTS,
            DEFAULT_WARMUP,
            DEFAULT_PIPELINE,
            DEFAULT_KEYSPACE,
            DEFAULT_SEED);
}

static int parse_u64(const char *text,
                     uint64_t minimum,
                     uint64_t maximum,
                     uint64_t *value)
{
    char *end = NULL;
    unsigned long long parsed;

    if (text == NULL || *text == '\0' || *text == '-' || value == NULL) {
        return -1;
    }
    errno = 0;
    parsed = strtoull(text, &end, 10);
    if (errno != 0 || *end != '\0' || parsed < minimum || parsed > maximum) {
        return -1;
    }
    *value = (uint64_t)parsed;
    return 0;
}

static int parse_options(int argc, char **argv, benchmark_options_t *options)
{
    int option;

    memset(options, 0, sizeof(*options));
    memcpy(options->server, DEFAULT_SERVER, sizeof(DEFAULT_SERVER));
    options->port = DEFAULT_PORT;
    options->connections = DEFAULT_CONNECTIONS;
    options->requests = DEFAULT_REQUESTS;
    options->warmup = DEFAULT_WARMUP;
    options->pipeline = DEFAULT_PIPELINE;
    options->keyspace = DEFAULT_KEYSPACE;
    options->seed = DEFAULT_SEED;
    options->cleanup = 1;

    while ((option = getopt(argc, argv, "s:p:c:n:w:P:k:T:S:CLh")) != -1) {
        uint64_t value;

        switch (option) {
        case 's':
            if (strlen(optarg) >= sizeof(options->server)) return -1;
            strcpy(options->server, optarg);
            break;
        case 'p':
            if (parse_u64(optarg, 1U, UINT16_MAX, &value) != 0) return -1;
            options->port = (uint16_t)value;
            break;
        case 'c':
            if (parse_u64(optarg, 1U, MAX_CONNECTIONS, &value) != 0) return -1;
            options->connections = (unsigned int)value;
            break;
        case 'n':
            if (parse_u64(optarg, 10U, UINT64_MAX, &options->requests) != 0) return -1;
            break;
        case 'w':
            if (parse_u64(optarg, 0U, UINT64_MAX, &options->warmup) != 0) return -1;
            break;
        case 'P':
            if (parse_u64(optarg, 1U, MAX_PIPELINE, &value) != 0) return -1;
            options->pipeline = (unsigned int)value;
            break;
        case 'k':
            if (parse_u64(optarg, 1U, UINT64_MAX, &options->keyspace) != 0) return -1;
            break;
        case 'T':
            if (parse_u64(optarg, 0U, UINT64_MAX, &options->ttl_ms) != 0) return -1;
            break;
        case 'S':
            if (parse_u64(optarg, 0U, UINT64_MAX, &options->seed) != 0) return -1;
            break;
        case 'C':
            options->cleanup = 0;
            break;
        case 'L':
            options->latency = 1;
            break;
        case 'h':
            print_usage(argv[0]);
            exit(0);
        default:
            return -1;
        }
    }
    if (optind != argc || options->requests % 10U != 0U ||
        options->requests < options->connections) {
        return -1;
    }
    return 0;
}

static int set_socket_options(int fd)
{
    int enabled = 1;
    struct timeval timeout;

    timeout.tv_sec = IO_TIMEOUT_SECONDS;
    timeout.tv_usec = 0;
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled)) != 0 ||
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) != 0 ||
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) != 0) {
        return -1;
    }
    return 0;
}

static int connect_server(const benchmark_options_t *options)
{
    struct sockaddr_in address;
    struct timespec retry_delay = {0, CONNECT_RETRY_NS};
    unsigned int attempt;

    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_port = htons(options->port);
    if (inet_pton(AF_INET, options->server, &address.sin_addr) != 1) {
        errno = EINVAL;
        return -1;
    }
    for (attempt = 0; attempt < CONNECT_RETRIES; ++attempt) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        int connect_error;

        if (fd < 0) return -1;
        if (set_socket_options(fd) != 0) {
            close(fd);
            return -1;
        }
        if (connect(fd, (struct sockaddr *)&address, sizeof(address)) == 0) return fd;
        connect_error = errno;
        close(fd);
        if (connect_error != ECONNREFUSED && connect_error != EINTR) {
            errno = connect_error;
            return -1;
        }
        nanosleep(&retry_delay, NULL);
    }
    errno = ECONNREFUSED;
    return -1;
}

static int send_all(int fd, const char *data, size_t length)
{
    size_t sent = 0;

    while (sent < length) {
        ssize_t result = send(fd, data + sent, length - sent, MSG_NOSIGNAL);

        if (result > 0) {
            sent += (size_t)result;
        } else if (result < 0 && errno == EINTR) {
            continue;
        } else {
            return -1;
        }
    }
    return 0;
}

static int receive_fill(worker_t *worker)
{
    for (;;) {
        ssize_t result = recv(worker->fd,
                              worker->receive_buffer,
                              RECEIVE_BUFFER_SIZE,
                              0);

        if (result > 0) {
            worker->receive_start = 0;
            worker->receive_end = (size_t)result;
            return 0;
        }
        if (result < 0 && errno == EINTR) continue;
        if (result == 0) errno = ECONNRESET;
        return -1;
    }
}

static int receive_byte(worker_t *worker, char *value)
{
    if (worker->receive_start == worker->receive_end &&
        receive_fill(worker) != 0) {
        return -1;
    }
    *value = worker->receive_buffer[worker->receive_start++];
    return 0;
}

static int receive_line(worker_t *worker, char *line, size_t capacity)
{
    size_t length = 0;
    char previous = '\0';

    for (;;) {
        char current;

        if (receive_byte(worker, &current) != 0) return -1;
        if (previous == '\r' && current == '\n') {
            if (length == 0U) {
                errno = EPROTO;
                return -1;
            }
            line[length - 1U] = '\0';
            return 0;
        }
        if (length + 1U >= capacity) {
            errno = EOVERFLOW;
            return -1;
        }
        line[length++] = current;
        previous = current;
    }
}

static int receive_exact(worker_t *worker, const char *expected, size_t length)
{
    size_t consumed = 0;

    while (consumed < length) {
        size_t available;
        size_t chunk;

        if (worker->receive_start == worker->receive_end &&
            receive_fill(worker) != 0) {
            return -1;
        }
        available = worker->receive_end - worker->receive_start;
        chunk = length - consumed < available ? length - consumed : available;
        if (expected != NULL &&
            memcmp(worker->receive_buffer + worker->receive_start,
                   expected + consumed,
                   chunk) != 0) {
            errno = EPROTO;
            return -1;
        }
        worker->receive_start += chunk;
        consumed += chunk;
    }
    return 0;
}

static int receive_copy(worker_t *worker, char *output, size_t length)
{
    size_t copied = 0;

    while (copied < length) {
        size_t available;
        size_t chunk;

        if (worker->receive_start == worker->receive_end &&
            receive_fill(worker) != 0) {
            return -1;
        }
        available = worker->receive_end - worker->receive_start;
        chunk = length - copied < available ? length - copied : available;
        memcpy(output + copied,
               worker->receive_buffer + worker->receive_start,
               chunk);
        worker->receive_start += chunk;
        copied += chunk;
    }
    return 0;
}

static int parse_signed(const char *text, int64_t *value)
{
    char *end = NULL;
    long long parsed;

    errno = 0;
    parsed = strtoll(text, &end, 10);
    if (errno != 0 || text == end || *end != '\0') {
        errno = EPROTO;
        return -1;
    }
    *value = (int64_t)parsed;
    return 0;
}

static int receive_get_reply(worker_t *worker, int *hit)
{
    char prefix;
    char line[64];
    int64_t length;

    if (receive_byte(worker, &prefix) != 0 || prefix != '$' ||
        receive_line(worker, line, sizeof(line)) != 0 ||
        parse_signed(line, &length) != 0) {
        errno = EPROTO;
        return -1;
    }
    if (length == -1) {
        *hit = 0;
        return 0;
    }
    if (length != VALUE_SIZE ||
        receive_exact(worker, VALUE_64, VALUE_SIZE) != 0 ||
        receive_exact(worker, "\r\n", 2U) != 0) {
        errno = EPROTO;
        return -1;
    }
    *hit = 1;
    return 0;
}

static int receive_set_reply(worker_t *worker, int *is_error)
{
    char prefix;
    char line[512];

    if (receive_byte(worker, &prefix) != 0 ||
        receive_line(worker, line, sizeof(line)) != 0) {
        return -1;
    }
    if (prefix == '+' && strcmp(line, "OK") == 0) {
        *is_error = 0;
        return 0;
    }
    if (prefix == '-') {
        *is_error = 1;
        return 0;
    }
    errno = EPROTO;
    return -1;
}

static int receive_del_reply(worker_t *worker)
{
    char prefix;
    char line[64];
    int64_t deleted;

    if (receive_byte(worker, &prefix) != 0 || prefix != ':' ||
        receive_line(worker, line, sizeof(line)) != 0 ||
        parse_signed(line, &deleted) != 0 ||
        (deleted != 0 && deleted != 1)) {
        errno = EPROTO;
        return -1;
    }
    return 0;
}

static int extract_info_u64(const char *payload,
                            const char *field,
                            uint64_t *value)
{
    const char *start = strstr(payload, field);
    char *end = NULL;
    unsigned long long parsed;

    if (start == NULL) return -1;
    start += strlen(field);
    errno = 0;
    parsed = strtoull(start, &end, 10);
    if (errno != 0 || end == start ||
        (end[0] != '\r' && end[0] != '\n' && end[0] != '\0')) {
        return -1;
    }
    *value = (uint64_t)parsed;
    return 0;
}

static int fetch_cache_snapshot(const benchmark_options_t *options,
                                cache_snapshot_t *snapshot)
{
    static const char INFO_COMMAND[] =
        "*2\r\n$4\r\nINFO\r\n$5\r\nCACHE\r\n";
    worker_t connection;
    char prefix;
    char line[64];
    int64_t payload_length;
    char *payload = NULL;
    int result = -1;

    memset(&connection, 0, sizeof(connection));
    connection.fd = -1;
    connection.receive_buffer = malloc(RECEIVE_BUFFER_SIZE);
    if (connection.receive_buffer == NULL) goto cleanup;
    connection.fd = connect_server(options);
    if (connection.fd < 0 ||
        send_all(connection.fd,
                 INFO_COMMAND,
                 sizeof(INFO_COMMAND) - 1U) != 0 ||
        receive_byte(&connection, &prefix) != 0 ||
        prefix != '$' ||
        receive_line(&connection, line, sizeof(line)) != 0 ||
        parse_signed(line, &payload_length) != 0 ||
        payload_length < 0 ||
        (uint64_t)payload_length > SIZE_MAX - 1U) {
        errno = EPROTO;
        goto cleanup;
    }
    payload = malloc((size_t)payload_length + 1U);
    if (payload == NULL ||
        receive_copy(&connection, payload, (size_t)payload_length) != 0 ||
        receive_exact(&connection, "\r\n", 2U) != 0) {
        goto cleanup;
    }
    payload[payload_length] = '\0';
    memset(snapshot, 0, sizeof(*snapshot));
    if (extract_info_u64(payload, "keys:", &snapshot->keys) != 0 ||
        extract_info_u64(payload, "used_memory:", &snapshot->used_memory) != 0 ||
        extract_info_u64(payload, "index_memory:", &snapshot->index_memory) != 0 ||
        extract_info_u64(payload, "hits:", &snapshot->hits) != 0 ||
        extract_info_u64(payload, "misses:", &snapshot->misses) != 0 ||
        extract_info_u64(payload, "expired_keys:", &snapshot->expired_keys) != 0 ||
        extract_info_u64(payload, "evicted_keys:", &snapshot->evicted_keys) != 0 ||
        extract_info_u64(payload, "hash_slots:", &snapshot->hash_slots) != 0 ||
        extract_info_u64(payload, "rehashing:", &snapshot->rehashing) != 0) {
        errno = EPROTO;
        goto cleanup;
    }
    result = 0;

cleanup:
    if (connection.fd >= 0) close(connection.fd);
    free(payload);
    free(connection.receive_buffer);
    return result;
}

static uint64_t counter_delta(uint64_t before, uint64_t after)
{
    return after >= before ? after - before : 0U;
}

static uint64_t mix_u64(uint64_t value)
{
    value ^= value >> 30U;
    value *= UINT64_C(0xbf58476d1ce4e5b9);
    value ^= value >> 27U;
    value *= UINT64_C(0x94d049bb133111eb);
    return value ^ (value >> 31U);
}

static uint64_t next_random(worker_t *worker)
{
    uint64_t value = worker->random_state;

    value ^= value >> 12U;
    value ^= value << 25U;
    value ^= value >> 27U;
    worker->random_state = value;
    return value * UINT64_C(2685821657736338717);
}

static int operation_is_set(uint64_t global_index, uint64_t seed)
{
    uint64_t block = global_index / 10U;
    uint64_t slot = global_index % 10U;
    uint64_t set_slot = mix_u64(block ^ seed) % 10U;

    return slot == set_slot;
}

static uint64_t count_set_operations(uint64_t start,
                                     uint64_t count,
                                     uint64_t seed)
{
    uint64_t sets = 0;

    while (count > 0U && start % 10U != 0U) {
        if (operation_is_set(start, seed)) sets++;
        start++;
        count--;
    }
    sets += count / 10U;
    start += (count / 10U) * 10U;
    count %= 10U;
    while (count > 0U) {
        if (operation_is_set(start, seed)) sets++;
        start++;
        count--;
    }
    return sets;
}

static uint64_t elapsed_nanoseconds(const struct timespec *start,
                                    const struct timespec *end)
{
    time_t seconds = end->tv_sec - start->tv_sec;
    long nanoseconds = end->tv_nsec - start->tv_nsec;

    if (nanoseconds < 0) {
        seconds--;
        nanoseconds += 1000000000L;
    }
    return (uint64_t)seconds * UINT64_C(1000000000) +
           (uint64_t)nanoseconds;
}

static int record_latency(worker_t *worker,
                          operation_type_t operation,
                          const struct timespec *batch_start)
{
    struct timespec response_time;
    uint64_t latency;

    if (!worker->latency_enabled) return 0;
    if (batch_start == NULL ||
        clock_gettime(CLOCK_MONOTONIC, &response_time) != 0) {
        return -1;
    }
    latency = elapsed_nanoseconds(batch_start, &response_time);
    if (operation == OPERATION_GET) {
        if (worker->get_latency_count >= worker->get_latency_capacity) {
            errno = EOVERFLOW;
            return -1;
        }
        worker->get_latencies_ns[worker->get_latency_count++] = latency;
    } else if (operation == OPERATION_SET) {
        if (worker->set_latency_count >= worker->set_latency_capacity) {
            errno = EOVERFLOW;
            return -1;
        }
        worker->set_latencies_ns[worker->set_latency_count++] = latency;
    }
    return 0;
}

static int format_key(const worker_t *worker,
                      uint64_t key_index,
                      char *key,
                      size_t capacity)
{
    int written = snprintf(key,
                           capacity,
                           "mixed:%" PRIu64 ":%" PRIu64,
                           worker->run_id,
                           key_index);

    return written < 0 || (size_t)written >= capacity ? -1 : written;
}

static int format_command(char *output,
                          size_t capacity,
                          operation_type_t operation,
                          const char *key,
                          uint64_t ttl_ms)
{
    int written;

    if (operation == OPERATION_GET || operation == OPERATION_DEL) {
        const char *name = operation == OPERATION_GET ? "GET" : "DEL";

        written = snprintf(output,
                           capacity,
                           "*2\r\n$3\r\n%s\r\n$%zu\r\n%s\r\n",
                           name,
                           strlen(key),
                           key);
    } else if (ttl_ms == 0U) {
        written = snprintf(output,
                           capacity,
                           "*3\r\n$3\r\nSET\r\n$%zu\r\n%s\r\n$%u\r\n%s\r\n",
                           strlen(key),
                           key,
                           VALUE_SIZE,
                           VALUE_64);
    } else {
        char ttl[32];
        int ttl_length = snprintf(ttl, sizeof(ttl), "%" PRIu64, ttl_ms);

        if (ttl_length < 0 || (size_t)ttl_length >= sizeof(ttl)) return -1;
        written = snprintf(output,
                           capacity,
                           "*5\r\n$3\r\nSET\r\n$%zu\r\n%s\r\n$%u\r\n%s\r\n"
                           "$2\r\nPX\r\n$%d\r\n%s\r\n",
                           strlen(key),
                           key,
                           VALUE_SIZE,
                           VALUE_64,
                           ttl_length,
                           ttl);
    }
    return written < 0 || (size_t)written >= capacity ? -1 : written;
}

static int append_operation(worker_t *worker,
                            size_t *used,
                            unsigned int position,
                            operation_type_t operation,
                            uint64_t key_index)
{
    char command[COMMAND_CAPACITY];
    char key[128];
    int length;

    if (format_key(worker, key_index, key, sizeof(key)) < 0) return -1;
    length = format_command(command,
                            sizeof(command),
                            operation,
                            key,
                            worker->ttl_ms);
    if (length < 0 || *used + (size_t)length > worker->command_buffer_capacity) {
        errno = EOVERFLOW;
        return -1;
    }
    memcpy(worker->command_buffer + *used, command, (size_t)length);
    *used += (size_t)length;
    worker->pipeline_operations[position] = operation;
    return 0;
}

static int receive_batch(worker_t *worker,
                         unsigned int count,
                         batch_phase_t phase,
                         const struct timespec *batch_start)
{
    unsigned int index;

    for (index = 0; index < count; ++index) {
        operation_type_t operation = worker->pipeline_operations[index];

        if (operation == OPERATION_GET) {
            int hit;

            if (receive_get_reply(worker, &hit) != 0) return -1;
            if (phase == PHASE_MEASURED) {
                worker->get_completed++;
                if (hit) {
                    worker->get_hits++;
                } else {
                    worker->get_misses++;
                }
            }
        } else if (operation == OPERATION_SET) {
            int is_error;

            if (receive_set_reply(worker, &is_error) != 0) return -1;
            if (phase == PHASE_PRELOAD && is_error) {
                errno = ENOSPC;
                return -1;
            }
            if (phase == PHASE_MEASURED) {
                worker->set_completed++;
                if (is_error) worker->set_errors++;
            }
        } else if (receive_del_reply(worker) != 0) {
            return -1;
        }
        if (phase == PHASE_MEASURED &&
            record_latency(worker, operation, batch_start) != 0) {
            return -1;
        }
    }
    return 0;
}

static int execute_preload_or_cleanup(worker_t *worker, batch_phase_t phase)
{
    uint64_t offset = 0;
    operation_type_t operation = phase == PHASE_PRELOAD
                                     ? OPERATION_SET
                                     : OPERATION_DEL;

    while (offset < worker->preload_count) {
        uint64_t remaining = worker->preload_count - offset;
        unsigned int count = remaining < worker->pipeline_depth
                                 ? (unsigned int)remaining
                                 : worker->pipeline_depth;
        size_t used = 0;
        unsigned int index;

        for (index = 0; index < count; ++index) {
            if (append_operation(worker,
                                 &used,
                                 index,
                                 operation,
                                 worker->preload_start + offset + index) != 0) {
                return -1;
            }
        }
        if (send_all(worker->fd, worker->command_buffer, used) != 0 ||
            receive_batch(worker, count, phase, NULL) != 0) {
            return -1;
        }
        offset += count;
    }
    return 0;
}

static int execute_random_requests(worker_t *worker,
                                   uint64_t total,
                                   batch_phase_t phase)
{
    uint64_t offset = 0;

    while (offset < total) {
        uint64_t remaining = total - offset;
        unsigned int count = remaining < worker->pipeline_depth
                                 ? (unsigned int)remaining
                                 : worker->pipeline_depth;
        size_t used = 0;
        struct timespec batch_start;
        const struct timespec *batch_start_pointer = NULL;
        unsigned int index;

        for (index = 0; index < count; ++index) {
            uint64_t key_index = next_random(worker) % worker->keyspace;
            operation_type_t operation = OPERATION_GET;

            if (phase == PHASE_MEASURED &&
                operation_is_set(worker->request_start + offset + index,
                                 worker->schedule_seed)) {
                operation = OPERATION_SET;
            }
            if (append_operation(worker,
                                 &used,
                                 index,
                                 operation,
                                 key_index) != 0) {
                return -1;
            }
        }
        if (phase == PHASE_MEASURED && worker->latency_enabled) {
            if (clock_gettime(CLOCK_MONOTONIC, &batch_start) != 0) {
                return -1;
            }
            batch_start_pointer = &batch_start;
        }
        if (send_all(worker->fd, worker->command_buffer, used) != 0 ||
            receive_batch(worker,
                          count,
                          phase,
                          batch_start_pointer) != 0) {
            return -1;
        }
        offset += count;
    }
    return 0;
}

static int wait_for_start(worker_t *worker)
{
    benchmark_control_t *control = worker->control;
    int abort;

    pthread_mutex_lock(&control->mutex);
    control->ready_workers++;
    pthread_cond_broadcast(&control->condition);
    while (!control->start) pthread_cond_wait(&control->condition, &control->mutex);
    abort = control->abort;
    pthread_mutex_unlock(&control->mutex);
    return abort;
}

static void *worker_main(void *argument)
{
    worker_t *worker = argument;

    if (execute_preload_or_cleanup(worker, PHASE_PRELOAD) != 0 ||
        execute_random_requests(worker,
                                worker->warmup_count,
                                PHASE_WARMUP) != 0) {
        worker->error_number = errno != 0 ? errno : EIO;
    }
    if (wait_for_start(worker) || worker->error_number != 0) return NULL;
    if (execute_random_requests(worker,
                                worker->request_count,
                                PHASE_MEASURED) != 0) {
        worker->error_number = errno != 0 ? errno : EIO;
    }
    return NULL;
}

static double elapsed_seconds(const struct timespec *start, const struct timespec *end)
{
    time_t seconds = end->tv_sec - start->tv_sec;
    long nanoseconds = end->tv_nsec - start->tv_nsec;

    return (double)seconds + (double)nanoseconds / 1000000000.0;
}

static int compare_u64(const void *left, const void *right)
{
    uint64_t left_value = *(const uint64_t *)left;
    uint64_t right_value = *(const uint64_t *)right;

    return left_value < right_value ? -1 : left_value > right_value;
}

static size_t percentile_index(size_t count,
                               size_t numerator,
                               size_t denominator)
{
    size_t rank = (count / denominator) * numerator;
    size_t remainder = count % denominator;

    rank += (remainder * numerator + denominator - 1U) / denominator;
    return rank == 0U ? 0U : rank - 1U;
}

static uint64_t merged_value_at(const uint64_t *left,
                                size_t left_count,
                                const uint64_t *right,
                                size_t right_count,
                                size_t index)
{
    size_t selected = index + 1U;
    size_t low = selected > right_count ? selected - right_count : 0U;
    size_t high = selected < left_count ? selected : left_count;

    for (;;) {
        size_t left_selected = low + (high - low) / 2U;
        size_t right_selected = selected - left_selected;

        if (left_selected > 0U && right_selected < right_count &&
            left[left_selected - 1U] > right[right_selected]) {
            high = left_selected - 1U;
        } else if (right_selected > 0U && left_selected < left_count &&
                   right[right_selected - 1U] > left[left_selected]) {
            low = left_selected + 1U;
        } else if (left_selected == 0U) {
            return right[right_selected - 1U];
        } else if (right_selected == 0U) {
            return left[left_selected - 1U];
        } else {
            uint64_t left_value = left[left_selected - 1U];
            uint64_t right_value = right[right_selected - 1U];

            return left_value > right_value ? left_value : right_value;
        }
    }
}

static long double latency_sum(const uint64_t *values, size_t count)
{
    long double sum = 0.0L;
    size_t index;

    for (index = 0; index < count; ++index) sum += values[index];
    return sum;
}

static void print_latency_summary(const char *prefix,
                                  const uint64_t *values,
                                  size_t count)
{
    uint64_t p50;
    uint64_t p95;
    uint64_t p99;
    uint64_t p999;
    uint64_t maximum;
    long double mean;

    if (count == 0U) {
        printf("%s_latency_samples: 0\n", prefix);
        return;
    }
    p50 = values[percentile_index(count, 50U, 100U)];
    p95 = values[percentile_index(count, 95U, 100U)];
    p99 = values[percentile_index(count, 99U, 100U)];
    p999 = values[percentile_index(count, 999U, 1000U)];
    maximum = values[count - 1U];
    mean = latency_sum(values, count) / (long double)count;
    printf("%s_latency_samples: %zu\n", prefix, count);
    printf("%s_latency_mean_us: %.3Lf\n", prefix, mean / 1000.0L);
    printf("%s_latency_p50_us: %.3f\n", prefix, (double)p50 / 1000.0);
    printf("%s_latency_p95_us: %.3f\n", prefix, (double)p95 / 1000.0);
    printf("%s_latency_p99_us: %.3f\n", prefix, (double)p99 / 1000.0);
    printf("%s_latency_p999_us: %.3f\n", prefix, (double)p999 / 1000.0);
    printf("%s_latency_max_us: %.3f\n", prefix, (double)maximum / 1000.0);
    printf("%s_latency_p99_over_p50: %.3f\n",
           prefix,
           p50 > 0U ? (double)p99 / (double)p50 : 0.0);
}

static void print_merged_latency_summary(const uint64_t *get_values,
                                         size_t get_count,
                                         const uint64_t *set_values,
                                         size_t set_count)
{
    size_t count = get_count + set_count;
    uint64_t p50;
    uint64_t p95;
    uint64_t p99;
    uint64_t p999;
    uint64_t maximum;
    long double mean;

    if (count == 0U) {
        printf("latency_samples: 0\n");
        return;
    }
    p50 = merged_value_at(get_values,
                          get_count,
                          set_values,
                          set_count,
                          percentile_index(count, 50U, 100U));
    p95 = merged_value_at(get_values,
                          get_count,
                          set_values,
                          set_count,
                          percentile_index(count, 95U, 100U));
    p99 = merged_value_at(get_values,
                          get_count,
                          set_values,
                          set_count,
                          percentile_index(count, 99U, 100U));
    p999 = merged_value_at(get_values,
                           get_count,
                           set_values,
                           set_count,
                           percentile_index(count, 999U, 1000U));
    maximum = get_count == 0U
                  ? set_values[set_count - 1U]
                  : set_count == 0U
                        ? get_values[get_count - 1U]
                        : get_values[get_count - 1U] > set_values[set_count - 1U]
                              ? get_values[get_count - 1U]
                              : set_values[set_count - 1U];
    mean = (latency_sum(get_values, get_count) +
            latency_sum(set_values, set_count)) /
           (long double)count;
    printf("latency_samples: %zu\n", count);
    printf("latency_mean_us: %.3Lf\n", mean / 1000.0L);
    printf("latency_p50_us: %.3f\n", (double)p50 / 1000.0);
    printf("latency_p95_us: %.3f\n", (double)p95 / 1000.0);
    printf("latency_p99_us: %.3f\n", (double)p99 / 1000.0);
    printf("latency_p999_us: %.3f\n", (double)p999 / 1000.0);
    printf("latency_max_us: %.3f\n", (double)maximum / 1000.0);
    printf("latency_p99_over_p50: %.3f\n",
           p50 > 0U ? (double)p99 / (double)p50 : 0.0);
}

static int prepare_worker(worker_t *worker,
                          const benchmark_options_t *options,
                          benchmark_control_t *control,
                          uint64_t run_id,
                          uint64_t request_start,
                          uint64_t request_count,
                          uint64_t preload_start,
                          uint64_t preload_count)
{
    unsigned int worker_id = worker->id;

    memset(worker, 0, sizeof(*worker));
    worker->id = worker_id;
    worker->fd = -1;
    worker->run_id = run_id;
    worker->request_start = request_start;
    worker->request_count = request_count;
    worker->preload_start = preload_start;
    worker->preload_count = preload_count;
    worker->keyspace = options->keyspace;
    worker->warmup_count = options->warmup;
    worker->ttl_ms = options->ttl_ms;
    worker->pipeline_depth = options->pipeline;
    worker->schedule_seed = options->seed;
    worker->latency_enabled = options->latency;
    worker->random_state = mix_u64(options->seed ^ ((uint64_t)worker_id + 1U));
    if (worker->random_state == 0U) worker->random_state = 1U;
    worker->control = control;
    worker->command_buffer_capacity = COMMAND_CAPACITY * options->pipeline;
    worker->command_buffer = malloc(worker->command_buffer_capacity);
    worker->pipeline_operations =
        malloc(sizeof(*worker->pipeline_operations) * options->pipeline);
    worker->receive_buffer = malloc(RECEIVE_BUFFER_SIZE);
    if (worker->latency_enabled) {
        worker->set_latency_capacity = count_set_operations(request_start,
                                                            request_count,
                                                            options->seed);
        worker->get_latency_capacity = request_count -
                                       worker->set_latency_capacity;
        if (worker->get_latency_capacity > SIZE_MAX / sizeof(uint64_t) ||
            worker->set_latency_capacity > SIZE_MAX / sizeof(uint64_t)) {
            errno = EOVERFLOW;
            return -1;
        }
        if (worker->get_latency_capacity > 0U) {
            worker->get_latencies_ns =
                malloc((size_t)worker->get_latency_capacity * sizeof(uint64_t));
        }
        if (worker->set_latency_capacity > 0U) {
            worker->set_latencies_ns =
                malloc((size_t)worker->set_latency_capacity * sizeof(uint64_t));
        }
    }
    if (worker->command_buffer == NULL ||
        worker->pipeline_operations == NULL ||
        worker->receive_buffer == NULL ||
        (worker->get_latency_capacity > 0U &&
         worker->get_latencies_ns == NULL) ||
        (worker->set_latency_capacity > 0U &&
         worker->set_latencies_ns == NULL)) {
        return -1;
    }
    worker->fd = connect_server(options);
    return worker->fd < 0 ? -1 : 0;
}

static void cleanup_worker(worker_t *worker, int remove_keys)
{
    if (worker->fd >= 0) {
        if (remove_keys && worker->error_number == 0 &&
            execute_preload_or_cleanup(worker, PHASE_CLEANUP) != 0) {
            fprintf(stderr,
                    "failed to remove key range for worker %u: %s\n",
                    worker->id,
                    strerror(errno));
        }
        close(worker->fd);
        worker->fd = -1;
    }
    free(worker->receive_buffer);
    free(worker->pipeline_operations);
    free(worker->command_buffer);
    free(worker->get_latencies_ns);
    free(worker->set_latencies_ns);
    worker->receive_buffer = NULL;
    worker->pipeline_operations = NULL;
    worker->command_buffer = NULL;
    worker->get_latencies_ns = NULL;
    worker->set_latencies_ns = NULL;
}

int main(int argc, char **argv)
{
    benchmark_options_t options;
    benchmark_control_t control;
    struct timespec realtime, start, end;
    worker_t *workers = NULL;
    pthread_t *threads = NULL;
    uint64_t request_base, request_remainder, request_start = 0;
    uint64_t preload_base, preload_remainder, preload_start = 0;
    uint64_t run_id;
    uint64_t gets_completed = 0, get_hits = 0, get_misses = 0;
    uint64_t sets_completed = 0, set_errors = 0;
    uint64_t requested_gets, requested_sets;
    uint64_t *get_latencies_ns = NULL;
    uint64_t *set_latencies_ns = NULL;
    size_t get_latency_count = 0;
    size_t set_latency_count = 0;
    cache_snapshot_t before_snapshot;
    cache_snapshot_t after_snapshot;
    int before_snapshot_available = 0;
    int after_snapshot_available = 0;
    unsigned int created_threads = 0;
    unsigned int index;
    int exit_code = 1;
    char client_host[256] = "unknown";

    if (sizeof(VALUE_64) - 1U != VALUE_SIZE) {
        fprintf(stderr, "internal error: value payload is not 64 bytes\n");
        return 1;
    }
    if (parse_options(argc, argv, &options) != 0) {
        print_usage(argv[0]);
        return 2;
    }
    if (pthread_mutex_init(&control.mutex, NULL) != 0) return 1;
    if (pthread_cond_init(&control.condition, NULL) != 0) {
        pthread_mutex_destroy(&control.mutex);
        return 1;
    }
    control.ready_workers = 0;
    control.start = 0;
    control.abort = 0;
    workers = calloc(options.connections, sizeof(*workers));
    threads = calloc(options.connections, sizeof(*threads));
    if (workers == NULL || threads == NULL) goto cleanup;
    for (index = 0; index < options.connections; ++index) workers[index].fd = -1;

    clock_gettime(CLOCK_REALTIME, &realtime);
    run_id = ((uint64_t)(unsigned int)getpid() << 32U) ^
             (uint64_t)realtime.tv_sec ^ (uint64_t)realtime.tv_nsec;
    request_base = options.requests / options.connections;
    request_remainder = options.requests % options.connections;
    preload_base = options.keyspace / options.connections;
    preload_remainder = options.keyspace % options.connections;

    for (index = 0; index < options.connections; ++index) {
        uint64_t request_count = request_base +
                                 (index < request_remainder ? 1U : 0U);
        uint64_t preload_count = preload_base +
                                 (index < preload_remainder ? 1U : 0U);

        workers[index].id = index;
        if (prepare_worker(&workers[index],
                           &options,
                           &control,
                           run_id,
                           request_start,
                           request_count,
                           preload_start,
                           preload_count) != 0) {
            fprintf(stderr,
                    "failed to prepare connection %u: %s\n",
                    index,
                    strerror(errno));
            goto release_workers;
        }
        request_start += request_count;
        preload_start += preload_count;
        if (pthread_create(&threads[index], NULL, worker_main, &workers[index]) != 0) {
            fprintf(stderr, "failed to create worker thread %u\n", index);
            goto release_workers;
        }
        created_threads++;
    }

    pthread_mutex_lock(&control.mutex);
    while (control.ready_workers < options.connections) {
        pthread_cond_wait(&control.condition, &control.mutex);
    }
    for (index = 0; index < options.connections; ++index) {
        if (workers[index].error_number != 0) control.abort = 1;
    }
    if (!control.abort) {
        if (fetch_cache_snapshot(&options, &before_snapshot) == 0) {
            before_snapshot_available = 1;
        } else {
            fprintf(stderr,
                    "warning: failed to read INFO CACHE before timing: %s\n",
                    strerror(errno));
        }
    }
    clock_gettime(CLOCK_MONOTONIC, &start);
    control.start = 1;
    pthread_cond_broadcast(&control.condition);
    pthread_mutex_unlock(&control.mutex);

    for (index = 0; index < created_threads; ++index) pthread_join(threads[index], NULL);
    created_threads = 0;
    clock_gettime(CLOCK_MONOTONIC, &end);
    if (fetch_cache_snapshot(&options, &after_snapshot) == 0) {
        after_snapshot_available = 1;
    } else {
        fprintf(stderr,
                "warning: failed to read INFO CACHE after timing: %s\n",
                strerror(errno));
    }

    for (index = 0; index < options.connections; ++index) {
        gets_completed += workers[index].get_completed;
        get_hits += workers[index].get_hits;
        get_misses += workers[index].get_misses;
        sets_completed += workers[index].set_completed;
        set_errors += workers[index].set_errors;
        if (workers[index].error_number != 0) {
            fprintf(stderr,
                    "worker %u failed after %" PRIu64 " requests: %s\n",
                    index,
                    workers[index].get_completed + workers[index].set_completed,
                    strerror(workers[index].error_number));
        }
    }
    if (gethostname(client_host, sizeof(client_host) - 1U) != 0) {
        strcpy(client_host, "unknown");
    }
    client_host[sizeof(client_host) - 1U] = '\0';
    requested_sets = options.requests / 10U;
    requested_gets = options.requests - requested_sets;
    if (options.latency) {
        size_t get_position = 0;
        size_t set_position = 0;

        for (index = 0; index < options.connections; ++index) {
            if (workers[index].get_latency_count > SIZE_MAX - get_latency_count ||
                workers[index].set_latency_count > SIZE_MAX - set_latency_count) {
                errno = EOVERFLOW;
                goto cleanup;
            }
            get_latency_count += (size_t)workers[index].get_latency_count;
            set_latency_count += (size_t)workers[index].set_latency_count;
        }
        if (get_latency_count > SIZE_MAX / sizeof(uint64_t) ||
            set_latency_count > SIZE_MAX / sizeof(uint64_t)) {
            errno = EOVERFLOW;
            goto cleanup;
        }
        if (get_latency_count > 0U) {
            get_latencies_ns = malloc(get_latency_count * sizeof(uint64_t));
        }
        if (set_latency_count > 0U) {
            set_latencies_ns = malloc(set_latency_count * sizeof(uint64_t));
        }
        if ((get_latency_count > 0U && get_latencies_ns == NULL) ||
            (set_latency_count > 0U && set_latencies_ns == NULL)) {
            fprintf(stderr, "failed to allocate aggregate latency samples\n");
            goto cleanup;
        }
        for (index = 0; index < options.connections; ++index) {
            size_t worker_get_count =
                (size_t)workers[index].get_latency_count;
            size_t worker_set_count =
                (size_t)workers[index].set_latency_count;

            if (worker_get_count > 0U) {
                memcpy(get_latencies_ns + get_position,
                       workers[index].get_latencies_ns,
                       worker_get_count * sizeof(uint64_t));
                get_position += worker_get_count;
            }
            if (worker_set_count > 0U) {
                memcpy(set_latencies_ns + set_position,
                       workers[index].set_latencies_ns,
                       worker_set_count * sizeof(uint64_t));
                set_position += worker_set_count;
            }
        }
        if (get_latency_count > 0U) {
            qsort(get_latencies_ns,
                  get_latency_count,
                  sizeof(uint64_t),
                  compare_u64);
        }
        if (set_latency_count > 0U) {
            qsort(set_latencies_ns,
                  set_latency_count,
                  sizeof(uint64_t),
                  compare_u64);
        }
    }
    {
        uint64_t completed = gets_completed + sets_completed;
        double seconds = elapsed_seconds(&start, &end);
        double qps = seconds > 0.0 ? (double)completed / seconds : 0.0;
        double hit_rate = gets_completed > 0U
                              ? (double)get_hits * 100.0 / (double)gets_completed
                              : 0.0;

        printf("benchmark: RESP2 shared-keyspace 90%% GET / 10%% SET\n");
        printf("server: %s:%u\n", options.server, options.port);
        printf("client_host: %s\n", client_host);
        printf("connections: %u\n", options.connections);
        printf("pipeline_depth: %u\n", options.pipeline);
        printf("keyspace: %" PRIu64 "\n", options.keyspace);
        printf("access_distribution: uniform\n");
        printf("value_bytes: %u\n", VALUE_SIZE);
        printf("ttl_ms: %" PRIu64 "\n", options.ttl_ms);
        printf("random_seed: %" PRIu64 "\n", options.seed);
        printf("warmup_gets_per_connection: %" PRIu64 "\n", options.warmup);
        printf("cleanup: %s\n", options.cleanup ? "enabled" : "disabled");
        printf("latency_measurement: %s\n",
               options.latency ? "enabled" : "disabled");
        printf("requests_requested: %" PRIu64 "\n", options.requests);
        printf("get_requested: %" PRIu64 "\n", requested_gets);
        printf("set_requested: %" PRIu64 "\n", requested_sets);
        printf("requests_completed: %" PRIu64 "\n", completed);
        printf("get_completed: %" PRIu64 "\n", gets_completed);
        printf("get_hits: %" PRIu64 "\n", get_hits);
        printf("get_misses: %" PRIu64 "\n", get_misses);
        printf("get_hit_rate_percent: %.4f\n", hit_rate);
        printf("set_completed: %" PRIu64 "\n", sets_completed);
        printf("set_errors: %" PRIu64 "\n", set_errors);
        if (before_snapshot_available && after_snapshot_available) {
            printf("server_keys_before: %" PRIu64 "\n", before_snapshot.keys);
            printf("server_keys_after: %" PRIu64 "\n", after_snapshot.keys);
            printf("server_used_memory_before: %" PRIu64 "\n",
                   before_snapshot.used_memory);
            printf("server_used_memory_after: %" PRIu64 "\n",
                   after_snapshot.used_memory);
            printf("server_index_memory_before: %" PRIu64 "\n",
                   before_snapshot.index_memory);
            printf("server_index_memory_after: %" PRIu64 "\n",
                   after_snapshot.index_memory);
            printf("server_hits_delta: %" PRIu64 "\n",
                   counter_delta(before_snapshot.hits, after_snapshot.hits));
            printf("server_misses_delta: %" PRIu64 "\n",
                   counter_delta(before_snapshot.misses, after_snapshot.misses));
            printf("server_expired_keys_delta: %" PRIu64 "\n",
                   counter_delta(before_snapshot.expired_keys,
                                 after_snapshot.expired_keys));
            printf("server_evicted_keys_delta: %" PRIu64 "\n",
                   counter_delta(before_snapshot.evicted_keys,
                                 after_snapshot.evicted_keys));
            printf("server_hash_slots_before: %" PRIu64 "\n",
                   before_snapshot.hash_slots);
            printf("server_hash_slots_after: %" PRIu64 "\n",
                   after_snapshot.hash_slots);
            printf("server_rehashing_before: %" PRIu64 "\n",
                   before_snapshot.rehashing);
            printf("server_rehashing_after: %" PRIu64 "\n",
                   after_snapshot.rehashing);
        } else {
            printf("server_cache_stats: unavailable\n");
        }
        printf("duration_seconds: %.6f\n", seconds);
        printf("qps: %.2f\n", qps);
        if (options.latency) {
            printf("latency_unit: microseconds\n");
            printf("latency_clock: CLOCK_MONOTONIC\n");
            printf("latency_definition: pipeline_send_start_to_response_parsed\n");
            print_merged_latency_summary(get_latencies_ns,
                                         get_latency_count,
                                         set_latencies_ns,
                                         set_latency_count);
            print_latency_summary("get",
                                  get_latencies_ns,
                                  get_latency_count);
            print_latency_summary("set",
                                  set_latencies_ns,
                                  set_latency_count);
        }
        printf("tcp_nodelay: on\n");
    }
    exit_code = gets_completed == requested_gets &&
                        sets_completed == requested_sets &&
                        set_errors == 0U
                    ? 0
                    : 1;
    goto cleanup;

release_workers:
    pthread_mutex_lock(&control.mutex);
    control.abort = 1;
    control.start = 1;
    pthread_cond_broadcast(&control.condition);
    pthread_mutex_unlock(&control.mutex);
    for (index = 0; index < created_threads; ++index) pthread_join(threads[index], NULL);

cleanup:
    if (workers != NULL) {
        for (index = 0; index < options.connections; ++index) {
            cleanup_worker(&workers[index], options.cleanup);
        }
    }
    free(threads);
    free(workers);
    free(get_latencies_ns);
    free(set_latencies_ns);
    pthread_cond_destroy(&control.condition);
    pthread_mutex_destroy(&control.mutex);
    return exit_code;
}
