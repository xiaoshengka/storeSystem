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

#define COMMAND_CAPACITY 512U
#define LINE_CAPACITY 128U

typedef enum workload {
    WORKLOAD_HASH_INSERT,
    WORKLOAD_HGET,
    WORKLOAD_HASH_MIXED,
    WORKLOAD_ZSET_INSERT,
    WORKLOAD_ZSCORE,
    WORKLOAD_ZSET_MIXED,
    WORKLOAD_ZRANGE
} workload_t;

typedef enum reply_kind {
    REPLY_INTEGER,
    REPLY_INTEGER_ZERO,
    REPLY_INTEGER_ONE,
    REPLY_BULK_NONNULL,
    REPLY_ARRAY_TEN
} reply_kind_t;

typedef struct options {
    char host[INET_ADDRSTRLEN];
    uint16_t port;
    unsigned int connections;
    unsigned int pipeline;
    uint64_t requests;
    uint64_t warmup;
    uint64_t keyspace;
    uint64_t seed;
    workload_t workload;
    int keep_data;
} options_t;

typedef struct control {
    pthread_mutex_t mutex;
    pthread_cond_t condition;
    unsigned int ready;
    int start;
    int abort;
} control_t;

typedef struct worker {
    unsigned int id;
    const options_t *options;
    control_t *control;
    int fd;
    uint64_t first_request;
    uint64_t requests;
    uint64_t completed;
    uint64_t errors;
    uint64_t *latencies;
    char *commands;
    reply_kind_t *expected;
    int error_number;
} worker_t;

static uint64_t mix64(uint64_t value)
{
    value ^= value >> 30;
    value *= UINT64_C(0xbf58476d1ce4e5b9);
    value ^= value >> 27;
    value *= UINT64_C(0x94d049bb133111eb);
    return value ^ (value >> 31);
}

static uint64_t elapsed_ns(const struct timespec *start, const struct timespec *end)
{
    return (uint64_t)(end->tv_sec - start->tv_sec) * UINT64_C(1000000000) +
           (uint64_t)(end->tv_nsec - start->tv_nsec);
}

static int parse_u64(const char *text, uint64_t minimum, uint64_t maximum,
                     uint64_t *value)
{
    char *end = NULL;
    unsigned long long parsed;

    if (text == NULL || *text == '\0') return -1;
    errno = 0;
    parsed = strtoull(text, &end, 10);
    if (errno != 0 || *end != '\0' || parsed < minimum || parsed > maximum)
        return -1;
    *value = (uint64_t)parsed;
    return 0;
}

static int parse_workload(const char *name, workload_t *workload)
{
    static const struct { const char *name; workload_t value; } names[] = {
        {"hash-insert", WORKLOAD_HASH_INSERT}, {"hget", WORKLOAD_HGET},
        {"hash-mixed", WORKLOAD_HASH_MIXED}, {"zset-insert", WORKLOAD_ZSET_INSERT},
        {"zscore", WORKLOAD_ZSCORE}, {"zset-mixed", WORKLOAD_ZSET_MIXED},
        {"zrange", WORKLOAD_ZRANGE}
    };
    size_t index;

    for (index = 0; index < sizeof(names) / sizeof(names[0]); ++index) {
        if (strcmp(name, names[index].name) == 0) {
            *workload = names[index].value;
            return 0;
        }
    }
    return -1;
}

static const char *workload_name(workload_t workload)
{
    static const char *names[] = {
        "hash-insert", "hget", "hash-mixed", "zset-insert",
        "zscore", "zset-mixed", "zrange"
    };
    return names[(int)workload];
}

static int workload_is_hash(workload_t workload)
{
    return workload == WORKLOAD_HASH_INSERT || workload == WORKLOAD_HGET ||
           workload == WORKLOAD_HASH_MIXED;
}

static int workload_is_insert(workload_t workload)
{
    return workload == WORKLOAD_HASH_INSERT ||
           workload == WORKLOAD_ZSET_INSERT;
}

static int workload_needs_preload(workload_t workload)
{
    return !workload_is_insert(workload);
}

static int format_dataset_key(char *output,
                              size_t capacity,
                              const options_t *options)
{
    int written = snprintf(output,
                           capacity,
                           "collection:%s:%" PRIu64,
                           workload_is_hash(options->workload) ? "hash" : "zset",
                           options->seed);

    return written < 0 || (size_t)written >= capacity ? -1 : written;
}

static int format_warmup_key(char *output,
                             size_t capacity,
                             const options_t *options,
                             unsigned int worker_id)
{
    int written = snprintf(output,
                           capacity,
                           "collection:warmup:%s:%u:%" PRIu64,
                           workload_is_hash(options->workload) ? "hash" : "zset",
                           worker_id,
                           options->seed);

    return written < 0 || (size_t)written >= capacity ? -1 : written;
}

static void usage(const char *program)
{
    fprintf(stderr,
            "Usage: %s -m WORKLOAD [-s IPv4] [-p port] [-c connections] "
            "[-n requests] [-w warmup] [-P pipeline] [-k keyspace] [-S seed] [-C]\n"
            "WORKLOAD: hash-insert|hget|hash-mixed|zset-insert|zscore|"
            "zset-mixed|zrange\n"
            "  keyspace is one shared field/member range for read/mixed/range;\n"
            "  insert workloads start empty and grow to total requests.\n",
            program);
}

static int parse_options(int argc, char **argv, options_t *options)
{
    int option;
    uint64_t value;
    int have_mode = 0;

    memset(options, 0, sizeof(*options));
    strcpy(options->host, "127.0.0.1");
    options->port = 9096;
    options->connections = 16U;
    options->pipeline = 16U;
    options->requests = 100000U;
    options->warmup = 100U;
    options->keyspace = 1000U;
    options->seed = 20260820U;
    while ((option = getopt(argc, argv, "m:s:p:c:n:w:P:k:S:Ch")) != -1) {
        switch (option) {
        case 'm': have_mode = parse_workload(optarg, &options->workload) == 0; break;
        case 's':
            if (strlen(optarg) >= sizeof(options->host)) return -1;
            strcpy(options->host, optarg);
            break;
        case 'p':
            if (parse_u64(optarg, 1U, UINT16_MAX, &value) != 0) return -1;
            options->port = (uint16_t)value;
            break;
        case 'c':
            if (parse_u64(optarg, 1U, 1024U, &value) != 0) return -1;
            options->connections = (unsigned int)value;
            break;
        case 'n':
            if (parse_u64(optarg, 1U, UINT64_MAX, &options->requests) != 0) return -1;
            break;
        case 'w':
            if (parse_u64(optarg, 0U, UINT64_MAX, &options->warmup) != 0) return -1;
            break;
        case 'P':
            if (parse_u64(optarg, 1U, 1024U, &value) != 0) return -1;
            options->pipeline = (unsigned int)value;
            break;
        case 'k':
            if (parse_u64(optarg, 10U, UINT64_MAX, &options->keyspace) != 0) return -1;
            break;
        case 'S':
            if (parse_u64(optarg, 1U, UINT64_MAX, &options->seed) != 0) return -1;
            break;
        case 'C': options->keep_data = 1; break;
        case 'h': usage(argv[0]); exit(0);
        default: return -1;
        }
    }
    return have_mode && optind == argc &&
           options->requests >= options->connections ? 0 : -1;
}

static int connect_server(const options_t *options)
{
    struct sockaddr_in address;
    struct timeval timeout = {10, 0};
    int enabled = 1;
    int fd = socket(AF_INET, SOCK_STREAM, 0);

    if (fd < 0) return -1;
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_port = htons(options->port);
    if (inet_pton(AF_INET, options->host, &address.sin_addr) != 1 ||
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled)) != 0 ||
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) != 0 ||
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) != 0 ||
        connect(fd, (struct sockaddr *)&address, sizeof(address)) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int send_all(int fd, const char *data, size_t length)
{
    size_t sent = 0;
    while (sent < length) {
        ssize_t result = send(fd, data + sent, length - sent, MSG_NOSIGNAL);
        if (result > 0) sent += (size_t)result;
        else if (result < 0 && errno == EINTR) continue;
        else return -1;
    }
    return 0;
}

static int receive_exact(int fd, void *data, size_t length)
{
    size_t received = 0;
    while (received < length) {
        ssize_t result = recv(fd, (char *)data + received, length - received, 0);
        if (result > 0) received += (size_t)result;
        else if (result < 0 && errno == EINTR) continue;
        else {
            if (result == 0) errno = ECONNRESET;
            return -1;
        }
    }
    return 0;
}

static int receive_line(int fd, char *line, size_t capacity)
{
    size_t used = 0;
    char byte;
    while (used + 1U < capacity) {
        if (receive_exact(fd, &byte, 1U) != 0) return -1;
        if (byte == '\r') {
            if (receive_exact(fd, &byte, 1U) != 0 || byte != '\n') return -1;
            line[used] = '\0';
            return 0;
        }
        line[used++] = byte;
    }
    errno = EOVERFLOW;
    return -1;
}

static int receive_reply(int fd, reply_kind_t expected, int nested)
{
    char prefix;
    char line[128];
    long long count;
    char *end;

    if (receive_exact(fd, &prefix, 1U) != 0 ||
        receive_line(fd, line, sizeof(line)) != 0) return -1;
    if (prefix == '-') return nested ? -1 : 1;
    if (prefix == '+') return nested ? 0 : 1;
    if (prefix == ':') {
        count = strtoll(line, &end, 10);
        if (*end != '\0') return -1;
        if (nested) return 0;
        if (expected == REPLY_INTEGER) return 0;
        if (expected == REPLY_INTEGER_ZERO) return count == 0 ? 0 : 1;
        if (expected == REPLY_INTEGER_ONE) return count == 1 ? 0 : 1;
        return 1;
    }
    if (prefix == '$') {
        count = strtoll(line, &end, 10);
        if (*end != '\0' || count < -1) return -1;
        if (count >= 0) {
            char buffer[4096];
            uint64_t remaining = (uint64_t)count + 2U;
            while (remaining > 0U) {
                size_t part = remaining < sizeof(buffer)
                                  ? (size_t)remaining : sizeof(buffer);
                if (receive_exact(fd, buffer, part) != 0) return -1;
                remaining -= part;
            }
        }
        return nested || (expected == REPLY_BULK_NONNULL && count >= 0) ? 0 : 1;
    }
    if (prefix == '*') {
        int matches_expected;

        count = strtoll(line, &end, 10);
        if (*end != '\0' || count < 0) return -1;
        matches_expected = nested ||
                           (expected == REPLY_ARRAY_TEN && count == 10);
        while (count-- > 0)
            if (receive_reply(fd, REPLY_BULK_NONNULL, 1) != 0) return -1;
        return matches_expected ? 0 : 1;
    }
    errno = EPROTO;
    return -1;
}

static int format_command(char *output, size_t capacity, const char *key,
                          workload_t workload, uint64_t sequence,
                          uint64_t field_index, int measured,
                          reply_kind_t *expected)
{
    const char *command;
    char field[48];
    char score[48];
    static const char base_value[] =
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    char value[sizeof(base_value)];
    char sequence_hex[17];
    int mixed_write = 0;
    int written;

    if (workload == WORKLOAD_HASH_MIXED) {
        mixed_write = sequence % 10U == 0U;
        workload = mixed_write ? WORKLOAD_HASH_INSERT : WORKLOAD_HGET;
    } else if (workload == WORKLOAD_ZSET_MIXED) {
        mixed_write = sequence % 10U == 0U;
        workload = mixed_write ? WORKLOAD_ZSET_INSERT : WORKLOAD_ZSCORE;
    }
    memcpy(value, base_value, sizeof(value));
    if (mixed_write) {
        if (snprintf(sequence_hex, sizeof(sequence_hex), "%016" PRIx64,
                     sequence) != 16)
            return -1;
        memcpy(value, sequence_hex, 16);
        value[16] = measured ? 't' : 'w';
    }
    snprintf(field, sizeof(field), "item-%" PRIu64, field_index);
    snprintf(score, sizeof(score), "%" PRIu64 ".%u",
             field_index, (unsigned int)(sequence % 10U));
    if (workload == WORKLOAD_ZRANGE) {
        *expected = REPLY_ARRAY_TEN;
        written = snprintf(output, capacity,
                           "*4\r\n$6\r\nZRANGE\r\n$%zu\r\n%s\r\n"
                           "$1\r\n0\r\n$1\r\n9\r\n", strlen(key), key);
    } else if (workload == WORKLOAD_HASH_INSERT) {
        *expected = mixed_write ? REPLY_INTEGER_ZERO : REPLY_INTEGER_ONE;
        written = snprintf(output, capacity,
                           "*4\r\n$4\r\nHSET\r\n$%zu\r\n%s\r\n"
                           "$%zu\r\n%s\r\n$64\r\n%s\r\n",
                           strlen(key), key, strlen(field), field, value);
    } else if (workload == WORKLOAD_ZSET_INSERT) {
        *expected = mixed_write ? REPLY_INTEGER_ZERO : REPLY_INTEGER_ONE;
        written = snprintf(output, capacity,
                           "*4\r\n$4\r\nZADD\r\n$%zu\r\n%s\r\n"
                           "$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                           strlen(key), key, strlen(score), score,
                           strlen(field), field);
    } else {
        command = workload == WORKLOAD_HGET ? "HGET" : "ZSCORE";
        *expected = REPLY_BULK_NONNULL;
        written = snprintf(output, capacity,
                           "*3\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                           strlen(command), command, strlen(key), key,
                           strlen(field), field);
    }
    return written < 0 || (size_t)written >= capacity ? -1 : written;
}

static int run_requests(worker_t *worker,
                        const char *key,
                        uint64_t first,
                        uint64_t count,
                        int preload,
                        int measure)
{
    uint64_t offset = 0;
    const options_t *options = worker->options;

    while (offset < count) {
        unsigned int batch = count - offset < options->pipeline
                                 ? (unsigned int)(count - offset)
                                 : options->pipeline;
        size_t used = 0;
        unsigned int index;
        struct timespec start;

        for (index = 0; index < batch; ++index) {
            uint64_t sequence = first + offset + index;
            uint64_t field = preload ? offset + index
                                     : mix64(sequence ^ options->seed) %
                                           options->keyspace;
            workload_t mode = options->workload;
            int length;

            if (!preload && workload_is_insert(mode)) field = sequence;
            if (preload)
                mode = mode == WORKLOAD_HGET || mode == WORKLOAD_HASH_MIXED
                           ? WORKLOAD_HASH_INSERT : WORKLOAD_ZSET_INSERT;
            length = format_command(worker->commands + used,
                                    COMMAND_CAPACITY * options->pipeline - used,
                                    key, mode, sequence, field, measure,
                                    &worker->expected[index]);
            if (length < 0) return -1;
            used += (size_t)length;
        }
        if (clock_gettime(CLOCK_MONOTONIC, &start) != 0 ||
            send_all(worker->fd, worker->commands, used) != 0) return -1;
        for (index = 0; index < batch; ++index) {
            struct timespec end;
            int response = receive_reply(worker->fd, worker->expected[index], 0);
            if (response < 0) return -1;
            if (response > 0) worker->errors++;
            if (measure) {
                if (clock_gettime(CLOCK_MONOTONIC, &end) != 0) return -1;
                worker->latencies[worker->completed++] = elapsed_ns(&start, &end);
            }
        }
        offset += batch;
    }
    return 0;
}

static void delete_key(worker_t *worker, const char *key)
{
    char command[256];
    int length;

    length = snprintf(command, sizeof(command),
                      "*2\r\n$3\r\nDEL\r\n$%zu\r\n%s\r\n", strlen(key), key);
    if (length > 0 && send_all(worker->fd, command, (size_t)length) == 0)
        (void)receive_reply(worker->fd, REPLY_INTEGER, 0);
}

static void *worker_main(void *argument)
{
    worker_t *worker = argument;
    const options_t *options = worker->options;
    char dataset_key[96];
    char warmup_key[128];

    if (format_dataset_key(dataset_key, sizeof(dataset_key), options) < 0)
        worker->error_number = errno != 0 ? errno : EIO;
    if (worker->error_number == 0 && options->warmup > 0U) {
        const char *key = dataset_key;

        if (workload_is_insert(options->workload)) {
            if (format_warmup_key(warmup_key,
                                  sizeof(warmup_key),
                                  options,
                                  worker->id) < 0) {
                worker->error_number = EOVERFLOW;
            } else {
                key = warmup_key;
            }
        }
        if (worker->error_number == 0 &&
            run_requests(worker, key, 0U, options->warmup, 0, 0) != 0) {
            worker->error_number = errno != 0 ? errno : EIO;
        }
        if (worker->error_number == 0 && key == warmup_key) {
            delete_key(worker, warmup_key);
        }
    }
    pthread_mutex_lock(&worker->control->mutex);
    worker->control->ready++;
    pthread_cond_broadcast(&worker->control->condition);
    while (!worker->control->start)
        pthread_cond_wait(&worker->control->condition, &worker->control->mutex);
    pthread_mutex_unlock(&worker->control->mutex);
    if (worker->error_number == 0 && !worker->control->abort &&
        run_requests(worker,
                     dataset_key,
                     worker->first_request,
                     worker->requests,
                     0,
                     1) != 0)
        worker->error_number = errno != 0 ? errno : EIO;
    return NULL;
}

static int preload_dataset(const options_t *options)
{
    worker_t setup;
    char dataset_key[96];
    int result = -1;

    if (!workload_needs_preload(options->workload)) return 0;
    memset(&setup, 0, sizeof(setup));
    setup.options = options;
    setup.fd = -1;
    if (format_dataset_key(dataset_key, sizeof(dataset_key), options) < 0)
        return -1;
    setup.fd = connect_server(options);
    setup.commands = malloc(COMMAND_CAPACITY * options->pipeline);
    setup.expected = malloc(sizeof(*setup.expected) * options->pipeline);
    if (setup.fd >= 0 && setup.commands != NULL && setup.expected != NULL &&
        run_requests(&setup,
                     dataset_key,
                     0U,
                     options->keyspace,
                     1,
                     0) == 0 &&
        setup.errors == 0U) {
        result = 0;
    }
    if (setup.fd >= 0) close(setup.fd);
    free(setup.commands);
    free(setup.expected);
    return result;
}

static int compare_u64(const void *left, const void *right)
{
    uint64_t a = *(const uint64_t *)left;
    uint64_t b = *(const uint64_t *)right;
    return a < b ? -1 : a > b;
}

static size_t percentile(size_t count, size_t numerator, size_t denominator)
{
    size_t rank = (count * numerator + denominator - 1U) / denominator;
    return rank == 0U ? 0U : rank - 1U;
}

int main(int argc, char **argv)
{
    options_t options;
    control_t control;
    worker_t *workers = NULL;
    pthread_t *threads = NULL;
    uint64_t *latencies = NULL;
    struct timespec start = {0}, end = {0};
    uint64_t completed = 0, errors = 0, first = 0;
    char dataset_key[96];
    unsigned int index, created = 0;
    int status = 1;

    if (parse_options(argc, argv, &options) != 0) {
        usage(argv[0]);
        return 2;
    }
    memset(&control, 0, sizeof(control));
    if (pthread_mutex_init(&control.mutex, NULL) != 0 ||
        pthread_cond_init(&control.condition, NULL) != 0) return 1;
    workers = calloc(options.connections, sizeof(*workers));
    threads = calloc(options.connections, sizeof(*threads));
    latencies = malloc((size_t)options.requests * sizeof(*latencies));
    if (workers == NULL || threads == NULL || latencies == NULL) goto cleanup;
    if (format_dataset_key(dataset_key, sizeof(dataset_key), &options) < 0 ||
        preload_dataset(&options) != 0) {
        fprintf(stderr, "failed to preload shared keyspace: %s\n", strerror(errno));
        goto cleanup;
    }
    for (index = 0; index < options.connections; ++index) {
        uint64_t count = options.requests / options.connections +
                         (index < options.requests % options.connections);
        worker_t *worker = &workers[index];

        worker->id = index;
        worker->options = &options;
        worker->control = &control;
        worker->first_request = first;
        worker->requests = count;
        worker->latencies = latencies + first;
        worker->fd = connect_server(&options);
        worker->commands = malloc(COMMAND_CAPACITY * options.pipeline);
        worker->expected = malloc(sizeof(*worker->expected) * options.pipeline);
        if (worker->fd < 0 || worker->commands == NULL ||
            worker->expected == NULL ||
            pthread_create(&threads[index], NULL, worker_main, worker) != 0)
            goto stop;
        created++;
        first += count;
    }
    pthread_mutex_lock(&control.mutex);
    while (control.ready < options.connections)
        pthread_cond_wait(&control.condition, &control.mutex);
    for (index = 0; index < options.connections; ++index)
        if (workers[index].error_number != 0) control.abort = 1;
    clock_gettime(CLOCK_MONOTONIC, &start);
    control.start = 1;
    pthread_cond_broadcast(&control.condition);
    pthread_mutex_unlock(&control.mutex);
stop:
    if (created < options.connections) {
        pthread_mutex_lock(&control.mutex);
        control.abort = 1;
        control.start = 1;
        pthread_cond_broadcast(&control.condition);
        pthread_mutex_unlock(&control.mutex);
    }
    for (index = 0; index < created; ++index)
        pthread_join(threads[index], NULL);
    clock_gettime(CLOCK_MONOTONIC, &end);
    if (!options.keep_data && created > 0U && workers[0].fd >= 0)
        delete_key(&workers[0], dataset_key);
    for (index = 0; index < options.connections; ++index) {
        completed += workers[index].completed;
        errors += workers[index].errors + (workers[index].error_number != 0);
    }
    if (completed > 0U) {
        long double sum = 0.0L;
        double seconds = (double)elapsed_ns(&start, &end) / 1e9;

        qsort(latencies, (size_t)completed, sizeof(*latencies), compare_u64);
        for (first = 0; first < completed; ++first) sum += latencies[first];
        printf("workload: %s\ncompleted: %" PRIu64 "\nerrors: %" PRIu64 "\n",
               workload_name(options.workload), completed, errors);
        printf("dataset_key: %s\nkeyspace_scope: shared\nkeyspace: %" PRIu64 "\n",
               dataset_key,
               options.keyspace);
        printf("initial_cardinality: %" PRIu64 "\n"
               "expected_final_cardinality: %" PRIu64 "\n",
               workload_needs_preload(options.workload) ? options.keyspace : 0U,
               workload_is_insert(options.workload)
                   ? options.requests
                   : options.keyspace);
        printf("elapsed_seconds: %.6f\nqps: %.2f\n", seconds, completed / seconds);
        printf("latency_mean_us: %.3Lf\n", sum / completed / 1000.0L);
        printf("latency_p50_us: %.3f\nlatency_p95_us: %.3f\n",
               latencies[percentile((size_t)completed, 50U, 100U)] / 1000.0,
               latencies[percentile((size_t)completed, 95U, 100U)] / 1000.0);
        printf("latency_p99_us: %.3f\nlatency_p999_us: %.3f\n"
               "latency_max_us: %.3f\n",
               latencies[percentile((size_t)completed, 99U, 100U)] / 1000.0,
               latencies[percentile((size_t)completed, 999U, 1000U)] / 1000.0,
               latencies[completed - 1U] / 1000.0);
        status = completed == options.requests && errors == 0U ? 0 : 1;
    }
cleanup:
    if (workers != NULL) for (index = 0; index < options.connections; ++index) {
        if (workers[index].fd >= 0) close(workers[index].fd);
        free(workers[index].commands);
        free(workers[index].expected);
    }
    free(latencies);
    free(threads);
    free(workers);
    pthread_cond_destroy(&control.condition);
    pthread_mutex_destroy(&control.mutex);
    return status;
}
