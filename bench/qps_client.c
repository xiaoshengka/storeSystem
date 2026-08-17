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
#define MAX_CONNECTIONS 1024U
#define MAX_PIPELINE 1024U
#define IO_TIMEOUT_SECONDS 10
#define CONNECT_RETRIES 50U
#define CONNECT_RETRY_NS 20000000L

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
} benchmark_options_t;

typedef struct worker {
    unsigned int id;
    int fd;
    uint64_t request_count;
    uint64_t completed;
    uint64_t warmup_count;
    unsigned int pipeline_depth;
    char get_command[256];
    size_t get_command_length;
    char expected_response[256];
    size_t expected_response_length;
    char delete_command[256];
    size_t delete_command_length;
    char *pipeline_buffer;
    int error_number;
    benchmark_control_t *control;
} worker_t;

static void print_usage(const char *program)
{
    fprintf(stderr,
            "Usage: %s [-s IPv4] [-p port] [-c connections] "
            "[-n total_requests] [-w warmup_per_connection] [-P pipeline_depth]\n"
            "Defaults: -s %s -p %u -c %u -n %u -w %u -P %u\n",
            program,
            DEFAULT_SERVER,
            DEFAULT_PORT,
            DEFAULT_CONNECTIONS,
            DEFAULT_REQUESTS,
            DEFAULT_WARMUP,
            DEFAULT_PIPELINE);
}

static int parse_u64(const char *text,
                     uint64_t minimum,
                     uint64_t maximum,
                     uint64_t *value)
{
    char *end = NULL;
    unsigned long long parsed;

    if (text == NULL || *text == '\0' || value == NULL) {
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

    while ((option = getopt(argc, argv, "s:p:c:n:w:P:h")) != -1) {
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
            if (parse_u64(optarg, 1U, UINT64_MAX, &options->requests) != 0) return -1;
            break;
        case 'w':
            if (parse_u64(optarg, 0U, UINT64_MAX, &options->warmup) != 0) return -1;
            break;
        case 'P':
            if (parse_u64(optarg, 1U, MAX_PIPELINE, &value) != 0) return -1;
            options->pipeline = (unsigned int)value;
            break;
        case 'h':
            print_usage(argv[0]);
            exit(0);
        default:
            return -1;
        }
    }
    if (optind != argc || options->requests < options->connections) {
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
    struct timespec retry_delay;
    unsigned int attempt;

    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_port = htons(options->port);
    if (inet_pton(AF_INET, options->server, &address.sin_addr) != 1) {
        errno = EINVAL;
        return -1;
    }
    retry_delay.tv_sec = 0;
    retry_delay.tv_nsec = CONNECT_RETRY_NS;
    for (attempt = 0; attempt < CONNECT_RETRIES; ++attempt) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        int connect_error;

        if (fd < 0) return -1;
        if (set_socket_options(fd) != 0) {
            close(fd);
            return -1;
        }
        if (connect(fd, (struct sockaddr *)&address, sizeof(address)) == 0) {
            return fd;
        }
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

static int receive_expected(int fd, const char *expected, size_t expected_length)
{
    char response[256];
    size_t received = 0;

    if (expected_length > sizeof(response)) {
        errno = EOVERFLOW;
        return -1;
    }
    while (received < expected_length) {
        ssize_t result = recv(fd, response + received, expected_length - received, 0);

        if (result > 0) {
            received += (size_t)result;
        } else if (result < 0 && errno == EINTR) {
            continue;
        } else {
            if (result == 0) errno = ECONNRESET;
            return -1;
        }
    }
    if (memcmp(response, expected, expected_length) != 0) {
        errno = EPROTO;
        return -1;
    }
    return 0;
}

static int execute_request(int fd,
                           const char *command,
                           size_t command_length,
                           const char *expected,
                           size_t expected_length)
{
    if (send_all(fd, command, command_length) != 0) return -1;
    return receive_expected(fd, expected, expected_length);
}

static int format_command(char *output,
                          size_t capacity,
                          const char *command,
                          const char *key,
                          const char *value)
{
    int argument_count = value == NULL ? 2 : 3;
    int written;

    if (value == NULL) {
        written = snprintf(output,
                           capacity,
                           "*%d\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                           argument_count,
                           strlen(command), command,
                           strlen(key), key);
    } else {
        written = snprintf(output,
                           capacity,
                           "*%d\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                           argument_count,
                           strlen(command), command,
                           strlen(key), key,
                           strlen(value), value);
    }
    return written < 0 || (size_t)written >= capacity ? -1 : written;
}

static int execute_batch(worker_t *worker, uint64_t count)
{
    uint64_t completed;
    size_t bytes = worker->get_command_length * (size_t)count;

    if (send_all(worker->fd, worker->pipeline_buffer, bytes) != 0) return -1;
    for (completed = 0; completed < count; ++completed) {
        if (receive_expected(worker->fd,
                             worker->expected_response,
                             worker->expected_response_length) != 0) {
            return -1;
        }
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

static int run_batches(worker_t *worker, uint64_t total, int measured)
{
    uint64_t remaining = total;

    while (remaining > 0) {
        uint64_t batch = remaining < worker->pipeline_depth
                             ? remaining
                             : worker->pipeline_depth;
        if (execute_batch(worker, batch) != 0) return -1;
        if (measured) worker->completed += batch;
        remaining -= batch;
    }
    return 0;
}

static void *worker_main(void *argument)
{
    worker_t *worker = argument;

    if (run_batches(worker, worker->warmup_count, 0) != 0) {
        worker->error_number = errno != 0 ? errno : EIO;
    }
    if (wait_for_start(worker) || worker->error_number != 0) return NULL;
    if (run_batches(worker, worker->request_count, 1) != 0) {
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

static int prepare_worker(worker_t *worker,
                          const benchmark_options_t *options,
                          benchmark_control_t *control,
                          uint64_t run_id,
                          uint64_t request_count)
{
    char set_command[256];
    char key[128];
    char value[96];
    unsigned int worker_id = worker->id;
    int written;
    size_t set_command_length;
    unsigned int index;

    memset(worker, 0, sizeof(*worker));
    worker->id = worker_id;
    worker->fd = -1;
    worker->control = control;
    worker->request_count = request_count;
    worker->warmup_count = options->warmup;
    worker->pipeline_depth = options->pipeline;

    written = snprintf(key, sizeof(key), "bench:%" PRIu64 ":%u", run_id, worker->id);
    if (written < 0 || (size_t)written >= sizeof(key)) return -1;
    written = snprintf(value, sizeof(value), "benchmark-value-%u", worker->id);
    if (written < 0 || (size_t)written >= sizeof(value)) return -1;

    written = format_command(set_command, sizeof(set_command), "SET", key, value);
    if (written < 0) return -1;
    set_command_length = (size_t)written;
    worker->get_command_length = (size_t)format_command(worker->get_command,
                                                        sizeof(worker->get_command),
                                                        "GET", key, NULL);
    if (worker->get_command_length == (size_t)-1) return -1;
    worker->delete_command_length = (size_t)format_command(worker->delete_command,
                                                           sizeof(worker->delete_command),
                                                           "DEL", key, NULL);
    if (worker->delete_command_length == (size_t)-1) return -1;
    written = snprintf(worker->expected_response,
                       sizeof(worker->expected_response),
                       "$%zu\r\n%s\r\n", strlen(value), value);
    if (written < 0 || (size_t)written >= sizeof(worker->expected_response)) return -1;
    worker->expected_response_length = (size_t)written;

    worker->pipeline_buffer = malloc(worker->get_command_length * options->pipeline);
    if (worker->pipeline_buffer == NULL) return -1;
    for (index = 0; index < options->pipeline; ++index) {
        memcpy(worker->pipeline_buffer + index * worker->get_command_length,
               worker->get_command,
               worker->get_command_length);
    }

    worker->fd = connect_server(options);
    if (worker->fd < 0 ||
        execute_request(worker->fd, set_command, set_command_length,
                        "+OK\r\n", sizeof("+OK\r\n") - 1U) != 0) {
        if (worker->fd >= 0) close(worker->fd);
        worker->fd = -1;
        return -1;
    }
    return 0;
}

static void cleanup_worker(worker_t *worker)
{
    if (worker->fd >= 0) {
        if (worker->delete_command_length > 0 && worker->error_number == 0 &&
            execute_request(worker->fd,
                            worker->delete_command,
                            worker->delete_command_length,
                            ":1\r\n",
                            sizeof(":1\r\n") - 1U) != 0) {
            fprintf(stderr, "failed to remove benchmark key for worker %u: %s\n",
                    worker->id, strerror(errno));
        }
        close(worker->fd);
        worker->fd = -1;
    }
    free(worker->pipeline_buffer);
    worker->pipeline_buffer = NULL;
}

int main(int argc, char **argv)
{
    benchmark_options_t options;
    benchmark_control_t control;
    struct timespec realtime, start, end;
    worker_t *workers = NULL;
    pthread_t *threads = NULL;
    uint64_t base_requests, remainder, run_id, completed = 0;
    unsigned int created_threads = 0;
    unsigned int index;
    int exit_code = 1;
    char client_host[256] = "unknown";

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

    clock_gettime(CLOCK_REALTIME, &realtime);
    run_id = ((uint64_t)(unsigned int)getpid() << 32U) ^
             (uint64_t)realtime.tv_sec ^ (uint64_t)realtime.tv_nsec;
    base_requests = options.requests / options.connections;
    remainder = options.requests % options.connections;

    for (index = 0; index < options.connections; ++index) {
        workers[index].id = index;
        if (prepare_worker(&workers[index], &options,
                           &control, run_id,
                           base_requests + (index < remainder ? 1U : 0U)) != 0) {
            fprintf(stderr, "failed to prepare connection %u: %s\n", index, strerror(errno));
            goto release_workers;
        }
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
    clock_gettime(CLOCK_MONOTONIC, &start);
    control.start = 1;
    pthread_cond_broadcast(&control.condition);
    pthread_mutex_unlock(&control.mutex);

    for (index = 0; index < created_threads; ++index) pthread_join(threads[index], NULL);
    created_threads = 0;
    clock_gettime(CLOCK_MONOTONIC, &end);

    for (index = 0; index < options.connections; ++index) {
        completed += workers[index].completed;
        if (workers[index].error_number != 0) {
            fprintf(stderr, "worker %u failed after %" PRIu64 " requests: %s\n",
                    index, workers[index].completed,
                    strerror(workers[index].error_number));
        }
    }
    if (gethostname(client_host, sizeof(client_host) - 1U) != 0) {
        strcpy(client_host, "unknown");
    }
    client_host[sizeof(client_host) - 1U] = '\0';
    {
        double seconds = elapsed_seconds(&start, &end);
        double qps = seconds > 0.0 ? (double)completed / seconds : 0.0;

        printf("benchmark: RESP2 GET hit\n");
        printf("server: %s:%u\n", options.server, options.port);
        printf("client_host: %s\n", client_host);
        printf("connections: %u\n", options.connections);
        printf("pipeline_depth: %u\n", options.pipeline);
        printf("warmup_per_connection: %" PRIu64 "\n", options.warmup);
        printf("requests_requested: %" PRIu64 "\n", options.requests);
        printf("requests_completed: %" PRIu64 "\n", completed);
        printf("duration_seconds: %.6f\n", seconds);
        printf("qps: %.2f\n", qps);
        printf("tcp_nodelay: on\n");
    }
    exit_code = completed == options.requests ? 0 : 1;
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
        for (index = 0; index < options.connections; ++index) cleanup_worker(&workers[index]);
    }
    free(threads);
    free(workers);
    pthread_cond_destroy(&control.condition);
    pthread_mutex_destroy(&control.mutex);
    return exit_code;
}
