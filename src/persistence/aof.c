#define _POSIX_C_SOURCE 200809L

#include "persistence/aof.h"

#include "protocol/resp.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define AOF_MAX_RECORD_SIZE (RESP_MAX_FRAME_SIZE + 1024U)
#define AOF_BUFFER_CAPACITY (256U * 1024U)
#define AOF_FLUSH_THRESHOLD (64U * 1024U)

struct aof {
    int fd;
    aof_fsync_policy_t fsync_policy;
    unsigned char *buffer;
    size_t buffer_used;
    size_t buffer_capacity;
    uint64_t last_sync_request_ms;
    int failed;
    int last_error;

    pthread_t sync_thread;
    pthread_mutex_t sync_mutex;
    pthread_cond_t sync_condition;
    int sync_thread_started;
    int sync_stop;
    int sync_requested;
    uint64_t write_generation;
    uint64_t requested_generation;
    uint64_t syncing_generation;
    uint64_t synced_generation;
    atomic_int async_error;
};

static uint64_t monotonic_now_ms(void)
{
    struct timespec value;

    if (clock_gettime(CLOCK_MONOTONIC, &value) != 0) {
        return 0;
    }
    return (uint64_t)value.tv_sec * 1000U +
           (uint64_t)value.tv_nsec / 1000000U;
}

static void remember_failure(aof_t *aof, int error_number)
{
    if (!aof->failed) {
        aof->failed = 1;
        aof->last_error = error_number == 0 ? EIO : error_number;
    }
}

static int check_async_failure(aof_t *aof)
{
    int error_number;

    if (aof->failed) {
        errno = aof->last_error;
        return -1;
    }
    error_number = atomic_load_explicit(&aof->async_error,
                                        memory_order_acquire);
    if (error_number != 0) {
        remember_failure(aof, error_number);
        errno = aof->last_error;
        return -1;
    }
    return 0;
}

static size_t decimal_length(size_t value)
{
    size_t length = 1U;

    while (value >= 10U) {
        value /= 10U;
        length++;
    }
    return length;
}

static int checked_add(size_t *total, size_t amount)
{
    if (amount > SIZE_MAX - *total) {
        return -1;
    }
    *total += amount;
    return 0;
}

static int encoded_size(const aof_argument_t *arguments,
                        size_t argument_count,
                        size_t *result)
{
    size_t total;
    size_t index;

    if (arguments == NULL || result == NULL || argument_count == 0 ||
        argument_count > RESP_MAX_ARGUMENTS) {
        errno = EINVAL;
        return -1;
    }
    total = 1U + decimal_length(argument_count) + 2U;
    for (index = 0; index < argument_count; ++index) {
        if (arguments[index].data == NULL && arguments[index].length != 0) {
            errno = EINVAL;
            return -1;
        }
        if (checked_add(&total,
                        1U + decimal_length(arguments[index].length) + 2U) != 0 ||
            checked_add(&total, arguments[index].length) != 0 ||
            checked_add(&total, 2U) != 0) {
            errno = EOVERFLOW;
            return -1;
        }
    }
    if (total > AOF_MAX_RECORD_SIZE) {
        errno = EFBIG;
        return -1;
    }
    *result = total;
    return 0;
}

static int encode_record(unsigned char *output,
                         size_t capacity,
                         const aof_argument_t *arguments,
                         size_t argument_count,
                         size_t expected_length)
{
    size_t position;
    size_t index;
    int written;

    if (output == NULL || capacity < expected_length) {
        errno = ENOBUFS;
        return -1;
    }
    written = snprintf((char *)output, capacity, "*%zu\r\n", argument_count);
    if (written < 0 || (size_t)written >= capacity) {
        errno = EINVAL;
        return -1;
    }
    position = (size_t)written;
    for (index = 0; index < argument_count; ++index) {
        written = snprintf((char *)output + position,
                           capacity - position,
                           "$%zu\r\n",
                           arguments[index].length);
        if (written < 0 || (size_t)written >= capacity - position) {
            errno = EINVAL;
            return -1;
        }
        position += (size_t)written;
        if (arguments[index].length > 0) {
            memcpy(output + position,
                   arguments[index].data,
                   arguments[index].length);
        }
        position += arguments[index].length;
        output[position++] = '\r';
        output[position++] = '\n';
    }
    if (position != expected_length) {
        errno = EINVAL;
        return -1;
    }
    return 0;
}

static int write_all(aof_t *aof, const unsigned char *data, size_t length)
{
    size_t written = 0;

    while (written < length) {
        ssize_t result = write(aof->fd, data + written, length - written);

        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            remember_failure(aof, errno);
            return -1;
        }
        if (result == 0) {
            remember_failure(aof, EIO);
            errno = EIO;
            return -1;
        }
        written += (size_t)result;
    }
    return 0;
}

static void note_completed_write(aof_t *aof)
{
    if (aof->sync_thread_started) {
        pthread_mutex_lock(&aof->sync_mutex);
        aof->write_generation++;
        pthread_mutex_unlock(&aof->sync_mutex);
    } else {
        aof->write_generation++;
    }
}

static int sync_file_direct(aof_t *aof)
{
    if (fdatasync(aof->fd) != 0) {
        remember_failure(aof, errno);
        return -1;
    }
    aof->synced_generation = aof->write_generation;
    aof->last_sync_request_ms = monotonic_now_ms();
    return 0;
}

static void publish_async_error(aof_t *aof, int error_number)
{
    int expected = 0;

    if (error_number == 0) {
        error_number = EIO;
    }
    (void)atomic_compare_exchange_strong_explicit(&aof->async_error,
                                                  &expected,
                                                  error_number,
                                                  memory_order_release,
                                                  memory_order_relaxed);
}

static void *sync_worker(void *context)
{
    aof_t *aof = context;

    for (;;) {
        uint64_t target;
        int result;
        int error_number;

        pthread_mutex_lock(&aof->sync_mutex);
        while (!aof->sync_requested && !aof->sync_stop) {
            pthread_cond_wait(&aof->sync_condition, &aof->sync_mutex);
        }
        if (!aof->sync_requested && aof->sync_stop) {
            pthread_mutex_unlock(&aof->sync_mutex);
            break;
        }
        target = aof->requested_generation;
        aof->sync_requested = 0;
        aof->syncing_generation = target;
        pthread_mutex_unlock(&aof->sync_mutex);

        result = fdatasync(aof->fd);
        error_number = errno;

        pthread_mutex_lock(&aof->sync_mutex);
        aof->syncing_generation = 0;
        if (result == 0 && target > aof->synced_generation) {
            aof->synced_generation = target;
        }
        pthread_cond_broadcast(&aof->sync_condition);
        pthread_mutex_unlock(&aof->sync_mutex);

        if (result != 0) {
            publish_async_error(aof, error_number);
            break;
        }
    }
    return NULL;
}

static int start_sync_worker(aof_t *aof)
{
    int result;

    if (aof->fsync_policy != AOF_FSYNC_EVERYSEC) {
        return 0;
    }
    result = pthread_create(&aof->sync_thread, NULL, sync_worker, aof);
    if (result != 0) {
        errno = result;
        return -1;
    }
    aof->sync_thread_started = 1;
    return 0;
}

static void request_sync_locked(aof_t *aof, uint64_t generation)
{
    if (generation > aof->requested_generation) {
        aof->requested_generation = generation;
    }
    aof->sync_requested = 1;
    pthread_cond_signal(&aof->sync_condition);
}

static int stop_sync_worker(aof_t *aof, int request_final_sync)
{
    int result;

    if (!aof->sync_thread_started) {
        return 0;
    }
    pthread_mutex_lock(&aof->sync_mutex);
    if (request_final_sync &&
        aof->write_generation > aof->synced_generation &&
        aof->write_generation > aof->syncing_generation) {
        request_sync_locked(aof, aof->write_generation);
    }
    aof->sync_stop = 1;
    pthread_cond_broadcast(&aof->sync_condition);
    pthread_mutex_unlock(&aof->sync_mutex);

    result = pthread_join(aof->sync_thread, NULL);
    aof->sync_thread_started = 0;
    if (result != 0) {
        errno = result;
        return -1;
    }
    return 0;
}

int aof_open(aof_t **out_aof,
             const char *path,
             aof_fsync_policy_t fsync_policy)
{
    aof_t *aof;
    int fd;
    int result;

    if (out_aof == NULL || path == NULL || path[0] == '\0' ||
        (fsync_policy != AOF_FSYNC_ALWAYS &&
         fsync_policy != AOF_FSYNC_EVERYSEC &&
         fsync_policy != AOF_FSYNC_NO)) {
        errno = EINVAL;
        return -1;
    }
    *out_aof = NULL;
    fd = open(path, O_CREAT | O_RDWR | O_APPEND | O_CLOEXEC, 0644);
    if (fd < 0) {
        return -1;
    }
    aof = calloc(1, sizeof(*aof));
    if (aof == NULL) {
        int saved_errno = errno;

        close(fd);
        errno = saved_errno;
        return -1;
    }
    aof->buffer = malloc(AOF_BUFFER_CAPACITY);
    if (aof->buffer == NULL) {
        int saved_errno = errno;

        close(fd);
        free(aof);
        errno = saved_errno;
        return -1;
    }
    aof->fd = fd;
    aof->fsync_policy = fsync_policy;
    aof->buffer_capacity = AOF_BUFFER_CAPACITY;
    aof->last_sync_request_ms = monotonic_now_ms();
    atomic_init(&aof->async_error, 0);
    result = pthread_mutex_init(&aof->sync_mutex, NULL);
    if (result == 0) {
        result = pthread_cond_init(&aof->sync_condition, NULL);
        if (result != 0) {
            pthread_mutex_destroy(&aof->sync_mutex);
        }
    }
    if (result != 0) {
        close(fd);
        free(aof->buffer);
        free(aof);
        errno = result;
        return -1;
    }
    if (start_sync_worker(aof) != 0) {
        int saved_errno = errno;

        pthread_cond_destroy(&aof->sync_condition);
        pthread_mutex_destroy(&aof->sync_mutex);
        close(fd);
        free(aof->buffer);
        free(aof);
        errno = saved_errno;
        return -1;
    }
    *out_aof = aof;
    return 0;
}

int aof_replay(aof_t *aof,
               aof_replay_callback callback,
               void *context,
               aof_replay_stats_t *stats)
{
    unsigned char *buffer;
    size_t used = 0;
    off_t valid_length = 0;

    if (aof == NULL || callback == NULL || check_async_failure(aof) != 0) {
        errno = EINVAL;
        return -1;
    }
    if (stats != NULL) {
        memset(stats, 0, sizeof(*stats));
    }
    buffer = aof->buffer;
    if (lseek(aof->fd, 0, SEEK_SET) < 0) {
        return -1;
    }
    for (;;) {
        resp_request_t request;
        size_t consumed = 0;
        int parse_result;

        if (used > 0) {
            parse_result = resp_parse_request_with_limit(buffer,
                                                         used,
                                                         0,
                                                         AOF_MAX_RECORD_SIZE,
                                                         &request,
                                                         &consumed);
            if (parse_result == RESP_PARSE_ERROR) {
                errno = EINVAL;
                return -1;
            }
            if (parse_result == RESP_PARSE_COMPLETE) {
                aof_argument_t arguments[RESP_MAX_ARGUMENTS];
                size_t index;

                for (index = 0; index < request.argument_count; ++index) {
                    arguments[index].data = request.arguments[index].data;
                    arguments[index].length = request.arguments[index].length;
                }
                if (callback(arguments, request.argument_count, context) != 0) {
                    errno = EINVAL;
                    return -1;
                }
                valid_length += (off_t)consumed;
                used -= consumed;
                if (used > 0) {
                    memmove(buffer, buffer + consumed, used);
                }
                if (stats != NULL) {
                    stats->commands_loaded++;
                }
                continue;
            }
        }
        if (used == AOF_MAX_RECORD_SIZE) {
            errno = EFBIG;
            return -1;
        }
        {
            ssize_t count;

            do {
                count = read(aof->fd,
                             buffer + used,
                             AOF_MAX_RECORD_SIZE - used);
            } while (count < 0 && errno == EINTR);
            if (count < 0) {
                return -1;
            }
            if (count == 0) {
                if (used > 0) {
                    if (ftruncate(aof->fd, valid_length) != 0) {
                        return -1;
                    }
                    if (stats != NULL) {
                        stats->truncated_tail_repaired = 1;
                    }
                }
                break;
            }
            used += (size_t)count;
        }
    }
    if (lseek(aof->fd, 0, SEEK_END) < 0) {
        return -1;
    }
    return 0;
}

int aof_flush(aof_t *aof)
{
    if (aof == NULL) {
        return 0;
    }
    if (check_async_failure(aof) != 0) {
        return -1;
    }
    if (aof->buffer_used == 0) {
        return 0;
    }
    if (write_all(aof, aof->buffer, aof->buffer_used) != 0) {
        return -1;
    }
    aof->buffer_used = 0;
    note_completed_write(aof);
    return 0;
}

int aof_append(aof_t *aof,
               const aof_argument_t *arguments,
               size_t argument_count)
{
    size_t record_length;

    if (aof == NULL || check_async_failure(aof) != 0) {
        errno = aof != NULL && aof->last_error != 0 ? aof->last_error : EIO;
        return -1;
    }
    if (encoded_size(arguments, argument_count, &record_length) != 0) {
        remember_failure(aof, errno);
        return -1;
    }
    if (record_length > aof->buffer_capacity) {
        remember_failure(aof, EFBIG);
        errno = EFBIG;
        return -1;
    }
    if (record_length > aof->buffer_capacity - aof->buffer_used &&
        aof_flush(aof) != 0) {
        return -1;
    }
    if (encode_record(aof->buffer + aof->buffer_used,
                      aof->buffer_capacity - aof->buffer_used,
                      arguments,
                      argument_count,
                      record_length) != 0) {
        remember_failure(aof, errno);
        return -1;
    }
    aof->buffer_used += record_length;
    if (aof->fsync_policy == AOF_FSYNC_ALWAYS) {
        if (aof_flush(aof) != 0) {
            return -1;
        }
        return sync_file_direct(aof);
    }
    if (aof->buffer_used >= AOF_FLUSH_THRESHOLD) {
        return aof_flush(aof);
    }
    return 0;
}

int aof_maintain(aof_t *aof)
{
    uint64_t now;
    uint64_t covered_generation;

    if (aof == NULL) {
        return 0;
    }
    if (aof_flush(aof) != 0) {
        return -1;
    }
    if (aof->fsync_policy != AOF_FSYNC_EVERYSEC) {
        return 0;
    }
    now = monotonic_now_ms();
    if (now < aof->last_sync_request_ms ||
        now - aof->last_sync_request_ms < 1000U) {
        return 0;
    }
    pthread_mutex_lock(&aof->sync_mutex);
    covered_generation = aof->synced_generation;
    if (aof->syncing_generation > covered_generation) {
        covered_generation = aof->syncing_generation;
    }
    if (aof->requested_generation > covered_generation) {
        covered_generation = aof->requested_generation;
    }
    if (aof->write_generation > covered_generation) {
        request_sync_locked(aof, aof->write_generation);
    }
    pthread_mutex_unlock(&aof->sync_mutex);
    aof->last_sync_request_ms = now;
    return check_async_failure(aof);
}

int aof_is_failed(aof_t *aof)
{
    return aof != NULL && check_async_failure(aof) != 0;
}

int aof_last_error(aof_t *aof)
{
    int async_error;

    if (aof == NULL) {
        return 0;
    }
    if (aof->last_error != 0) {
        return aof->last_error;
    }
    async_error = atomic_load_explicit(&aof->async_error,
                                       memory_order_acquire);
    return async_error;
}

int aof_close(aof_t *aof)
{
    int result = 0;
    int saved_errno = 0;
    int can_sync;

    if (aof == NULL) {
        return 0;
    }
    can_sync = check_async_failure(aof) == 0;
    if (can_sync && aof_flush(aof) != 0) {
        result = -1;
        saved_errno = aof->last_error;
        can_sync = 0;
    }
    if (stop_sync_worker(aof,
                         can_sync &&
                         aof->fsync_policy == AOF_FSYNC_EVERYSEC) != 0) {
        result = -1;
        saved_errno = errno;
    }
    if (check_async_failure(aof) != 0 && result == 0) {
        result = -1;
        saved_errno = aof->last_error;
    }
    if (close(aof->fd) != 0 && result == 0) {
        result = -1;
        saved_errno = errno;
    }
    pthread_cond_destroy(&aof->sync_condition);
    pthread_mutex_destroy(&aof->sync_mutex);
    free(aof->buffer);
    free(aof);
    if (result != 0) {
        errno = saved_errno == 0 ? EIO : saved_errno;
    }
    return result;
}
