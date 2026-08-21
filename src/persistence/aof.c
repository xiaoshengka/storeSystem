#define _GNU_SOURCE

#include "persistence/aof.h"
#include "protocol/resp.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/random.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

#ifdef KVSTORE_HELGRIND
#include <valgrind/helgrind.h>
#endif

#define AOF_MAX_RECORD_SIZE (RESP_MAX_FRAME_SIZE + 1024U)
#define AOF_CHUNK_CAPACITY (256U * 1024U)
#define AOF_QUEUE_HIGH_WATER (4U * 1024U * 1024U)
#define AOF_QUEUE_LOW_WATER (2U * 1024U * 1024U)
#define AOF_WRITEV_MAX 16U

typedef struct aof_chunk {
    struct aof_chunk *next;
    size_t length;
    size_t offset;
    uint64_t sequence;
    int sequence_end;
    unsigned char data[AOF_CHUNK_CAPACITY];
} aof_chunk_t;

struct aof {
    int fd;
    int notify_fd;
    aof_fsync_policy_t fsync_policy;
    pthread_t writer_thread;
    pthread_mutex_t mutex;
    pthread_mutex_t sync_mutex;
    pthread_cond_t condition;
    int writer_started;
    int stopping;
    aof_chunk_t *queue_head;
    aof_chunk_t *queue_tail;
    aof_chunk_t *free_chunks;
    aof_chunk_t *producer_chunk;
    size_t queue_bytes;
    _Atomic uint64_t next_sequence;
    _Atomic uint64_t written_sequence;
    _Atomic uint64_t synced_sequence;
    uint64_t written_bytes;
    uint64_t backpressure_events;
    _Atomic int backpressured;
    _Atomic int async_error;
    unsigned char *pending;
    size_t pending_used;
    size_t pending_capacity;
    unsigned char *transaction;
    size_t transaction_used;
    size_t transaction_capacity;
    int transaction_active;
    int pause_requested;
    int paused;
    uint64_t fsync_count;
    uint64_t fsync_total_us;
    uint64_t fsync_max_us;
};

static uint64_t load_written_sequence(aof_t *aof, memory_order order)
{
#ifdef KVSTORE_HELGRIND
    ANNOTATE_HAPPENS_AFTER(&aof->written_sequence);
#endif
    return atomic_load_explicit(&aof->written_sequence, order);
}

static void store_written_sequence(aof_t *aof,
                                   uint64_t sequence,
                                   memory_order order)
{
    atomic_store_explicit(&aof->written_sequence, sequence, order);
#ifdef KVSTORE_HELGRIND
    ANNOTATE_HAPPENS_BEFORE(&aof->written_sequence);
#endif
}

static void annotate_atomic_fields(aof_t *aof)
{
#ifdef KVSTORE_HELGRIND
    ANNOTATE_BENIGN_RACE_SIZED(&aof->next_sequence,
                               sizeof(aof->next_sequence),
                               "C11 atomic next_sequence");
    ANNOTATE_BENIGN_RACE_SIZED(&aof->written_sequence,
                               sizeof(aof->written_sequence),
                               "C11 atomic written_sequence");
    ANNOTATE_BENIGN_RACE_SIZED(&aof->synced_sequence,
                               sizeof(aof->synced_sequence),
                               "C11 atomic synced_sequence");
    ANNOTATE_BENIGN_RACE_SIZED(&aof->backpressured,
                               sizeof(aof->backpressured),
                               "C11 atomic backpressured");
    ANNOTATE_BENIGN_RACE_SIZED(&aof->async_error,
                               sizeof(aof->async_error),
                               "C11 atomic async_error");
#else
    (void)aof;
#endif
}

static size_t decimal_length(size_t value)
{
    size_t length = 1U;
    while (value >= 10U) { value /= 10U; length++; }
    return length;
}

static int checked_add(size_t *total, size_t amount)
{
    if (amount > SIZE_MAX - *total) return -1;
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
        argument_count > RESP_MAX_ARGUMENTS) { errno = EINVAL; return -1; }
    total = 1U + decimal_length(argument_count) + 2U;
    for (index = 0; index < argument_count; ++index) {
        if (arguments[index].data == NULL && arguments[index].length != 0) {
            errno = EINVAL; return -1;
        }
        if (checked_add(&total, 1U + decimal_length(arguments[index].length) + 2U) != 0 ||
            checked_add(&total, arguments[index].length) != 0 ||
            checked_add(&total, 2U) != 0) { errno = EOVERFLOW; return -1; }
    }
    if (total > AOF_MAX_RECORD_SIZE) { errno = EFBIG; return -1; }
    *result = total;
    return 0;
}

static size_t encode_unsigned(unsigned char prefix, size_t value,
                              unsigned char *output)
{
    unsigned char reverse[32];
    size_t count = 0;
    size_t position = 0;
    size_t index;

    do { reverse[count++] = (unsigned char)('0' + value % 10U); value /= 10U; }
    while (value != 0);
    output[position++] = prefix;
    for (index = 0; index < count; ++index) output[position++] = reverse[count - index - 1U];
    output[position++] = '\r';
    output[position++] = '\n';
    return position;
}

static int encode_record(unsigned char *output, size_t capacity,
                         const aof_argument_t *arguments,
                         size_t argument_count, size_t expected_length)
{
    size_t position;
    size_t index;

    if (output == NULL || capacity < expected_length) { errno = ENOBUFS; return -1; }
    position = encode_unsigned('*', argument_count, output);
    for (index = 0; index < argument_count; ++index) {
        position += encode_unsigned('$', arguments[index].length, output + position);
        if (arguments[index].length > 0) {
            memcpy(output + position, arguments[index].data, arguments[index].length);
        }
        position += arguments[index].length;
        output[position++] = '\r';
        output[position++] = '\n';
    }
    if (position != expected_length) { errno = EINVAL; return -1; }
    return 0;
}

static int reserve_bytes(unsigned char **buffer, size_t *capacity, size_t needed)
{
    size_t next;
    unsigned char *replacement;

    if (needed <= *capacity) return 0;
    next = *capacity == 0 ? AOF_CHUNK_CAPACITY : *capacity;
    while (next < needed) {
        if (next > SIZE_MAX / 2U) { errno = EOVERFLOW; return -1; }
        next *= 2U;
    }
    replacement = realloc(*buffer, next);
    if (replacement == NULL) return -1;
    *buffer = replacement;
    *capacity = next;
    return 0;
}

static void notify_reactor(aof_t *aof)
{
    uint64_t value = 1U;
    if (aof->notify_fd >= 0) {
        ssize_t ignored = write(aof->notify_fd, &value, sizeof(value));
        (void)ignored;
    }
}

static void publish_error(aof_t *aof, int error_number)
{
    if (error_number == 0) error_number = EIO;
    pthread_mutex_lock(&aof->mutex);
    if (atomic_load_explicit(&aof->async_error, memory_order_relaxed) == 0) {
        atomic_store_explicit(&aof->async_error, error_number,
                              memory_order_release);
        pthread_cond_broadcast(&aof->condition);
        pthread_mutex_unlock(&aof->mutex);
        notify_reactor(aof);
    } else {
        pthread_mutex_unlock(&aof->mutex);
    }
}

static aof_chunk_t *acquire_chunk_locked(aof_t *aof)
{
    aof_chunk_t *chunk = aof->free_chunks;

    if (chunk != NULL) aof->free_chunks = chunk->next;
    return chunk == NULL ? malloc(sizeof(*chunk)) : chunk;
}

static aof_chunk_t *acquire_chunk(aof_t *aof)
{
    aof_chunk_t *chunk;

    pthread_mutex_lock(&aof->mutex);
    chunk = acquire_chunk_locked(aof);
    pthread_mutex_unlock(&aof->mutex);
    return chunk;
}

static void queue_chunk_locked(aof_t *aof, aof_chunk_t *chunk)
{
    chunk->next = NULL;
    if (aof->queue_tail == NULL) aof->queue_head = chunk;
    else aof->queue_tail->next = chunk;
    aof->queue_tail = chunk;
    aof->queue_bytes += chunk->length;
    if (aof->queue_bytes >= AOF_QUEUE_HIGH_WATER &&
        !atomic_load_explicit(&aof->backpressured, memory_order_relaxed)) {
        atomic_store_explicit(&aof->backpressured, 1, memory_order_release);
        aof->backpressure_events++;
    }
    pthread_cond_signal(&aof->condition);
}

static int enqueue_bytes(aof_t *aof, const unsigned char *data, size_t length,
                         uint64_t *sequence)
{
    aof_chunk_t *first = NULL;
    aof_chunk_t *last = NULL;
    size_t position = 0;
    uint64_t assigned;

    if (sequence != NULL) *sequence = 0;
    if (length == 0) return 0;
    while (position < length) {
        size_t amount = length - position;
        aof_chunk_t *chunk = acquire_chunk(aof);
        if (chunk == NULL) {
            while (first != NULL) { chunk = first->next; free(first); first = chunk; }
            return -1;
        }
        if (amount > AOF_CHUNK_CAPACITY) amount = AOF_CHUNK_CAPACITY;
        chunk->next = NULL; chunk->length = amount; chunk->offset = 0;
        chunk->sequence = 0; chunk->sequence_end = 0;
        memcpy(chunk->data, data + position, amount);
        if (last == NULL) first = chunk; else last->next = chunk;
        last = chunk;
        position += amount;
    }
    pthread_mutex_lock(&aof->mutex);
    assigned = atomic_fetch_add_explicit(&aof->next_sequence, 1U,
                                         memory_order_relaxed) + 1U;
    last->sequence = assigned;
    last->sequence_end = 1;
    while (first != NULL) {
        aof_chunk_t *next = first->next;

        queue_chunk_locked(aof, first);
        first = next;
    }
    pthread_mutex_unlock(&aof->mutex);
    if (sequence != NULL) *sequence = assigned;
    return 0;
}

static int sync_written(aof_t *aof, uint64_t target)
{
    struct timespec start;
    struct timespec end;
    uint64_t elapsed_us = 0;

    pthread_mutex_lock(&aof->sync_mutex);
    pthread_mutex_lock(&aof->mutex);
    if (target <= atomic_load_explicit(&aof->synced_sequence,
                                       memory_order_acquire)) {
        pthread_mutex_unlock(&aof->mutex);
        pthread_mutex_unlock(&aof->sync_mutex);
        return 0;
    }
    pthread_mutex_unlock(&aof->mutex);
    (void)clock_gettime(CLOCK_MONOTONIC, &start);
    if (fdatasync(aof->fd) != 0) {
        pthread_mutex_unlock(&aof->sync_mutex);
        return -1;
    }
    if (clock_gettime(CLOCK_MONOTONIC, &end) == 0) {
        time_t seconds = end.tv_sec - start.tv_sec;
        long nanoseconds = end.tv_nsec - start.tv_nsec;

        if (nanoseconds < 0) {
            seconds--;
            nanoseconds += 1000000000L;
        }
        if (seconds >= 0) {
            elapsed_us = (uint64_t)seconds * UINT64_C(1000000) +
                         (uint64_t)nanoseconds / 1000U;
        }
    }
    pthread_mutex_lock(&aof->mutex);
    if (target > atomic_load_explicit(&aof->synced_sequence,
                                      memory_order_relaxed)) {
        atomic_store_explicit(&aof->synced_sequence, target,
                              memory_order_release);
    }
    aof->fsync_count++;
    aof->fsync_total_us += elapsed_us;
    if (elapsed_us > aof->fsync_max_us) aof->fsync_max_us = elapsed_us;
    pthread_cond_broadcast(&aof->condition);
    pthread_mutex_unlock(&aof->mutex);
    pthread_mutex_unlock(&aof->sync_mutex);
    return 0;
}

/* Called with mutex held. */
static uint64_t consume_written(aof_t *aof, size_t amount)
{
    uint64_t completed_sequence = 0;

    while (amount > 0) {
        aof_chunk_t *chunk = aof->queue_head;
        size_t available = chunk->length - chunk->offset;
        size_t consumed = amount < available ? amount : available;
        chunk->offset += consumed;
        aof->queue_bytes -= consumed;
        amount -= consumed;
        if (chunk->offset == chunk->length) {
            uint64_t sequence = chunk->sequence;
            int sequence_end = chunk->sequence_end;
            aof->queue_head = chunk->next;
            if (aof->queue_head == NULL) aof->queue_tail = NULL;
            chunk->next = aof->free_chunks;
            aof->free_chunks = chunk;
            if (sequence_end) {
                completed_sequence = sequence;
            }
        }
    }
    if (aof->queue_bytes <= AOF_QUEUE_LOW_WATER) {
        atomic_store_explicit(&aof->backpressured, 0, memory_order_release);
    }
    return completed_sequence;
}

static void realtime_after_one_second(struct timespec *deadline)
{
    if (clock_gettime(CLOCK_MONOTONIC, deadline) != 0) {
        deadline->tv_sec = 1; deadline->tv_nsec = 0;
    } else deadline->tv_sec++;
}

static int deadline_reached(const struct timespec *deadline)
{
    struct timespec now;

    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) return 0;
    return now.tv_sec > deadline->tv_sec ||
           (now.tv_sec == deadline->tv_sec &&
            now.tv_nsec >= deadline->tv_nsec);
}

static void *writer_main(void *context)
{
    aof_t *aof = context;
    sigset_t signals;
    struct timespec sync_deadline = {0, 0};

    sigfillset(&signals);
    (void)pthread_sigmask(SIG_BLOCK, &signals, NULL);
    if (aof->fsync_policy == AOF_FSYNC_EVERYSEC) {
        realtime_after_one_second(&sync_deadline);
    }

    for (;;) {
        struct iovec vectors[AOF_WRITEV_MAX];
        size_t count = 0;
        aof_chunk_t *chunk;
        ssize_t result;
        uint64_t completed_sequence;

        pthread_mutex_lock(&aof->mutex);
        for (;;) {
            while (aof->pause_requested && !aof->stopping) {
                aof->paused = 1;
                pthread_cond_broadcast(&aof->condition);
                pthread_cond_wait(&aof->condition, &aof->mutex);
            }
            if (aof->paused) {
                aof->paused = 0;
                pthread_cond_broadcast(&aof->condition);
            }
            if (aof->queue_head != NULL || aof->stopping) break;
            if (aof->fsync_policy == AOF_FSYNC_EVERYSEC) {
                int waited;

                waited = pthread_cond_timedwait(&aof->condition,
                                                &aof->mutex,
                                                &sync_deadline);
                if (waited == ETIMEDOUT) {
                    uint64_t written = load_written_sequence(
                        aof, memory_order_acquire);

                    pthread_mutex_unlock(&aof->mutex);
                    if (sync_written(aof, written) != 0) {
                        int saved_errno = errno;

                        publish_error(aof, saved_errno);
                        return NULL;
                    }
                    realtime_after_one_second(&sync_deadline);
                    pthread_mutex_lock(&aof->mutex);
                }
            } else pthread_cond_wait(&aof->condition, &aof->mutex);
        }
        if (aof->queue_head == NULL && aof->stopping) { pthread_mutex_unlock(&aof->mutex); break; }
        chunk = aof->queue_head;
        while (chunk != NULL && count < AOF_WRITEV_MAX) {
            vectors[count].iov_base = chunk->data + chunk->offset;
            vectors[count].iov_len = chunk->length - chunk->offset;
            count++;
            chunk = chunk->next;
        }
        pthread_mutex_unlock(&aof->mutex);
        do { result = writev(aof->fd, vectors, (int)count); } while (result < 0 && errno == EINTR);
        if (result <= 0) { publish_error(aof, result == 0 ? EIO : errno); break; }
        pthread_mutex_lock(&aof->mutex);
        aof->written_bytes += (uint64_t)result;
        completed_sequence = consume_written(aof, (size_t)result);
        if (completed_sequence != 0 &&
            aof->fsync_policy != AOF_FSYNC_ALWAYS &&
            completed_sequence > load_written_sequence(
                                     aof, memory_order_relaxed)) {
            store_written_sequence(aof,
                                   completed_sequence,
                                   memory_order_release);
            pthread_cond_broadcast(&aof->condition);
        }
        pthread_mutex_unlock(&aof->mutex);
        if (completed_sequence != 0) {
            if (aof->fsync_policy == AOF_FSYNC_ALWAYS &&
                sync_written(aof, completed_sequence) != 0) {
                publish_error(aof, errno);
                return NULL;
            }
            if (aof->fsync_policy == AOF_FSYNC_ALWAYS) {
                pthread_mutex_lock(&aof->mutex);
                if (completed_sequence > load_written_sequence(
                                             aof, memory_order_relaxed)) {
                    store_written_sequence(aof,
                                           completed_sequence,
                                           memory_order_release);
                }
                pthread_cond_broadcast(&aof->condition);
                pthread_mutex_unlock(&aof->mutex);
            }
            notify_reactor(aof);
        }
        if (aof->fsync_policy == AOF_FSYNC_EVERYSEC &&
            deadline_reached(&sync_deadline)) {
            uint64_t written = load_written_sequence(aof,
                                                     memory_order_acquire);

            if (sync_written(aof, written) != 0) {
                int saved_errno = errno;

                publish_error(aof, saved_errno);
                return NULL;
            }
            realtime_after_one_second(&sync_deadline);
        }
    }
    if (aof->fsync_policy == AOF_FSYNC_EVERYSEC) {
        uint64_t written;

        pthread_mutex_lock(&aof->mutex);
        written = load_written_sequence(aof, memory_order_acquire);
        pthread_mutex_unlock(&aof->mutex);
        if (sync_written(aof, written) != 0) {
            int saved_errno = errno;

            publish_error(aof, saved_errno);
            return NULL;
        }
    }
    return NULL;
}

int aof_open(aof_t **out_aof, const char *path, aof_fsync_policy_t fsync_policy)
{
    aof_t *aof;
    pthread_condattr_t condition_attributes;
    int condition_attributes_initialized = 0;
    int result;

    if (out_aof == NULL || path == NULL || path[0] == '\0' ||
        (fsync_policy != AOF_FSYNC_ALWAYS && fsync_policy != AOF_FSYNC_EVERYSEC && fsync_policy != AOF_FSYNC_NO)) {
        errno = EINVAL; return -1;
    }
    *out_aof = NULL;
    aof = calloc(1, sizeof(*aof));
    if (aof == NULL) return -1;
    atomic_init(&aof->next_sequence, 0);
    atomic_init(&aof->written_sequence, 0);
    atomic_init(&aof->synced_sequence, 0);
    atomic_init(&aof->backpressured, 0);
    atomic_init(&aof->async_error, 0);
    annotate_atomic_fields(aof);
    aof->fd = open(path, O_CREAT | O_RDWR | O_APPEND | O_CLOEXEC, 0644);
    aof->notify_fd = -1;
    aof->fsync_policy = fsync_policy;
    if (aof->fd < 0) { free(aof); return -1; }
    result = pthread_mutex_init(&aof->mutex, NULL);
    if (result != 0) { close(aof->fd); free(aof); errno = result; return -1; }
    result = pthread_mutex_init(&aof->sync_mutex, NULL);
    if (result != 0) {
        pthread_mutex_destroy(&aof->mutex);
        close(aof->fd); free(aof); errno = result; return -1;
    }
    result = pthread_condattr_init(&condition_attributes);
    if (result == 0) condition_attributes_initialized = 1;
    if (result == 0)
        result = pthread_condattr_setclock(&condition_attributes,
                                           CLOCK_MONOTONIC);
    if (result == 0)
        result = pthread_cond_init(&aof->condition, &condition_attributes);
    if (condition_attributes_initialized)
        (void)pthread_condattr_destroy(&condition_attributes);
    if (result != 0) { pthread_mutex_destroy(&aof->sync_mutex); pthread_mutex_destroy(&aof->mutex); close(aof->fd); free(aof); errno = result; return -1; }
    result = pthread_create(&aof->writer_thread, NULL, writer_main, aof);
    if (result != 0) {
        pthread_cond_destroy(&aof->condition); pthread_mutex_destroy(&aof->sync_mutex); pthread_mutex_destroy(&aof->mutex);
        close(aof->fd); free(aof); errno = result; return -1;
    }
    aof->writer_started = 1;
    *out_aof = aof;
    return 0;
}

int aof_replay_from(aof_t *aof, uint64_t offset,
                    aof_replay_callback callback, void *context,
                    aof_replay_stats_t *stats)
{
    struct stat status;
    unsigned char *mapping;
    size_t position;

    if (aof == NULL || callback == NULL) { errno = EINVAL; return -1; }
    if (stats != NULL) memset(stats, 0, sizeof(*stats));
    if (fstat(aof->fd, &status) != 0) return -1;
    if (status.st_size < 0 || (uintmax_t)status.st_size > SIZE_MAX ||
        offset > (uint64_t)status.st_size) { errno = EINVAL; return -1; }
    if (status.st_size == 0 || offset == (uint64_t)status.st_size) return 0;
    if ((uintmax_t)status.st_size > SIZE_MAX) { errno = EFBIG; return -1; }
    position = (size_t)offset;
    mapping = mmap(NULL, (size_t)status.st_size, PROT_READ, MAP_PRIVATE, aof->fd, 0);
    if (mapping == MAP_FAILED) return -1;
    (void)madvise(mapping, (size_t)status.st_size, MADV_SEQUENTIAL);
    while (position < (size_t)status.st_size) {
        resp_request_t request;
        size_t consumed = 0;
        int parsed = resp_parse_request_with_limit(mapping + position,
                                                    (size_t)status.st_size - position,
                                                    0, AOF_MAX_RECORD_SIZE,
                                                    &request, &consumed);
        if (parsed == RESP_PARSE_INCOMPLETE) break;
        if (parsed != RESP_PARSE_COMPLETE) { munmap(mapping, (size_t)status.st_size); errno = EINVAL; return -1; }
        {
            aof_argument_t arguments[RESP_MAX_ARGUMENTS];
            size_t index;
            for (index = 0; index < request.argument_count; ++index) {
                arguments[index].data = request.arguments[index].data;
                arguments[index].length = request.arguments[index].length;
            }
            if (callback(arguments, request.argument_count, context) != 0) {
                munmap(mapping, (size_t)status.st_size); errno = EINVAL; return -1;
            }
        }
        position += consumed;
        if (stats != NULL) stats->commands_loaded++;
    }
    munmap(mapping, (size_t)status.st_size);
    if (position != (size_t)status.st_size) {
        if (ftruncate(aof->fd, (off_t)position) != 0) return -1;
        if (stats != NULL) stats->truncated_tail_repaired = 1;
    }
    return 0;
}

int aof_replay(aof_t *aof, aof_replay_callback callback, void *context,
               aof_replay_stats_t *stats)
{
    return aof_replay_from(aof, 0, callback, context, stats);
}

static void checkpoint_token_text(
    const unsigned char token[RDB_CHECKPOINT_TOKEN_SIZE],
    unsigned char output[RDB_CHECKPOINT_TOKEN_SIZE * 2U])
{
    static const unsigned char hex[] = "0123456789abcdef";
    size_t index;

    for (index = 0; index < RDB_CHECKPOINT_TOKEN_SIZE; ++index) {
        output[index * 2U] = hex[token[index] >> 4U];
        output[index * 2U + 1U] = hex[token[index] & 0x0fU];
    }
}

static int fill_random_token(unsigned char *token, size_t length)
{
    size_t position = 0;

    while (position < length) {
        ssize_t result = getrandom(token + position, length - position, 0);

        if (result > 0) position += (size_t)result;
        else if (result < 0 && errno == EINTR) continue;
        else return -1;
    }
    return 0;
}

static int checkpoint_record(const rdb_checkpoint_t *checkpoint,
                             unsigned char *output,
                             size_t capacity,
                             size_t *length)
{
    unsigned char token_text[RDB_CHECKPOINT_TOKEN_SIZE * 2U];
    aof_argument_t arguments[2];

    checkpoint_token_text(checkpoint->token, token_text);
    arguments[0].data = (const unsigned char *)"PING";
    arguments[0].length = 4U;
    arguments[1].data = token_text;
    arguments[1].length = sizeof(token_text);
    if (encoded_size(arguments, 2U, length) != 0 || *length > capacity)
        return -1;
    return encode_record(output, capacity, arguments, 2U, *length);
}

int aof_create_checkpoint(aof_t *aof, rdb_checkpoint_t *checkpoint)
{
    unsigned char token_text[RDB_CHECKPOINT_TOKEN_SIZE * 2U];
    aof_argument_t arguments[2];
    struct stat status;
    uint64_t sequence = 0;
    int error_number;

    if (aof == NULL || checkpoint == NULL) {
        errno = EINVAL;
        return -1;
    }
    memset(checkpoint, 0, sizeof(*checkpoint));
    if (fill_random_token(checkpoint->token, sizeof(checkpoint->token)) != 0)
        return -1;
    checkpoint_token_text(checkpoint->token, token_text);
    arguments[0].data = (const unsigned char *)"PING";
    arguments[0].length = 4U;
    arguments[1].data = token_text;
    arguments[1].length = sizeof(token_text);
    if (aof_transaction_begin(aof) != 0 ||
        aof_append(aof, arguments, 2U) != 0 ||
        aof_transaction_commit(aof, &sequence) != 0) {
        aof_transaction_rollback(aof);
        return -1;
    }
    pthread_mutex_lock(&aof->mutex);
    if (aof->producer_chunk != NULL) {
        queue_chunk_locked(aof, aof->producer_chunk);
        aof->producer_chunk = NULL;
    }
    while (load_written_sequence(aof, memory_order_acquire) < sequence &&
           atomic_load_explicit(&aof->async_error,
                                memory_order_acquire) == 0) {
        pthread_cond_wait(&aof->condition, &aof->mutex);
    }
    error_number = atomic_load_explicit(&aof->async_error,
                                        memory_order_acquire);
    pthread_mutex_unlock(&aof->mutex);
    if (error_number == 0 && sync_written(aof, sequence) != 0)
        error_number = errno;
    if (error_number != 0) {
        errno = error_number;
        return -1;
    }
    if (fstat(aof->fd, &status) != 0 || status.st_size < 0) return -1;
    checkpoint->aof_offset = (uint64_t)status.st_size;
    checkpoint->valid = 1;
    return 0;
}

int aof_validate_checkpoint(aof_t *aof,
                            const rdb_checkpoint_t *checkpoint)
{
    unsigned char expected[128];
    unsigned char actual[128];
    size_t length;
    size_t position = 0;

    if (aof == NULL || checkpoint == NULL || !checkpoint->valid ||
        checkpoint_record(checkpoint, expected, sizeof(expected), &length) != 0 ||
        checkpoint->aof_offset < length) {
        errno = EINVAL;
        return -1;
    }
    while (position < length) {
        ssize_t result = pread(aof->fd,
                               actual + position,
                               length - position,
                               (off_t)(checkpoint->aof_offset - length + position));

        if (result > 0) position += (size_t)result;
        else if (result < 0 && errno == EINTR) continue;
        else {
            if (result == 0) errno = EINVAL;
            return -1;
        }
    }
    if (memcmp(actual, expected, length) != 0) {
        errno = EINVAL;
        return -1;
    }
    return 0;
}

int aof_pause_for_fork(aof_t *aof)
{
    if (aof == NULL) return 0;
    pthread_mutex_lock(&aof->mutex);
    if (aof->pause_requested || aof->stopping ||
        atomic_load_explicit(&aof->async_error, memory_order_acquire) != 0) {
        int async_error = atomic_load_explicit(&aof->async_error,
                                               memory_order_acquire);
        int error_number = async_error != 0 ? async_error : EBUSY;

        pthread_mutex_unlock(&aof->mutex);
        errno = error_number;
        return -1;
    }
    aof->pause_requested = 1;
    pthread_cond_broadcast(&aof->condition);
    while (!aof->paused &&
           atomic_load_explicit(&aof->async_error,
                                memory_order_acquire) == 0) {
        pthread_cond_wait(&aof->condition, &aof->mutex);
    }
    if (atomic_load_explicit(&aof->async_error, memory_order_acquire) != 0) {
        int error_number = atomic_load_explicit(&aof->async_error,
                                                memory_order_acquire);

        aof->pause_requested = 0;
        pthread_cond_broadcast(&aof->condition);
        pthread_mutex_unlock(&aof->mutex);
        errno = error_number;
        return -1;
    }
    pthread_mutex_unlock(&aof->mutex);
    return 0;
}

void aof_resume_after_fork(aof_t *aof)
{
    if (aof == NULL) return;
    pthread_mutex_lock(&aof->mutex);
    aof->pause_requested = 0;
    pthread_cond_broadcast(&aof->condition);
    pthread_mutex_unlock(&aof->mutex);
}

int aof_append(aof_t *aof, const aof_argument_t *arguments, size_t argument_count)
{
    unsigned char **buffer;
    size_t *used;
    size_t *capacity;
    size_t record_length;

    if (aof == NULL || aof_last_error(aof) != 0) { errno = aof == NULL ? EINVAL : aof_last_error(aof); return -1; }
    if (encoded_size(arguments, argument_count, &record_length) != 0) return -1;
    if (aof->transaction_active) {
        buffer = &aof->transaction; used = &aof->transaction_used; capacity = &aof->transaction_capacity;
    } else {
        buffer = &aof->pending; used = &aof->pending_used; capacity = &aof->pending_capacity;
    }
    if (record_length > SIZE_MAX - *used || reserve_bytes(buffer, capacity, *used + record_length) != 0) return -1;
    if (encode_record(*buffer + *used, *capacity - *used, arguments, argument_count, record_length) != 0) return -1;
    *used += record_length;
    return 0;
}

int aof_transaction_begin(aof_t *aof)
{
    if (aof == NULL) return 0;
    if (aof->transaction_active || !aof_can_accept_write(aof)) {
        errno = aof->transaction_active ? EBUSY : EAGAIN; return -1;
    }
    aof->transaction_used = 0; aof->transaction_active = 1;
    return 0;
}

int aof_transaction_commit(aof_t *aof, uint64_t *sequence)
{
    size_t position = 0;
    uint64_t assigned = 0;

    if (sequence != NULL) *sequence = 0;
    if (aof == NULL) return 0;
    if (!aof->transaction_active) { errno = EINVAL; return -1; }
    if (aof->transaction_used > 0) {
        assigned = atomic_fetch_add_explicit(&aof->next_sequence, 1U,
                                             memory_order_relaxed) + 1U;
    }
    while (position < aof->transaction_used) {
        size_t available;
        size_t amount;

        if (aof->producer_chunk == NULL) {
            aof->producer_chunk = acquire_chunk(aof);
            if (aof->producer_chunk == NULL) {
                aof->transaction_used = 0;
                aof->transaction_active = 0;
                return -1;
            }
            aof->producer_chunk->next = NULL;
            aof->producer_chunk->length = 0;
            aof->producer_chunk->offset = 0;
            aof->producer_chunk->sequence = 0;
            aof->producer_chunk->sequence_end = 0;
        }
        available = AOF_CHUNK_CAPACITY - aof->producer_chunk->length;
        amount = aof->transaction_used - position;
        if (amount > available) amount = available;
        memcpy(aof->producer_chunk->data + aof->producer_chunk->length,
               aof->transaction + position, amount);
        aof->producer_chunk->length += amount;
        position += amount;
        aof->producer_chunk->sequence = assigned;
        aof->producer_chunk->sequence_end = position == aof->transaction_used;
        if (aof->producer_chunk->length == AOF_CHUNK_CAPACITY) {
            pthread_mutex_lock(&aof->mutex);
            queue_chunk_locked(aof, aof->producer_chunk);
            pthread_mutex_unlock(&aof->mutex);
            aof->producer_chunk = NULL;
        }
    }
    aof->transaction_used = 0; aof->transaction_active = 0;
    if (sequence != NULL) *sequence = assigned;
    return 0;
}

void aof_transaction_rollback(aof_t *aof)
{
    if (aof != NULL) { aof->transaction_used = 0; aof->transaction_active = 0; }
}

int aof_can_accept_write(aof_t *aof)
{
    if (aof == NULL) return 1;
    return !atomic_load_explicit(&aof->backpressured, memory_order_acquire) &&
           atomic_load_explicit(&aof->async_error, memory_order_acquire) == 0;
}

int aof_flush(aof_t *aof)
{
    uint64_t sequence = 0;
    int error_number;
    if (aof == NULL) return 0;
    if (aof->transaction_active) { errno = EBUSY; return -1; }
    pthread_mutex_lock(&aof->mutex);
    if (aof->producer_chunk != NULL) {
        queue_chunk_locked(aof, aof->producer_chunk);
        aof->producer_chunk = NULL;
    }
    pthread_mutex_unlock(&aof->mutex);
    if (aof->pending_used == 0) return 0;
    if (enqueue_bytes(aof, aof->pending, aof->pending_used, &sequence) != 0) return -1;
    aof->pending_used = 0;
    pthread_mutex_lock(&aof->mutex);
    while (load_written_sequence(aof, memory_order_acquire) < sequence &&
           atomic_load_explicit(&aof->async_error,
                                memory_order_acquire) == 0)
        pthread_cond_wait(&aof->condition, &aof->mutex);
    error_number = atomic_load_explicit(&aof->async_error,
                                        memory_order_acquire);
    pthread_mutex_unlock(&aof->mutex);
    if (error_number != 0) { errno = error_number; return -1; }
    return 0;
}

int aof_maintain(aof_t *aof)
{
    if (aof != NULL && aof_last_error(aof) != 0) { errno = aof_last_error(aof); return -1; }
    return 0;
}

int aof_is_failed(aof_t *aof) { return aof != NULL && aof_last_error(aof) != 0; }
int aof_last_error(aof_t *aof)
{
    if (aof == NULL) return 0;
    return atomic_load_explicit(&aof->async_error, memory_order_acquire);
}

int aof_set_notify_fd(aof_t *aof, int notify_fd)
{
    if (aof == NULL || notify_fd < 0) { errno = EINVAL; return -1; }
    aof->notify_fd = notify_fd; return 0;
}

int aof_sequence_ready(aof_t *aof, uint64_t sequence)
{
    if (aof == NULL || sequence == 0) return 1;
    return load_written_sequence(aof, memory_order_acquire) >= sequence;
}

void aof_get_info(aof_t *aof, aof_info_t *info)
{
    if (info == NULL) return;
    memset(info, 0, sizeof(*info));
    info->queue_high_water = AOF_QUEUE_HIGH_WATER; info->queue_low_water = AOF_QUEUE_LOW_WATER;
    if (aof == NULL) return;
    pthread_mutex_lock(&aof->mutex);
    info->queue_bytes = aof->queue_bytes +
                        (aof->producer_chunk == NULL ? 0 :
                         aof->producer_chunk->length);
    info->enqueued_sequence = atomic_load_explicit(&aof->next_sequence,
                                                   memory_order_relaxed);
    info->written_sequence = load_written_sequence(aof,
                                                   memory_order_acquire);
    info->synced_sequence = atomic_load_explicit(&aof->synced_sequence,
                                                 memory_order_acquire);
    info->written_bytes = aof->written_bytes;
    info->backpressure_events = aof->backpressure_events;
    info->fsync_count = aof->fsync_count;
    info->fsync_total_us = aof->fsync_total_us;
    info->fsync_max_us = aof->fsync_max_us;
    info->backpressured = atomic_load_explicit(&aof->backpressured,
                                               memory_order_acquire);
    info->last_error = atomic_load_explicit(&aof->async_error,
                                            memory_order_acquire);
    info->failed = info->last_error != 0;
    pthread_mutex_unlock(&aof->mutex);
}

int aof_close(aof_t *aof)
{
    int result = 0;
    int saved_errno = 0;
    aof_chunk_t *chunk;
    if (aof == NULL) return 0;
    if (aof->transaction_active) aof_transaction_rollback(aof);
    if (aof_flush(aof) != 0) { result = -1; saved_errno = errno; }
    pthread_mutex_lock(&aof->mutex); aof->stopping = 1; pthread_cond_broadcast(&aof->condition); pthread_mutex_unlock(&aof->mutex);
    if (aof->writer_started && pthread_join(aof->writer_thread, NULL) != 0 && result == 0) { result = -1; saved_errno = EIO; }
    if (aof_last_error(aof) != 0 && result == 0) { result = -1; saved_errno = aof_last_error(aof); }
    while ((chunk = aof->queue_head) != NULL) { aof->queue_head = chunk->next; free(chunk); }
    free(aof->producer_chunk);
    while ((chunk = aof->free_chunks) != NULL) { aof->free_chunks = chunk->next; free(chunk); }
    if (close(aof->fd) != 0 && result == 0) { result = -1; saved_errno = errno; }
    pthread_cond_destroy(&aof->condition); pthread_mutex_destroy(&aof->sync_mutex); pthread_mutex_destroy(&aof->mutex);
    free(aof->pending); free(aof->transaction); free(aof);
    if (result != 0) errno = saved_errno == 0 ? EIO : saved_errno;
    return result;
}

void aof_close_in_child(aof_t *aof)
{
    if (aof != NULL && aof->fd >= 0) (void)close(aof->fd);
}
