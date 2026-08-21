#define _GNU_SOURCE

#include "persistence/rdb.h"

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define RDB_MAGIC "KVRDB001"
#define RDB_MAGIC_SIZE 8U
#define RDB_TYPE_STRING 0U
#define RDB_TYPE_HASH 1U
#define RDB_TYPE_ZSET 2U
#define RDB_TYPE_END 255U
#define RDB_CRC64_POLY UINT64_C(0x42f0e1eba9ea3693)

typedef struct rdb_writer {
    int fd;
    uint64_t crc;
    uint64_t bytes;
    uint64_t snapshot_time_ms;
    rdb_stats_t *stats;
} rdb_writer_t;

typedef struct rdb_counter {
    uint64_t snapshot_time_ms;
    uint64_t keys;
} rdb_counter_t;

typedef struct rdb_reader {
    const unsigned char *data;
    size_t size;
    size_t position;
} rdb_reader_t;

static uint64_t crc64_update(uint64_t crc,
                             const unsigned char *data,
                             size_t length)
{
    size_t index;

    for (index = 0; index < length; ++index) {
        unsigned int bit;

        crc ^= (uint64_t)data[index] << 56U;
        for (bit = 0; bit < 8U; ++bit) {
            crc = (crc & UINT64_C(0x8000000000000000)) != 0
                      ? (crc << 1U) ^ RDB_CRC64_POLY
                      : crc << 1U;
        }
    }
    return crc;
}

static void encode_u64(unsigned char output[8], uint64_t value)
{
    unsigned int index;

    for (index = 0; index < 8U; ++index) {
        output[7U - index] = (unsigned char)(value & 0xffU);
        value >>= 8U;
    }
}

static uint64_t decode_u64(const unsigned char input[8])
{
    uint64_t value = 0;
    unsigned int index;

    for (index = 0; index < 8U; ++index) {
        value = (value << 8U) | input[index];
    }
    return value;
}

static int write_all_fd(int fd, const void *data, size_t length)
{
    const unsigned char *cursor = data;

    while (length > 0) {
        ssize_t result = write(fd, cursor, length);

        if (result > 0) {
            cursor += (size_t)result;
            length -= (size_t)result;
        } else if (result < 0 && errno == EINTR) {
            continue;
        } else {
            if (result == 0) errno = EIO;
            return -1;
        }
    }
    return 0;
}

static int writer_bytes(rdb_writer_t *writer, const void *data, size_t length)
{
    if (length > UINT64_MAX - writer->bytes) {
        errno = EOVERFLOW;
        return -1;
    }
    if (write_all_fd(writer->fd, data, length) != 0) return -1;
    writer->crc = crc64_update(writer->crc, data, length);
    writer->bytes += length;
    return 0;
}

static int writer_u8(rdb_writer_t *writer, unsigned int value)
{
    unsigned char encoded = (unsigned char)value;

    return writer_bytes(writer, &encoded, sizeof(encoded));
}

static int writer_u64(rdb_writer_t *writer, uint64_t value)
{
    unsigned char encoded[8];

    encode_u64(encoded, value);
    return writer_bytes(writer, encoded, sizeof(encoded));
}

static int write_hash_field(const void *field,
                            size_t field_length,
                            const void *value,
                            size_t value_length,
                            void *context)
{
    rdb_writer_t *writer = context;

    return writer_u64(writer, (uint64_t)field_length) != 0 ||
           writer_bytes(writer, field, field_length) != 0 ||
           writer_u64(writer, (uint64_t)value_length) != 0 ||
           writer_bytes(writer, value, value_length) != 0
               ? -1 : 0;
}

static int write_zset_member(const void *member,
                             size_t member_length,
                             double score,
                             void *context)
{
    rdb_writer_t *writer = context;
    uint64_t score_bits;

    memcpy(&score_bits, &score, sizeof(score_bits));
    return writer_u64(writer, (uint64_t)member_length) != 0 ||
           writer_bytes(writer, member, member_length) != 0 ||
           writer_u64(writer, score_bits) != 0
               ? -1 : 0;
}

static int write_cache_entry(const void *key,
                             size_t key_length,
                             kv_object_t *object,
                             uint64_t expire_at_ms,
                             void *context)
{
    rdb_writer_t *writer = context;
    kv_object_type_t type = kv_object_type(object);
    size_t count = 0;

    if (expire_at_ms != 0 && expire_at_ms <= writer->snapshot_time_ms)
        return 0;

    if (type != KV_OBJECT_STRING && type != KV_OBJECT_HASH &&
        type != KV_OBJECT_ZSET) {
        errno = EINVAL;
        return -1;
    }
    if (writer_u8(writer, (unsigned int)type) != 0 ||
        writer_u64(writer, expire_at_ms) != 0 ||
        writer_u64(writer, (uint64_t)key_length) != 0 ||
        writer_bytes(writer, key, key_length) != 0) return -1;

    if (type == KV_OBJECT_STRING) {
        const void *value = kv_object_string_value(object, &count);

        if (writer_u64(writer, (uint64_t)count) != 0 ||
            writer_bytes(writer, value, count) != 0) return -1;
        writer->stats->string_keys++;
    } else if (type == KV_OBJECT_HASH) {
        count = kv_object_hash_length(object);
        if (writer_u64(writer, (uint64_t)count) != 0 ||
            kv_object_hash_visit(object, write_hash_field, writer) != 0)
            return -1;
        writer->stats->hash_keys++;
        writer->stats->hash_fields += count;
    } else {
        size_t visited = 0;

        count = kv_object_zset_length(object);
        if (writer_u64(writer, (uint64_t)count) != 0 ||
            kv_object_zset_range(object,
                                 0,
                                 -1,
                                 write_zset_member,
                                 writer,
                                 &visited) != 0 || visited != count) return -1;
        writer->stats->zset_keys++;
        writer->stats->zset_members += count;
    }
    writer->stats->keys++;
    return 0;
}

static int count_cache_entry(const void *key,
                             size_t key_length,
                             kv_object_t *object,
                             uint64_t expire_at_ms,
                             void *context)
{
    rdb_counter_t *counter = context;

    (void)key;
    (void)key_length;
    (void)object;
    if (expire_at_ms == 0 || expire_at_ms > counter->snapshot_time_ms) {
        if (counter->keys == UINT64_MAX) {
            errno = EOVERFLOW;
            return -1;
        }
        counter->keys++;
    }
    return 0;
}

static int sync_parent_directory(const char *path)
{
    const char *slash = strrchr(path, '/');
    char *directory = NULL;
    int fd;
    int result;

    if (slash == NULL) {
        fd = open(".", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    } else {
        size_t length = slash == path ? 1U : (size_t)(slash - path);

        directory = malloc(length + 1U);
        if (directory == NULL) return -1;
        memcpy(directory, path, length);
        directory[length] = '\0';
        fd = open(directory, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    }
    free(directory);
    if (fd < 0) return -1;
    result = fsync(fd);
    if (close(fd) != 0 && result == 0) result = -1;
    return result;
}

int rdb_save(const char *path,
             cache_t *cache,
             const rdb_checkpoint_t *checkpoint,
             rdb_stats_t *stats)
{
    rdb_stats_t local_stats;
    rdb_writer_t writer;
    rdb_counter_t counter;
    struct timespec now;
    unsigned char footer[8];
    char *temporary;
    size_t path_length;
    int fd = -1;
    int result = -1;
    int saved_errno = 0;

    if (path == NULL || path[0] == '\0' || cache == NULL) {
        errno = EINVAL;
        return -1;
    }
    path_length = strlen(path);
    if (path_length > SIZE_MAX - 16U) {
        errno = ENAMETOOLONG;
        return -1;
    }
    temporary = malloc(path_length + 16U);
    if (temporary == NULL) return -1;
    memcpy(temporary, path, path_length);
    memcpy(temporary + path_length, ".tmp.XXXXXX", 12U);
    temporary[path_length + 11U] = '\0';
    fd = mkstemp(temporary);
    if (fd < 0) goto cleanup;
    (void)fcntl(fd, F_SETFD, FD_CLOEXEC);
    if (fchmod(fd, 0644) != 0) goto cleanup;

    memset(&local_stats, 0, sizeof(local_stats));
    if (clock_gettime(CLOCK_REALTIME, &now) != 0) goto cleanup;
    local_stats.snapshot_time_ms = (uint64_t)now.tv_sec * 1000U +
                                   (uint64_t)now.tv_nsec / 1000000U;
    memset(&counter, 0, sizeof(counter));
    counter.snapshot_time_ms = local_stats.snapshot_time_ms;
    if (cache_visit(cache, count_cache_entry, &counter) != 0) goto cleanup;
    memset(&writer, 0, sizeof(writer));
    writer.fd = fd;
    writer.snapshot_time_ms = local_stats.snapshot_time_ms;
    writer.stats = &local_stats;
    if (writer_bytes(&writer, RDB_MAGIC, RDB_MAGIC_SIZE) != 0 ||
        writer_u64(&writer, local_stats.snapshot_time_ms) != 0 ||
        writer_u64(&writer, counter.keys) != 0 ||
        writer_u64(&writer, checkpoint != NULL && checkpoint->valid
                                ? checkpoint->aof_offset : 0) != 0 ||
        writer_u8(&writer, checkpoint != NULL && checkpoint->valid) != 0)
        goto cleanup;
    if (checkpoint != NULL && checkpoint->valid) {
        if (writer_bytes(&writer,
                         checkpoint->token,
                         RDB_CHECKPOINT_TOKEN_SIZE) != 0) goto cleanup;
    } else {
        static const unsigned char zero[RDB_CHECKPOINT_TOKEN_SIZE] = {0};

        if (writer_bytes(&writer, zero, sizeof(zero)) != 0) goto cleanup;
    }
    if (cache_visit(cache, write_cache_entry, &writer) != 0 ||
        writer_u8(&writer, RDB_TYPE_END) != 0) goto cleanup;
    if ((uint64_t)local_stats.keys != counter.keys) {
        errno = EINVAL;
        goto cleanup;
    }
    encode_u64(footer, writer.crc);
    if (write_all_fd(fd, footer, sizeof(footer)) != 0) goto cleanup;
    writer.bytes += sizeof(footer);
    if (fdatasync(fd) != 0 || close(fd) != 0) {
        fd = -1;
        goto cleanup;
    }
    fd = -1;
    if (rename(temporary, path) != 0 || sync_parent_directory(path) != 0)
        goto cleanup;
    local_stats.bytes = writer.bytes;
    if (stats != NULL) *stats = local_stats;
    result = 0;

cleanup:
    saved_errno = errno;
    if (fd >= 0) (void)close(fd);
    if (result != 0) (void)unlink(temporary);
    free(temporary);
    if (result != 0) errno = saved_errno == 0 ? EIO : saved_errno;
    return result;
}

static int reader_bytes(rdb_reader_t *reader,
                        const unsigned char **data,
                        size_t length)
{
    if (length > reader->size - reader->position) {
        errno = EINVAL;
        return -1;
    }
    *data = reader->data + reader->position;
    reader->position += length;
    return 0;
}

static int reader_u8(rdb_reader_t *reader, unsigned int *value)
{
    const unsigned char *data;

    if (reader_bytes(reader, &data, 1U) != 0) return -1;
    *value = data[0];
    return 0;
}

static int reader_u64(rdb_reader_t *reader, uint64_t *value)
{
    const unsigned char *data;

    if (reader_bytes(reader, &data, 8U) != 0) return -1;
    *value = decode_u64(data);
    return 0;
}

static int u64_size(uint64_t value, size_t *result)
{
    if (value > SIZE_MAX) {
        errno = EOVERFLOW;
        return -1;
    }
    *result = (size_t)value;
    return 0;
}

static int load_object(rdb_reader_t *reader,
                       cache_t *cache,
                       kv_zset_engine_t zset_engine,
                       unsigned int type,
                       uint64_t expire_at,
                       const unsigned char *key,
                       size_t key_length,
                       uint64_t now_ms,
                       rdb_stats_t *stats)
{
    kv_object_t *object = NULL;
    uint64_t raw_count;
    size_t count;
    size_t index;
    int expired = expire_at != 0 && expire_at <= now_ms;

    if (reader_u64(reader, &raw_count) != 0 ||
        u64_size(raw_count, &count) != 0) return -1;
    if (type == RDB_TYPE_STRING) {
        const unsigned char *value;

        if (reader_bytes(reader, &value, count) != 0 ||
            kv_object_create_string(&object, value, count) != 0) return -1;
        if (!expired) stats->string_keys++;
    } else if (type == RDB_TYPE_HASH) {
        if (kv_object_create_hash(&object) != 0) return -1;
        for (index = 0; index < count; ++index) {
            uint64_t raw_field_length;
            uint64_t raw_value_length;
            size_t field_length;
            size_t value_length;
            const unsigned char *field;
            const unsigned char *value;
            int added = 0;
            int changed = 0;

            if (reader_u64(reader, &raw_field_length) != 0 ||
                u64_size(raw_field_length, &field_length) != 0 ||
                reader_bytes(reader, &field, field_length) != 0 ||
                reader_u64(reader, &raw_value_length) != 0 ||
                u64_size(raw_value_length, &value_length) != 0 ||
                reader_bytes(reader, &value, value_length) != 0 ||
                kv_object_hash_set(object, field, field_length,
                                   value, value_length, &added, &changed) != 0 ||
                !added) {
                kv_object_destroy(object);
                errno = EINVAL;
                return -1;
            }
        }
        if (!expired) {
            stats->hash_keys++;
            stats->hash_fields += count;
        }
    } else if (type == RDB_TYPE_ZSET) {
        if (kv_object_create_zset(&object, zset_engine) != 0) return -1;
        for (index = 0; index < count; ++index) {
            uint64_t raw_member_length;
            uint64_t score_bits;
            size_t member_length;
            const unsigned char *member;
            double score;
            int added = 0;
            int changed = 0;

            if (reader_u64(reader, &raw_member_length) != 0 ||
                u64_size(raw_member_length, &member_length) != 0 ||
                reader_bytes(reader, &member, member_length) != 0 ||
                reader_u64(reader, &score_bits) != 0) {
                kv_object_destroy(object);
                return -1;
            }
            memcpy(&score, &score_bits, sizeof(score));
            if (kv_object_zset_add(object, score, member, member_length,
                                   &added, &changed) != 0 || !added) {
                kv_object_destroy(object);
                errno = EINVAL;
                return -1;
            }
        }
        if (!expired) {
            stats->zset_keys++;
            stats->zset_members += count;
        }
    } else {
        errno = EINVAL;
        return -1;
    }
    if (expired) {
        kv_object_destroy(object);
        return 0;
    }
    if (cache_store_object(cache,
                           key,
                           key_length,
                           object,
                           expire_at,
                           0) != CACHE_SET_OK) {
        kv_object_destroy(object);
        return -1;
    }
    stats->keys++;
    return 0;
}

int rdb_load(const char *path,
             cache_t *cache,
             kv_zset_engine_t zset_engine,
             rdb_checkpoint_t *checkpoint,
             rdb_stats_t *stats)
{
    struct stat status;
    unsigned char *mapping = MAP_FAILED;
    rdb_reader_t reader;
    rdb_stats_t local_stats;
    rdb_checkpoint_t local_checkpoint;
    const unsigned char *bytes;
    uint64_t declared_keys;
    uint64_t records_loaded = 0;
    uint64_t stored_crc;
    uint64_t calculated_crc;
    uint64_t now_ms;
    unsigned int checkpoint_valid;
    int fd = -1;
    int result = -1;
    int saved_errno;

    if (path == NULL || path[0] == '\0' || cache == NULL ||
        (zset_engine != KV_ZSET_SKIPLIST && zset_engine != KV_ZSET_RBTREE)) {
        errno = EINVAL;
        return -1;
    }
    fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return -1;
    if (fstat(fd, &status) != 0 || status.st_size < 58 ||
        (uintmax_t)status.st_size > SIZE_MAX) goto cleanup;
    mapping = mmap(NULL, (size_t)status.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mapping == MAP_FAILED) goto cleanup;
    (void)madvise(mapping, (size_t)status.st_size, MADV_SEQUENTIAL);
    stored_crc = decode_u64(mapping + (size_t)status.st_size - 8U);
    calculated_crc = crc64_update(0, mapping, (size_t)status.st_size - 8U);
    if (stored_crc != calculated_crc) {
        errno = EINVAL;
        goto cleanup;
    }
    memset(&reader, 0, sizeof(reader));
    reader.data = mapping;
    reader.size = (size_t)status.st_size - 8U;
    if (reader_bytes(&reader, &bytes, RDB_MAGIC_SIZE) != 0 ||
        memcmp(bytes, RDB_MAGIC, RDB_MAGIC_SIZE) != 0) {
        errno = EINVAL;
        goto cleanup;
    }
    memset(&local_stats, 0, sizeof(local_stats));
    memset(&local_checkpoint, 0, sizeof(local_checkpoint));
    if (reader_u64(&reader, &local_stats.snapshot_time_ms) != 0 ||
        reader_u64(&reader, &declared_keys) != 0 ||
        reader_u64(&reader, &local_checkpoint.aof_offset) != 0 ||
        reader_u8(&reader, &checkpoint_valid) != 0 || checkpoint_valid > 1U ||
        reader_bytes(&reader, &bytes, RDB_CHECKPOINT_TOKEN_SIZE) != 0)
        goto cleanup;
    memcpy(local_checkpoint.token, bytes, RDB_CHECKPOINT_TOKEN_SIZE);
    local_checkpoint.valid = (int)checkpoint_valid;
    now_ms = cache_current_time_ms(cache);
    for (;;) {
        unsigned int type;
        uint64_t expire_at;
        uint64_t raw_key_length;
        size_t key_length;
        const unsigned char *key;

        if (reader_u8(&reader, &type) != 0) goto cleanup;
        if (type == RDB_TYPE_END) break;
        if (reader_u64(&reader, &expire_at) != 0 ||
            reader_u64(&reader, &raw_key_length) != 0 ||
            u64_size(raw_key_length, &key_length) != 0 ||
            reader_bytes(&reader, &key, key_length) != 0 ||
            load_object(&reader, cache, zset_engine, type, expire_at,
                        key, key_length, now_ms, &local_stats) != 0)
            goto cleanup;
        if (records_loaded == UINT64_MAX) {
            errno = EOVERFLOW;
            goto cleanup;
        }
        records_loaded++;
    }
    if (reader.position != reader.size) {
        errno = EINVAL;
        goto cleanup;
    }
    if (records_loaded != declared_keys) {
        errno = EINVAL;
        goto cleanup;
    }
    local_stats.bytes = (uint64_t)status.st_size;
    if (checkpoint != NULL) *checkpoint = local_checkpoint;
    if (stats != NULL) *stats = local_stats;
    result = 0;

cleanup:
    saved_errno = errno;
    if (mapping != MAP_FAILED) munmap(mapping, (size_t)status.st_size);
    close(fd);
    if (result != 0) errno = saved_errno == 0 ? EINVAL : saved_errno;
    return result;
}
