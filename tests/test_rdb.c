#define _POSIX_C_SOURCE 200809L

#include "cache/cache.h"
#include "engine/object.h"
#include "persistence/rdb.h"

#include <assert.h>
#include <fcntl.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define TEST_CRC64_POLY UINT64_C(0x42f0e1eba9ea3693)

static uint64_t test_crc64(const unsigned char *data, size_t length)
{
    uint64_t crc = 0;
    size_t index;

    for (index = 0; index < length; ++index) {
        unsigned int bit;

        crc ^= (uint64_t)data[index] << 56U;
        for (bit = 0; bit < 8U; ++bit) {
            crc = (crc & UINT64_C(0x8000000000000000)) != 0
                      ? (crc << 1U) ^ TEST_CRC64_POLY : crc << 1U;
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

static void mutate_valid_rdb(const char *path,
                             size_t offset,
                             const unsigned char *replacement,
                             size_t replacement_length)
{
    struct stat status;
    unsigned char *data;
    unsigned char encoded_crc[8];
    int fd = open(path, O_RDWR);

    assert(fd >= 0 && fstat(fd, &status) == 0 && status.st_size >= 8);
    data = malloc((size_t)status.st_size);
    assert(data != NULL);
    assert(pread(fd, data, (size_t)status.st_size, 0) == status.st_size);
    assert(offset <= (size_t)status.st_size - 8U &&
           replacement_length <= (size_t)status.st_size - 8U - offset);
    memcpy(data + offset, replacement, replacement_length);
    encode_u64(encoded_crc, test_crc64(data, (size_t)status.st_size - 8U));
    memcpy(data + (size_t)status.st_size - 8U, encoded_crc, 8U);
    assert(pwrite(fd, data, (size_t)status.st_size, 0) == status.st_size);
    assert(close(fd) == 0);
    free(data);
}

static void populate(cache_t *cache, kv_zset_engine_t engine)
{
    static const unsigned char binary[] = {'v', 0, 'x'};
    kv_object_t *hash;
    kv_object_t *zset;
    int added;
    int changed;

    assert(cache_set_expire_at(cache, "string", 6U,
                               binary, sizeof(binary), 0) == CACHE_SET_OK);
    assert(kv_object_create_hash(&hash) == 0);
    assert(kv_object_hash_set(hash, "field", 5U, "value", 5U,
                              &added, &changed) == 0 && added && changed);
    assert(cache_store_object(cache, "hash", 4U, hash, 0, 0) == CACHE_SET_OK);
    assert(kv_object_create_zset(&zset, engine) == 0);
    assert(kv_object_zset_add(zset, 2.5, "member", 6U,
                              &added, &changed) == 0 && added && changed);
    assert(kv_object_zset_add(zset, INFINITY, "positive", 8U,
                              &added, &changed) == 0 && added && changed);
    assert(kv_object_zset_add(zset, -INFINITY, "negative", 8U,
                              &added, &changed) == 0 && added && changed);
    assert(cache_store_object(cache, "zset", 4U, zset, 0, 0) == CACHE_SET_OK);
    assert(cache_set_expire_at(cache, "expired", 7U,
                               "gone", 4U, 1U) == CACHE_SET_OK);
}

static void verify(cache_t *cache)
{
    const unsigned char expected[] = {'v', 0, 'x'};
    const void *value;
    size_t length;
    kv_object_t *object;
    double score;

    value = cache_get(cache, "string", 6U, &length);
    assert(length == sizeof(expected) && memcmp(value, expected, length) == 0);
    object = cache_get_object(cache, "hash", 4U, 0);
    value = kv_object_hash_get(object, "field", 5U, &length);
    assert(length == 5U && memcmp(value, "value", 5U) == 0);
    object = cache_get_object(cache, "zset", 4U, 0);
    assert(kv_object_zset_score(object, "member", 6U, &score) == 1);
    assert(score == 2.5);
    assert(kv_object_zset_score(object, "positive", 8U, &score) == 1 &&
           isinf(score) && score > 0);
    assert(kv_object_zset_score(object, "negative", 8U, &score) == 1 &&
           isinf(score) && score < 0);
    assert(cache_get(cache, "expired", 7U, &length) == NULL);
}

int main(void)
{
    char path[] = "/tmp/storeSystem-rdb-XXXXXX";
    cache_t *source;
    cache_t *loaded;
    cache_config_t config = {0};
    rdb_checkpoint_t checkpoint = {0};
    rdb_checkpoint_t loaded_checkpoint;
    rdb_stats_t stats;
    int fd = mkstemp(path);
    unsigned char byte;
    unsigned char invalid_type = 99U;
    unsigned char overflow[8];

    assert(fd >= 0);
    assert(close(fd) == 0);
    assert(unlink(path) == 0);
    assert(cache_create(&source, &config) == 0);
    populate(source, KV_ZSET_SKIPLIST);
    checkpoint.valid = 1;
    checkpoint.aof_offset = 1234U;
    memset(checkpoint.token, 0x5a, sizeof(checkpoint.token));
    assert(rdb_save(path, source, &checkpoint, &stats) == 0);
    assert(stats.keys == 3U && stats.hash_fields == 1U &&
           stats.zset_members == 3U);
    assert(cache_create(&loaded, &config) == 0);
    assert(rdb_load(path, loaded, KV_ZSET_RBTREE,
                    &loaded_checkpoint, &stats) == 0);
    assert(loaded_checkpoint.valid && loaded_checkpoint.aof_offset == 1234U);
    assert(memcmp(loaded_checkpoint.token, checkpoint.token,
                  sizeof(checkpoint.token)) == 0);
    verify(loaded);
    cache_destroy(loaded);

    fd = open(path, O_RDWR);
    assert(fd >= 0);
    assert(pread(fd, &byte, 1U, 8) == 1);
    byte ^= 1U;
    assert(pwrite(fd, &byte, 1U, 8) == 1);
    assert(close(fd) == 0);
    assert(cache_create(&loaded, &config) == 0);
    assert(rdb_load(path, loaded, KV_ZSET_SKIPLIST, NULL, NULL) != 0);
    cache_destroy(loaded);

    assert(rdb_save(path, source, &checkpoint, &stats) == 0);
    byte = 'X';
    mutate_valid_rdb(path, 7U, &byte, 1U);
    assert(cache_create(&loaded, &config) == 0);
    assert(rdb_load(path, loaded, KV_ZSET_SKIPLIST, NULL, NULL) != 0);
    cache_destroy(loaded);

    assert(rdb_save(path, source, &checkpoint, &stats) == 0);
    mutate_valid_rdb(path, 49U, &invalid_type, 1U);
    assert(cache_create(&loaded, &config) == 0);
    assert(rdb_load(path, loaded, KV_ZSET_SKIPLIST, NULL, NULL) != 0);
    cache_destroy(loaded);

    assert(rdb_save(path, source, &checkpoint, &stats) == 0);
    memset(overflow, 0xff, sizeof(overflow));
    mutate_valid_rdb(path, 58U, overflow, sizeof(overflow));
    assert(cache_create(&loaded, &config) == 0);
    assert(rdb_load(path, loaded, KV_ZSET_SKIPLIST, NULL, NULL) != 0);
    cache_destroy(loaded);

    assert(rdb_save(path, source, &checkpoint, &stats) == 0);
    fd = open(path, O_RDWR);
    assert(fd >= 0);
    {
        struct stat status;

        assert(fstat(fd, &status) == 0 && status.st_size > 8);
        assert(ftruncate(fd, status.st_size - 1) == 0);
    }
    assert(close(fd) == 0);
    assert(cache_create(&loaded, &config) == 0);
    assert(rdb_load(path, loaded, KV_ZSET_SKIPLIST, NULL, NULL) != 0);
    cache_destroy(loaded);
    cache_destroy(source);
    assert(unlink(path) == 0);
    puts("test_rdb: PASS");
    return 0;
}
