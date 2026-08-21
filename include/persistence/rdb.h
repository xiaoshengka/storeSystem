#ifndef STORE_SYSTEM_PERSISTENCE_RDB_H
#define STORE_SYSTEM_PERSISTENCE_RDB_H

#include <stddef.h>
#include <stdint.h>

#include "cache/cache.h"

#define RDB_CHECKPOINT_TOKEN_SIZE 16U

typedef struct rdb_checkpoint {
    uint64_t aof_offset;
    unsigned char token[RDB_CHECKPOINT_TOKEN_SIZE];
    int valid;
} rdb_checkpoint_t;

typedef struct rdb_stats {
    uint64_t snapshot_time_ms;
    size_t keys;
    size_t string_keys;
    size_t hash_keys;
    size_t zset_keys;
    size_t hash_fields;
    size_t zset_members;
    uint64_t bytes;
} rdb_stats_t;

int rdb_save(const char *path,
             cache_t *cache,
             const rdb_checkpoint_t *checkpoint,
             rdb_stats_t *stats);
int rdb_load(const char *path,
             cache_t *cache,
             kv_zset_engine_t zset_engine,
             rdb_checkpoint_t *checkpoint,
             rdb_stats_t *stats);

#endif
