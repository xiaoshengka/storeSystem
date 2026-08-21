#ifndef STORE_SYSTEM_CACHE_CACHE_H
#define STORE_SYSTEM_CACHE_CACHE_H

#include <stddef.h>
#include <stdint.h>

#include "engine/object.h"

typedef struct cache cache_t;
typedef struct cache_entry cache_entry_ref_t;

typedef uint64_t (*cache_now_ms_fn)(void *context);
typedef void (*cache_eviction_fn)(const void *key,
                                  size_t key_length,
                                  void *context);
typedef int (*cache_visit_fn)(const void *key,
                              size_t key_length,
                              kv_object_t *object,
                              uint64_t expire_at_ms,
                              void *context);

typedef struct cache_config {
    size_t max_keys;
    size_t max_memory;
    cache_now_ms_fn now_ms;
    void *clock_context;
} cache_config_t;

typedef struct cache_stats {
    size_t keys;
    size_t used_memory;
    size_t index_memory;
    size_t max_keys;
    size_t max_memory;
    uint64_t hits;
    uint64_t misses;
    uint64_t expired_keys;
    uint64_t evicted_keys;
    size_t hash_slots;
    int rehashing;
    size_t string_keys;
    size_t hash_keys;
    size_t zset_keys;
    size_t hash_fields;
    size_t zset_members;
} cache_stats_t;

enum cache_set_result {
    CACHE_SET_ERROR = -1,
    CACHE_SET_OK = 0,
    CACHE_SET_CAPACITY = 1,
    CACHE_SET_RANGE = 2
};

int cache_create(cache_t **out_cache, const cache_config_t *config);
void cache_destroy(cache_t *cache);
void cache_set_eviction_callback(cache_t *cache,
                                 cache_eviction_fn callback,
                                 void *context);

int cache_set(cache_t *cache,
              const void *key,
              size_t key_length,
              const void *value,
              size_t value_length,
              uint64_t ttl_ms);
int cache_set_expire_at(cache_t *cache,
                        const void *key,
                        size_t key_length,
                        const void *value,
                        size_t value_length,
                        uint64_t expire_at_ms);
const void *cache_get(cache_t *cache,
                      const void *key,
                      size_t key_length,
                      size_t *value_length);
kv_object_t *cache_get_object(cache_t *cache,
                              const void *key,
                              size_t key_length,
                              int record_hit_or_miss);
kv_object_t *cache_get_object_ref(cache_t *cache,
                                  const void *key,
                                  size_t key_length,
                                  int record_hit_or_miss,
                                  cache_entry_ref_t **entry_ref);
kv_object_t *cache_get_hash_object_ref(
    cache_t *cache,
    const void *key,
    size_t key_length,
    int record_hit_or_miss,
    cache_entry_ref_t **entry_ref);
int cache_store_object(cache_t *cache,
                       const void *key,
                       size_t key_length,
                       kv_object_t *object,
                       uint64_t expire_at_ms,
                       int preserve_existing_ttl);
int cache_recharge_object(cache_t *cache,
                          const void *key,
                          size_t key_length);
int cache_recharge_ref(cache_t *cache, cache_entry_ref_t *entry_ref);
size_t cache_max_memory(const cache_t *cache);
int cache_delete(cache_t *cache, const void *key, size_t key_length);
int cache_expire(cache_t *cache,
                 const void *key,
                 size_t key_length,
                 uint64_t ttl_ms);
int cache_expire_at(cache_t *cache,
                    const void *key,
                    size_t key_length,
                    uint64_t expire_at_ms);
int cache_persist(cache_t *cache, const void *key, size_t key_length);
int cache_ttl_ms(cache_t *cache,
                 const void *key,
                 size_t key_length,
                 int64_t *ttl_ms);

size_t cache_maintain(cache_t *cache, size_t expire_budget, size_t rehash_budget);
void cache_get_stats(const cache_t *cache, cache_stats_t *stats);
uint64_t cache_current_time_ms(const cache_t *cache);
int cache_visit(cache_t *cache, cache_visit_fn callback, void *context);

#endif
