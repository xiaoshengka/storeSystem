#define _POSIX_C_SOURCE 200809L

#include "cache/cache.h"

#include "engine/hash.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define CACHE_HEAP_NONE SIZE_MAX
#define CACHE_HEAP_INITIAL_CAPACITY 16U

enum removal_reason {
    REMOVE_NORMAL,
    REMOVE_EXPIRED,
    REMOVE_EVICTED
};

typedef struct cache_entry {
    kv_object_t *object;
    size_t memory_charge;
    size_t object_items;
    uint64_t expire_at_ms;
    size_t heap_index;
    kv_hash_node_t *index_node;
    struct cache_entry *lru_previous;
    struct cache_entry *lru_next;
} cache_entry_t;

struct cache {
    hashtable_t *index;
    cache_entry_t *lru_head;
    cache_entry_t *lru_tail;
    cache_entry_t **expire_heap;
    size_t heap_count;
    size_t heap_capacity;
    size_t used_memory;
    cache_config_t config;
    cache_eviction_fn on_evict;
    void *eviction_context;
    uint64_t hits;
    uint64_t misses;
    uint64_t expired_keys;
    uint64_t evicted_keys;
    size_t string_keys;
    size_t hash_keys;
    size_t zset_keys;
    size_t hash_fields;
    size_t zset_members;
};

static int valid_bytes(const void *data, size_t length)
{
    return data != NULL || length == 0;
}

static uint64_t system_now_ms(void *context)
{
    struct timespec time_value;

    (void)context;
    if (clock_gettime(CLOCK_REALTIME, &time_value) != 0) {
        return 0;
    }
    return (uint64_t)time_value.tv_sec * 1000U +
           (uint64_t)time_value.tv_nsec / 1000000U;
}

static uint64_t cache_now(const cache_t *cache)
{
    return cache->config.now_ms(cache->config.clock_context);
}

static size_t object_item_count(const kv_object_t *object)
{
    if (object == NULL) return 0;
    if (kv_object_type(object) == KV_OBJECT_HASH) {
        return kv_object_hash_length(object);
    }
    if (kv_object_type(object) == KV_OBJECT_ZSET) {
        return kv_object_zset_length(object);
    }
    return 0;
}

static size_t entry_memory_charge(size_t key_length, const kv_object_t *object)
{
    size_t node_charge = kv_hash_node_memory_for_key(key_length);
    size_t object_charge = kv_object_memory_usage(object);

    if (node_charge == SIZE_MAX || object == NULL) {
        return SIZE_MAX;
    }
    if (sizeof(cache_entry_t) > SIZE_MAX - node_charge ||
        object_charge > SIZE_MAX - sizeof(cache_entry_t) - node_charge) {
        return SIZE_MAX;
    }
    return sizeof(cache_entry_t) + node_charge + object_charge;
}

static void stats_add_object(cache_t *cache,
                             const kv_object_t *object,
                             size_t items)
{
    if (kv_object_type(object) == KV_OBJECT_STRING) cache->string_keys++;
    else if (kv_object_type(object) == KV_OBJECT_HASH) {
        cache->hash_keys++;
        cache->hash_fields += items;
    } else {
        cache->zset_keys++;
        cache->zset_members += items;
    }
}

static void stats_remove_object(cache_t *cache,
                                const kv_object_t *object,
                                size_t items)
{
    if (kv_object_type(object) == KV_OBJECT_STRING) cache->string_keys--;
    else if (kv_object_type(object) == KV_OBJECT_HASH) {
        cache->hash_keys--;
        cache->hash_fields -= items;
    } else {
        cache->zset_keys--;
        cache->zset_members -= items;
    }
}

static int heap_less(const cache_entry_t *left, const cache_entry_t *right)
{
    return left->expire_at_ms < right->expire_at_ms;
}

static void heap_swap(cache_t *cache, size_t left, size_t right)
{
    cache_entry_t *temporary = cache->expire_heap[left];

    cache->expire_heap[left] = cache->expire_heap[right];
    cache->expire_heap[right] = temporary;
    cache->expire_heap[left]->heap_index = left;
    cache->expire_heap[right]->heap_index = right;
}

static void heap_sift_up(cache_t *cache, size_t index)
{
    while (index > 0) {
        size_t parent = (index - 1U) / 2U;

        if (!heap_less(cache->expire_heap[index], cache->expire_heap[parent])) {
            break;
        }
        heap_swap(cache, index, parent);
        index = parent;
    }
}

static void heap_sift_down(cache_t *cache, size_t index)
{
    for (;;) {
        size_t left = index * 2U + 1U;
        size_t right = left + 1U;
        size_t smallest = index;

        if (left < cache->heap_count &&
            heap_less(cache->expire_heap[left], cache->expire_heap[smallest])) {
            smallest = left;
        }
        if (right < cache->heap_count &&
            heap_less(cache->expire_heap[right], cache->expire_heap[smallest])) {
            smallest = right;
        }
        if (smallest == index) {
            break;
        }
        heap_swap(cache, index, smallest);
        index = smallest;
    }
}

static int heap_reserve(cache_t *cache, size_t required)
{
    cache_entry_t **replacement;
    size_t capacity;

    if (required <= cache->heap_capacity) {
        return 0;
    }
    capacity = cache->heap_capacity == 0 ? CACHE_HEAP_INITIAL_CAPACITY
                                         : cache->heap_capacity;
    while (capacity < required) {
        if (capacity > SIZE_MAX / 2U) {
            return -1;
        }
        capacity *= 2U;
    }
    if (capacity > SIZE_MAX / sizeof(*replacement)) {
        return -1;
    }
    replacement = realloc(cache->expire_heap,
                          capacity * sizeof(*replacement));
    if (replacement == NULL) {
        return -1;
    }
    cache->expire_heap = replacement;
    cache->heap_capacity = capacity;
    return 0;
}

static void heap_insert_reserved(cache_t *cache, cache_entry_t *entry)
{
    size_t index = cache->heap_count++;

    cache->expire_heap[index] = entry;
    entry->heap_index = index;
    heap_sift_up(cache, index);
}

static void heap_remove_at(cache_t *cache, size_t index)
{
    cache_entry_t *removed;

    if (index >= cache->heap_count) {
        return;
    }
    removed = cache->expire_heap[index];
    cache->heap_count--;
    removed->heap_index = CACHE_HEAP_NONE;
    if (index == cache->heap_count) {
        return;
    }
    cache->expire_heap[index] = cache->expire_heap[cache->heap_count];
    cache->expire_heap[index]->heap_index = index;
    if (index > 0 &&
        heap_less(cache->expire_heap[index],
                  cache->expire_heap[(index - 1U) / 2U])) {
        heap_sift_up(cache, index);
    } else {
        heap_sift_down(cache, index);
    }
}

static void heap_expiration_changed(cache_t *cache, cache_entry_t *entry)
{
    size_t index = entry->heap_index;

    if (index > 0 &&
        heap_less(entry, cache->expire_heap[(index - 1U) / 2U])) {
        heap_sift_up(cache, index);
    } else {
        heap_sift_down(cache, index);
    }
}

static void lru_unlink(cache_t *cache, cache_entry_t *entry)
{
    if (entry->lru_previous != NULL) {
        entry->lru_previous->lru_next = entry->lru_next;
    } else {
        cache->lru_head = entry->lru_next;
    }
    if (entry->lru_next != NULL) {
        entry->lru_next->lru_previous = entry->lru_previous;
    } else {
        cache->lru_tail = entry->lru_previous;
    }
    entry->lru_previous = NULL;
    entry->lru_next = NULL;
}

static void lru_link_head(cache_t *cache, cache_entry_t *entry)
{
    entry->lru_previous = NULL;
    entry->lru_next = cache->lru_head;
    if (cache->lru_head != NULL) {
        cache->lru_head->lru_previous = entry;
    } else {
        cache->lru_tail = entry;
    }
    cache->lru_head = entry;
}

static void lru_touch(cache_t *cache, cache_entry_t *entry)
{
    if (cache->lru_head == entry) {
        return;
    }
    lru_unlink(cache, entry);
    lru_link_head(cache, entry);
}

static void entry_remove(cache_t *cache,
                         cache_entry_t *entry,
                         enum removal_reason reason)
{
    if (reason == REMOVE_EVICTED && cache->on_evict != NULL) {
        cache->on_evict(kv_hash_node_key(entry->index_node),
                        kv_hash_node_key_length(entry->index_node),
                        cache->eviction_context);
    }
    if (entry->heap_index != CACHE_HEAP_NONE) {
        heap_remove_at(cache, entry->heap_index);
    }
    lru_unlink(cache, entry);
    (void)kv_hash_remove_node(cache->index, entry->index_node);
    cache->used_memory -= entry->memory_charge;
    if (reason == REMOVE_EXPIRED) {
        cache->expired_keys++;
    } else if (reason == REMOVE_EVICTED) {
        cache->evicted_keys++;
    }
    stats_remove_object(cache, entry->object, entry->object_items);
    kv_object_destroy(entry->object);
    free(entry);
}

static cache_entry_t *find_live(cache_t *cache,
                                const void *key,
                                size_t key_length,
                                uint64_t now)
{
    kv_hash_node_t *node;
    cache_entry_t *entry;

    kv_hash_rehash_step(cache->index, 1U);
    node = kv_hash_find(cache->index, key, key_length);
    if (node == NULL) {
        return NULL;
    }
    entry = kv_hash_node_payload(node);
    if (entry->expire_at_ms != 0 && now >= entry->expire_at_ms) {
        entry_remove(cache, entry, REMOVE_EXPIRED);
        return NULL;
    }
    return entry;
}

static int limits_exceeded(const cache_t *cache,
                           size_t projected_keys,
                           size_t projected_memory)
{
    return (cache->config.max_keys != 0 &&
            projected_keys > cache->config.max_keys) ||
           (cache->config.max_memory != 0 &&
            projected_memory > cache->config.max_memory);
}

static cache_entry_t *eviction_candidate(cache_t *cache,
                                         cache_entry_t *protected_entry)
{
    cache_entry_t *candidate = cache->lru_tail;

    if (candidate == protected_entry) {
        candidate = candidate->lru_previous;
    }
    return candidate;
}

int cache_create(cache_t **out_cache, const cache_config_t *config)
{
    cache_t *cache;

    if (out_cache == NULL) {
        return -1;
    }
    *out_cache = NULL;
    cache = calloc(1, sizeof(*cache));
    if (cache == NULL) {
        return -1;
    }
    if (config != NULL) {
        cache->config = *config;
    }
    if (cache->config.now_ms == NULL) {
        cache->config.now_ms = system_now_ms;
        cache->config.clock_context = NULL;
    }
    if (kv_hash_create(&cache->index) != 0) {
        free(cache);
        return -1;
    }
    *out_cache = cache;
    return 0;
}

void cache_destroy(cache_t *cache)
{
    if (cache == NULL) {
        return;
    }
    while (cache->lru_tail != NULL) {
        entry_remove(cache, cache->lru_tail, REMOVE_NORMAL);
    }
    kv_hash_release(cache->index, NULL);
    free(cache->expire_heap);
    free(cache);
}

void cache_set_eviction_callback(cache_t *cache,
                                 cache_eviction_fn callback,
                                 void *context)
{
    if (cache != NULL) {
        cache->on_evict = callback;
        cache->eviction_context = context;
    }
}

int cache_store_object(cache_t *cache,
                       const void *key,
                       size_t key_length,
                       kv_object_t *object,
                       uint64_t expire_at_ms,
                       int preserve_existing_ttl)
{
    cache_entry_t *entry;
    uint64_t now;
    uint64_t expire_at = expire_at_ms;
    size_t new_charge;
    size_t new_items;

    if (cache == NULL || !valid_bytes(key, key_length) || object == NULL) {
        return CACHE_SET_ERROR;
    }
    now = cache_now(cache);
    new_charge = entry_memory_charge(key_length, object);
    new_items = object_item_count(object);
    if (new_charge == SIZE_MAX ||
        (cache->config.max_memory != 0 &&
         new_charge > cache->config.max_memory)) {
        return CACHE_SET_CAPACITY;
    }
    entry = find_live(cache, key, key_length, now);
    if (entry != NULL && preserve_existing_ttl) {
        expire_at = entry->expire_at_ms;
    }
    if (expire_at != 0 && entry != NULL && entry->expire_at_ms == 0 &&
        heap_reserve(cache, cache->heap_count + 1U) != 0) {
        return CACHE_SET_ERROR;
    }

    if (entry != NULL) {
        size_t projected_memory = cache->used_memory - entry->memory_charge +
                                  new_charge;

        while (limits_exceeded(cache,
                               kv_hash_count(cache->index),
                               projected_memory)) {
            cache_entry_t *candidate = eviction_candidate(cache, entry);

            if (candidate == NULL) return CACHE_SET_CAPACITY;
            projected_memory -= candidate->memory_charge;
            entry_remove(cache,
                         candidate,
                         candidate->expire_at_ms != 0 &&
                                 now >= candidate->expire_at_ms
                             ? REMOVE_EXPIRED
                             : REMOVE_EVICTED);
        }

        cache->used_memory = cache->used_memory - entry->memory_charge +
                             new_charge;
        stats_remove_object(cache, entry->object, entry->object_items);
        kv_object_destroy(entry->object);
        entry->object = object;
        entry->object_items = new_items;
        stats_add_object(cache, object, new_items);
        entry->memory_charge = new_charge;
        if (entry->expire_at_ms != 0 && expire_at == 0) {
            heap_remove_at(cache, entry->heap_index);
        } else if (entry->expire_at_ms == 0 && expire_at != 0) {
            entry->expire_at_ms = expire_at;
            heap_insert_reserved(cache, entry);
        } else if (entry->expire_at_ms != 0 && expire_at != 0) {
            entry->expire_at_ms = expire_at;
            heap_expiration_changed(cache, entry);
        }
        entry->expire_at_ms = expire_at;
        lru_touch(cache, entry);
        return CACHE_SET_OK;
    }

    if (expire_at != 0 &&
        heap_reserve(cache, cache->heap_count + 1U) != 0) {
        return CACHE_SET_ERROR;
    }
    entry = calloc(1, sizeof(*entry));
    if (entry == NULL) return CACHE_SET_ERROR;
    entry->object = object;
    entry->object_items = new_items;
    entry->memory_charge = new_charge;
    entry->expire_at_ms = expire_at;
    entry->heap_index = CACHE_HEAP_NONE;
    if (kv_hash_insert(cache->index,
                       key,
                       key_length,
                       entry,
                       &entry->index_node) != 0) {
        free(entry);
        return CACHE_SET_ERROR;
    }
    stats_add_object(cache, object, new_items);
    lru_link_head(cache, entry);
    cache->used_memory += new_charge;
    if (expire_at != 0) heap_insert_reserved(cache, entry);
    while (limits_exceeded(cache,
                           kv_hash_count(cache->index),
                           cache->used_memory)) {
        cache_entry_t *candidate = eviction_candidate(cache, entry);

        if (candidate == NULL) {
            entry->object = NULL;
            stats_remove_object(cache, object, new_items);
            if (entry->heap_index != CACHE_HEAP_NONE) {
                heap_remove_at(cache, entry->heap_index);
            }
            lru_unlink(cache, entry);
            (void)kv_hash_remove_node(cache->index, entry->index_node);
            cache->used_memory -= entry->memory_charge;
            free(entry);
            return CACHE_SET_CAPACITY;
        }
        entry_remove(cache,
                     candidate,
                     candidate->expire_at_ms != 0 && now >= candidate->expire_at_ms
                         ? REMOVE_EXPIRED
                         : REMOVE_EVICTED);
    }
    return CACHE_SET_OK;
}

int cache_set_expire_at(cache_t *cache,
                        const void *key,
                        size_t key_length,
                        const void *value,
                        size_t value_length,
                        uint64_t expire_at_ms)
{
    kv_object_t *object;
    int result;

    if (!valid_bytes(value, value_length) ||
        kv_object_create_string(&object, value, value_length) != 0) {
        return CACHE_SET_ERROR;
    }
    result = cache_store_object(cache,
                                key,
                                key_length,
                                object,
                                expire_at_ms,
                                0);
    if (result != CACHE_SET_OK) kv_object_destroy(object);
    return result;
}

int cache_set(cache_t *cache,
              const void *key,
              size_t key_length,
              const void *value,
              size_t value_length,
              uint64_t ttl_ms)
{
    uint64_t now;

    if (cache == NULL) {
        return CACHE_SET_ERROR;
    }
    now = cache_now(cache);
    if (ttl_ms != 0 && ttl_ms > UINT64_MAX - now) {
        return CACHE_SET_RANGE;
    }
    return cache_set_expire_at(cache,
                               key,
                               key_length,
                               value,
                               value_length,
                               ttl_ms == 0 ? 0 : now + ttl_ms);
}

kv_object_t *cache_get_object(cache_t *cache,
                              const void *key,
                              size_t key_length,
                              int record_hit_or_miss)
{
    cache_entry_t *entry;

    if (cache == NULL || !valid_bytes(key, key_length)) return NULL;
    entry = find_live(cache, key, key_length, cache_now(cache));
    if (entry == NULL) {
        if (record_hit_or_miss) cache->misses++;
        return NULL;
    }
    if (record_hit_or_miss) cache->hits++;
    lru_touch(cache, entry);
    return entry->object;
}

const void *cache_get(cache_t *cache,
                      const void *key,
                      size_t key_length,
                      size_t *value_length)
{
    kv_object_t *object;

    if (value_length == NULL) return NULL;
    *value_length = 0;
    object = cache_get_object(cache, key, key_length, 1);
    return kv_object_string_value(object, value_length);
}

int cache_recharge_object(cache_t *cache,
                          const void *key,
                          size_t key_length)
{
    cache_entry_t *entry;
    size_t new_charge;
    size_t new_items;
    size_t projected;
    uint64_t now;

    if (cache == NULL || !valid_bytes(key, key_length)) return CACHE_SET_ERROR;
    now = cache_now(cache);
    entry = find_live(cache, key, key_length, now);
    if (entry == NULL) return CACHE_SET_ERROR;
    new_charge = entry_memory_charge(key_length, entry->object);
    if (new_charge == SIZE_MAX ||
        (cache->config.max_memory != 0 &&
         new_charge > cache->config.max_memory)) return CACHE_SET_CAPACITY;
    projected = cache->used_memory - entry->memory_charge + new_charge;
    while (limits_exceeded(cache, kv_hash_count(cache->index), projected)) {
        cache_entry_t *candidate = eviction_candidate(cache, entry);

        if (candidate == NULL) return CACHE_SET_CAPACITY;
        projected -= candidate->memory_charge;
        entry_remove(cache,
                     candidate,
                     candidate->expire_at_ms != 0 && now >= candidate->expire_at_ms
                         ? REMOVE_EXPIRED
                         : REMOVE_EVICTED);
    }
    new_items = object_item_count(entry->object);
    if (kv_object_type(entry->object) == KV_OBJECT_HASH) {
        cache->hash_fields = cache->hash_fields - entry->object_items + new_items;
    } else if (kv_object_type(entry->object) == KV_OBJECT_ZSET) {
        cache->zset_members = cache->zset_members - entry->object_items + new_items;
    }
    entry->object_items = new_items;
    cache->used_memory = projected;
    entry->memory_charge = new_charge;
    lru_touch(cache, entry);
    return CACHE_SET_OK;
}

size_t cache_max_memory(const cache_t *cache)
{
    return cache == NULL ? 0 : cache->config.max_memory;
}

int cache_delete(cache_t *cache, const void *key, size_t key_length)
{
    cache_entry_t *entry;

    if (cache == NULL || !valid_bytes(key, key_length)) {
        return -1;
    }
    entry = find_live(cache, key, key_length, cache_now(cache));
    if (entry == NULL) {
        return 0;
    }
    entry_remove(cache, entry, REMOVE_NORMAL);
    return 1;
}

int cache_expire_at(cache_t *cache,
                    const void *key,
                    size_t key_length,
                    uint64_t expire_at_ms)
{
    cache_entry_t *entry;
    uint64_t now;

    if (cache == NULL || !valid_bytes(key, key_length)) {
        return -1;
    }
    if (expire_at_ms == 0) {
        return cache_delete(cache, key, key_length);
    }
    now = cache_now(cache);
    entry = find_live(cache, key, key_length, now);
    if (entry == NULL) {
        return 0;
    }
    if (entry->expire_at_ms == 0) {
        if (heap_reserve(cache, cache->heap_count + 1U) != 0) {
            return -1;
        }
        entry->expire_at_ms = expire_at_ms;
        heap_insert_reserved(cache, entry);
    } else {
        entry->expire_at_ms = expire_at_ms;
        heap_expiration_changed(cache, entry);
    }
    return 1;
}

int cache_expire(cache_t *cache,
                 const void *key,
                 size_t key_length,
                 uint64_t ttl_ms)
{
    uint64_t now;

    if (cache == NULL) {
        return -1;
    }
    if (ttl_ms == 0) {
        return cache_delete(cache, key, key_length);
    }
    now = cache_now(cache);
    if (ttl_ms > UINT64_MAX - now) {
        return -2;
    }
    return cache_expire_at(cache, key, key_length, now + ttl_ms);
}

int cache_persist(cache_t *cache, const void *key, size_t key_length)
{
    cache_entry_t *entry;

    if (cache == NULL || !valid_bytes(key, key_length)) {
        return -1;
    }
    entry = find_live(cache, key, key_length, cache_now(cache));
    if (entry == NULL || entry->expire_at_ms == 0) {
        return 0;
    }
    heap_remove_at(cache, entry->heap_index);
    entry->expire_at_ms = 0;
    return 1;
}

int cache_ttl_ms(cache_t *cache,
                 const void *key,
                 size_t key_length,
                 int64_t *ttl_ms)
{
    cache_entry_t *entry;
    uint64_t now;
    uint64_t remaining;

    if (cache == NULL || ttl_ms == NULL || !valid_bytes(key, key_length)) {
        return -1;
    }
    now = cache_now(cache);
    entry = find_live(cache, key, key_length, now);
    if (entry == NULL) {
        *ttl_ms = -2;
        return 0;
    }
    if (entry->expire_at_ms == 0) {
        *ttl_ms = -1;
        return 0;
    }
    remaining = entry->expire_at_ms - now;
    *ttl_ms = remaining > (uint64_t)INT64_MAX ? INT64_MAX
                                               : (int64_t)remaining;
    return 0;
}

size_t cache_maintain(cache_t *cache,
                      size_t expire_budget,
                      size_t rehash_budget)
{
    size_t expired = 0;
    uint64_t now;

    if (cache == NULL) {
        return 0;
    }
    now = cache_now(cache);
    while (expired < expire_budget && cache->heap_count > 0) {
        cache_entry_t *entry = cache->expire_heap[0];

        if (entry->expire_at_ms > now) {
            break;
        }
        entry_remove(cache, entry, REMOVE_EXPIRED);
        expired++;
    }
    kv_hash_rehash_step(cache->index, rehash_budget);
    return expired;
}

void cache_get_stats(const cache_t *cache, cache_stats_t *stats)
{
    if (stats == NULL) {
        return;
    }
    memset(stats, 0, sizeof(*stats));
    if (cache == NULL) {
        return;
    }
    stats->keys = kv_hash_count(cache->index);
    stats->used_memory = cache->used_memory;
    stats->index_memory = kv_hash_index_memory(cache->index) +
                          cache->heap_capacity * sizeof(*cache->expire_heap);
    stats->max_keys = cache->config.max_keys;
    stats->max_memory = cache->config.max_memory;
    stats->hits = cache->hits;
    stats->misses = cache->misses;
    stats->expired_keys = cache->expired_keys;
    stats->evicted_keys = cache->evicted_keys;
    stats->hash_slots = kv_hash_slot_count(cache->index);
    stats->rehashing = kv_hash_is_rehashing(cache->index);
    stats->string_keys = cache->string_keys;
    stats->hash_keys = cache->hash_keys;
    stats->zset_keys = cache->zset_keys;
    stats->hash_fields = cache->hash_fields;
    stats->zset_members = cache->zset_members;
}

uint64_t cache_current_time_ms(const cache_t *cache)
{
    return cache == NULL ? 0 : cache_now(cache);
}
