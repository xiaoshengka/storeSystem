#include "cache/cache.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

typedef struct fake_clock {
    uint64_t now_ms;
} fake_clock_t;

static uint64_t fake_now(void *context)
{
    return ((fake_clock_t *)context)->now_ms;
}

static void assert_value(cache_t *cache, const char *key, const char *expected)
{
    size_t length = 0;
    const void *value = cache_get(cache, key, strlen(key), &length);

    if (expected == NULL) {
        assert(value == NULL);
    } else {
        assert(value != NULL);
        assert(length == strlen(expected));
        assert(memcmp(value, expected, length) == 0);
    }
}

static void test_lru_and_ttl(void)
{
    fake_clock_t clock = {1000U};
    cache_config_t config = {0};
    cache_stats_t stats;
    cache_t *cache;
    int64_t ttl;

    config.max_keys = 2U;
    config.now_ms = fake_now;
    config.clock_context = &clock;
    assert(cache_create(&cache, &config) == 0);

    assert(cache_set(cache, "a", 1, "one", 3, 0) == CACHE_SET_OK);
    assert(cache_set(cache, "b", 1, "two", 3, 0) == CACHE_SET_OK);
    assert_value(cache, "a", "one");
    assert(cache_set(cache, "c", 1, "three", 5, 0) == CACHE_SET_OK);
    assert_value(cache, "b", NULL);
    assert_value(cache, "a", "one");
    assert_value(cache, "c", "three");

    assert(cache_set(cache, "exp", 3, "soon", 4, 100U) == CACHE_SET_OK);
    assert(cache_ttl_ms(cache, "exp", 3, &ttl) == 0 && ttl == 100);
    clock.now_ms += 99U;
    assert_value(cache, "exp", "soon");
    clock.now_ms += 1U;
    assert_value(cache, "exp", NULL);
    assert(cache_ttl_ms(cache, "exp", 3, &ttl) == 0 && ttl == -2);

    assert(cache_set(cache, "persist", 7, "v", 1, 50U) == CACHE_SET_OK);
    assert(cache_persist(cache, "persist", 7) == 1);
    clock.now_ms += 100U;
    assert_value(cache, "persist", "v");
    assert(cache_expire(cache, "persist", 7, 25U) == 1);
    assert(cache_set(cache, "persist", 7, "new", 3, 0) == CACHE_SET_OK);
    clock.now_ms += 30U;
    assert_value(cache, "persist", "new");

    cache_get_stats(cache, &stats);
    assert(stats.keys <= 2U);
    assert(stats.evicted_keys >= 1U);
    assert(stats.expired_keys >= 1U);
    assert(stats.hits > 0U && stats.misses > 0U);
    cache_destroy(cache);
}

static void test_heap_and_memory_limit(void)
{
    fake_clock_t clock = {5000U};
    cache_config_t config = {0};
    cache_stats_t stats;
    cache_t *cache;
    size_t one_entry_memory;

    config.now_ms = fake_now;
    config.clock_context = &clock;
    assert(cache_create(&cache, &config) == 0);
    assert(cache_set(cache, "one", 3, "value", 5, 100U) == CACHE_SET_OK);
    cache_get_stats(cache, &stats);
    one_entry_memory = stats.used_memory;
    cache_destroy(cache);

    config.max_memory = one_entry_memory;
    assert(cache_create(&cache, &config) == 0);
    assert(cache_set(cache, "one", 3, "value", 5, 100U) == CACHE_SET_OK);
    assert(cache_set(cache, "one", 3, "value-is-too-large", 18, 0) ==
           CACHE_SET_CAPACITY);
    assert_value(cache, "one", "value");
    cache_destroy(cache);

    config.max_memory = 0;
    assert(cache_create(&cache, &config) == 0);
    assert(cache_set(cache, "late", 4, "l", 1, 300U) == CACHE_SET_OK);
    assert(cache_set(cache, "early", 5, "e", 1, 100U) == CACHE_SET_OK);
    assert(cache_set(cache, "middle", 6, "m", 1, 200U) == CACHE_SET_OK);
    assert(cache_delete(cache, "middle", 6) == 1);
    clock.now_ms += 100U;
    assert(cache_maintain(cache, 1U, 16U) == 1U);
    assert_value(cache, "early", NULL);
    assert_value(cache, "late", "l");
    clock.now_ms += 200U;
    assert(cache_maintain(cache, 64U, 16U) == 1U);
    cache_get_stats(cache, &stats);
    assert(stats.keys == 0U);
    assert(stats.expired_keys == 2U);
    cache_destroy(cache);
}

static void test_hash_hot_entry_invalidation(void)
{
    fake_clock_t clock = {9000U};
    cache_config_t config = {0};
    cache_entry_ref_t *entry_ref = NULL;
    kv_object_t *object;
    cache_t *cache;

    config.now_ms = fake_now;
    config.clock_context = &clock;
    assert(cache_create(&cache, &config) == 0);
    assert(kv_object_create_hash(&object) == 0);
    assert(cache_store_object(cache, "hash", 4, object, 9010U, 0) ==
           CACHE_SET_OK);
    assert(cache_get_hash_object_ref(cache, "hash", 4, 1,
                                     &entry_ref) == object);
    assert(entry_ref != NULL);
    clock.now_ms = 9010U;
    assert(cache_get_hash_object_ref(cache, "hash", 4, 1, NULL) == NULL);

    assert(kv_object_create_hash(&object) == 0);
    assert(cache_store_object(cache, "next", 4, object, 0, 0) == CACHE_SET_OK);
    assert(cache_get_hash_object_ref(cache, "next", 4, 0, NULL) == object);
    assert(cache_delete(cache, "next", 4) == 1);
    assert(cache_get_hash_object_ref(cache, "next", 4, 0, NULL) == NULL);
    cache_destroy(cache);

    config.max_keys = 1U;
    assert(cache_create(&cache, &config) == 0);
    assert(kv_object_create_hash(&object) == 0);
    assert(cache_store_object(cache, "old", 3, object, 0, 0) == CACHE_SET_OK);
    assert(cache_get_hash_object_ref(cache, "old", 3, 0, NULL) == object);
    assert(kv_object_create_hash(&object) == 0);
    assert(cache_store_object(cache, "new", 3, object, 0, 0) == CACHE_SET_OK);
    assert(cache_get_hash_object_ref(cache, "old", 3, 0, NULL) == NULL);
    assert(cache_get_hash_object_ref(cache, "new", 3, 0, NULL) == object);
    cache_destroy(cache);
}

int main(void)
{
    test_lru_and_ttl();
    test_heap_and_memory_limit();
    test_hash_hot_entry_invalidation();
    puts("test_cache: PASS");
    return 0;
}
