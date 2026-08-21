#include "engine/hash.h"
#include "kvstore.h"

#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define HASH_INITIAL_SLOTS 16U
#define HASH_REHASH_NONE SIZE_MAX

typedef struct kv_hash_table {
    kv_hash_node_t **buckets;
    size_t slots;
} kv_hash_table_t;

struct kv_hash_node {
    size_t key_length;
    uint64_t hash_code;
    void *payload;
    kv_hash_node_t *next;
    unsigned char key[];
};

struct hashtable_s {
    kv_hash_table_t tables[2];
    size_t count;
    size_t rehash_index;
};

typedef struct legacy_value {
    unsigned char *data;
    size_t length;
} legacy_value_t;

hashtable_t Hash;

static int valid_bytes(const void *data, size_t length)
{
    return data != NULL || length == 0;
}

static uint64_t hash_bytes(const void *key, size_t key_length)
{
    const unsigned char *cursor = key;
    size_t index;
    uint64_t value = UINT64_C(14695981039346656037);

    for (index = 0; index < key_length; ++index) {
        value ^= cursor[index];
        value *= UINT64_C(1099511628211);
    }
    value ^= value >> 33U;
    value *= UINT64_C(0xff51afd7ed558ccd);
    value ^= value >> 33U;
    value *= UINT64_C(0xc4ceb9fe1a85ec53);
    value ^= value >> 33U;
    return value;
}

static int key_equals(const kv_hash_node_t *node,
                      const void *key,
                      size_t key_length,
                      uint64_t hash_code)
{
    return node->hash_code == hash_code && node->key_length == key_length &&
           (key_length == 0 || memcmp(node->key, key, key_length) == 0);
}

static unsigned char *copy_bytes(const void *data, size_t length)
{
    unsigned char *copy;

    if (!valid_bytes(data, length) || length == SIZE_MAX) return NULL;
    copy = malloc(length + 1U);
    if (copy == NULL) return NULL;
    if (length > 0) memcpy(copy, data, length);
    copy[length] = '\0';
    return copy;
}

static int table_allocate(kv_hash_table_t *table, size_t slots)
{
    if (slots == 0 || slots > SIZE_MAX / sizeof(*table->buckets)) {
        return -1;
    }
    table->buckets = calloc(slots, sizeof(*table->buckets));
    if (table->buckets == NULL) {
        return -1;
    }
    table->slots = slots;
    return 0;
}

static int hash_init(hashtable_t *hash)
{
    if (hash == NULL) {
        return -1;
    }
    memset(hash, 0, sizeof(*hash));
    hash->rehash_index = HASH_REHASH_NONE;
    return table_allocate(&hash->tables[0], HASH_INITIAL_SLOTS);
}

static void hash_clear(hashtable_t *hash,
                       kv_hash_payload_destroy_fn destroy_payload)
{
    size_t table_index;

    if (hash == NULL) {
        return;
    }
    for (table_index = 0; table_index < 2U; ++table_index) {
        kv_hash_table_t *table = &hash->tables[table_index];
        size_t bucket;

        for (bucket = 0; bucket < table->slots; ++bucket) {
            kv_hash_node_t *node = table->buckets[bucket];

            while (node != NULL) {
                kv_hash_node_t *next = node->next;

                if (destroy_payload != NULL) {
                    destroy_payload(node->payload);
                }
                free(node);
                node = next;
            }
        }
        free(table->buckets);
        memset(table, 0, sizeof(*table));
    }
    hash->count = 0;
    hash->rehash_index = HASH_REHASH_NONE;
}

static int start_expand(hashtable_t *hash)
{
    size_t new_slots;

    if (kv_hash_is_rehashing(hash)) {
        return 0;
    }
    if (hash->tables[0].slots > SIZE_MAX / 2U) {
        return -1;
    }
    new_slots = hash->tables[0].slots * 2U;
    if (table_allocate(&hash->tables[1], new_slots) != 0) {
        return -1;
    }
    hash->rehash_index = 0;
    return 0;
}

static int maybe_expand(hashtable_t *hash)
{
    size_t slots;
    size_t threshold;

    if (kv_hash_is_rehashing(hash)) {
        return 0;
    }
    slots = hash->tables[0].slots;
    threshold = slots - slots / 4U;
    if (hash->count + 1U <= threshold) {
        return 0;
    }
    return start_expand(hash);
}

static kv_hash_node_t *find_in_table(kv_hash_table_t *table,
                                     const void *key,
                                     size_t key_length,
                                     uint64_t hash_code)
{
    kv_hash_node_t *node;
    size_t bucket;

    if (table->slots == 0) {
        return NULL;
    }
    bucket = (size_t)(hash_code & (uint64_t)(table->slots - 1U));
    node = table->buckets[bucket];
    while (node != NULL) {
        if (key_equals(node, key, key_length, hash_code)) {
            return node;
        }
        node = node->next;
    }
    return NULL;
}

static void *remove_target(hashtable_t *hash, kv_hash_node_t *target)
{
    size_t table_index;

    if (hash == NULL || target == NULL) {
        return NULL;
    }
    for (table_index = 0; table_index < 2U; ++table_index) {
        kv_hash_table_t *table = &hash->tables[table_index];
        kv_hash_node_t **link;
        size_t bucket;

        if (table->slots == 0) {
            continue;
        }
        bucket = (size_t)(target->hash_code &
                          (uint64_t)(table->slots - 1U));
        link = &table->buckets[bucket];
        while (*link != NULL && *link != target) {
            link = &(*link)->next;
        }
        if (*link == target) {
            void *payload = target->payload;

            *link = target->next;
            free(target);
            hash->count--;
            return payload;
        }
    }
    return NULL;
}

int kv_hash_create(hashtable_t **out_hash)
{
    hashtable_t *hash;

    if (out_hash == NULL) {
        return -1;
    }
    *out_hash = NULL;
    hash = malloc(sizeof(*hash));
    if (hash == NULL || hash_init(hash) != 0) {
        free(hash);
        return -1;
    }
    *out_hash = hash;
    return 0;
}

void kv_hash_release(hashtable_t *hash,
                     kv_hash_payload_destroy_fn destroy_payload)
{
    if (hash == NULL) {
        return;
    }
    hash_clear(hash, destroy_payload);
    free(hash);
}

kv_hash_node_t *kv_hash_find(hashtable_t *hash,
                             const void *key,
                             size_t key_length)
{
    uint64_t hash_code;
    kv_hash_node_t *node;

    if (hash == NULL || !valid_bytes(key, key_length)) {
        return NULL;
    }
    hash_code = hash_bytes(key, key_length);
    node = find_in_table(&hash->tables[0], key, key_length, hash_code);
    if (node == NULL && kv_hash_is_rehashing(hash)) {
        node = find_in_table(&hash->tables[1], key, key_length, hash_code);
    }
    return node;
}

int kv_hash_insert(hashtable_t *hash,
                   const void *key,
                   size_t key_length,
                   void *payload,
                   kv_hash_node_t **out_node)
{
    kv_hash_node_t *node;
    kv_hash_table_t *destination;
    uint64_t hash_code;
    size_t bucket;

    if (out_node != NULL) {
        *out_node = NULL;
    }
    if (hash == NULL || !valid_bytes(key, key_length) || key_length == SIZE_MAX) {
        return -1;
    }
    hash_code = hash_bytes(key, key_length);
    if (find_in_table(&hash->tables[0], key, key_length, hash_code) != NULL ||
        (kv_hash_is_rehashing(hash) &&
         find_in_table(&hash->tables[1], key, key_length, hash_code) != NULL) ||
        maybe_expand(hash) != 0 ||
        sizeof(*node) > SIZE_MAX - key_length - 1U) {
        return -1;
    }
    node = calloc(1, sizeof(*node) + key_length + 1U);
    if (node == NULL) {
        return -1;
    }
    if (key_length > 0) memcpy(node->key, key, key_length);
    node->key[key_length] = '\0';
    node->key_length = key_length;
    node->hash_code = hash_code;
    node->payload = payload;
    destination = kv_hash_is_rehashing(hash) ? &hash->tables[1]
                                             : &hash->tables[0];
    bucket = (size_t)(node->hash_code &
                      (uint64_t)(destination->slots - 1U));
    node->next = destination->buckets[bucket];
    destination->buckets[bucket] = node;
    hash->count++;
    if (out_node != NULL) {
        *out_node = node;
    }
    return 0;
}

void *kv_hash_remove(hashtable_t *hash,
                     const void *key,
                     size_t key_length)
{
    kv_hash_node_t *node = kv_hash_find(hash, key, key_length);

    return remove_target(hash, node);
}

void *kv_hash_remove_node(hashtable_t *hash, kv_hash_node_t *target)
{
    return remove_target(hash, target);
}

void *kv_hash_node_payload(const kv_hash_node_t *node)
{
    return node != NULL ? node->payload : NULL;
}

void kv_hash_node_set_payload(kv_hash_node_t *node, void *payload)
{
    if (node != NULL) {
        node->payload = payload;
    }
}

const void *kv_hash_node_key(const kv_hash_node_t *node)
{
    return node != NULL ? node->key : NULL;
}

size_t kv_hash_node_key_length(const kv_hash_node_t *node)
{
    return node != NULL ? node->key_length : 0;
}

size_t kv_hash_count(const hashtable_t *hash)
{
    return hash != NULL ? hash->count : 0;
}

size_t kv_hash_slot_count(const hashtable_t *hash)
{
    if (hash == NULL) {
        return 0;
    }
    return kv_hash_is_rehashing(hash) ? hash->tables[1].slots
                                      : hash->tables[0].slots;
}

size_t kv_hash_index_memory(const hashtable_t *hash)
{
    size_t result;

    if (hash == NULL) {
        return 0;
    }
    result = sizeof(*hash);
    result += hash->tables[0].slots * sizeof(*hash->tables[0].buckets);
    result += hash->tables[1].slots * sizeof(*hash->tables[1].buckets);
    return result;
}

size_t kv_hash_node_memory_for_key(size_t key_length)
{
    if (key_length == SIZE_MAX ||
        sizeof(kv_hash_node_t) > SIZE_MAX - key_length - 1U) {
        return SIZE_MAX;
    }
    return sizeof(kv_hash_node_t) + key_length + 1U;
}

int kv_hash_is_rehashing(const hashtable_t *hash)
{
    return hash != NULL && hash->rehash_index != HASH_REHASH_NONE;
}

size_t kv_hash_rehash_step(hashtable_t *hash, size_t bucket_budget)
{
    size_t empty_budget;
    size_t moved = 0;

    if (hash == NULL || !kv_hash_is_rehashing(hash)) {
        return 0;
    }
    empty_budget = bucket_budget > SIZE_MAX / 10U
                       ? SIZE_MAX
                       : bucket_budget * 10U;
    while (bucket_budget > 0 && hash->rehash_index < hash->tables[0].slots) {
        kv_hash_node_t *node = hash->tables[0].buckets[hash->rehash_index];

        if (node == NULL && empty_budget > 0) {
            hash->rehash_index++;
            empty_budget--;
            continue;
        }

        hash->tables[0].buckets[hash->rehash_index] = NULL;
        while (node != NULL) {
            kv_hash_node_t *next = node->next;
            size_t bucket = (size_t)(node->hash_code &
                                     (uint64_t)(hash->tables[1].slots - 1U));

            node->next = hash->tables[1].buckets[bucket];
            hash->tables[1].buckets[bucket] = node;
            node = next;
        }
        hash->rehash_index++;
        bucket_budget--;
        moved++;
    }
    if (hash->rehash_index == hash->tables[0].slots) {
        free(hash->tables[0].buckets);
        hash->tables[0] = hash->tables[1];
        memset(&hash->tables[1], 0, sizeof(hash->tables[1]));
        hash->rehash_index = HASH_REHASH_NONE;
    }
    return moved;
}

void kv_hash_iterator_begin(hashtable_t *hash, kv_hash_iterator_t *iterator)
{
    if (iterator == NULL) {
        return;
    }
    iterator->hash = hash;
    iterator->table_index = 0;
    iterator->bucket_index = 0;
    iterator->next = NULL;
}

kv_hash_node_t *kv_hash_iterator_next(kv_hash_iterator_t *iterator)
{
    if (iterator == NULL || iterator->hash == NULL) {
        return NULL;
    }
    if (iterator->next != NULL) {
        kv_hash_node_t *result = iterator->next;

        iterator->next = result->next;
        return result;
    }
    while (iterator->table_index < 2U) {
        kv_hash_table_t *table = &iterator->hash->tables[iterator->table_index];

        while (iterator->bucket_index < table->slots) {
            kv_hash_node_t *result = table->buckets[iterator->bucket_index++];

            if (result != NULL) {
                iterator->next = result->next;
                return result;
            }
        }
        iterator->table_index++;
        iterator->bucket_index = 0;
    }
    return NULL;
}

static void legacy_value_destroy(void *payload)
{
    legacy_value_t *value = payload;

    if (value != NULL) {
        free(value->data);
        free(value);
    }
}

static legacy_value_t *legacy_value_create(const void *data, size_t length)
{
    legacy_value_t *value = malloc(sizeof(*value));

    if (value == NULL) {
        return NULL;
    }
    value->data = copy_bytes(data, length);
    if (value->data == NULL) {
        free(value);
        return NULL;
    }
    value->length = length;
    return value;
}

static int set_bytes(hashtable_t *hash,
                     const void *key,
                     size_t key_length,
                     const void *value,
                     size_t value_length,
                     int overwrite)
{
    kv_hash_node_t *node;
    legacy_value_t *replacement;

    if (hash == NULL || !valid_bytes(key, key_length) ||
        !valid_bytes(value, value_length)) {
        return -1;
    }
    kv_hash_rehash_step(hash, 1U);
    node = kv_hash_find(hash, key, key_length);
    if (node != NULL && !overwrite) {
        return 1;
    }
    replacement = legacy_value_create(value, value_length);
    if (replacement == NULL) {
        return -1;
    }
    if (node != NULL) {
        legacy_value_destroy(kv_hash_node_payload(node));
        kv_hash_node_set_payload(node, replacement);
        return 0;
    }
    if (kv_hash_insert(hash, key, key_length, replacement, NULL) != 0) {
        legacy_value_destroy(replacement);
        return -1;
    }
    return 0;
}

int kvstore_hash_create(hashtable_t *hash)
{
    return hash_init(hash);
}

void kvstore_hash_destory(hashtable_t *hash)
{
    hash_clear(hash, legacy_value_destroy);
}

int kvs_hash_set(hashtable_t *hash, char *key, char *value)
{
    if (key == NULL || value == NULL) {
        return -1;
    }
    return set_bytes(hash, key, strlen(key), value, strlen(value), 0);
}

char *kvs_hash_get(hashtable_t *hash, char *key)
{
    legacy_value_t *value;

    if (key == NULL) {
        return NULL;
    }
    value = kv_hash_node_payload(kv_hash_find(hash, key, strlen(key)));
    return value != NULL ? (char *)value->data : NULL;
}

int kvs_hash_delete(hashtable_t *hash, char *key)
{
    legacy_value_t *value;

    if (hash == NULL || key == NULL) {
        return -2;
    }
    value = kv_hash_remove(hash, key, strlen(key));
    if (value == NULL) {
        return -1;
    }
    legacy_value_destroy(value);
    return 0;
}

int kvs_hash_modify(hashtable_t *hash, char *key, char *value)
{
    kv_hash_node_t *node;
    legacy_value_t *replacement;

    if (hash == NULL || key == NULL || value == NULL) {
        return -1;
    }
    node = kv_hash_find(hash, key, strlen(key));
    if (node == NULL) {
        return -1;
    }
    replacement = legacy_value_create(value, strlen(value));
    if (replacement == NULL) {
        return -1;
    }
    legacy_value_destroy(kv_hash_node_payload(node));
    kv_hash_node_set_payload(node, replacement);
    return 0;
}

int kvs_hash_count(hashtable_t *hash)
{
    size_t count = kv_hash_count(hash);

    return count > (size_t)INT_MAX ? INT_MAX : (int)count;
}

int kvs_hash_upsert_bytes(hashtable_t *hash,
                          const void *key,
                          size_t key_length,
                          const void *value,
                          size_t value_length)
{
    return set_bytes(hash, key, key_length, value, value_length, 1);
}

const void *kvs_hash_get_bytes(hashtable_t *hash,
                               const void *key,
                               size_t key_length,
                               size_t *value_length)
{
    legacy_value_t *value;

    if (value_length == NULL) {
        return NULL;
    }
    *value_length = 0;
    value = kv_hash_node_payload(kv_hash_find(hash, key, key_length));
    if (value == NULL) {
        return NULL;
    }
    *value_length = value->length;
    return value->data;
}

int kvs_hash_delete_bytes(hashtable_t *hash,
                          const void *key,
                          size_t key_length)
{
    legacy_value_t *value;

    if (hash == NULL || !valid_bytes(key, key_length)) {
        return -1;
    }
    value = kv_hash_remove(hash, key, key_length);
    if (value == NULL) {
        return 0;
    }
    legacy_value_destroy(value);
    return 1;
}

int put_kv_hashtable(hashtable_t *hash, char *key, char *value)
{
    return kvs_hash_set(hash, key, value);
}

char *get_kv_hashtable(hashtable_t *hash, char *key)
{
    return kvs_hash_get(hash, key);
}

int count_kv_hashtable(hashtable_t *hash)
{
    return kvs_hash_count(hash);
}

int delete_kv_hashtable(hashtable_t *hash, char *key)
{
    return kvs_hash_delete(hash, key);
}

int exist_kv_hashtable(hashtable_t *hash, char *key)
{
    return kvs_hash_get(hash, key) != NULL;
}
