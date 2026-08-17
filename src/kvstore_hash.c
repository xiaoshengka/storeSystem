#include "kvstore.h"

#include <stdint.h>
#include <string.h>

#define MAX_TABLE_SIZE 102400

typedef struct hashnode_s {
    unsigned char *key;
    size_t key_length;
    unsigned char *value;
    size_t value_length;
    struct hashnode_s *next;
} hashnode_t;

struct hashtable_s {
    hashnode_t **nodes;
    int max_slots;
    int count;
};

hashtable_t Hash;

static int valid_bytes(const void *data, size_t length)
{
    return data != NULL || length == 0;
}

static size_t hash_bytes(const void *key, size_t key_length, size_t slot_count)
{
    const unsigned char *cursor = key;
    size_t index;
    uint64_t value = 5381U;

    for (index = 0; index < key_length; ++index) {
        value = ((value << 5U) + value) + cursor[index];
    }
    return (size_t)(value % slot_count);
}

static int key_equals(const hashnode_t *node, const void *key, size_t key_length)
{
    return node->key_length == key_length &&
           (key_length == 0 || memcmp(node->key, key, key_length) == 0);
}

static unsigned char *copy_bytes(const void *data, size_t length)
{
    unsigned char *copy;

    if (length == SIZE_MAX) {
        return NULL;
    }
    copy = kvstore_malloc(length + 1U);
    if (copy == NULL) {
        return NULL;
    }
    if (length > 0) {
        memcpy(copy, data, length);
    }
    copy[length] = '\0';
    return copy;
}

static hashnode_t *create_node(const void *key,
                               size_t key_length,
                               const void *value,
                               size_t value_length)
{
    hashnode_t *node = kvstore_malloc(sizeof(*node));

    if (node == NULL) {
        return NULL;
    }
    memset(node, 0, sizeof(*node));
    node->key = copy_bytes(key, key_length);
    if (node->key == NULL) {
        kvstore_free(node);
        return NULL;
    }
    node->value = copy_bytes(value, value_length);
    if (node->value == NULL) {
        kvstore_free(node->key);
        kvstore_free(node);
        return NULL;
    }
    node->key_length = key_length;
    node->value_length = value_length;
    return node;
}

static hashnode_t *find_node(hashtable_t *hash,
                             const void *key,
                             size_t key_length,
                             size_t *slot)
{
    hashnode_t *node;
    size_t index;

    if (hash == NULL || hash->nodes == NULL || hash->max_slots <= 0 ||
        !valid_bytes(key, key_length)) {
        return NULL;
    }
    index = hash_bytes(key, key_length, (size_t)hash->max_slots);
    if (slot != NULL) {
        *slot = index;
    }
    node = hash->nodes[index];
    while (node != NULL) {
        if (key_equals(node, key, key_length)) {
            return node;
        }
        node = node->next;
    }
    return NULL;
}

static int set_bytes(hashtable_t *hash,
                     const void *key,
                     size_t key_length,
                     const void *value,
                     size_t value_length,
                     int overwrite)
{
    hashnode_t *node;
    size_t slot = 0;

    if (hash == NULL || hash->nodes == NULL || !valid_bytes(key, key_length) ||
        !valid_bytes(value, value_length)) {
        return -1;
    }
    node = find_node(hash, key, key_length, &slot);
    if (node != NULL) {
        unsigned char *replacement;

        if (!overwrite) {
            return 1;
        }
        replacement = copy_bytes(value, value_length);
        if (replacement == NULL) {
            return -1;
        }
        kvstore_free(node->value);
        node->value = replacement;
        node->value_length = value_length;
        return 0;
    }

    node = create_node(key, key_length, value, value_length);
    if (node == NULL) {
        return -1;
    }
    node->next = hash->nodes[slot];
    hash->nodes[slot] = node;
    hash->count++;
    return 0;
}

int init_hashtable(hashtable_t *hash)
{
    if (hash == NULL) {
        return -1;
    }
    hash->nodes = kvstore_malloc(sizeof(*hash->nodes) * MAX_TABLE_SIZE);
    if (hash->nodes == NULL) {
        return -1;
    }
    memset(hash->nodes, 0, sizeof(*hash->nodes) * MAX_TABLE_SIZE);
    hash->max_slots = MAX_TABLE_SIZE;
    hash->count = 0;
    return 0;
}

void dest_hashtable(hashtable_t *hash)
{
    int index;

    if (hash == NULL || hash->nodes == NULL) {
        return;
    }
    for (index = 0; index < hash->max_slots; ++index) {
        hashnode_t *node = hash->nodes[index];

        while (node != NULL) {
            hashnode_t *next = node->next;

            kvstore_free(node->key);
            kvstore_free(node->value);
            kvstore_free(node);
            node = next;
        }
    }
    kvstore_free(hash->nodes);
    hash->nodes = NULL;
    hash->max_slots = 0;
    hash->count = 0;
}

int put_kv_hashtable(hashtable_t *hash, char *key, char *value)
{
    if (key == NULL || value == NULL) {
        return -1;
    }
    return set_bytes(hash, key, strlen(key), value, strlen(value), 0);
}

char *get_kv_hashtable(hashtable_t *hash, char *key)
{
    hashnode_t *node;

    if (key == NULL) {
        return NULL;
    }
    node = find_node(hash, key, strlen(key), NULL);
    return node != NULL ? (char *)node->value : NULL;
}

int count_kv_hashtable(hashtable_t *hash)
{
    return hash != NULL ? hash->count : -1;
}

int delete_kv_hashtable(hashtable_t *hash, char *key)
{
    int result;

    if (hash == NULL || key == NULL) {
        return -2;
    }
    result = kvs_hash_delete_bytes(hash, key, strlen(key));
    return result == 1 ? 0 : -1;
}

int exist_kv_hashtable(hashtable_t *hash, char *key)
{
    return get_kv_hashtable(hash, key) != NULL;
}

int kvstore_hash_create(hashtable_t *hash)
{
    return init_hashtable(hash);
}

void kvstore_hash_destory(hashtable_t *hash)
{
    dest_hashtable(hash);
}

int kvs_hash_set(hashtable_t *hash, char *key, char *value)
{
    return put_kv_hashtable(hash, key, value);
}

char *kvs_hash_get(hashtable_t *hash, char *key)
{
    return get_kv_hashtable(hash, key);
}

int kvs_hash_delete(hashtable_t *hash, char *key)
{
    return delete_kv_hashtable(hash, key);
}

int kvs_hash_modify(hashtable_t *hash, char *key, char *value)
{
    hashnode_t *node;
    unsigned char *replacement;
    size_t value_length;

    if (hash == NULL || key == NULL || value == NULL) {
        return -1;
    }
    node = find_node(hash, key, strlen(key), NULL);
    if (node == NULL) {
        return -1;
    }
    value_length = strlen(value);
    replacement = copy_bytes(value, value_length);
    if (replacement == NULL) {
        return -1;
    }
    kvstore_free(node->value);
    node->value = replacement;
    node->value_length = value_length;
    return 0;
}

int kvs_hash_count(hashtable_t *hash)
{
    return hash != NULL ? hash->count : -1;
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
    hashnode_t *node;

    if (value_length == NULL) {
        return NULL;
    }
    *value_length = 0;
    node = find_node(hash, key, key_length, NULL);
    if (node == NULL) {
        return NULL;
    }
    *value_length = node->value_length;
    return node->value;
}

int kvs_hash_delete_bytes(hashtable_t *hash,
                          const void *key,
                          size_t key_length)
{
    hashnode_t *node;
    hashnode_t *previous = NULL;
    size_t slot;

    if (hash == NULL || hash->nodes == NULL || hash->max_slots <= 0 ||
        !valid_bytes(key, key_length)) {
        return -1;
    }
    slot = hash_bytes(key, key_length, (size_t)hash->max_slots);
    node = hash->nodes[slot];
    while (node != NULL && !key_equals(node, key, key_length)) {
        previous = node;
        node = node->next;
    }
    if (node == NULL) {
        return 0;
    }
    if (previous == NULL) {
        hash->nodes[slot] = node->next;
    } else {
        previous->next = node->next;
    }
    kvstore_free(node->key);
    kvstore_free(node->value);
    kvstore_free(node);
    hash->count--;
    return 1;
}
