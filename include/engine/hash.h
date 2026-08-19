#ifndef STORE_SYSTEM_ENGINE_HASH_H
#define STORE_SYSTEM_ENGINE_HASH_H

#include <stddef.h>

typedef struct hashtable_s hashtable_t;
typedef struct kv_hash_node kv_hash_node_t;

typedef void (*kv_hash_payload_destroy_fn)(void *payload);

int kv_hash_create(hashtable_t **out_hash);
void kv_hash_release(hashtable_t *hash, kv_hash_payload_destroy_fn destroy_payload);

kv_hash_node_t *kv_hash_find(hashtable_t *hash,
                             const void *key,
                             size_t key_length);
int kv_hash_insert(hashtable_t *hash,
                   const void *key,
                   size_t key_length,
                   void *payload,
                   kv_hash_node_t **out_node);
void *kv_hash_remove(hashtable_t *hash,
                     const void *key,
                     size_t key_length);
void *kv_hash_remove_node(hashtable_t *hash, kv_hash_node_t *target);

void *kv_hash_node_payload(const kv_hash_node_t *node);
void kv_hash_node_set_payload(kv_hash_node_t *node, void *payload);
const void *kv_hash_node_key(const kv_hash_node_t *node);
size_t kv_hash_node_key_length(const kv_hash_node_t *node);

size_t kv_hash_count(const hashtable_t *hash);
size_t kv_hash_slot_count(const hashtable_t *hash);
size_t kv_hash_index_memory(const hashtable_t *hash);
size_t kv_hash_node_memory_for_key(size_t key_length);
int kv_hash_is_rehashing(const hashtable_t *hash);
size_t kv_hash_rehash_step(hashtable_t *hash, size_t bucket_budget);

#endif
