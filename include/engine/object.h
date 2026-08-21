#ifndef STORE_SYSTEM_ENGINE_OBJECT_H
#define STORE_SYSTEM_ENGINE_OBJECT_H

#include <stddef.h>

typedef enum kv_object_type {
    KV_OBJECT_STRING = 0,
    KV_OBJECT_HASH = 1,
    KV_OBJECT_ZSET = 2
} kv_object_type_t;

typedef enum kv_zset_engine {
    KV_ZSET_SKIPLIST = 0,
    KV_ZSET_RBTREE = 1
} kv_zset_engine_t;

typedef struct kv_object kv_object_t;

typedef int (*kv_hash_visit_fn)(const void *field,
                                size_t field_length,
                                const void *value,
                                size_t value_length,
                                void *context);
typedef int (*kv_zset_visit_fn)(const void *member,
                                size_t member_length,
                                double score,
                                void *context);

int kv_object_create_string(kv_object_t **out_object,
                            const void *value,
                            size_t value_length);
int kv_object_create_hash(kv_object_t **out_object);
int kv_object_create_zset(kv_object_t **out_object,
                          kv_zset_engine_t engine);
int kv_object_clone(const kv_object_t *object, kv_object_t **out_object);
void kv_object_destroy(kv_object_t *object);

kv_object_type_t kv_object_type(const kv_object_t *object);
size_t kv_object_memory_usage(const kv_object_t *object);
const void *kv_object_string_value(const kv_object_t *object,
                                   size_t *value_length);
int kv_object_string_update(kv_object_t *object,
                            const void *value,
                            size_t value_length);

int kv_object_hash_set(kv_object_t *object,
                       const void *field,
                       size_t field_length,
                       const void *value,
                       size_t value_length,
                       int *added,
                       int *changed);
const void *kv_object_hash_get(kv_object_t *object,
                               const void *field,
                               size_t field_length,
                               size_t *value_length);
int kv_object_hash_delete(kv_object_t *object,
                          const void *field,
                          size_t field_length);
size_t kv_object_hash_length(const kv_object_t *object);
int kv_object_hash_visit(kv_object_t *object,
                         kv_hash_visit_fn callback,
                         void *context);

int kv_object_zset_add(kv_object_t *object,
                       double score,
                       const void *member,
                       size_t member_length,
                       int *added,
                       int *changed);
int kv_object_zset_score(kv_object_t *object,
                         const void *member,
                         size_t member_length,
                         double *score);
int kv_object_zset_remove(kv_object_t *object,
                          const void *member,
                          size_t member_length);
size_t kv_object_zset_length(const kv_object_t *object);
int kv_object_zset_range(kv_object_t *object,
                         long long start,
                         long long stop,
                         kv_zset_visit_fn callback,
                         void *context,
                         size_t *visited);
kv_zset_engine_t kv_object_zset_engine(const kv_object_t *object);
const char *kv_zset_engine_name(kv_zset_engine_t engine);
int kv_object_zset_validate(const kv_object_t *object);

#endif
