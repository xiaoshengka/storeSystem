#include "engine/object.h"

#include <assert.h>
#include <math.h>
#include <stdio.h>
#include <string.h>

typedef struct range_result {
    char members[16][32];
    double scores[16];
    size_t count;
} range_result_t;

static void test_string_object(void)
{
    static const unsigned char binary[] = {'a', 0, 'b', 'c'};
    kv_object_t *object;
    const void *value;
    size_t length;
    size_t memory;

    assert(kv_object_create_string(&object, "initial", 7U) == 0);
    memory = kv_object_memory_usage(object);
    assert(kv_object_string_update(object, binary, sizeof(binary)) == 1);
    value = kv_object_string_value(object, &length);
    assert(length == sizeof(binary));
    assert(memcmp(value, binary, sizeof(binary)) == 0);
    assert(kv_object_memory_usage(object) == memory);
    assert(kv_object_string_update(object, "too-large", 9U) == 0);
    kv_object_destroy(object);
}

static int collect_member(const void *member,
                          size_t member_length,
                          double score,
                          void *context)
{
    range_result_t *result = context;

    assert(result->count < 16U);
    assert(member_length < sizeof(result->members[0]));
    memcpy(result->members[result->count], member, member_length);
    result->members[result->count][member_length] = '\0';
    result->scores[result->count] = score;
    result->count++;
    return 0;
}

static void test_hash_object(void)
{
    static const unsigned char binary_field[] = {'f', 0, 'x'};
    static const unsigned char binary_value[] = {'v', 0, 'x'};
    kv_object_t *object;
    kv_object_t *copy;
    const void *value;
    size_t length;
    int added;
    int changed;

    assert(kv_object_create_hash(&object) == 0);
    assert(kv_object_hash_set(object,
                              binary_field,
                              sizeof(binary_field),
                              binary_value,
                              sizeof(binary_value),
                              &added,
                              &changed) == 0);
    assert(added == 1 && changed == 1);
    assert(kv_object_hash_set(object, "name", 4, "one", 3, &added, &changed) == 0);
    assert(added == 1);
    assert(kv_object_hash_set(object, "name", 4, "two", 3, &added, &changed) == 0);
    assert(added == 0 && changed == 1);
    value = kv_object_hash_get(object, "name", 4, &length);
    assert(length == 3 && memcmp(value, "two", 3) == 0);
    assert(kv_object_clone(object, &copy) == 0);
    assert(kv_object_hash_length(copy) == 2U);
    assert(kv_object_hash_delete(copy, binary_field, sizeof(binary_field)) == 1);
    assert(kv_object_hash_length(copy) == 1U);
    kv_object_destroy(copy);
    kv_object_destroy(object);
}

static void test_zset_engine(kv_zset_engine_t engine)
{
    kv_object_t *object;
    kv_object_t *copy;
    range_result_t result = {0};
    size_t visited;
    double score;
    int added;
    int changed;

    assert(kv_object_create_zset(&object, engine) == 0);
    assert(kv_object_zset_add(object, 2.0, "two", 3, &added, &changed) == 0);
    assert(added == 1 && changed == 1);
    assert(kv_object_zset_add(object, 1.0, "beta", 4, &added, &changed) == 0);
    assert(kv_object_zset_add(object, 1.0, "alpha", 5, &added, &changed) == 0);
    assert(kv_object_zset_add(object, 3.0, "three", 5, &added, &changed) == 0);
    assert(kv_object_zset_length(object) == 4U);
    assert(kv_object_zset_validate(object) == 1);
    assert(kv_object_zset_range(object,
                                0,
                                -1,
                                collect_member,
                                &result,
                                &visited) == 0);
    assert(visited == 4U && result.count == 4U);
    assert(strcmp(result.members[0], "alpha") == 0);
    assert(strcmp(result.members[1], "beta") == 0);
    assert(strcmp(result.members[2], "two") == 0);
    assert(strcmp(result.members[3], "three") == 0);

    assert(kv_object_zset_add(object, 0.5, "three", 5, &added, &changed) == 0);
    assert(added == 0 && changed == 1);
    assert(kv_object_zset_score(object, "three", 5, &score) == 1 && score == 0.5);
    memset(&result, 0, sizeof(result));
    assert(kv_object_zset_range(object,
                                -2,
                                -1,
                                collect_member,
                                &result,
                                &visited) == 0);
    assert(visited == 2U);
    assert(strcmp(result.members[0], "beta") == 0);
    assert(strcmp(result.members[1], "two") == 0);
    assert(kv_object_zset_add(object, NAN, "bad", 3, NULL, NULL) != 0);
    assert(kv_object_clone(object, &copy) == 0);
    assert(kv_object_zset_remove(copy, "alpha", 5) == 1);
    assert(kv_object_zset_length(copy) == 3U);
    assert(kv_object_zset_validate(copy) == 1);
    kv_object_destroy(copy);
    kv_object_destroy(object);
}

static void test_zset_update_stress(kv_zset_engine_t engine)
{
    kv_object_t *object;
    char member[32];
    int index;

    assert(kv_object_create_zset(&object, engine) == 0);
    for (index = 0; index < 100; ++index) {
        int length = snprintf(member, sizeof(member), "item-%d", index);
        assert(length > 0);
        assert(kv_object_zset_add(object,
                                  (double)index,
                                  member,
                                  (size_t)length,
                                  NULL,
                                  NULL) == 0);
    }
    assert(kv_object_zset_validate(object) == 1);
    for (index = 0; index < 1000; ++index) {
        int member_index = (index * 37) % 100;
        int length = snprintf(member, sizeof(member), "item-%d", member_index);
        assert(length > 0);
        assert(kv_object_zset_add(object,
                                  (double)((index * 53) % 211) / 10.0,
                                  member,
                                  (size_t)length,
                                  NULL,
                                  NULL) == 0);
        if (index % 10 == 0) assert(kv_object_zset_validate(object) == 1);
    }
    assert(kv_object_zset_length(object) == 100U);
    assert(kv_object_zset_validate(object) == 1);
    kv_object_destroy(object);
}

int main(void)
{
    test_string_object();
    test_hash_object();
    test_zset_engine(KV_ZSET_SKIPLIST);
    test_zset_engine(KV_ZSET_RBTREE);
    test_zset_update_stress(KV_ZSET_SKIPLIST);
    test_zset_update_stress(KV_ZSET_RBTREE);
    puts("test_object: PASS");
    return 0;
}
