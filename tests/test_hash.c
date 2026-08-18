#include "engine/hash.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

int main(void)
{
    hashtable_t *hash;
    int values[256];
    unsigned char binary_key[] = {'k', 0, 'y'};
    char key[32];
    size_t index;

    assert(kv_hash_create(&hash) == 0);
    assert(kv_hash_count(hash) == 0);
    assert(kv_hash_slot_count(hash) == 16U);

    values[0] = 10;
    assert(kv_hash_insert(hash, NULL, 0, &values[0], NULL) == 0);
    assert(kv_hash_node_payload(kv_hash_find(hash, NULL, 0)) == &values[0]);
    assert(kv_hash_insert(hash, NULL, 0, &values[0], NULL) != 0);

    values[1] = 11;
    assert(kv_hash_insert(hash,
                          binary_key,
                          sizeof(binary_key),
                          &values[1],
                          NULL) == 0);
    assert(kv_hash_node_payload(kv_hash_find(hash,
                                             binary_key,
                                             sizeof(binary_key))) == &values[1]);

    for (index = 2; index < 256U; ++index) {
        int length = snprintf(key, sizeof(key), "key-%zu", index);

        assert(length > 0 && (size_t)length < sizeof(key));
        values[index] = (int)index;
        assert(kv_hash_insert(hash,
                              key,
                              (size_t)length,
                              &values[index],
                              NULL) == 0);
        (void)kv_hash_rehash_step(hash, 1U);
    }
    assert(kv_hash_count(hash) == 256U);
    assert(kv_hash_slot_count(hash) > 16U);
    assert(kv_hash_index_memory(hash) > 0U);

    for (index = 2; index < 256U; ++index) {
        int length = snprintf(key, sizeof(key), "key-%zu", index);
        kv_hash_node_t *node = kv_hash_find(hash, key, (size_t)length);

        assert(node != NULL);
        assert(kv_hash_node_payload(node) == &values[index]);
    }
    assert(kv_hash_remove(hash, binary_key, sizeof(binary_key)) == &values[1]);
    assert(kv_hash_find(hash, binary_key, sizeof(binary_key)) == NULL);
    while (kv_hash_is_rehashing(hash)) {
        (void)kv_hash_rehash_step(hash, 3U);
    }
    assert(kv_hash_remove(hash, NULL, 0) == &values[0]);
    assert(kv_hash_count(hash) == 254U);

    kv_hash_release(hash, NULL);
    puts("test_hash: PASS");
    return 0;
}
