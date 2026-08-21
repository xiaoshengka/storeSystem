#include "service/kvstore_service.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

typedef struct fake_clock {
    uint64_t now_ms;
} fake_clock_t;

typedef struct fake_persistence {
    int foreground_saves;
    int background_saves;
    uint64_t lastsave;
} fake_persistence_t;

static int fake_save(void *context, int background)
{
    fake_persistence_t *persistence = context;

    if (background) persistence->background_saves++;
    else persistence->foreground_saves++;
    return 0;
}

static uint64_t fake_lastsave(void *context)
{
    return ((fake_persistence_t *)context)->lastsave;
}

static void fake_persistence_info(void *context,
                                  kvstore_persistence_info_t *info)
{
    fake_persistence_t *persistence = context;

    info->rdb_enabled = 1;
    info->bgsave_in_progress = persistence->background_saves != 0;
    info->last_save_time = persistence->lastsave;
    info->checkpoint_offset = 321U;
}

static uint64_t fake_now(void *context)
{
    return ((fake_clock_t *)context)->now_ms;
}

static kvstore_argument_t argument(const void *data, size_t length)
{
    kvstore_argument_t result;

    result.data = data;
    result.length = length;
    return result;
}

static kvstore_reply_t execute(kvstore_service_t *service,
                               kvstore_argument_t *arguments,
                               size_t count)
{
    kvstore_reply_t reply;

    assert(kvstore_service_execute(service, arguments, count, &reply) == 0);
    return reply;
}

int main(void)
{
    static const unsigned char key[] = {'k', 0, 'y'};
    static const unsigned char value[] = {'v', 0, '1'};
    static const unsigned char replacement[] = {'v', 0, '2', 0};
    fake_clock_t clock = {10000U};
    cache_config_t config = {0};
    kvstore_argument_t arguments[8];
    kvstore_service_t service;
    kvstore_reply_t reply;
    fake_persistence_t persistence = {0, 0, 123U};
    kvstore_persistence_admin_t admin = {
        fake_save, fake_lastsave, fake_persistence_info
    };

    config.max_keys = 32U;
    config.now_ms = fake_now;
    config.clock_context = &clock;
    assert(kvstore_service_init(&service, &config) == 0);

    arguments[0] = argument("sEt", 3);
    arguments[1] = argument(key, sizeof(key));
    arguments[2] = argument(value, sizeof(value));
    reply = execute(&service, arguments, 3U);
    assert(reply.type == KVSTORE_REPLY_SIMPLE);

    arguments[0] = argument("GET", 3);
    reply = execute(&service, arguments, 2U);
    assert(reply.type == KVSTORE_REPLY_BULK);
    assert(reply.length == sizeof(value));
    assert(memcmp(reply.data, value, sizeof(value)) == 0);

    arguments[0] = argument("SET", 3);
    arguments[2] = argument(replacement, sizeof(replacement));
    arguments[3] = argument("pX", 2);
    arguments[4] = argument("100", 3);
    reply = execute(&service, arguments, 5U);
    assert(reply.type == KVSTORE_REPLY_SIMPLE);
    arguments[0] = argument("PTTL", 4);
    reply = execute(&service, arguments, 2U);
    assert(reply.type == KVSTORE_REPLY_INTEGER && reply.integer == 100);
    arguments[0] = argument("TTL", 3);
    reply = execute(&service, arguments, 2U);
    assert(reply.integer == 0);
    clock.now_ms += 100U;
    arguments[0] = argument("GET", 3);
    reply = execute(&service, arguments, 2U);
    assert(reply.type == KVSTORE_REPLY_NULL_BULK);

    arguments[0] = argument("SET", 3);
    arguments[1] = argument("persist", 7);
    arguments[2] = argument("value", 5);
    assert(execute(&service, arguments, 3U).type == KVSTORE_REPLY_SIMPLE);
    arguments[0] = argument("EXPIRE", 6);
    arguments[2] = argument("10", 2);
    reply = execute(&service, arguments, 3U);
    assert(reply.type == KVSTORE_REPLY_INTEGER && reply.integer == 1);
    arguments[0] = argument("PERSIST", 7);
    reply = execute(&service, arguments, 2U);
    assert(reply.integer == 1);
    reply = execute(&service, arguments, 2U);
    assert(reply.integer == 0);

    arguments[0] = argument("PEXPIRE", 7);
    arguments[2] = argument("0", 1);
    reply = execute(&service, arguments, 3U);
    assert(reply.integer == 1);
    arguments[0] = argument("PTTL", 4);
    reply = execute(&service, arguments, 2U);
    assert(reply.integer == -2);

    arguments[0] = argument("SET", 3);
    arguments[1] = argument("bad", 3);
    arguments[2] = argument("value", 5);
    arguments[3] = argument("EX", 2);
    arguments[4] = argument("-1", 2);
    reply = execute(&service, arguments, 5U);
    assert(reply.type == KVSTORE_REPLY_ERROR);
    arguments[4] = argument("not-a-number", 12);
    reply = execute(&service, arguments, 5U);
    assert(reply.type == KVSTORE_REPLY_ERROR);

    arguments[0] = argument("INFO", 4);
    arguments[1] = argument("CACHE", 5);
    reply = execute(&service, arguments, 2U);
    assert(reply.type == KVSTORE_REPLY_BULK);
    assert(strstr((const char *)reply.data, "keys:") != NULL);
    assert(strstr((const char *)reply.data, "hits:1\r\n") != NULL);
    assert(strstr((const char *)reply.data, "misses:1\r\n") != NULL);

    kvstore_service_set_persistence_admin(&service, &admin, &persistence);
    arguments[0] = argument("SAVE", 4);
    reply = execute(&service, arguments, 1U);
    assert(reply.type == KVSTORE_REPLY_SIMPLE &&
           persistence.foreground_saves == 1);
    arguments[0] = argument("BGSAVE", 6);
    reply = execute(&service, arguments, 1U);
    assert(reply.type == KVSTORE_REPLY_SIMPLE &&
           persistence.background_saves == 1);
    arguments[0] = argument("LASTSAVE", 8);
    reply = execute(&service, arguments, 1U);
    assert(reply.type == KVSTORE_REPLY_INTEGER && reply.integer == 123);
    arguments[0] = argument("INFO", 4);
    arguments[1] = argument("PERSISTENCE", 11);
    reply = execute(&service, arguments, 2U);
    assert(reply.type == KVSTORE_REPLY_BULK);
    assert(strstr((const char *)reply.data, "rdb_enabled:1\r\n") != NULL);
    assert(strstr((const char *)reply.data,
                  "rdb_checkpoint_offset:321\r\n") != NULL);

    arguments[0] = argument("PING", 4);
    reply = execute(&service, arguments, 1U);
    assert(reply.type == KVSTORE_REPLY_SIMPLE);
    arguments[1] = argument(value, sizeof(value));
    reply = execute(&service, arguments, 2U);
    assert(reply.type == KVSTORE_REPLY_BULK && reply.length == sizeof(value));

    arguments[0] = argument("HSET", 4);
    arguments[1] = argument("hash", 4);
    arguments[2] = argument("field-1", 7);
    arguments[3] = argument("one", 3);
    arguments[4] = argument("field-2", 7);
    arguments[5] = argument("two", 3);
    reply = execute(&service, arguments, 6U);
    assert(reply.type == KVSTORE_REPLY_INTEGER && reply.integer == 2);
    arguments[0] = argument("HGET", 4);
    reply = execute(&service, arguments, 3U);
    assert(reply.type == KVSTORE_REPLY_BULK && reply.length == 3U);
    arguments[0] = argument("HLEN", 4);
    reply = execute(&service, arguments, 2U);
    assert(reply.integer == 2);
    arguments[0] = argument("HGETALL", 7);
    reply = execute(&service, arguments, 2U);
    assert(reply.type == KVSTORE_REPLY_ARRAY && reply.element_count == 4U);
    arguments[0] = argument("GET", 3);
    reply = execute(&service, arguments, 2U);
    assert(reply.type == KVSTORE_REPLY_ERROR);
    arguments[0] = argument("EXPIRE", 6);
    arguments[2] = argument("10", 2);
    assert(execute(&service, arguments, 3U).integer == 1);
    arguments[0] = argument("HSET", 4);
    arguments[2] = argument("field-1", 7);
    arguments[3] = argument("updated", 7);
    assert(execute(&service, arguments, 4U).integer == 0);
    arguments[0] = argument("PTTL", 4);
    assert(execute(&service, arguments, 2U).integer == 10000);
    arguments[0] = argument("HDEL", 4);
    arguments[2] = argument("field-1", 7);
    arguments[3] = argument("field-2", 7);
    assert(execute(&service, arguments, 4U).integer == 2);
    arguments[0] = argument("HLEN", 4);
    assert(execute(&service, arguments, 2U).integer == 0);

    arguments[0] = argument("ZADD", 4);
    arguments[1] = argument("zset", 4);
    arguments[2] = argument("2", 1);
    arguments[3] = argument("two", 3);
    arguments[4] = argument("1", 1);
    arguments[5] = argument("one", 3);
    assert(execute(&service, arguments, 6U).integer == 2);
    arguments[0] = argument("ZSCORE", 6);
    arguments[2] = argument("two", 3);
    reply = execute(&service, arguments, 3U);
    assert(reply.type == KVSTORE_REPLY_BULK &&
           reply.length == 1U && reply.data[0] == '2');
    arguments[0] = argument("ZRANGE", 6);
    arguments[2] = argument("0", 1);
    arguments[3] = argument("-1", 2);
    reply = execute(&service, arguments, 4U);
    assert(reply.type == KVSTORE_REPLY_ARRAY && reply.element_count == 2U);
    assert(reply.elements[0].length == 3U &&
           memcmp(reply.elements[0].data, "one", 3) == 0);
    arguments[4] = argument("WITHSCORES", 10);
    reply = execute(&service, arguments, 5U);
    assert(reply.type == KVSTORE_REPLY_ARRAY && reply.element_count == 4U);
    arguments[0] = argument("ZREM", 4);
    arguments[2] = argument("one", 3);
    arguments[3] = argument("two", 3);
    assert(execute(&service, arguments, 4U).integer == 2);
    arguments[0] = argument("ZCARD", 5);
    assert(execute(&service, arguments, 2U).integer == 0);

    arguments[0] = argument("UNKNOWN", 7);
    reply = execute(&service, arguments, 1U);
    assert(reply.type == KVSTORE_REPLY_ERROR);
    arguments[0] = argument("GET", 3);
    reply = execute(&service, arguments, 1U);
    assert(reply.type == KVSTORE_REPLY_ERROR);

    kvstore_service_destroy(&service);

    {
        kvstore_service_config_t service_config = {0};

        service_config.cache = config;
        service_config.zset_engine = KV_ZSET_RBTREE;
        assert(kvstore_service_init_with_config(&service, &service_config) == 0);
        arguments[0] = argument("ZADD", 4);
        arguments[1] = argument("tree", 4);
        arguments[2] = argument("3", 1);
        arguments[3] = argument("three", 5);
        arguments[4] = argument("1", 1);
        arguments[5] = argument("one", 3);
        assert(execute(&service, arguments, 6U).integer == 2);
        arguments[0] = argument("ZRANGE", 6);
        arguments[2] = argument("0", 1);
        arguments[3] = argument("-1", 2);
        reply = execute(&service, arguments, 4U);
        assert(reply.type == KVSTORE_REPLY_ARRAY && reply.element_count == 2U);
        assert(memcmp(reply.elements[0].data, "one", 3) == 0);
        kvstore_service_destroy(&service);
    }
    puts("test_resp_service: PASS");
    return 0;
}
