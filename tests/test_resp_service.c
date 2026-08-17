#include "service/kvstore_service.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

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

static void run_backend_case(kvstore_backend_t backend)
{
    static const unsigned char set_command[] = "sEt";
    static const unsigned char get_command[] = "GET";
    static const unsigned char del_command[] = "del";
    static const unsigned char ping_command[] = "PING";
    static const unsigned char unknown_command[] = "HGET";
    static const unsigned char key[] = {'k', 0, 'y'};
    static const unsigned char value[] = {'v', 0, '1'};
    static const unsigned char replacement[] = {'v', 0, '2', 0};
    static const unsigned char prefix_key[] = {'a', 0};
    static const unsigned char longer_key[] = {'a', 0, 0};
    kvstore_argument_t arguments[3];
    kvstore_service_t service;
    kvstore_reply_t reply;

    assert(kvstore_service_init(&service, backend) == 0);

    arguments[0] = argument(set_command, sizeof(set_command) - 1U);
    arguments[1] = argument(key, sizeof(key));
    arguments[2] = argument(value, sizeof(value));
    reply = execute(&service, arguments, 3U);
    assert(reply.type == KVSTORE_REPLY_SIMPLE);
    assert(reply.length == 2U && memcmp(reply.data, "OK", 2U) == 0);

    arguments[2] = argument(replacement, sizeof(replacement));
    reply = execute(&service, arguments, 3U);
    assert(reply.type == KVSTORE_REPLY_SIMPLE);

    arguments[0] = argument(get_command, sizeof(get_command) - 1U);
    reply = execute(&service, arguments, 2U);
    assert(reply.type == KVSTORE_REPLY_BULK);
    assert(reply.length == sizeof(replacement));
    assert(memcmp(reply.data, replacement, sizeof(replacement)) == 0);

    arguments[0] = argument(del_command, sizeof(del_command) - 1U);
    reply = execute(&service, arguments, 2U);
    assert(reply.type == KVSTORE_REPLY_INTEGER && reply.integer == 1);
    reply = execute(&service, arguments, 2U);
    assert(reply.type == KVSTORE_REPLY_INTEGER && reply.integer == 0);

    arguments[0] = argument(get_command, sizeof(get_command) - 1U);
    reply = execute(&service, arguments, 2U);
    assert(reply.type == KVSTORE_REPLY_NULL_BULK);

    arguments[0] = argument(set_command, sizeof(set_command) - 1U);
    arguments[1] = argument(NULL, 0);
    arguments[2] = argument(NULL, 0);
    reply = execute(&service, arguments, 3U);
    assert(reply.type == KVSTORE_REPLY_SIMPLE);
    arguments[0] = argument(get_command, sizeof(get_command) - 1U);
    reply = execute(&service, arguments, 2U);
    assert(reply.type == KVSTORE_REPLY_BULK && reply.length == 0);

    arguments[0] = argument(set_command, sizeof(set_command) - 1U);
    arguments[1] = argument(prefix_key, sizeof(prefix_key));
    arguments[2] = argument(value, sizeof(value));
    assert(execute(&service, arguments, 3U).type == KVSTORE_REPLY_SIMPLE);
    arguments[1] = argument(longer_key, sizeof(longer_key));
    arguments[2] = argument(replacement, sizeof(replacement));
    assert(execute(&service, arguments, 3U).type == KVSTORE_REPLY_SIMPLE);
    arguments[0] = argument(get_command, sizeof(get_command) - 1U);
    arguments[1] = argument(prefix_key, sizeof(prefix_key));
    reply = execute(&service, arguments, 2U);
    assert(reply.length == sizeof(value) && memcmp(reply.data, value, sizeof(value)) == 0);
    arguments[1] = argument(longer_key, sizeof(longer_key));
    reply = execute(&service, arguments, 2U);
    assert(reply.length == sizeof(replacement));
    assert(memcmp(reply.data, replacement, sizeof(replacement)) == 0);

    arguments[0] = argument(ping_command, sizeof(ping_command) - 1U);
    reply = execute(&service, arguments, 1U);
    assert(reply.type == KVSTORE_REPLY_SIMPLE);
    assert(reply.length == 4U && memcmp(reply.data, "PONG", 4U) == 0);
    arguments[1] = argument(value, sizeof(value));
    reply = execute(&service, arguments, 2U);
    assert(reply.type == KVSTORE_REPLY_BULK);
    assert(reply.length == sizeof(value) && memcmp(reply.data, value, sizeof(value)) == 0);

    arguments[0] = argument(unknown_command, sizeof(unknown_command) - 1U);
    reply = execute(&service, arguments, 1U);
    assert(reply.type == KVSTORE_REPLY_ERROR);
    arguments[0] = argument(get_command, sizeof(get_command) - 1U);
    reply = execute(&service, arguments, 1U);
    assert(reply.type == KVSTORE_REPLY_ERROR);

    kvstore_service_destroy(&service);
}

int main(void)
{
    run_backend_case(KVSTORE_BACKEND_HASH);
    run_backend_case(KVSTORE_BACKEND_RBTREE);
    puts("test_resp_service: PASS");
    return 0;
}
