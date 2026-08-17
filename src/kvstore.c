#define _POSIX_C_SOURCE 200809L

#include "kvstore.h"
#include "service/kvstore_service.h"

#include <stdarg.h>

#define KVSTORE_MAX_TOKENS 128U

static const char *const commands[] = {
    "SET", "GET", "DEL", "MOD", "COUNT",
    "RSET", "RGET", "RDEL", "RMOD", "RCOUNT",
    "HSET", "HGET", "HDEL", "HMOD", "HCOUNT",
};

enum kvstore_command {
    KVS_CMD_SET = 0,
    KVS_CMD_GET,
    KVS_CMD_DEL,
    KVS_CMD_MOD,
    KVS_CMD_COUNT,
    KVS_CMD_RSET,
    KVS_CMD_RGET,
    KVS_CMD_RDEL,
    KVS_CMD_RMOD,
    KVS_CMD_RCOUNT,
    KVS_CMD_HSET,
    KVS_CMD_HGET,
    KVS_CMD_HDEL,
    KVS_CMD_HMOD,
    KVS_CMD_HCOUNT,
    KVS_CMD_SIZE
};

static int array_initialized;
static int rbtree_initialized;
static int hash_initialized;

void *kvstore_malloc(size_t size)
{
#if ENABLE_MEM_POOL
    (void)size;
    return mp_alloc(&m);
#else
    return malloc(size);
#endif
}

void kvstore_free(void *ptr)
{
    if (ptr == NULL) {
        return;
    }
#if ENABLE_MEM_POOL
    mp_free(&m, ptr);
#else
    free(ptr);
#endif
}

static int write_response(char *response, size_t capacity, const char *format, ...)
{
    va_list arguments;
    int written;

    if (response == NULL || capacity == 0 || format == NULL) {
        return -1;
    }
    va_start(arguments, format);
    written = vsnprintf(response, capacity, format, arguments);
    va_end(arguments);
    if (written < 0 || (size_t)written >= capacity) {
        response[capacity - 1U] = '\0';
        return -1;
    }
    return written;
}

static size_t split_tokens(char *message, char **tokens, size_t capacity)
{
    char *save_pointer = NULL;
    char *token;
    size_t count = 0;

    token = strtok_r(message, " \t\r\n", &save_pointer);
    while (token != NULL && count < capacity) {
        tokens[count++] = token;
        token = strtok_r(NULL, " \t\r\n", &save_pointer);
    }
    return count;
}

static enum kvstore_command find_command(const char *name)
{
    int command;

    for (command = KVS_CMD_SET; command < KVS_CMD_SIZE; ++command) {
        if (strcmp(commands[command], name) == 0) {
            return (enum kvstore_command)command;
        }
    }
    return KVS_CMD_SIZE;
}

static size_t command_arity(enum kvstore_command command)
{
    switch (command) {
    case KVS_CMD_COUNT:
    case KVS_CMD_RCOUNT:
    case KVS_CMD_HCOUNT:
        return 1U;
    case KVS_CMD_GET:
    case KVS_CMD_DEL:
    case KVS_CMD_RGET:
    case KVS_CMD_RDEL:
    case KVS_CMD_HGET:
    case KVS_CMD_HDEL:
        return 2U;
    default:
        return 3U;
    }
}

static int format_mutation_result(char *response, size_t capacity, int result)
{
    if (result < 0) {
        return write_response(response, capacity, "ERROR");
    }
    if (result == 0) {
        return write_response(response, capacity, "SUCCESS");
    }
    return write_response(response, capacity, "NO EXIST");
}

static int execute_command(enum kvstore_command command,
                           char **tokens,
                           char *response,
                           size_t response_capacity)
{
    char *value;
    int result;

    switch (command) {
#if ENABLE_ARRAY_KVENGINE
    case KVS_CMD_SET:
        result = kvs_array_set(&Array, tokens[1], tokens[2]);
        return write_response(response, response_capacity, result == 0 ? "SUCCESS" : "FAILED");
    case KVS_CMD_GET:
        value = kvs_array_get(&Array, tokens[1]);
        return value != NULL ? write_response(response, response_capacity, "%s", value)
                             : write_response(response, response_capacity, "NO EXIST");
    case KVS_CMD_DEL:
        return format_mutation_result(response, response_capacity,
                                      kvs_array_del(&Array, tokens[1]));
    case KVS_CMD_MOD:
        return format_mutation_result(response, response_capacity,
                                      kvs_array_mod(&Array, tokens[1], tokens[2]));
    case KVS_CMD_COUNT:
        result = kvs_array_count(&Array);
        return result < 0 ? write_response(response, response_capacity, "ERROR")
                          : write_response(response, response_capacity, "%d", result);
#endif

#if ENABLE_RBTREE_KVENGINE
    case KVS_CMD_RSET:
        result = kvs_rbtree_set(&Tree, tokens[1], tokens[2]);
        return write_response(response, response_capacity, result == 0 ? "SUCCESS" : "FAILED");
    case KVS_CMD_RGET:
        value = kvs_rbtree_get(&Tree, tokens[1]);
        return value != NULL ? write_response(response, response_capacity, "%s", value)
                             : write_response(response, response_capacity, "NO EXIST");
    case KVS_CMD_RDEL:
        return format_mutation_result(response, response_capacity,
                                      kvs_rbtree_delete(&Tree, tokens[1]));
    case KVS_CMD_RMOD:
        return format_mutation_result(response, response_capacity,
                                      kvs_rbtree_modify(&Tree, tokens[1], tokens[2]));
    case KVS_CMD_RCOUNT:
        result = kvs_rbtree_count(&Tree);
        return result < 0 ? write_response(response, response_capacity, "ERROR")
                          : write_response(response, response_capacity, "%d", result);
#endif

#if ENABLE_HASH_KVENGINE
    case KVS_CMD_HSET:
        result = kvs_hash_set(&Hash, tokens[1], tokens[2]);
        return write_response(response, response_capacity, result == 0 ? "SUCCESS" : "FAILED");
    case KVS_CMD_HGET:
        value = kvs_hash_get(&Hash, tokens[1]);
        return value != NULL ? write_response(response, response_capacity, "%s", value)
                             : write_response(response, response_capacity, "NO EXIST");
    case KVS_CMD_HDEL:
        return format_mutation_result(response, response_capacity,
                                      kvs_hash_delete(&Hash, tokens[1]));
    case KVS_CMD_HMOD:
        return format_mutation_result(response, response_capacity,
                                      kvs_hash_modify(&Hash, tokens[1], tokens[2]));
    case KVS_CMD_HCOUNT:
        result = kvs_hash_count(&Hash);
        return result < 0 ? write_response(response, response_capacity, "ERROR")
                          : write_response(response, response_capacity, "%d", result);
#endif
    default:
        return write_response(response, response_capacity, "ERROR unknown command");
    }
}

int kvstore_execute_request(const char *request,
                            size_t request_length,
                            char *response,
                            size_t response_capacity,
                            size_t *response_length)
{
    char request_copy[BUFFER_LENGTH];
    char *tokens[KVSTORE_MAX_TOKENS] = {0};
    enum kvstore_command command;
    size_t token_count;
    int written;

    if (response_length != NULL) {
        *response_length = 0;
    }
    if (request == NULL || response == NULL || response_capacity == 0 ||
        response_length == NULL) {
        return -1;
    }
    if (request_length == 0 || request_length >= sizeof(request_copy)) {
        written = write_response(response, response_capacity,
                                 request_length == 0 ? "ERROR empty request" : "ERROR request too large");
        if (written >= 0) {
            *response_length = (size_t)written;
        }
        return -1;
    }

    memcpy(request_copy, request, request_length);
    request_copy[request_length] = '\0';
    token_count = split_tokens(request_copy, tokens, KVSTORE_MAX_TOKENS);
    if (token_count == 0) {
        written = write_response(response, response_capacity, "ERROR empty request");
        if (written >= 0) {
            *response_length = (size_t)written;
        }
        return -1;
    }

    command = find_command(tokens[0]);
    if (command == KVS_CMD_SIZE) {
        written = write_response(response, response_capacity, "ERROR unknown command");
        if (written >= 0) {
            *response_length = (size_t)written;
        }
        return -1;
    }
    if (token_count != command_arity(command)) {
        written = write_response(response, response_capacity, "ERROR wrong number of arguments");
        if (written >= 0) {
            *response_length = (size_t)written;
        }
        return -1;
    }

    written = execute_command(command, tokens, response, response_capacity);
    if (written < 0) {
        return -1;
    }
    *response_length = (size_t)written;
    return 0;
}

int kvstore_request(struct conn_item *item)
{
    size_t request_length;
    size_t response_length = 0;
    int result;

    if (item == NULL) {
        return -1;
    }
    request_length = item->rlen > 0 ? (size_t)item->rlen
                                    : strnlen(item->rbuffer, BUFFER_LENGTH);
    result = kvstore_execute_request(item->rbuffer,
                                     request_length,
                                     item->wbuffer,
                                     BUFFER_LENGTH,
                                     &response_length);
    item->wlen = (int)response_length;
    return result;
}

int kvstore_engine_init(void)
{
#if ENABLE_ARRAY_KVENGINE
    if (kvstore_array_create(&Array) != 0) {
        goto fail;
    }
    array_initialized = 1;
#endif
#if ENABLE_RBTREE_KVENGINE
    if (kvstore_rbtree_create(&Tree) != 0) {
        goto fail;
    }
    rbtree_initialized = 1;
#endif
#if ENABLE_HASH_KVENGINE
    if (kvstore_hash_create(&Hash) != 0) {
        goto fail;
    }
    hash_initialized = 1;
#endif
    return 0;

fail:
    kvstore_engine_destroy();
    return -1;
}

void kvstore_engine_destroy(void)
{
#if ENABLE_HASH_KVENGINE
    if (hash_initialized) {
        kvstore_hash_destory(&Hash);
        hash_initialized = 0;
    }
#endif
#if ENABLE_RBTREE_KVENGINE
    if (rbtree_initialized) {
        kvstore_rbtree_destory(&Tree);
        rbtree_initialized = 0;
    }
#endif
#if ENABLE_ARRAY_KVENGINE
    if (array_initialized) {
        kvstore_array_destory(&Array);
        array_initialized = 0;
    }
#endif
#if ENABLE_MEM_POOL
    mp_dest(&m);
#endif
}
