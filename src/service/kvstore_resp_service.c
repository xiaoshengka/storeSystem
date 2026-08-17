#include "service/kvstore_service.h"

#include "kvstore.h"

#include <string.h>

static const unsigned char reply_ok[] = "OK";
static const unsigned char reply_pong[] = "PONG";
static const unsigned char error_unknown[] = "ERR unknown command";
static const unsigned char error_arity[] = "ERR wrong number of arguments";
static const unsigned char error_internal[] = "ERR internal error";

enum service_command {
    SERVICE_COMMAND_SET,
    SERVICE_COMMAND_GET,
    SERVICE_COMMAND_DEL,
    SERVICE_COMMAND_PING,
    SERVICE_COMMAND_UNKNOWN
};

static unsigned char ascii_upper(unsigned char value)
{
    if (value >= 'a' && value <= 'z') {
        return (unsigned char)(value - ('a' - 'A'));
    }
    return value;
}

static int command_equals(const kvstore_argument_t *argument, const char *name)
{
    size_t length = strlen(name);
    size_t index;

    if (argument->length != length) {
        return 0;
    }
    for (index = 0; index < length; ++index) {
        if (ascii_upper(argument->data[index]) != (unsigned char)name[index]) {
            return 0;
        }
    }
    return 1;
}

static enum service_command find_command(const kvstore_argument_t *argument)
{
    if (command_equals(argument, "SET")) return SERVICE_COMMAND_SET;
    if (command_equals(argument, "GET")) return SERVICE_COMMAND_GET;
    if (command_equals(argument, "DEL")) return SERVICE_COMMAND_DEL;
    if (command_equals(argument, "PING")) return SERVICE_COMMAND_PING;
    return SERVICE_COMMAND_UNKNOWN;
}

static void set_data_reply(kvstore_reply_t *reply,
                           kvstore_reply_type_t type,
                           const unsigned char *data,
                           size_t length)
{
    reply->type = type;
    reply->data = data;
    reply->length = length;
    reply->integer = 0;
}

static void set_error(kvstore_reply_t *reply,
                      const unsigned char *message,
                      size_t length)
{
    set_data_reply(reply, KVSTORE_REPLY_ERROR, message, length);
}

const char *kvstore_backend_name(kvstore_backend_t backend)
{
    switch (backend) {
    case KVSTORE_BACKEND_HASH:
        return "hash";
    case KVSTORE_BACKEND_RBTREE:
        return "rbtree";
    default:
        return "unknown";
    }
}

int kvstore_backend_parse(const char *name, kvstore_backend_t *backend)
{
    if (name == NULL || backend == NULL) {
        return -1;
    }
    if (strcmp(name, "hash") == 0) {
        *backend = KVSTORE_BACKEND_HASH;
        return 0;
    }
    if (strcmp(name, "rbtree") == 0) {
        *backend = KVSTORE_BACKEND_RBTREE;
        return 0;
    }
    return -1;
}

int kvstore_service_init(kvstore_service_t *service, kvstore_backend_t backend)
{
    int result;

    if (service == NULL) {
        return -1;
    }
    memset(service, 0, sizeof(*service));
    switch (backend) {
    case KVSTORE_BACKEND_HASH:
        result = kvstore_hash_create(&Hash);
        break;
    case KVSTORE_BACKEND_RBTREE:
        result = kvstore_rbtree_create(&Tree);
        break;
    default:
        return -1;
    }
    if (result != 0) {
        return -1;
    }
    service->backend = backend;
    service->initialized = 1;
    return 0;
}

void kvstore_service_destroy(kvstore_service_t *service)
{
    if (service == NULL || !service->initialized) {
        return;
    }
    if (service->backend == KVSTORE_BACKEND_HASH) {
        kvstore_hash_destory(&Hash);
    } else if (service->backend == KVSTORE_BACKEND_RBTREE) {
        kvstore_rbtree_destory(&Tree);
    }
    memset(service, 0, sizeof(*service));
}

static int backend_set(kvstore_service_t *service,
                       const kvstore_argument_t *key,
                       const kvstore_argument_t *value)
{
    if (service->backend == KVSTORE_BACKEND_HASH) {
        return kvs_hash_upsert_bytes(&Hash,
                                     key->data,
                                     key->length,
                                     value->data,
                                     value->length);
    }
    return kvs_rbtree_upsert_bytes(&Tree,
                                   key->data,
                                   key->length,
                                   value->data,
                                   value->length);
}

static const void *backend_get(kvstore_service_t *service,
                               const kvstore_argument_t *key,
                               size_t *value_length)
{
    if (service->backend == KVSTORE_BACKEND_HASH) {
        return kvs_hash_get_bytes(&Hash, key->data, key->length, value_length);
    }
    return kvs_rbtree_get_bytes(&Tree, key->data, key->length, value_length);
}

static int backend_delete(kvstore_service_t *service,
                          const kvstore_argument_t *key)
{
    if (service->backend == KVSTORE_BACKEND_HASH) {
        return kvs_hash_delete_bytes(&Hash, key->data, key->length);
    }
    return kvs_rbtree_delete_bytes(&Tree, key->data, key->length);
}

int kvstore_service_execute(kvstore_service_t *service,
                            const kvstore_argument_t *arguments,
                            size_t argument_count,
                            kvstore_reply_t *reply)
{
    enum service_command command;

    if (service == NULL || !service->initialized || arguments == NULL ||
        argument_count == 0 || reply == NULL) {
        return -1;
    }
    memset(reply, 0, sizeof(*reply));
    command = find_command(&arguments[0]);
    if (command == SERVICE_COMMAND_UNKNOWN) {
        set_error(reply, error_unknown, sizeof(error_unknown) - 1U);
        return 0;
    }

    if ((command == SERVICE_COMMAND_SET && argument_count != 3U) ||
        ((command == SERVICE_COMMAND_GET || command == SERVICE_COMMAND_DEL) &&
         argument_count != 2U) ||
        (command == SERVICE_COMMAND_PING &&
         argument_count != 1U && argument_count != 2U)) {
        set_error(reply, error_arity, sizeof(error_arity) - 1U);
        return 0;
    }

    switch (command) {
    case SERVICE_COMMAND_SET:
        if (backend_set(service, &arguments[1], &arguments[2]) != 0) {
            set_error(reply, error_internal, sizeof(error_internal) - 1U);
        } else {
            set_data_reply(reply,
                           KVSTORE_REPLY_SIMPLE,
                           reply_ok,
                           sizeof(reply_ok) - 1U);
        }
        return 0;
    case SERVICE_COMMAND_GET:
        reply->data = backend_get(service, &arguments[1], &reply->length);
        reply->type = reply->data != NULL ? KVSTORE_REPLY_BULK
                                          : KVSTORE_REPLY_NULL_BULK;
        return 0;
    case SERVICE_COMMAND_DEL:
        {
            int deleted = backend_delete(service, &arguments[1]);

            if (deleted < 0) {
                set_error(reply, error_internal, sizeof(error_internal) - 1U);
            } else {
                reply->type = KVSTORE_REPLY_INTEGER;
                reply->integer = deleted;
            }
        }
        return 0;
    case SERVICE_COMMAND_PING:
        if (argument_count == 1U) {
            set_data_reply(reply,
                           KVSTORE_REPLY_SIMPLE,
                           reply_pong,
                           sizeof(reply_pong) - 1U);
        } else {
            set_data_reply(reply,
                           KVSTORE_REPLY_BULK,
                           arguments[1].data,
                           arguments[1].length);
        }
        return 0;
    default:
        return -1;
    }
}
