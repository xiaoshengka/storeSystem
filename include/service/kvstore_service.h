#ifndef STORE_SYSTEM_KVSTORE_SERVICE_H
#define STORE_SYSTEM_KVSTORE_SERVICE_H

#include <stddef.h>
#include <stdint.h>

#include "cache/cache.h"
#include "persistence/aof.h"

typedef struct kvstore_service {
    cache_t *cache;
    aof_t *aof;
    int aof_flush_failure_reported;
    int initialized;
    unsigned char info_buffer[1024];
} kvstore_service_t;

typedef struct kvstore_argument {
    const unsigned char *data;
    size_t length;
} kvstore_argument_t;

typedef enum kvstore_reply_type {
    KVSTORE_REPLY_SIMPLE,
    KVSTORE_REPLY_ERROR,
    KVSTORE_REPLY_INTEGER,
    KVSTORE_REPLY_BULK,
    KVSTORE_REPLY_NULL_BULK
} kvstore_reply_type_t;

typedef struct kvstore_reply {
    kvstore_reply_type_t type;
    const unsigned char *data;
    size_t length;
    int64_t integer;
} kvstore_reply_t;

int kvstore_service_init(kvstore_service_t *service,
                         const cache_config_t *cache_config);
void kvstore_service_destroy(kvstore_service_t *service);
int kvstore_service_maintain(kvstore_service_t *service);
int kvstore_service_flush(kvstore_service_t *service);
void kvstore_service_attach_aof(kvstore_service_t *service, aof_t *aof);
int kvstore_service_replay_aof(const aof_argument_t *arguments,
                               size_t argument_count,
                               void *context);
int kvstore_service_execute(kvstore_service_t *service,
                            const kvstore_argument_t *arguments,
                            size_t argument_count,
                            kvstore_reply_t *reply);

int kvstore_engine_init(void);
void kvstore_engine_destroy(void);

int kvstore_execute_request(const char *request,
                            size_t request_length,
                            char *response,
                            size_t response_capacity,
                            size_t *response_length);

#endif
