#ifndef STORE_SYSTEM_KVSTORE_SERVICE_H
#define STORE_SYSTEM_KVSTORE_SERVICE_H

#include <stddef.h>

int kvstore_engine_init(void);
void kvstore_engine_destroy(void);

int kvstore_execute_request(const char *request,
                            size_t request_length,
                            char *response,
                            size_t response_capacity,
                            size_t *response_length);

#endif
