#ifndef STORE_SYSTEM_STORAGE_MYSQL_STORE_H
#define STORE_SYSTEM_STORAGE_MYSQL_STORE_H

#include <stddef.h>
#include <stdint.h>

#include "engine/object.h"

typedef struct mysql_store mysql_store_t;

typedef struct mysql_store_config {
    const char *host;
    unsigned int port;
    const char *user;
    const char *password;
    const char *database;
    size_t read_workers;
    unsigned int connect_timeout_seconds;
    unsigned int io_timeout_seconds;
    size_t read_queue_limit;
    size_t write_queue_max_bytes;
    size_t load_max_bytes;
    kv_zset_engine_t zset_engine;
    int notify_fd;
    void (*destroy_waiter_context)(void *context);
    unsigned char writer_uuid[16];
} mysql_store_config_t;

typedef struct mysql_store_argument {
    const unsigned char *data;
    size_t length;
} mysql_store_argument_t;

typedef struct mysql_store_waiter {
    void *context;
    struct mysql_store_waiter *next;
} mysql_store_waiter_t;

typedef enum mysql_store_status {
    MYSQL_STORE_OK = 0,
    MYSQL_STORE_UNAVAILABLE = 1,
    MYSQL_STORE_TOO_LARGE = 2,
    MYSQL_STORE_COLLISION = 3,
    MYSQL_STORE_INTERNAL = 4
} mysql_store_status_t;

typedef struct mysql_store_load_result {
    mysql_store_status_t status;
    int found;
    kv_object_t *object;
    uint64_t expire_at_ms;
    mysql_store_waiter_t *waiters;
    unsigned int mysql_error;
    struct mysql_store_load_result *next;
} mysql_store_load_result_t;

typedef struct mysql_store_stats {
    size_t read_workers;
    size_t connected_readers;
    int connected_writer;
    size_t pending_reads;
    size_t read_queue_limit;
    size_t pending_write_bytes;
    size_t write_queue_max_bytes;
    size_t load_max_bytes;
    uint64_t submitted_sequence;
    uint64_t applied_sequence;
    uint64_t loads;
    uint64_t coalesced_loads;
    uint64_t negative_cache_hits;
    uint64_t load_errors;
    uint64_t write_errors;
    uint64_t reconnects;
    unsigned int last_error;
} mysql_store_stats_t;

typedef enum mysql_store_bootstrap_state {
    MYSQL_STORE_BOOTSTRAP_EMPTY = 0,
    MYSQL_STORE_BOOTSTRAP_IN_PROGRESS = 1,
    MYSQL_STORE_BOOTSTRAP_READY = 2
} mysql_store_bootstrap_state_t;

typedef int (*mysql_store_change_visitor_fn)(const void *key,
                                             size_t key_length,
                                             void *context);

int mysql_store_library_init(void);
void mysql_store_library_end(void);
int mysql_store_open(mysql_store_t **out_store,
                     const mysql_store_config_t *config);
int mysql_store_check_schema(mysql_store_t *store);
int mysql_store_check_permissions(mysql_store_t *store);
int mysql_store_get_startup_state(mysql_store_t *store,
                                  mysql_store_bootstrap_state_t *state,
                                  unsigned char active_writer_uuid[16],
                                  int *has_active_writer,
                                  uint64_t *applied_sequence);
int mysql_store_set_writer_uuid(mysql_store_t *store,
                                const unsigned char writer_uuid[16]);
int mysql_store_recover_mutation(mysql_store_t *store,
                                 uint64_t sequence,
                                 const mysql_store_argument_t *arguments,
                                 size_t argument_count);
int mysql_store_visit_changes_after(mysql_store_t *store,
                                    uint64_t sequence,
                                    mysql_store_change_visitor_fn visitor,
                                    void *context);
int mysql_store_bootstrap_begin(mysql_store_t *store);
int mysql_store_bootstrap_object(mysql_store_t *store,
                                 const void *key,
                                 size_t key_length,
                                 kv_object_t *object,
                                 uint64_t expire_at_ms);
int mysql_store_bootstrap_finish(mysql_store_t *store,
                                 uint64_t applied_sequence);
int mysql_store_set_notify_fd(mysql_store_t *store, int notify_fd);
int mysql_store_submit_load(mysql_store_t *store,
                            const void *key,
                            size_t key_length,
                            void *waiter_context);
int mysql_store_can_accept_write(mysql_store_t *store, size_t encoded_bytes);
int mysql_store_submit_mutation(mysql_store_t *store,
                                uint64_t sequence,
                                const mysql_store_argument_t *arguments,
                                size_t argument_count,
                                size_t encoded_bytes);
mysql_store_load_result_t *mysql_store_take_load_results(mysql_store_t *store);
void mysql_store_release_load_result(mysql_store_load_result_t *result);
void mysql_store_get_stats(mysql_store_t *store, mysql_store_stats_t *stats);
void mysql_store_note_negative_hit(mysql_store_t *store);
int mysql_store_close(mysql_store_t *store, uint64_t drain_timeout_ms);

#endif
