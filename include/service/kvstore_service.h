#ifndef STORE_SYSTEM_KVSTORE_SERVICE_H
#define STORE_SYSTEM_KVSTORE_SERVICE_H

#include <stddef.h>
#include <stdint.h>

#include "cache/cache.h"
#include "persistence/aof.h"
#include "storage/mysql_store.h"

typedef struct kvstore_persistence_info {
    int rdb_enabled;
    int bgsave_in_progress;
    uint64_t dirty_changes;
    uint64_t last_save_time;
    uint64_t last_save_duration_us;
    uint64_t last_fork_pause_us;
    uint64_t last_child_peak_rss_kb;
    uint64_t last_child_minor_faults;
    uint64_t last_child_major_faults;
    uint64_t checkpoint_offset;
    int last_save_status;
} kvstore_persistence_info_t;

typedef struct kvstore_persistence_admin {
    int (*save)(void *context, int background);
    uint64_t (*lastsave)(void *context);
    void (*get_info)(void *context, kvstore_persistence_info_t *info);
} kvstore_persistence_admin_t;

typedef struct kvstore_service {
    cache_t *cache;
    aof_t *aof;
    int aof_flush_failure_reported;
    int initialized;
    unsigned char info_buffer[1024];
    kv_zset_engine_t zset_engine;
    struct kvstore_argument *reply_elements;
    size_t reply_element_capacity;
    unsigned char *reply_score_buffer;
    size_t reply_score_capacity;
    double zadd_scores[64];
    kvstore_persistence_admin_t persistence_admin;
    void *persistence_context;
    uint64_t dirty_changes;
    mysql_store_t *mysql_store;
    unsigned char mysql_writer_uuid[16];
    uint64_t mysql_next_sequence;
    uint64_t mysql_candidate_sequence;
    int mysql_recording_command;
    mysql_store_argument_t mysql_mutation_arguments[128];
    size_t mysql_mutation_count;
    unsigned char *mysql_mutation_buffer;
    size_t mysql_mutation_buffer_capacity;
    size_t mysql_mutation_buffer_used;
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
    KVSTORE_REPLY_NULL_BULK,
    KVSTORE_REPLY_ARRAY
} kvstore_reply_type_t;

typedef struct kvstore_reply {
    kvstore_reply_type_t type;
    const unsigned char *data;
    size_t length;
    int64_t integer;
    const kvstore_argument_t *elements;
    size_t element_count;
} kvstore_reply_t;

typedef struct kvstore_service_config {
    cache_config_t cache;
    kv_zset_engine_t zset_engine;
} kvstore_service_config_t;

int kvstore_service_init(kvstore_service_t *service,
                         const cache_config_t *cache_config);
int kvstore_service_init_with_config(kvstore_service_t *service,
                                     const kvstore_service_config_t *config);
void kvstore_service_destroy(kvstore_service_t *service);
int kvstore_service_maintain(kvstore_service_t *service);
int kvstore_service_flush(kvstore_service_t *service);
void kvstore_service_attach_aof(kvstore_service_t *service, aof_t *aof);
void kvstore_service_attach_mysql(kvstore_service_t *service,
                                  mysql_store_t *store,
                                  const unsigned char writer_uuid[16],
                                  uint64_t next_sequence);
void kvstore_service_set_persistence_admin(
    kvstore_service_t *service,
    const kvstore_persistence_admin_t *admin,
    void *context);
uint64_t kvstore_service_dirty_changes(const kvstore_service_t *service);
void kvstore_service_snapshot_committed(kvstore_service_t *service,
                                        uint64_t covered_changes);
int kvstore_service_replay_aof(const aof_argument_t *arguments,
                               size_t argument_count,
                               void *context);
int kvstore_service_execute(kvstore_service_t *service,
                            const kvstore_argument_t *arguments,
                            size_t argument_count,
                            kvstore_reply_t *reply);
int kvstore_service_execute_with_barrier(kvstore_service_t *service,
                                         const kvstore_argument_t *arguments,
                                         size_t argument_count,
                                         kvstore_reply_t *reply,
                                         uint64_t *response_barrier);

int kvstore_engine_init(void);
void kvstore_engine_destroy(void);

int kvstore_execute_request(const char *request,
                            size_t request_length,
                            char *response,
                            size_t response_capacity,
                            size_t *response_length);

#endif
