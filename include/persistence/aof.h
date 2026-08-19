#ifndef STORE_SYSTEM_PERSISTENCE_AOF_H
#define STORE_SYSTEM_PERSISTENCE_AOF_H

#include <stddef.h>

typedef struct aof aof_t;

typedef enum aof_fsync_policy {
    AOF_FSYNC_ALWAYS,
    AOF_FSYNC_EVERYSEC,
    AOF_FSYNC_NO
} aof_fsync_policy_t;

typedef struct aof_argument {
    const unsigned char *data;
    size_t length;
} aof_argument_t;

typedef int (*aof_replay_callback)(const aof_argument_t *arguments,
                                   size_t argument_count,
                                   void *context);

typedef struct aof_replay_stats {
    size_t commands_loaded;
    int truncated_tail_repaired;
} aof_replay_stats_t;

int aof_open(aof_t **out_aof,
             const char *path,
             aof_fsync_policy_t fsync_policy);
int aof_replay(aof_t *aof,
               aof_replay_callback callback,
               void *context,
               aof_replay_stats_t *stats);
int aof_append(aof_t *aof,
               const aof_argument_t *arguments,
               size_t argument_count);
int aof_flush(aof_t *aof);
int aof_maintain(aof_t *aof);
int aof_is_failed(aof_t *aof);
int aof_last_error(aof_t *aof);
int aof_close(aof_t *aof);

#endif
