#define _POSIX_C_SOURCE 200809L

#include "storage/mysql_store.h"

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <time.h>
#include <unistd.h>

#ifdef KVSTORE_WITH_MYSQL
#include <mysql.h>

typedef struct mysql_load_job mysql_load_job_t;
typedef struct mysql_write_job mysql_write_job_t;

typedef struct mysql_reader {
    struct mysql_store *store;
    pthread_t thread;
    MYSQL *connection;
    int started;
} mysql_reader_t;

typedef struct mysql_writer {
    struct mysql_store *store;
    pthread_t thread;
    MYSQL *connection;
    int started;
} mysql_writer_t;

struct mysql_load_job {
    unsigned char *key;
    size_t key_length;
    mysql_store_waiter_t *waiters;
    mysql_load_job_t *queue_next;
    mysql_load_job_t *inflight_next;
};

struct mysql_write_job {
    uint64_t sequence;
    mysql_store_argument_t *arguments;
    unsigned char *payload;
    size_t argument_count;
    size_t encoded_bytes;
    mysql_write_job_t *next;
};

struct mysql_store {
    mysql_store_config_t config;
    char *host;
    char *user;
    char *password;
    char *database;
    mysql_reader_t *readers;
    mysql_writer_t writer;
    MYSQL *startup_connection;
    pthread_mutex_t mutex;
    pthread_cond_t read_condition;
    pthread_cond_t write_condition;
    mysql_load_job_t *queue_head;
    mysql_load_job_t *queue_tail;
    mysql_load_job_t *inflight;
    mysql_store_load_result_t *completed_head;
    mysql_store_load_result_t *completed_tail;
    mysql_write_job_t *write_head;
    mysql_write_job_t *write_tail;
    size_t pending_write_bytes;
    size_t pending_reads;
    size_t connected_readers;
    int stopping;
    uint64_t shutdown_deadline_ms;
    mysql_store_stats_t stats;
};

static char *duplicate_text(const char *text)
{
    size_t length;
    char *copy;

    if (text == NULL) return NULL;
    length = strlen(text);
    copy = malloc(length + 1U);
    if (copy != NULL) memcpy(copy, text, length + 1U);
    return copy;
}

static uint64_t realtime_ms(void)
{
    struct timespec now;

    if (clock_gettime(CLOCK_REALTIME, &now) != 0) return 0;
    return (uint64_t)now.tv_sec * 1000U + (uint64_t)now.tv_nsec / 1000000U;
}

static MYSQL *connect_database(mysql_store_t *store)
{
    MYSQL *connection = mysql_init(NULL);

    if (connection == NULL) return NULL;
    (void)mysql_options(connection, MYSQL_OPT_CONNECT_TIMEOUT,
                        &store->config.connect_timeout_seconds);
    (void)mysql_options(connection, MYSQL_OPT_READ_TIMEOUT,
                        &store->config.io_timeout_seconds);
    (void)mysql_options(connection, MYSQL_OPT_WRITE_TIMEOUT,
                        &store->config.io_timeout_seconds);
    if (mysql_real_connect(connection,
                           store->host,
                           store->user,
                           store->password,
                           store->database,
                           store->config.port,
                           NULL,
                           CLIENT_FOUND_ROWS) == NULL) {
        mysql_close(connection);
        return NULL;
    }
    return connection;
}

static int prepare_key_query(MYSQL *connection,
                             const char *sql,
                             const void *key,
                             size_t key_length,
                             MYSQL_STMT **out_statement)
{
    MYSQL_STMT *statement = mysql_stmt_init(connection);
    MYSQL_BIND parameter;
    unsigned long amount;

    *out_statement = NULL;
    if (statement == NULL || key_length > ULONG_MAX) {
        if (statement != NULL) mysql_stmt_close(statement);
        return -1;
    }
    amount = (unsigned long)key_length;
    memset(&parameter, 0, sizeof(parameter));
    parameter.buffer_type = MYSQL_TYPE_BLOB;
    parameter.buffer = (void *)key;
    parameter.buffer_length = amount;
    parameter.length = &amount;
    if (mysql_stmt_prepare(statement, sql, (unsigned long)strlen(sql)) != 0 ||
        mysql_stmt_bind_param(statement, &parameter) != 0 ||
        mysql_stmt_execute(statement) != 0) {
        mysql_stmt_close(statement);
        return -1;
    }
    *out_statement = statement;
    return 0;
}

static unsigned char *fetch_column_blob(MYSQL_STMT *statement,
                                        unsigned int column,
                                        unsigned long length)
{
    MYSQL_BIND value;
    unsigned char *data = malloc(length == 0 ? 1U : (size_t)length);

    if (data == NULL) return NULL;
    memset(&value, 0, sizeof(value));
    value.buffer_type = MYSQL_TYPE_BLOB;
    value.buffer = data;
    value.buffer_length = length;
    value.length = &length;
    if (mysql_stmt_fetch_column(statement, &value, column, 0) != 0) {
        free(data);
        return NULL;
    }
    return data;
}

static int load_string(MYSQL *connection,
                       const void *key,
                       size_t key_length,
                       size_t limit,
                       kv_object_t **out_object)
{
    static const char sql[] =
        "SELECT value_data FROM kv_strings "
        "WHERE key_hash=UNHEX(SHA2(?,256))";
    MYSQL_STMT *statement;
    MYSQL_BIND result;
    unsigned char probe = 0;
    unsigned long length = 0;
    _Bool is_null = 0;
    _Bool error = 0;
    unsigned char *value;
    int fetched;

    if (prepare_key_query(connection, sql, key, key_length, &statement) != 0)
        return -1;
    memset(&result, 0, sizeof(result));
    result.buffer_type = MYSQL_TYPE_BLOB;
    result.buffer = &probe;
    result.buffer_length = 1U;
    result.length = &length;
    result.is_null = &is_null;
    result.error = &error;
    if (mysql_stmt_bind_result(statement, &result) != 0 ||
        mysql_stmt_store_result(statement) != 0) {
        mysql_stmt_close(statement);
        return -1;
    }
    fetched = mysql_stmt_fetch(statement);
    if ((fetched != 0 && fetched != MYSQL_DATA_TRUNCATED) || is_null ||
        (size_t)length > limit) {
        mysql_stmt_close(statement);
        return (size_t)length > limit ? -2 : -1;
    }
    value = fetch_column_blob(statement, 0, length);
    mysql_stmt_close(statement);
    if (value == NULL) return -1;
    fetched = kv_object_create_string(out_object, value, (size_t)length);
    free(value);
    return fetched == 0 ? 0 : -1;
}

static int load_hash(MYSQL *connection,
                     const void *key,
                     size_t key_length,
                     size_t limit,
                     kv_object_t **out_object)
{
    static const char sql[] =
        "SELECT field_data,value_data FROM kv_hash_fields "
        "WHERE key_hash=UNHEX(SHA2(?,256))";
    MYSQL_STMT *statement;
    MYSQL_BIND results[2];
    unsigned char probes[2] = {0, 0};
    unsigned long lengths[2] = {0, 0};
    _Bool nulls[2] = {0, 0};
    _Bool errors[2] = {0, 0};
    size_t loaded = 0;
    int fetched;

    if (kv_object_create_hash(out_object) != 0) return -1;
    if (prepare_key_query(connection, sql, key, key_length, &statement) != 0)
        goto fail;
    memset(results, 0, sizeof(results));
    for (size_t index = 0; index < 2U; ++index) {
        results[index].buffer_type = MYSQL_TYPE_BLOB;
        results[index].buffer = &probes[index];
        results[index].buffer_length = 1U;
        results[index].length = &lengths[index];
        results[index].is_null = &nulls[index];
        results[index].error = &errors[index];
    }
    if (mysql_stmt_bind_result(statement, results) != 0 ||
        mysql_stmt_store_result(statement) != 0) {
        mysql_stmt_close(statement);
        goto fail;
    }
    while ((fetched = mysql_stmt_fetch(statement)) != MYSQL_NO_DATA) {
        unsigned char *field;
        unsigned char *value;
        int added;
        int changed;

        if ((fetched != 0 && fetched != MYSQL_DATA_TRUNCATED) ||
            nulls[0] || nulls[1] ||
            lengths[0] > SIZE_MAX - lengths[1] ||
            (size_t)lengths[0] + (size_t)lengths[1] > limit ||
            loaded > limit - ((size_t)lengths[0] + (size_t)lengths[1])) {
            mysql_stmt_close(statement);
            kv_object_destroy(*out_object);
            *out_object = NULL;
            return -2;
        }
        field = fetch_column_blob(statement, 0, lengths[0]);
        value = fetch_column_blob(statement, 1, lengths[1]);
        if (field == NULL || value == NULL ||
            kv_object_hash_set(*out_object,
                               field, (size_t)lengths[0],
                               value, (size_t)lengths[1],
                               &added, &changed) != 0) {
            free(field);
            free(value);
            mysql_stmt_close(statement);
            goto fail;
        }
        loaded += (size_t)lengths[0] + (size_t)lengths[1];
        free(field);
        free(value);
    }
    mysql_stmt_close(statement);
    return 0;

fail:
    kv_object_destroy(*out_object);
    *out_object = NULL;
    return -1;
}

static int load_zset(MYSQL *connection,
                     const void *key,
                     size_t key_length,
                     size_t limit,
                     kv_zset_engine_t engine,
                     kv_object_t **out_object)
{
    static const char sql[] =
        "SELECT member_data,score FROM kv_zset_members "
        "WHERE key_hash=UNHEX(SHA2(?,256))";
    MYSQL_STMT *statement;
    MYSQL_BIND results[2];
    unsigned char probe = 0;
    unsigned long length = 0;
    _Bool nulls[2] = {0, 0};
    _Bool errors[2] = {0, 0};
    double score = 0.0;
    size_t loaded = 0;
    int fetched;

    if (kv_object_create_zset(out_object, engine) != 0) return -1;
    if (prepare_key_query(connection, sql, key, key_length, &statement) != 0)
        goto fail;
    memset(results, 0, sizeof(results));
    results[0].buffer_type = MYSQL_TYPE_BLOB;
    results[0].buffer = &probe;
    results[0].buffer_length = 1U;
    results[0].length = &length;
    results[0].is_null = &nulls[0];
    results[0].error = &errors[0];
    results[1].buffer_type = MYSQL_TYPE_DOUBLE;
    results[1].buffer = &score;
    results[1].is_null = &nulls[1];
    results[1].error = &errors[1];
    if (mysql_stmt_bind_result(statement, results) != 0 ||
        mysql_stmt_store_result(statement) != 0) {
        mysql_stmt_close(statement);
        goto fail;
    }
    while ((fetched = mysql_stmt_fetch(statement)) != MYSQL_NO_DATA) {
        unsigned char *member;
        int added;
        int changed;

        if ((fetched != 0 && fetched != MYSQL_DATA_TRUNCATED) ||
            nulls[0] || nulls[1] || (size_t)length > limit ||
            loaded > limit - (size_t)length) {
            mysql_stmt_close(statement);
            kv_object_destroy(*out_object);
            *out_object = NULL;
            return -2;
        }
        member = fetch_column_blob(statement, 0, length);
        if (member == NULL ||
            kv_object_zset_add(*out_object, score, member, (size_t)length,
                               &added, &changed) != 0) {
            free(member);
            mysql_stmt_close(statement);
            goto fail;
        }
        loaded += (size_t)length;
        free(member);
    }
    mysql_stmt_close(statement);
    return 0;

fail:
    kv_object_destroy(*out_object);
    *out_object = NULL;
    return -1;
}

static mysql_store_status_t load_object(MYSQL *connection,
                                        mysql_store_t *store,
                                        mysql_load_job_t *job,
                                        kv_object_t **out_object,
                                        uint64_t *expire_at_ms,
                                        int *found,
                                        unsigned int *mysql_error)
{
    static const char sql[] =
        "SELECT key_data,object_type,expire_at_ms FROM kv_keys "
        "WHERE key_hash=UNHEX(SHA2(?,256))";
    MYSQL_STMT *statement;
    MYSQL_BIND results[3];
    unsigned char key_probe = 0;
    unsigned long stored_key_length = 0;
    unsigned char type = 0;
    unsigned long type_length = 0;
    unsigned long long deadline = 0;
    unsigned long deadline_length = 0;
    _Bool nulls[3] = {0, 0, 0};
    _Bool errors[3] = {0, 0, 0};
    unsigned char *stored_key;
    int fetched;
    int result;

    *out_object = NULL;
    *expire_at_ms = 0;
    *found = 0;
    *mysql_error = 0;
    if (prepare_key_query(connection, sql, job->key, job->key_length,
                          &statement) != 0) {
        *mysql_error = mysql_errno(connection);
        return MYSQL_STORE_UNAVAILABLE;
    }
    memset(results, 0, sizeof(results));
    results[0].buffer_type = MYSQL_TYPE_BLOB;
    results[0].buffer = &key_probe;
    results[0].buffer_length = 1U;
    results[0].length = &stored_key_length;
    results[0].is_null = &nulls[0];
    results[0].error = &errors[0];
    results[1].buffer_type = MYSQL_TYPE_TINY;
    results[1].buffer = &type;
    results[1].length = &type_length;
    results[1].is_unsigned = 1;
    results[1].is_null = &nulls[1];
    results[1].error = &errors[1];
    results[2].buffer_type = MYSQL_TYPE_LONGLONG;
    results[2].buffer = &deadline;
    results[2].length = &deadline_length;
    results[2].is_unsigned = 1;
    results[2].is_null = &nulls[2];
    results[2].error = &errors[2];
    if (mysql_stmt_bind_result(statement, results) != 0 ||
        mysql_stmt_store_result(statement) != 0) {
        *mysql_error = mysql_stmt_errno(statement);
        mysql_stmt_close(statement);
        return MYSQL_STORE_UNAVAILABLE;
    }
    fetched = mysql_stmt_fetch(statement);
    if (fetched == MYSQL_NO_DATA) {
        mysql_stmt_close(statement);
        return MYSQL_STORE_OK;
    }
    if ((fetched != 0 && fetched != MYSQL_DATA_TRUNCATED) || nulls[0] ||
        nulls[1]) {
        *mysql_error = mysql_stmt_errno(statement);
        mysql_stmt_close(statement);
        return MYSQL_STORE_INTERNAL;
    }
    stored_key = fetch_column_blob(statement, 0, stored_key_length);
    mysql_stmt_close(statement);
    if (stored_key == NULL) return MYSQL_STORE_INTERNAL;
    if ((size_t)stored_key_length != job->key_length ||
        memcmp(stored_key, job->key, job->key_length) != 0) {
        free(stored_key);
        return MYSQL_STORE_COLLISION;
    }
    free(stored_key);
    if (!nulls[2] && deadline <= realtime_ms()) return MYSQL_STORE_OK;
    if (type == KV_OBJECT_STRING) {
        result = load_string(connection, job->key, job->key_length,
                             store->config.load_max_bytes, out_object);
    } else if (type == KV_OBJECT_HASH) {
        result = load_hash(connection, job->key, job->key_length,
                           store->config.load_max_bytes, out_object);
    } else if (type == KV_OBJECT_ZSET) {
        result = load_zset(connection, job->key, job->key_length,
                           store->config.load_max_bytes,
                           store->config.zset_engine, out_object);
    } else {
        return MYSQL_STORE_INTERNAL;
    }
    if (result == -2) return MYSQL_STORE_TOO_LARGE;
    if (result != 0) {
        *mysql_error = mysql_errno(connection);
        return *mysql_error == 0 ? MYSQL_STORE_INTERNAL
                                 : MYSQL_STORE_UNAVAILABLE;
    }
    *found = 1;
    *expire_at_ms = nulls[2] ? 0 : (uint64_t)deadline;
    return MYSQL_STORE_OK;
}

static int argument_equals(const mysql_store_argument_t *argument,
                           const char *text)
{
    size_t length = strlen(text);

    if (argument->length != length) return 0;
    for (size_t index = 0; index < length; ++index) {
        unsigned char value = argument->data[index];
        if (value >= 'a' && value <= 'z') value -= (unsigned char)('a' - 'A');
        if (value != (unsigned char)text[index]) return 0;
    }
    return 1;
}

static int bind_blob_value(MYSQL_BIND *binding,
                           unsigned long *length,
                           const void *data,
                           size_t amount)
{
    if (amount > ULONG_MAX) return -1;
    memset(binding, 0, sizeof(*binding));
    *length = (unsigned long)amount;
    binding->buffer_type = MYSQL_TYPE_BLOB;
    binding->buffer = (void *)data;
    binding->buffer_length = *length;
    binding->length = length;
    return 0;
}

static void bind_u64_value(MYSQL_BIND *binding,
                           unsigned long long *value,
                           _Bool *is_null)
{
    memset(binding, 0, sizeof(*binding));
    binding->buffer_type = MYSQL_TYPE_LONGLONG;
    binding->buffer = value;
    binding->is_unsigned = 1;
    binding->is_null = is_null;
}

static int execute_bound(MYSQL *connection,
                         const char *sql,
                         MYSQL_BIND *parameters)
{
    MYSQL_STMT *statement = mysql_stmt_init(connection);
    int result = -1;

    if (statement != NULL &&
        mysql_stmt_prepare(statement, sql, (unsigned long)strlen(sql)) == 0 &&
        (mysql_stmt_param_count(statement) == 0 ||
         mysql_stmt_bind_param(statement, parameters) == 0) &&
        mysql_stmt_execute(statement) == 0) {
        result = 0;
    }
    if (statement != NULL) mysql_stmt_close(statement);
    return result;
}

static int verify_typed_key(MYSQL *connection,
                            const mysql_store_argument_t *key,
                            unsigned int type)
{
    static const char sql[] =
        "SELECT COUNT(*) FROM kv_keys WHERE key_hash=UNHEX(SHA2(?,256)) "
        "AND key_data=? AND object_type=?";
    MYSQL_STMT *statement = NULL;
    MYSQL_BIND parameters[3];
    MYSQL_BIND result;
    unsigned long lengths[2];
    unsigned char bound_type = (unsigned char)type;
    unsigned long long count = 0;
    _Bool is_null = 0;
    int fetched;

    statement = mysql_stmt_init(connection);
    if (statement == NULL ||
        mysql_stmt_prepare(statement, sql, sizeof(sql) - 1U) != 0 ||
        bind_blob_value(&parameters[0], &lengths[0], key->data, key->length) != 0 ||
        bind_blob_value(&parameters[1], &lengths[1], key->data, key->length) != 0)
        goto fail;
    memset(&parameters[2], 0, sizeof(parameters[2]));
    parameters[2].buffer_type = MYSQL_TYPE_TINY;
    parameters[2].buffer = &bound_type;
    parameters[2].is_unsigned = 1;
    if (mysql_stmt_bind_param(statement, parameters) != 0) goto fail;
    memset(&result, 0, sizeof(result));
    result.buffer_type = MYSQL_TYPE_LONGLONG;
    result.buffer = &count;
    result.is_unsigned = 1;
    result.is_null = &is_null;
    if (mysql_stmt_bind_result(statement, &result) != 0 ||
        mysql_stmt_execute(statement) != 0 ||
        (fetched = mysql_stmt_fetch(statement)) != 0 || is_null || count != 1U)
        goto fail;
    mysql_stmt_close(statement);
    return 0;

fail:
    if (statement != NULL) mysql_stmt_close(statement);
    errno = EPROTO;
    return -1;
}

static int verify_member_identity(MYSQL *connection,
                                  const char *table,
                                  const char *hash_column,
                                  const char *data_column,
                                  const mysql_store_argument_t *key,
                                  const mysql_store_argument_t *member)
{
    char sql[512];
    MYSQL_STMT *statement = NULL;
    MYSQL_BIND parameters[4];
    MYSQL_BIND result;
    unsigned long lengths[4];
    unsigned long long count = 0;
    _Bool is_null = 0;
    int written;

    written = snprintf(sql, sizeof(sql),
        "SELECT COUNT(*) FROM %s AS child JOIN kv_keys AS parent "
        "ON child.key_hash=parent.key_hash "
        "WHERE parent.key_hash=UNHEX(SHA2(?,256)) AND parent.key_data=? "
        "AND child.%s=UNHEX(SHA2(?,256)) AND child.%s=?",
        table, hash_column, data_column);
    if (written < 0 || (size_t)written >= sizeof(sql)) return -1;
    statement = mysql_stmt_init(connection);
    if (statement == NULL ||
        mysql_stmt_prepare(statement, sql, (unsigned long)written) != 0 ||
        bind_blob_value(&parameters[0], &lengths[0], key->data, key->length) != 0 ||
        bind_blob_value(&parameters[1], &lengths[1], key->data, key->length) != 0 ||
        bind_blob_value(&parameters[2], &lengths[2], member->data, member->length) != 0 ||
        bind_blob_value(&parameters[3], &lengths[3], member->data, member->length) != 0 ||
        mysql_stmt_bind_param(statement, parameters) != 0) goto fail;
    memset(&result, 0, sizeof(result));
    result.buffer_type = MYSQL_TYPE_LONGLONG;
    result.buffer = &count;
    result.is_unsigned = 1;
    result.is_null = &is_null;
    if (mysql_stmt_bind_result(statement, &result) != 0 ||
        mysql_stmt_execute(statement) != 0 || mysql_stmt_fetch(statement) != 0 ||
        is_null || count != 1U) goto fail;
    mysql_stmt_close(statement);
    return 0;

fail:
    if (statement != NULL) mysql_stmt_close(statement);
    errno = EPROTO;
    return -1;
}

static int delete_key(MYSQL *connection,
                      const mysql_store_argument_t *key)
{
    static const char sql[] =
        "DELETE FROM kv_keys WHERE key_hash=UNHEX(SHA2(?,256)) "
        "AND key_data=?";
    MYSQL_BIND parameters[2];
    unsigned long lengths[2];

    if (bind_blob_value(&parameters[0], &lengths[0], key->data, key->length) != 0 ||
        bind_blob_value(&parameters[1], &lengths[1], key->data, key->length) != 0)
        return -1;
    return execute_bound(connection, sql, parameters);
}

static int insert_typed_key(MYSQL *connection,
                            const mysql_store_argument_t *key,
                            unsigned int type)
{
    static const char *sql[] = {
        "INSERT INTO kv_keys(key_hash,key_data,object_type,expire_at_ms,object_version) "
        "VALUES(UNHEX(SHA2(?,256)),?,0,NULL,1) "
        "ON DUPLICATE KEY UPDATE object_version=IF(key_data=VALUES(key_data) "
        "AND object_type=VALUES(object_type),object_version+1,object_version)",
        "INSERT INTO kv_keys(key_hash,key_data,object_type,expire_at_ms,object_version) "
        "VALUES(UNHEX(SHA2(?,256)),?,1,NULL,1) "
        "ON DUPLICATE KEY UPDATE object_version=IF(key_data=VALUES(key_data) "
        "AND object_type=VALUES(object_type),object_version+1,object_version)",
        "INSERT INTO kv_keys(key_hash,key_data,object_type,expire_at_ms,object_version) "
        "VALUES(UNHEX(SHA2(?,256)),?,2,NULL,1) "
        "ON DUPLICATE KEY UPDATE object_version=IF(key_data=VALUES(key_data) "
        "AND object_type=VALUES(object_type),object_version+1,object_version)"
    };
    MYSQL_BIND parameters[2];
    unsigned long lengths[2];

    if (type > 2U ||
        bind_blob_value(&parameters[0], &lengths[0], key->data, key->length) != 0 ||
        bind_blob_value(&parameters[1], &lengths[1], key->data, key->length) != 0)
        return -1;
    if (execute_bound(connection, sql[type], parameters) != 0) return -1;
    return verify_typed_key(connection, key, type);
}

static int apply_set(MYSQL *connection,
                     const mysql_store_argument_t *arguments,
                     size_t argument_count)
{
    static const char key_sql[] =
        "INSERT INTO kv_keys(key_hash,key_data,object_type,expire_at_ms,object_version) "
        "VALUES(UNHEX(SHA2(?,256)),?,0,?,1)";
    static const char value_sql[] =
        "INSERT INTO kv_strings(key_hash,value_data) "
        "VALUES(UNHEX(SHA2(?,256)),?)";
    MYSQL_BIND key_parameters[3];
    MYSQL_BIND value_parameters[2];
    unsigned long key_lengths[2];
    unsigned long value_lengths[2];
    unsigned long long deadline = 0;
    _Bool deadline_null = 1;
    char number[32];
    char *end;

    if (argument_count == 5U) {
        if (!argument_equals(&arguments[3], "PXAT") ||
            arguments[4].length == 0 || arguments[4].length >= sizeof(number))
            return -1;
        memcpy(number, arguments[4].data, arguments[4].length);
        number[arguments[4].length] = '\0';
        errno = 0;
        deadline = strtoull(number, &end, 10);
        if (errno != 0 || end != number + arguments[4].length || deadline == 0)
            return -1;
        deadline_null = 0;
    } else if (argument_count != 3U) {
        return -1;
    }
    if (delete_key(connection, &arguments[1]) != 0 ||
        bind_blob_value(&key_parameters[0], &key_lengths[0],
                        arguments[1].data, arguments[1].length) != 0 ||
        bind_blob_value(&key_parameters[1], &key_lengths[1],
                        arguments[1].data, arguments[1].length) != 0) return -1;
    bind_u64_value(&key_parameters[2], &deadline, &deadline_null);
    if (execute_bound(connection, key_sql, key_parameters) != 0 ||
        bind_blob_value(&value_parameters[0], &value_lengths[0],
                        arguments[1].data, arguments[1].length) != 0 ||
        bind_blob_value(&value_parameters[1], &value_lengths[1],
                        arguments[2].data, arguments[2].length) != 0)
        return -1;
    return execute_bound(connection, value_sql, value_parameters);
}

static int apply_hset(MYSQL *connection,
                      const mysql_store_argument_t *arguments,
                      size_t argument_count)
{
    static const char sql[] =
        "INSERT INTO kv_hash_fields(key_hash,field_hash,field_data,value_data) "
        "SELECT key_hash,UNHEX(SHA2(?,256)),?,? FROM kv_keys "
        "WHERE key_hash=UNHEX(SHA2(?,256)) AND key_data=? AND object_type=1 "
        "ON DUPLICATE KEY UPDATE value_data=IF(field_data=VALUES(field_data),"
        "VALUES(value_data),value_data)";

    if (insert_typed_key(connection, &arguments[1], KV_OBJECT_HASH) != 0)
        return -1;
    for (size_t index = 2U; index + 1U < argument_count; index += 2U) {
        MYSQL_BIND parameters[5];
        unsigned long lengths[5];
        const mysql_store_argument_t *field = &arguments[index];
        const mysql_store_argument_t *value = &arguments[index + 1U];

        if (bind_blob_value(&parameters[0], &lengths[0], field->data, field->length) != 0 ||
            bind_blob_value(&parameters[1], &lengths[1], field->data, field->length) != 0 ||
            bind_blob_value(&parameters[2], &lengths[2], value->data, value->length) != 0 ||
            bind_blob_value(&parameters[3], &lengths[3], arguments[1].data, arguments[1].length) != 0 ||
            bind_blob_value(&parameters[4], &lengths[4], arguments[1].data, arguments[1].length) != 0 ||
            execute_bound(connection, sql, parameters) != 0 ||
            verify_member_identity(connection, "kv_hash_fields", "field_hash",
                                   "field_data", &arguments[1], field) != 0)
            return -1;
    }
    return 0;
}

static int apply_hdel(MYSQL *connection,
                      const mysql_store_argument_t *arguments,
                      size_t argument_count)
{
    static const char delete_sql[] =
        "DELETE FROM kv_hash_fields WHERE key_hash=UNHEX(SHA2(?,256)) "
        "AND field_hash=UNHEX(SHA2(?,256)) AND field_data=?";
    static const char empty_sql[] =
        "DELETE FROM kv_keys WHERE key_hash=UNHEX(SHA2(?,256)) "
        "AND key_data=? AND object_type=1 AND NOT EXISTS "
        "(SELECT 1 FROM kv_hash_fields WHERE kv_hash_fields.key_hash=kv_keys.key_hash)";

    for (size_t index = 2U; index < argument_count; ++index) {
        MYSQL_BIND parameters[3];
        unsigned long lengths[3];
        if (bind_blob_value(&parameters[0], &lengths[0], arguments[1].data, arguments[1].length) != 0 ||
            bind_blob_value(&parameters[1], &lengths[1], arguments[index].data, arguments[index].length) != 0 ||
            bind_blob_value(&parameters[2], &lengths[2], arguments[index].data, arguments[index].length) != 0 ||
            execute_bound(connection, delete_sql, parameters) != 0) return -1;
    }
    {
        MYSQL_BIND parameters[2];
        unsigned long lengths[2];
        if (bind_blob_value(&parameters[0], &lengths[0], arguments[1].data, arguments[1].length) != 0 ||
            bind_blob_value(&parameters[1], &lengths[1], arguments[1].data, arguments[1].length) != 0)
            return -1;
        return execute_bound(connection, empty_sql, parameters);
    }
}

static int parse_score(const mysql_store_argument_t *argument, double *score)
{
    char stack[128];
    char *text = stack;
    char *end;

    if (argument->length >= sizeof(stack)) {
        text = malloc(argument->length + 1U);
        if (text == NULL) return -1;
    }
    memcpy(text, argument->data, argument->length);
    text[argument->length] = '\0';
    errno = 0;
    *score = strtod(text, &end);
    if (errno != 0 || end != text + argument->length) {
        if (text != stack) free(text);
        return -1;
    }
    if (text != stack) free(text);
    return 0;
}

static int apply_zadd(MYSQL *connection,
                      const mysql_store_argument_t *arguments,
                      size_t argument_count)
{
    static const char sql[] =
        "INSERT INTO kv_zset_members(key_hash,member_hash,member_data,score) "
        "SELECT key_hash,UNHEX(SHA2(?,256)),?,? FROM kv_keys "
        "WHERE key_hash=UNHEX(SHA2(?,256)) AND key_data=? AND object_type=2 "
        "ON DUPLICATE KEY UPDATE score=IF(member_data=VALUES(member_data),"
        "VALUES(score),score)";

    if (insert_typed_key(connection, &arguments[1], KV_OBJECT_ZSET) != 0)
        return -1;
    for (size_t index = 2U; index + 1U < argument_count; index += 2U) {
        MYSQL_BIND parameters[5];
        unsigned long lengths[4];
        double score;
        const mysql_store_argument_t *member = &arguments[index + 1U];

        if (parse_score(&arguments[index], &score) != 0 ||
            bind_blob_value(&parameters[0], &lengths[0], member->data, member->length) != 0 ||
            bind_blob_value(&parameters[1], &lengths[1], member->data, member->length) != 0)
            return -1;
        memset(&parameters[2], 0, sizeof(parameters[2]));
        parameters[2].buffer_type = MYSQL_TYPE_DOUBLE;
        parameters[2].buffer = &score;
        if (bind_blob_value(&parameters[3], &lengths[2], arguments[1].data, arguments[1].length) != 0 ||
            bind_blob_value(&parameters[4], &lengths[3], arguments[1].data, arguments[1].length) != 0 ||
            execute_bound(connection, sql, parameters) != 0 ||
            verify_member_identity(connection, "kv_zset_members", "member_hash",
                                   "member_data", &arguments[1], member) != 0)
            return -1;
    }
    return 0;
}

static int apply_zrem(MYSQL *connection,
                      const mysql_store_argument_t *arguments,
                      size_t argument_count)
{
    static const char delete_sql[] =
        "DELETE FROM kv_zset_members WHERE key_hash=UNHEX(SHA2(?,256)) "
        "AND member_hash=UNHEX(SHA2(?,256)) AND member_data=?";
    static const char empty_sql[] =
        "DELETE FROM kv_keys WHERE key_hash=UNHEX(SHA2(?,256)) "
        "AND key_data=? AND object_type=2 AND NOT EXISTS "
        "(SELECT 1 FROM kv_zset_members WHERE kv_zset_members.key_hash=kv_keys.key_hash)";

    for (size_t index = 2U; index < argument_count; ++index) {
        MYSQL_BIND parameters[3];
        unsigned long lengths[3];
        if (bind_blob_value(&parameters[0], &lengths[0], arguments[1].data, arguments[1].length) != 0 ||
            bind_blob_value(&parameters[1], &lengths[1], arguments[index].data, arguments[index].length) != 0 ||
            bind_blob_value(&parameters[2], &lengths[2], arguments[index].data, arguments[index].length) != 0 ||
            execute_bound(connection, delete_sql, parameters) != 0) return -1;
    }
    {
        MYSQL_BIND parameters[2];
        unsigned long lengths[2];
        if (bind_blob_value(&parameters[0], &lengths[0], arguments[1].data, arguments[1].length) != 0 ||
            bind_blob_value(&parameters[1], &lengths[1], arguments[1].data, arguments[1].length) != 0)
            return -1;
        return execute_bound(connection, empty_sql, parameters);
    }
}

static int apply_expire(MYSQL *connection,
                        const mysql_store_argument_t *arguments,
                        int persist)
{
    static const char persist_sql[] =
        "UPDATE kv_keys SET expire_at_ms=NULL,object_version=object_version+1 "
        "WHERE key_hash=UNHEX(SHA2(?,256)) AND key_data=?";
    static const char expire_sql[] =
        "UPDATE kv_keys SET expire_at_ms=?,object_version=object_version+1 "
        "WHERE key_hash=UNHEX(SHA2(?,256)) AND key_data=?";
    MYSQL_BIND parameters[3];
    unsigned long lengths[2];
    unsigned long long deadline = 0;
    _Bool deadline_null = 0;
    char number[32];
    char *end;

    if (persist) {
        if (bind_blob_value(&parameters[0], &lengths[0], arguments[1].data, arguments[1].length) != 0 ||
            bind_blob_value(&parameters[1], &lengths[1], arguments[1].data, arguments[1].length) != 0)
            return -1;
        return execute_bound(connection, persist_sql, parameters);
    }
    if (arguments[2].length == 0 || arguments[2].length >= sizeof(number)) return -1;
    memcpy(number, arguments[2].data, arguments[2].length);
    number[arguments[2].length] = '\0';
    errno = 0;
    deadline = strtoull(number, &end, 10);
    if (errno != 0 || end != number + arguments[2].length || deadline == 0)
        return -1;
    bind_u64_value(&parameters[0], &deadline, &deadline_null);
    if (bind_blob_value(&parameters[1], &lengths[0], arguments[1].data, arguments[1].length) != 0 ||
        bind_blob_value(&parameters[2], &lengths[1], arguments[1].data, arguments[1].length) != 0)
        return -1;
    return execute_bound(connection, expire_sql, parameters);
}

static int append_change_record(MYSQL *connection,
                                mysql_store_t *store,
                                mysql_write_job_t *job)
{
    static const char change_sql[] =
        "INSERT INTO kv_change_log(writer_uuid,mutation_sequence,key_hash,key_data,operation_type) "
        "VALUES(?,?,UNHEX(SHA2(?,256)),?,?)";
    static const char state_sql[] =
        "INSERT INTO kv_writer_state(writer_uuid,applied_sequence) VALUES(?,?) "
        "ON DUPLICATE KEY UPDATE applied_sequence=GREATEST(applied_sequence,VALUES(applied_sequence))";
    MYSQL_BIND change[5];
    MYSQL_BIND state[2];
    unsigned long lengths[5];
    unsigned long long sequence = job->sequence;
    unsigned char operation = job->arguments[0].length == 0
                                  ? 0 : job->arguments[0].data[0];

    if (bind_blob_value(&change[0], &lengths[0], store->config.writer_uuid, 16U) != 0)
        return -1;
    memset(&change[1], 0, sizeof(change[1]));
    change[1].buffer_type = MYSQL_TYPE_LONGLONG;
    change[1].buffer = &sequence;
    change[1].is_unsigned = 1;
    if (bind_blob_value(&change[2], &lengths[1], job->arguments[1].data, job->arguments[1].length) != 0 ||
        bind_blob_value(&change[3], &lengths[2], job->arguments[1].data, job->arguments[1].length) != 0)
        return -1;
    memset(&change[4], 0, sizeof(change[4]));
    change[4].buffer_type = MYSQL_TYPE_TINY;
    change[4].buffer = &operation;
    change[4].is_unsigned = 1;
    if (execute_bound(connection, change_sql, change) != 0 ||
        bind_blob_value(&state[0], &lengths[3], store->config.writer_uuid, 16U) != 0)
        return -1;
    memset(&state[1], 0, sizeof(state[1]));
    state[1].buffer_type = MYSQL_TYPE_LONGLONG;
    state[1].buffer = &sequence;
    state[1].is_unsigned = 1;
    return execute_bound(connection, state_sql, state);
}

static int apply_mutation(MYSQL *connection,
                          mysql_store_t *store,
                          mysql_write_job_t *job)
{
    const mysql_store_argument_t *arguments = job->arguments;
    size_t count = job->argument_count;
    int result = -1;

    if (count < 2U || mysql_autocommit(connection, 0) != 0) return -1;
    if (argument_equals(&arguments[0], "SET"))
        result = apply_set(connection, arguments, count);
    else if (argument_equals(&arguments[0], "DEL") && count == 2U)
        result = delete_key(connection, &arguments[1]);
    else if (argument_equals(&arguments[0], "HSET"))
        result = apply_hset(connection, arguments, count);
    else if (argument_equals(&arguments[0], "HDEL"))
        result = apply_hdel(connection, arguments, count);
    else if (argument_equals(&arguments[0], "ZADD"))
        result = apply_zadd(connection, arguments, count);
    else if (argument_equals(&arguments[0], "ZREM"))
        result = apply_zrem(connection, arguments, count);
    else if (argument_equals(&arguments[0], "PEXPIREAT") && count == 3U)
        result = apply_expire(connection, arguments, 0);
    else if (argument_equals(&arguments[0], "PERSIST") && count == 2U)
        result = apply_expire(connection, arguments, 1);
    if (result == 0) result = append_change_record(connection, store, job);
    if (result == 0) result = mysql_commit(connection);
    if (result != 0) (void)mysql_rollback(connection);
    (void)mysql_autocommit(connection, 1);
    return result;
}

static int mutation_is_applied(MYSQL *connection,
                               mysql_store_t *store,
                               uint64_t sequence)
{
    static const char sql[] =
        "SELECT applied_sequence FROM kv_writer_state WHERE writer_uuid=?";
    MYSQL_STMT *statement;
    MYSQL_BIND parameter;
    MYSQL_BIND result;
    unsigned long uuid_length;
    unsigned long long applied = 0;
    _Bool is_null = 0;
    int fetched;

    statement = mysql_stmt_init(connection);
    if (statement == NULL ||
        mysql_stmt_prepare(statement, sql, sizeof(sql) - 1U) != 0 ||
        bind_blob_value(&parameter, &uuid_length, store->config.writer_uuid, 16U) != 0 ||
        mysql_stmt_bind_param(statement, &parameter) != 0 ||
        mysql_stmt_execute(statement) != 0) {
        if (statement != NULL) mysql_stmt_close(statement);
        return -1;
    }
    memset(&result, 0, sizeof(result));
    result.buffer_type = MYSQL_TYPE_LONGLONG;
    result.buffer = &applied;
    result.is_unsigned = 1;
    result.is_null = &is_null;
    if (mysql_stmt_bind_result(statement, &result) != 0 ||
        mysql_stmt_store_result(statement) != 0) {
        mysql_stmt_close(statement);
        return -1;
    }
    fetched = mysql_stmt_fetch(statement);
    mysql_stmt_close(statement);
    if (fetched == MYSQL_NO_DATA || is_null) return 0;
    if (fetched != 0) return -1;
    return applied >= sequence ? 1 : 0;
}

static void remove_inflight(mysql_store_t *store, mysql_load_job_t *job)
{
    mysql_load_job_t **cursor = &store->inflight;

    while (*cursor != NULL && *cursor != job) cursor = &(*cursor)->inflight_next;
    if (*cursor == job) *cursor = job->inflight_next;
}

static void notify_reactor(mysql_store_t *store)
{
    uint64_t value = 1;
    ssize_t ignored;

    if (store->config.notify_fd < 0) return;
    do {
        ignored = write(store->config.notify_fd, &value, sizeof(value));
    } while (ignored < 0 && errno == EINTR);
}

static void *reader_main(void *context)
{
    mysql_reader_t *reader = context;
    mysql_store_t *store = reader->store;

    (void)mysql_thread_init();
    reader->connection = connect_database(store);
    pthread_mutex_lock(&store->mutex);
    if (reader->connection != NULL) store->connected_readers++;
    pthread_mutex_unlock(&store->mutex);
    for (;;) {
        mysql_load_job_t *job;
        mysql_store_load_result_t *result;

        pthread_mutex_lock(&store->mutex);
        while (!store->stopping && store->queue_head == NULL)
            pthread_cond_wait(&store->read_condition, &store->mutex);
        if (store->stopping) {
            pthread_mutex_unlock(&store->mutex);
            break;
        }
        job = store->queue_head;
        store->queue_head = job->queue_next;
        if (store->queue_head == NULL) store->queue_tail = NULL;
        pthread_mutex_unlock(&store->mutex);

        result = calloc(1, sizeof(*result));
        if (result == NULL) {
            result = NULL;
        } else if (reader->connection == NULL) {
            reader->connection = connect_database(store);
            if (reader->connection != NULL) {
                pthread_mutex_lock(&store->mutex);
                store->connected_readers++;
                store->stats.reconnects++;
                pthread_mutex_unlock(&store->mutex);
            }
        }
        if (result != NULL) {
            if (reader->connection == NULL) {
                result->status = MYSQL_STORE_UNAVAILABLE;
            } else {
                result->status = load_object(reader->connection, store, job,
                                             &result->object,
                                             &result->expire_at_ms,
                                             &result->found,
                                             &result->mysql_error);
                if (result->status == MYSQL_STORE_UNAVAILABLE) {
                    mysql_close(reader->connection);
                    reader->connection = NULL;
                    pthread_mutex_lock(&store->mutex);
                    if (store->connected_readers > 0) store->connected_readers--;
                    pthread_mutex_unlock(&store->mutex);
                }
            }
            result->waiters = job->waiters;
        }

        pthread_mutex_lock(&store->mutex);
        remove_inflight(store, job);
        if (store->pending_reads > 0) store->pending_reads--;
        store->stats.loads++;
        if (result == NULL || result->status != MYSQL_STORE_OK) {
            store->stats.load_errors++;
            if (result != NULL) store->stats.last_error = result->mysql_error;
        }
        if (result != NULL) {
            if (store->completed_tail == NULL) store->completed_head = result;
            else store->completed_tail->next = result;
            store->completed_tail = result;
        } else {
            mysql_store_waiter_t *waiter = job->waiters;
            while (waiter != NULL) {
                mysql_store_waiter_t *next = waiter->next;
                if (store->config.destroy_waiter_context != NULL)
                    store->config.destroy_waiter_context(waiter->context);
                free(waiter);
                waiter = next;
            }
        }
        pthread_mutex_unlock(&store->mutex);
        free(job->key);
        free(job);
        notify_reactor(store);
    }
    if (reader->connection != NULL) {
        mysql_close(reader->connection);
        reader->connection = NULL;
        pthread_mutex_lock(&store->mutex);
        if (store->connected_readers > 0) store->connected_readers--;
        pthread_mutex_unlock(&store->mutex);
    }
    mysql_thread_end();
    return NULL;
}

static void free_write_job(mysql_write_job_t *job)
{
    if (job == NULL) return;
    free(job->arguments);
    free(job->payload);
    free(job);
}

static void reconnect_delay(void)
{
    struct timespec delay = {0, 100000000L};
    while (nanosleep(&delay, &delay) != 0 && errno == EINTR) {
    }
}

static void *writer_main(void *context)
{
    mysql_writer_t *writer = context;
    mysql_store_t *store = writer->store;

    (void)mysql_thread_init();
    writer->connection = connect_database(store);
    pthread_mutex_lock(&store->mutex);
    store->stats.connected_writer = writer->connection != NULL;
    pthread_mutex_unlock(&store->mutex);
    for (;;) {
        mysql_write_job_t *job;
        int applied = 0;

        pthread_mutex_lock(&store->mutex);
        while (!store->stopping && store->write_head == NULL)
            pthread_cond_wait(&store->write_condition, &store->mutex);
        if (store->write_head == NULL && store->stopping) {
            pthread_mutex_unlock(&store->mutex);
            break;
        }
        job = store->write_head;
        store->write_head = job->next;
        if (store->write_head == NULL) store->write_tail = NULL;
        pthread_mutex_unlock(&store->mutex);

        while (!applied) {
            int state;

            pthread_mutex_lock(&store->mutex);
            if (store->stopping &&
                realtime_ms() >= store->shutdown_deadline_ms) {
                pthread_mutex_unlock(&store->mutex);
                break;
            }
            pthread_mutex_unlock(&store->mutex);

            if (writer->connection == NULL) {
                writer->connection = connect_database(store);
                if (writer->connection == NULL) {
                    pthread_mutex_lock(&store->mutex);
                    store->stats.last_error = 1U;
                    pthread_mutex_unlock(&store->mutex);
                    reconnect_delay();
                    continue;
                }
                pthread_mutex_lock(&store->mutex);
                store->stats.reconnects++;
                store->stats.connected_writer = 1;
                pthread_mutex_unlock(&store->mutex);
            }
            state = mutation_is_applied(writer->connection, store, job->sequence);
            if (state == 1 || apply_mutation(writer->connection, store, job) == 0) {
                applied = 1;
                break;
            }
            pthread_mutex_lock(&store->mutex);
            store->stats.last_error = mysql_errno(writer->connection);
            store->stats.write_errors++;
            store->stats.connected_writer = 0;
            pthread_mutex_unlock(&store->mutex);
            mysql_close(writer->connection);
            writer->connection = NULL;
            reconnect_delay();
        }
        pthread_mutex_lock(&store->mutex);
        if (store->pending_write_bytes >= job->encoded_bytes)
            store->pending_write_bytes -= job->encoded_bytes;
        else
            store->pending_write_bytes = 0;
        if (applied && job->sequence > store->stats.applied_sequence)
            store->stats.applied_sequence = job->sequence;
        pthread_mutex_unlock(&store->mutex);
        free_write_job(job);
        notify_reactor(store);
        if (!applied) break;
    }
    if (writer->connection != NULL) mysql_close(writer->connection);
    writer->connection = NULL;
    pthread_mutex_lock(&store->mutex);
    store->stats.connected_writer = 0;
    pthread_mutex_unlock(&store->mutex);
    mysql_thread_end();
    return NULL;
}

int mysql_store_library_init(void)
{
    return mysql_library_init(0, NULL, NULL) == 0 ? 0 : -1;
}

void mysql_store_library_end(void)
{
    mysql_library_end();
}

int mysql_store_open(mysql_store_t **out_store,
                     const mysql_store_config_t *config)
{
    mysql_store_t *store;
    size_t index;
    int mutex_initialized = 0;
    int read_condition_initialized = 0;
    int write_condition_initialized = 0;

    if (out_store == NULL || config == NULL || config->host == NULL ||
        config->user == NULL || config->database == NULL ||
        config->read_workers == 0 || config->read_queue_limit == 0 ||
        config->load_max_bytes == 0) {
        errno = EINVAL;
        return -1;
    }
    *out_store = NULL;
    store = calloc(1, sizeof(*store));
    if (store == NULL) return -1;
    store->config = *config;
    store->host = duplicate_text(config->host);
    store->user = duplicate_text(config->user);
    store->password = duplicate_text(config->password == NULL ? "" : config->password);
    store->database = duplicate_text(config->database);
    store->readers = calloc(config->read_workers, sizeof(*store->readers));
    if (store->host == NULL || store->user == NULL || store->password == NULL ||
        store->database == NULL || store->readers == NULL)
        goto init_failed;
    if (pthread_mutex_init(&store->mutex, NULL) != 0) goto init_failed;
    mutex_initialized = 1;
    if (pthread_cond_init(&store->read_condition, NULL) != 0)
        goto init_failed;
    read_condition_initialized = 1;
    if (pthread_cond_init(&store->write_condition, NULL) != 0)
        goto init_failed;
    write_condition_initialized = 1;
    {
        store->stats.read_workers = config->read_workers;
        store->stats.read_queue_limit = config->read_queue_limit;
        store->stats.write_queue_max_bytes = config->write_queue_max_bytes;
        store->stats.load_max_bytes = config->load_max_bytes;
    }
    for (index = 0; index < config->read_workers; ++index) {
        store->readers[index].store = store;
        if (pthread_create(&store->readers[index].thread, NULL,
                           reader_main, &store->readers[index]) != 0) {
            store->stopping = 1;
            pthread_cond_broadcast(&store->read_condition);
            while (index > 0) {
                index--;
                pthread_join(store->readers[index].thread, NULL);
            }
            pthread_cond_destroy(&store->write_condition);
            pthread_cond_destroy(&store->read_condition);
            pthread_mutex_destroy(&store->mutex);
            free(store->readers);
            free(store->host);
            free(store->user);
            free(store->password);
            free(store->database);
            free(store);
            return -1;
        }
        store->readers[index].started = 1;
    }
    store->writer.store = store;
    if (pthread_create(&store->writer.thread, NULL, writer_main,
                       &store->writer) != 0) {
        pthread_mutex_lock(&store->mutex);
        store->stopping = 1;
        pthread_cond_broadcast(&store->read_condition);
        pthread_mutex_unlock(&store->mutex);
        for (index = 0; index < config->read_workers; ++index)
            pthread_join(store->readers[index].thread, NULL);
        pthread_cond_destroy(&store->write_condition);
        pthread_cond_destroy(&store->read_condition);
        pthread_mutex_destroy(&store->mutex);
        free(store->readers);
        free(store->host);
        free(store->user);
        free(store->password);
        free(store->database);
        free(store);
        return -1;
    }
    store->writer.started = 1;
    *out_store = store;
    return 0;

init_failed:
    if (write_condition_initialized)
        pthread_cond_destroy(&store->write_condition);
    if (read_condition_initialized)
        pthread_cond_destroy(&store->read_condition);
    if (mutex_initialized) pthread_mutex_destroy(&store->mutex);
    free(store->readers);
    free(store->host);
    free(store->user);
    free(store->password);
    free(store->database);
    free(store);
    return -1;
}

int mysql_store_check_schema(mysql_store_t *store)
{
    static const char query[] =
        "SELECT schema_version,bootstrap_state FROM kv_schema_meta "
        "WHERE singleton_id=1";
    MYSQL *connection;
    MYSQL_RES *result;
    MYSQL_ROW row;
    int valid;

    if (store == NULL) {
        errno = EINVAL;
        return -1;
    }
    connection = connect_database(store);
    if (connection == NULL) return -1;
    if (mysql_real_query(connection, query, sizeof(query) - 1U) != 0 ||
        (result = mysql_store_result(connection)) == NULL) {
        mysql_close(connection);
        return -1;
    }
    row = mysql_fetch_row(result);
    valid = row != NULL && row[0] != NULL && strcmp(row[0], "7") == 0 &&
            row[1] != NULL &&
            (strcmp(row[1], "EMPTY") == 0 ||
             strcmp(row[1], "IN_PROGRESS") == 0 ||
             strcmp(row[1], "READY") == 0);
    mysql_free_result(result);
    mysql_close(connection);
    if (!valid) errno = EPROTO;
    return valid ? 0 : -1;
}

int mysql_store_check_permissions(mysql_store_t *store)
{
    static const char *const checks[] = {
        "SELECT key_hash FROM kv_keys WHERE 0",
        "INSERT INTO kv_keys(key_hash,key_data,object_type,expire_at_ms,object_version) "
            "SELECT UNHEX(SHA2('permission-probe',256)),X'00',0,NULL,1 WHERE 0",
        "INSERT INTO kv_strings(key_hash,value_data) "
            "SELECT UNHEX(SHA2('permission-probe',256)),X'00' WHERE 0",
        "INSERT INTO kv_hash_fields(key_hash,field_hash,field_data,value_data) "
            "SELECT UNHEX(SHA2('permission-probe',256)),UNHEX(SHA2('field',256)),"
            "X'00',X'00' WHERE 0",
        "INSERT INTO kv_zset_members(key_hash,member_hash,member_data,score) "
            "SELECT UNHEX(SHA2('permission-probe',256)),UNHEX(SHA2('member',256)),"
            "X'00',0.0 WHERE 0",
        "INSERT INTO kv_writer_state(writer_uuid,applied_sequence) "
            "SELECT UNHEX('00000000000000000000000000000000'),0 WHERE 0",
        "INSERT INTO kv_change_log(writer_uuid,mutation_sequence,key_hash,key_data,operation_type) "
            "SELECT UNHEX('00000000000000000000000000000000'),0,"
            "UNHEX(SHA2('permission-probe',256)),X'00',0 WHERE 0",
        "UPDATE kv_schema_meta SET schema_version=schema_version WHERE 0",
        "UPDATE kv_keys SET object_version=object_version WHERE 0",
        "UPDATE kv_strings SET value_data=value_data WHERE 0",
        "UPDATE kv_hash_fields SET value_data=value_data WHERE 0",
        "UPDATE kv_zset_members SET score=score WHERE 0",
        "UPDATE kv_writer_state SET applied_sequence=applied_sequence WHERE 0",
        "UPDATE kv_change_log SET operation_type=operation_type WHERE 0",
        "DELETE FROM kv_strings WHERE 0",
        "DELETE FROM kv_hash_fields WHERE 0",
        "DELETE FROM kv_zset_members WHERE 0",
        "DELETE FROM kv_change_log WHERE 0",
        "DELETE FROM kv_writer_state WHERE 0",
        "DELETE FROM kv_keys WHERE 0"
    };
    MYSQL *connection;
    size_t index;

    if (store == NULL) { errno = EINVAL; return -1; }
    connection = connect_database(store);
    if (connection == NULL) return -1;
    for (index = 0; index < sizeof(checks) / sizeof(checks[0]); ++index) {
        if (mysql_query(connection, checks[index]) != 0) {
            mysql_close(connection);
            errno = EACCES;
            return -1;
        }
        if (mysql_field_count(connection) != 0) {
            MYSQL_RES *result = mysql_store_result(connection);
            if (result == NULL) {
                mysql_close(connection);
                return -1;
            }
            mysql_free_result(result);
        }
    }
    mysql_close(connection);
    return 0;
}

static int decode_uuid_hex(const char *text, unsigned char uuid[16])
{
    size_t index;

    if (text == NULL || strlen(text) != 32U) return -1;
    for (index = 0; index < 16U; ++index) {
        unsigned int high;
        unsigned int low;
        unsigned char a = (unsigned char)text[index * 2U];
        unsigned char b = (unsigned char)text[index * 2U + 1U];

        high = a >= '0' && a <= '9' ? (unsigned int)(a - '0')
             : a >= 'A' && a <= 'F' ? (unsigned int)(a - 'A' + 10U)
             : a >= 'a' && a <= 'f' ? (unsigned int)(a - 'a' + 10U) : 16U;
        low = b >= '0' && b <= '9' ? (unsigned int)(b - '0')
            : b >= 'A' && b <= 'F' ? (unsigned int)(b - 'A' + 10U)
            : b >= 'a' && b <= 'f' ? (unsigned int)(b - 'a' + 10U) : 16U;
        if (high > 15U || low > 15U) return -1;
        uuid[index] = (unsigned char)((high << 4U) | low);
    }
    return 0;
}

static int read_applied_sequence(MYSQL *connection,
                                 const unsigned char uuid[16],
                                 uint64_t *sequence)
{
    static const char sql[] =
        "SELECT applied_sequence FROM kv_writer_state WHERE writer_uuid=?";
    MYSQL_STMT *statement = NULL;
    MYSQL_BIND parameter;
    MYSQL_BIND result;
    unsigned long uuid_length;
    unsigned long long value = 0;
    _Bool is_null = 0;
    int fetched;

    *sequence = 0;
    statement = mysql_stmt_init(connection);
    if (statement == NULL ||
        mysql_stmt_prepare(statement, sql, sizeof(sql) - 1U) != 0 ||
        bind_blob_value(&parameter, &uuid_length, uuid, 16U) != 0 ||
        mysql_stmt_bind_param(statement, &parameter) != 0) goto fail;
    memset(&result, 0, sizeof(result));
    result.buffer_type = MYSQL_TYPE_LONGLONG;
    result.buffer = &value;
    result.is_unsigned = 1;
    result.is_null = &is_null;
    if (mysql_stmt_bind_result(statement, &result) != 0 ||
        mysql_stmt_execute(statement) != 0) goto fail;
    fetched = mysql_stmt_fetch(statement);
    if (fetched == MYSQL_NO_DATA) {
        mysql_stmt_close(statement);
        return 0;
    }
    if (fetched != 0 || is_null) goto fail;
    *sequence = (uint64_t)value;
    mysql_stmt_close(statement);
    return 0;

fail:
    if (statement != NULL) mysql_stmt_close(statement);
    return -1;
}

int mysql_store_get_startup_state(mysql_store_t *store,
                                  mysql_store_bootstrap_state_t *state,
                                  unsigned char active_writer_uuid[16],
                                  int *has_active_writer,
                                  uint64_t *applied_sequence)
{
    static const char query[] =
        "SELECT bootstrap_state,HEX(active_writer_uuid) FROM kv_schema_meta "
        "WHERE singleton_id=1";
    MYSQL *connection;
    MYSQL_RES *result = NULL;
    MYSQL_ROW row;
    int status = -1;

    if (store == NULL || state == NULL || active_writer_uuid == NULL ||
        has_active_writer == NULL || applied_sequence == NULL) {
        errno = EINVAL;
        return -1;
    }
    *has_active_writer = 0;
    *applied_sequence = 0;
    connection = connect_database(store);
    if (connection == NULL) return -1;
    if (mysql_real_query(connection, query, sizeof(query) - 1U) != 0 ||
        (result = mysql_store_result(connection)) == NULL ||
        (row = mysql_fetch_row(result)) == NULL || row[0] == NULL) goto done;
    if (strcmp(row[0], "EMPTY") == 0) *state = MYSQL_STORE_BOOTSTRAP_EMPTY;
    else if (strcmp(row[0], "IN_PROGRESS") == 0)
        *state = MYSQL_STORE_BOOTSTRAP_IN_PROGRESS;
    else if (strcmp(row[0], "READY") == 0)
        *state = MYSQL_STORE_BOOTSTRAP_READY;
    else goto done;
    if (row[1] != NULL && row[1][0] != '\0') {
        if (decode_uuid_hex(row[1], active_writer_uuid) != 0 ||
            read_applied_sequence(connection, active_writer_uuid,
                                  applied_sequence) != 0) goto done;
        *has_active_writer = 1;
    }
    status = 0;

done:
    if (result != NULL) mysql_free_result(result);
    mysql_close(connection);
    if (status == 0) {
        pthread_mutex_lock(&store->mutex);
        store->stats.applied_sequence = *applied_sequence;
        store->stats.submitted_sequence = *applied_sequence;
        pthread_mutex_unlock(&store->mutex);
    }
    if (status != 0) errno = EPROTO;
    return status;
}

int mysql_store_set_writer_uuid(mysql_store_t *store,
                                const unsigned char writer_uuid[16])
{
    if (store == NULL || writer_uuid == NULL) {
        errno = EINVAL;
        return -1;
    }
    pthread_mutex_lock(&store->mutex);
    if (store->write_head != NULL || store->pending_write_bytes != 0) {
        pthread_mutex_unlock(&store->mutex);
        errno = EBUSY;
        return -1;
    }
    memcpy(store->config.writer_uuid, writer_uuid, 16U);
    pthread_mutex_unlock(&store->mutex);
    return 0;
}

static MYSQL *startup_connection(mysql_store_t *store)
{
    if (store->startup_connection == NULL)
        store->startup_connection = connect_database(store);
    return store->startup_connection;
}

int mysql_store_recover_mutation(mysql_store_t *store,
                                 uint64_t sequence,
                                 const mysql_store_argument_t *arguments,
                                 size_t argument_count)
{
    mysql_write_job_t job;
    MYSQL *connection;
    int state;

    if (store == NULL || sequence == 0 || arguments == NULL ||
        argument_count < 2U) {
        errno = EINVAL;
        return -1;
    }
    connection = startup_connection(store);
    if (connection == NULL) return -1;
    state = mutation_is_applied(connection, store, sequence);
    if (state < 0) return -1;
    if (state == 0) {
        memset(&job, 0, sizeof(job));
        job.sequence = sequence;
        job.arguments = (mysql_store_argument_t *)arguments;
        job.argument_count = argument_count;
        if (apply_mutation(connection, store, &job) != 0) return -1;
    }
    pthread_mutex_lock(&store->mutex);
    if (sequence > store->stats.applied_sequence)
        store->stats.applied_sequence = sequence;
    if (sequence > store->stats.submitted_sequence)
        store->stats.submitted_sequence = sequence;
    pthread_mutex_unlock(&store->mutex);
    return 0;
}

int mysql_store_visit_changes_after(mysql_store_t *store,
                                    uint64_t sequence,
                                    mysql_store_change_visitor_fn visitor,
                                    void *context)
{
    static const char sql[] =
        "SELECT key_data FROM kv_change_log WHERE writer_uuid=? "
        "AND mutation_sequence>? ORDER BY mutation_sequence";
    MYSQL *connection;
    MYSQL_STMT *statement = NULL;
    MYSQL_BIND parameters[2];
    MYSQL_BIND result;
    unsigned long uuid_length;
    unsigned long length = 0;
    unsigned long long after = sequence;
    unsigned char probe = 0;
    _Bool is_null = 0;
    _Bool error = 0;
    int fetched;
    int status = -1;

    if (store == NULL || visitor == NULL) { errno = EINVAL; return -1; }
    connection = startup_connection(store);
    if (connection == NULL) return -1;
    statement = mysql_stmt_init(connection);
    if (statement == NULL ||
        mysql_stmt_prepare(statement, sql, sizeof(sql) - 1U) != 0 ||
        bind_blob_value(&parameters[0], &uuid_length,
                        store->config.writer_uuid, 16U) != 0) goto done;
    memset(&parameters[1], 0, sizeof(parameters[1]));
    parameters[1].buffer_type = MYSQL_TYPE_LONGLONG;
    parameters[1].buffer = &after;
    parameters[1].is_unsigned = 1;
    if (mysql_stmt_bind_param(statement, parameters) != 0) goto done;
    memset(&result, 0, sizeof(result));
    result.buffer_type = MYSQL_TYPE_BLOB;
    result.buffer = &probe;
    result.buffer_length = 1U;
    result.length = &length;
    result.is_null = &is_null;
    result.error = &error;
    if (mysql_stmt_bind_result(statement, &result) != 0 ||
        mysql_stmt_execute(statement) != 0 ||
        mysql_stmt_store_result(statement) != 0) goto done;
    while ((fetched = mysql_stmt_fetch(statement)) != MYSQL_NO_DATA) {
        unsigned char *key;
        if ((fetched != 0 && fetched != MYSQL_DATA_TRUNCATED) || is_null)
            goto done;
        key = fetch_column_blob(statement, 0, length);
        if (key == NULL || visitor(key, (size_t)length, context) != 0) {
            free(key);
            goto done;
        }
        free(key);
    }
    status = 0;

done:
    if (statement != NULL) mysql_stmt_close(statement);
    return status;
}

int mysql_store_bootstrap_begin(mysql_store_t *store)
{
    static const char count_sql[] =
        "SELECT (SELECT COUNT(*) FROM kv_keys)+"
        "(SELECT COUNT(*) FROM kv_writer_state)+"
        "(SELECT COUNT(*) FROM kv_change_log)";
    static const char state_sql[] =
        "SELECT bootstrap_state FROM kv_schema_meta WHERE singleton_id=1";
    static const char update_sql[] =
        "UPDATE kv_schema_meta SET bootstrap_state='IN_PROGRESS',"
        "active_writer_uuid=? WHERE singleton_id=1";
    MYSQL *connection;
    MYSQL_RES *result = NULL;
    MYSQL_ROW row;
    MYSQL_STMT *statement = NULL;
    MYSQL_BIND parameter;
    unsigned long uuid_length;
    int retrying = 0;

    if (store == NULL) { errno = EINVAL; return -1; }
    connection = startup_connection(store);
    if (connection == NULL) return -1;
    if (mysql_real_query(connection, state_sql, sizeof(state_sql) - 1U) != 0 ||
        (result = mysql_store_result(connection)) == NULL ||
        (row = mysql_fetch_row(result)) == NULL || row[0] == NULL) goto fail;
    retrying = strcmp(row[0], "IN_PROGRESS") == 0;
    if (!retrying && strcmp(row[0], "EMPTY") != 0) goto fail;
    mysql_free_result(result);
    result = NULL;
    if (mysql_real_query(connection, count_sql, sizeof(count_sql) - 1U) != 0 ||
        (result = mysql_store_result(connection)) == NULL ||
        (row = mysql_fetch_row(result)) == NULL || row[0] == NULL) goto fail;
    if (!retrying && strtoull(row[0], NULL, 10) != 0) {
        errno = EEXIST;
        goto fail;
    }
    mysql_free_result(result);
    result = NULL;
    if (mysql_autocommit(connection, 0) != 0 ||
        mysql_query(connection, "DELETE FROM kv_change_log") != 0 ||
        mysql_query(connection, "DELETE FROM kv_writer_state") != 0 ||
        mysql_query(connection, "DELETE FROM kv_keys") != 0) goto rollback;
    statement = mysql_stmt_init(connection);
    if (statement == NULL ||
        mysql_stmt_prepare(statement, update_sql, sizeof(update_sql) - 1U) != 0 ||
        bind_blob_value(&parameter, &uuid_length,
                        store->config.writer_uuid, 16U) != 0 ||
        mysql_stmt_bind_param(statement, &parameter) != 0 ||
        mysql_stmt_execute(statement) != 0 || mysql_commit(connection) != 0)
        goto rollback;
    mysql_stmt_close(statement);
    (void)mysql_autocommit(connection, 1);
    return 0;

rollback:
    (void)mysql_rollback(connection);
    (void)mysql_autocommit(connection, 1);
fail:
    if (statement != NULL) mysql_stmt_close(statement);
    if (result != NULL) mysql_free_result(result);
    if (errno == 0) errno = EPROTO;
    return -1;
}

typedef struct bootstrap_member_context {
    MYSQL *connection;
    mysql_store_argument_t key;
    int failed;
} bootstrap_member_context_t;

static int bootstrap_hash_member(const void *field, size_t field_length,
                                 const void *value, size_t value_length,
                                 void *context)
{
    bootstrap_member_context_t *member = context;
    static const unsigned char command[] = "HSET";
    mysql_store_argument_t arguments[4] = {
        {command, sizeof(command) - 1U}, member->key,
        {(const unsigned char *)field, field_length},
        {(const unsigned char *)value, value_length}
    };
    if (apply_hset(member->connection, arguments, 4U) != 0) {
        member->failed = 1;
        return -1;
    }
    return 0;
}

static int bootstrap_zset_member(const void *value, size_t value_length,
                                 double score, void *context)
{
    bootstrap_member_context_t *member = context;
    static const unsigned char command[] = "ZADD";
    char score_text[64];
    int written = snprintf(score_text, sizeof(score_text), "%.17g", score);
    mysql_store_argument_t arguments[4];

    if (written < 0 || (size_t)written >= sizeof(score_text)) return -1;
    arguments[0].data = command;
    arguments[0].length = sizeof(command) - 1U;
    arguments[1] = member->key;
    arguments[2].data = (const unsigned char *)score_text;
    arguments[2].length = (size_t)written;
    arguments[3].data = value;
    arguments[3].length = value_length;
    if (apply_zadd(member->connection, arguments, 4U) != 0) {
        member->failed = 1;
        return -1;
    }
    return 0;
}

int mysql_store_bootstrap_object(mysql_store_t *store,
                                 const void *key,
                                 size_t key_length,
                                 kv_object_t *object,
                                 uint64_t expire_at_ms)
{
    MYSQL *connection;
    mysql_store_argument_t key_argument = {key, key_length};
    bootstrap_member_context_t member;
    int result = -1;

    if (store == NULL || (key == NULL && key_length != 0) || object == NULL) {
        errno = EINVAL;
        return -1;
    }
    connection = startup_connection(store);
    if (connection == NULL || mysql_autocommit(connection, 0) != 0) return -1;
    if (kv_object_type(object) == KV_OBJECT_STRING) {
        static const unsigned char command[] = "SET";
        static const unsigned char option[] = "PXAT";
        size_t value_length;
        const void *value = kv_object_string_value(object, &value_length);
        char deadline[32];
        mysql_store_argument_t arguments[5];
        size_t count = 3U;

        arguments[0].data = command;
        arguments[0].length = sizeof(command) - 1U;
        arguments[1] = key_argument;
        arguments[2].data = value;
        arguments[2].length = value_length;
        if (expire_at_ms != 0) {
            int written = snprintf(deadline, sizeof(deadline), "%" PRIu64,
                                   expire_at_ms);
            if (written < 0 || (size_t)written >= sizeof(deadline)) goto done;
            arguments[3].data = option;
            arguments[3].length = sizeof(option) - 1U;
            arguments[4].data = (const unsigned char *)deadline;
            arguments[4].length = (size_t)written;
            count = 5U;
        }
        result = apply_set(connection, arguments, count);
    } else {
        member.connection = connection;
        member.key = key_argument;
        member.failed = 0;
        if (insert_typed_key(connection, &key_argument,
                             kv_object_type(object)) != 0) goto done;
        if (kv_object_type(object) == KV_OBJECT_HASH) {
            result = kv_object_hash_visit(object, bootstrap_hash_member, &member);
        } else {
            size_t visited = 0;
            result = kv_object_zset_range(object, 0, -1,
                                          bootstrap_zset_member, &member,
                                          &visited);
        }
        if (result == 0 && !member.failed && expire_at_ms != 0) {
            static const unsigned char command[] = "PEXPIREAT";
            char deadline[32];
            int written = snprintf(deadline, sizeof(deadline), "%" PRIu64,
                                   expire_at_ms);
            mysql_store_argument_t arguments[3];
            if (written < 0 || (size_t)written >= sizeof(deadline)) goto done;
            arguments[0].data = command;
            arguments[0].length = sizeof(command) - 1U;
            arguments[1] = key_argument;
            arguments[2].data = (const unsigned char *)deadline;
            arguments[2].length = (size_t)written;
            result = apply_expire(connection, arguments, 0);
        }
    }

done:
    if (result == 0) result = mysql_commit(connection);
    if (result != 0) (void)mysql_rollback(connection);
    (void)mysql_autocommit(connection, 1);
    return result;
}

int mysql_store_bootstrap_finish(mysql_store_t *store,
                                 uint64_t applied_sequence)
{
    static const char state_sql[] =
        "INSERT INTO kv_writer_state(writer_uuid,applied_sequence) VALUES(?,?) "
        "ON DUPLICATE KEY UPDATE applied_sequence=VALUES(applied_sequence)";
    static const char meta_sql[] =
        "UPDATE kv_schema_meta SET bootstrap_state='READY',active_writer_uuid=? "
        "WHERE singleton_id=1";
    MYSQL *connection;
    MYSQL_BIND state[2];
    MYSQL_BIND meta;
    unsigned long lengths[2];
    unsigned long long sequence = applied_sequence;

    if (store == NULL) { errno = EINVAL; return -1; }
    connection = startup_connection(store);
    if (connection == NULL || mysql_autocommit(connection, 0) != 0) return -1;
    if (bind_blob_value(&state[0], &lengths[0],
                        store->config.writer_uuid, 16U) != 0) goto fail;
    memset(&state[1], 0, sizeof(state[1]));
    state[1].buffer_type = MYSQL_TYPE_LONGLONG;
    state[1].buffer = &sequence;
    state[1].is_unsigned = 1;
    if (execute_bound(connection, state_sql, state) != 0 ||
        bind_blob_value(&meta, &lengths[1],
                        store->config.writer_uuid, 16U) != 0 ||
        execute_bound(connection, meta_sql, &meta) != 0 ||
        mysql_commit(connection) != 0) goto fail;
    (void)mysql_autocommit(connection, 1);
    pthread_mutex_lock(&store->mutex);
    store->stats.applied_sequence = applied_sequence;
    store->stats.submitted_sequence = applied_sequence;
    pthread_mutex_unlock(&store->mutex);
    return 0;

fail:
    (void)mysql_rollback(connection);
    (void)mysql_autocommit(connection, 1);
    return -1;
}

int mysql_store_set_notify_fd(mysql_store_t *store, int notify_fd)
{
    if (store == NULL || notify_fd < 0) {
        errno = EINVAL;
        return -1;
    }
    pthread_mutex_lock(&store->mutex);
    store->config.notify_fd = notify_fd;
    pthread_mutex_unlock(&store->mutex);
    return 0;
}

int mysql_store_submit_load(mysql_store_t *store,
                            const void *key,
                            size_t key_length,
                            void *waiter_context)
{
    mysql_store_waiter_t *waiter;
    mysql_load_job_t *job;

    if (store == NULL || (key == NULL && key_length != 0)) {
        errno = EINVAL;
        return -1;
    }
    waiter = malloc(sizeof(*waiter));
    if (waiter == NULL) return -1;
    waiter->context = waiter_context;
    pthread_mutex_lock(&store->mutex);
    for (job = store->inflight; job != NULL; job = job->inflight_next) {
        if (job->key_length == key_length &&
            memcmp(job->key, key, key_length) == 0) {
            waiter->next = job->waiters;
            job->waiters = waiter;
            store->stats.coalesced_loads++;
            pthread_mutex_unlock(&store->mutex);
            return 0;
        }
    }
    if (store->stopping || store->pending_reads >= store->config.read_queue_limit) {
        pthread_mutex_unlock(&store->mutex);
        free(waiter);
        errno = EAGAIN;
        return -1;
    }
    job = calloc(1, sizeof(*job));
    if (job != NULL) job->key = malloc(key_length == 0 ? 1U : key_length);
    if (job == NULL || job->key == NULL) {
        pthread_mutex_unlock(&store->mutex);
        free(job);
        free(waiter);
        return -1;
    }
    memcpy(job->key, key, key_length);
    job->key_length = key_length;
    waiter->next = NULL;
    job->waiters = waiter;
    job->inflight_next = store->inflight;
    store->inflight = job;
    if (store->queue_tail == NULL) store->queue_head = job;
    else store->queue_tail->queue_next = job;
    store->queue_tail = job;
    store->pending_reads++;
    pthread_cond_signal(&store->read_condition);
    pthread_mutex_unlock(&store->mutex);
    return 1;
}

int mysql_store_can_accept_write(mysql_store_t *store, size_t encoded_bytes)
{
    int accepted;

    if (store == NULL || encoded_bytes == 0) return 0;
    pthread_mutex_lock(&store->mutex);
    accepted = !store->stopping &&
               encoded_bytes <= store->config.write_queue_max_bytes &&
               store->pending_write_bytes <=
                   store->config.write_queue_max_bytes - encoded_bytes;
    pthread_mutex_unlock(&store->mutex);
    return accepted;
}

int mysql_store_submit_mutation(mysql_store_t *store,
                                uint64_t sequence,
                                const mysql_store_argument_t *arguments,
                                size_t argument_count,
                                size_t encoded_bytes)
{
    mysql_write_job_t *job;
    size_t payload_bytes = 0;
    size_t offset = 0;

    if (store == NULL || sequence == 0 || arguments == NULL ||
        argument_count < 2U || encoded_bytes == 0) {
        errno = EINVAL;
        return -1;
    }
    for (size_t index = 0; index < argument_count; ++index) {
        if (arguments[index].length > SIZE_MAX - payload_bytes) return -1;
        payload_bytes += arguments[index].length;
    }
    job = calloc(1, sizeof(*job));
    if (job != NULL) {
        job->arguments = calloc(argument_count, sizeof(*job->arguments));
        job->payload = malloc(payload_bytes == 0 ? 1U : payload_bytes);
    }
    if (job == NULL || job->arguments == NULL || job->payload == NULL) {
        free_write_job(job);
        return -1;
    }
    for (size_t index = 0; index < argument_count; ++index) {
        job->arguments[index].data = job->payload + offset;
        job->arguments[index].length = arguments[index].length;
        memcpy(job->payload + offset, arguments[index].data,
               arguments[index].length);
        offset += arguments[index].length;
    }
    job->sequence = sequence;
    job->argument_count = argument_count;
    job->encoded_bytes = encoded_bytes;
    pthread_mutex_lock(&store->mutex);
    if (store->stopping || encoded_bytes > store->config.write_queue_max_bytes ||
        store->pending_write_bytes >
            store->config.write_queue_max_bytes - encoded_bytes) {
        pthread_mutex_unlock(&store->mutex);
        free_write_job(job);
        errno = EAGAIN;
        return -1;
    }
    if (store->write_tail == NULL) store->write_head = job;
    else store->write_tail->next = job;
    store->write_tail = job;
    store->pending_write_bytes += encoded_bytes;
    if (sequence > store->stats.submitted_sequence)
        store->stats.submitted_sequence = sequence;
    pthread_cond_signal(&store->write_condition);
    pthread_mutex_unlock(&store->mutex);
    return 0;
}

mysql_store_load_result_t *mysql_store_take_load_results(mysql_store_t *store)
{
    mysql_store_load_result_t *results;

    if (store == NULL) return NULL;
    pthread_mutex_lock(&store->mutex);
    results = store->completed_head;
    store->completed_head = NULL;
    store->completed_tail = NULL;
    pthread_mutex_unlock(&store->mutex);
    return results;
}

void mysql_store_release_load_result(mysql_store_load_result_t *result)
{
    while (result != NULL) {
        mysql_store_load_result_t *next_result = result->next;
        mysql_store_waiter_t *waiter = result->waiters;

        kv_object_destroy(result->object);
        while (waiter != NULL) {
            mysql_store_waiter_t *next_waiter = waiter->next;
            free(waiter);
            waiter = next_waiter;
        }
        free(result);
        result = next_result;
    }
}

void mysql_store_get_stats(mysql_store_t *store, mysql_store_stats_t *stats)
{
    if (stats == NULL) return;
    memset(stats, 0, sizeof(*stats));
    if (store == NULL) return;
    pthread_mutex_lock(&store->mutex);
    *stats = store->stats;
    stats->connected_readers = store->connected_readers;
    stats->pending_reads = store->pending_reads;
    stats->pending_write_bytes = store->pending_write_bytes;
    pthread_mutex_unlock(&store->mutex);
}

void mysql_store_note_negative_hit(mysql_store_t *store)
{
    if (store == NULL) return;
    pthread_mutex_lock(&store->mutex);
    store->stats.negative_cache_hits++;
    pthread_mutex_unlock(&store->mutex);
}

int mysql_store_close(mysql_store_t *store, uint64_t drain_timeout_ms)
{
    size_t index;
    mysql_store_load_result_t *results;

    if (store == NULL) return 0;
    pthread_mutex_lock(&store->mutex);
    store->stopping = 1;
    store->shutdown_deadline_ms = realtime_ms();
    if (drain_timeout_ms <= UINT64_MAX - store->shutdown_deadline_ms)
        store->shutdown_deadline_ms += drain_timeout_ms;
    else
        store->shutdown_deadline_ms = UINT64_MAX;
    pthread_cond_broadcast(&store->read_condition);
    pthread_cond_broadcast(&store->write_condition);
    pthread_mutex_unlock(&store->mutex);
    for (index = 0; index < store->config.read_workers; ++index) {
        if (store->readers[index].started)
            pthread_join(store->readers[index].thread, NULL);
    }
    if (store->writer.started) pthread_join(store->writer.thread, NULL);
    if (store->startup_connection != NULL) {
        mysql_close(store->startup_connection);
        store->startup_connection = NULL;
    }
    results = mysql_store_take_load_results(store);
    for (mysql_store_load_result_t *result = results;
         result != NULL; result = result->next) {
        for (mysql_store_waiter_t *waiter = result->waiters;
             waiter != NULL; waiter = waiter->next) {
            if (store->config.destroy_waiter_context != NULL)
                store->config.destroy_waiter_context(waiter->context);
        }
    }
    mysql_store_release_load_result(results);
    while (store->queue_head != NULL) {
        mysql_load_job_t *job = store->queue_head;
        mysql_store_waiter_t *waiter = job->waiters;
        store->queue_head = job->queue_next;
        while (waiter != NULL) {
            mysql_store_waiter_t *next = waiter->next;
            if (store->config.destroy_waiter_context != NULL)
                store->config.destroy_waiter_context(waiter->context);
            free(waiter);
            waiter = next;
        }
        free(job->key);
        free(job);
    }
    while (store->write_head != NULL) {
        mysql_write_job_t *job = store->write_head;
        store->write_head = job->next;
        free_write_job(job);
    }
    pthread_cond_destroy(&store->write_condition);
    pthread_cond_destroy(&store->read_condition);
    pthread_mutex_destroy(&store->mutex);
    free(store->readers);
    free(store->host);
    free(store->user);
    free(store->password);
    free(store->database);
    free(store);
    return 0;
}

#else

struct mysql_store { int unavailable; };

int mysql_store_library_init(void) { errno = ENOTSUP; return -1; }
void mysql_store_library_end(void) { }
int mysql_store_open(mysql_store_t **out_store,
                     const mysql_store_config_t *config)
{
    (void)config;
    if (out_store != NULL) *out_store = NULL;
    errno = ENOTSUP;
    return -1;
}
int mysql_store_check_schema(mysql_store_t *store)
{ (void)store; errno = ENOTSUP; return -1; }
int mysql_store_check_permissions(mysql_store_t *store)
{ (void)store; errno = ENOTSUP; return -1; }
int mysql_store_get_startup_state(mysql_store_t *store,
                                  mysql_store_bootstrap_state_t *state,
                                  unsigned char active_writer_uuid[16],
                                  int *has_active_writer,
                                  uint64_t *applied_sequence)
{
    (void)store; (void)state; (void)active_writer_uuid;
    (void)has_active_writer; (void)applied_sequence;
    errno = ENOTSUP; return -1;
}
int mysql_store_set_writer_uuid(mysql_store_t *store,
                                const unsigned char writer_uuid[16])
{ (void)store; (void)writer_uuid; errno = ENOTSUP; return -1; }
int mysql_store_recover_mutation(mysql_store_t *store, uint64_t sequence,
                                 const mysql_store_argument_t *arguments,
                                 size_t argument_count)
{
    (void)store; (void)sequence; (void)arguments; (void)argument_count;
    errno = ENOTSUP; return -1;
}
int mysql_store_visit_changes_after(mysql_store_t *store, uint64_t sequence,
                                    mysql_store_change_visitor_fn visitor,
                                    void *context)
{
    (void)store; (void)sequence; (void)visitor; (void)context;
    errno = ENOTSUP; return -1;
}
int mysql_store_bootstrap_begin(mysql_store_t *store)
{ (void)store; errno = ENOTSUP; return -1; }
int mysql_store_bootstrap_object(mysql_store_t *store, const void *key,
                                 size_t key_length, kv_object_t *object,
                                 uint64_t expire_at_ms)
{
    (void)store; (void)key; (void)key_length; (void)object; (void)expire_at_ms;
    errno = ENOTSUP; return -1;
}
int mysql_store_bootstrap_finish(mysql_store_t *store,
                                 uint64_t applied_sequence)
{ (void)store; (void)applied_sequence; errno = ENOTSUP; return -1; }
int mysql_store_set_notify_fd(mysql_store_t *store, int notify_fd)
{ (void)store; (void)notify_fd; errno = ENOTSUP; return -1; }
int mysql_store_submit_load(mysql_store_t *store, const void *key,
                            size_t key_length, void *waiter_context)
{
    (void)store; (void)key; (void)key_length; (void)waiter_context;
    errno = ENOTSUP;
    return -1;
}
int mysql_store_can_accept_write(mysql_store_t *store, size_t encoded_bytes)
{ (void)store; (void)encoded_bytes; return 0; }
int mysql_store_submit_mutation(mysql_store_t *store, uint64_t sequence,
                                const mysql_store_argument_t *arguments,
                                size_t argument_count, size_t encoded_bytes)
{
    (void)store; (void)sequence; (void)arguments;
    (void)argument_count; (void)encoded_bytes;
    errno = ENOTSUP;
    return -1;
}
mysql_store_load_result_t *mysql_store_take_load_results(mysql_store_t *store)
{ (void)store; return NULL; }
void mysql_store_release_load_result(mysql_store_load_result_t *result)
{ (void)result; }
void mysql_store_get_stats(mysql_store_t *store, mysql_store_stats_t *stats)
{ (void)store; if (stats != NULL) memset(stats, 0, sizeof(*stats)); }
void mysql_store_note_negative_hit(mysql_store_t *store) { (void)store; }
int mysql_store_close(mysql_store_t *store, uint64_t drain_timeout_ms)
{ (void)store; (void)drain_timeout_ms; return 0; }

#endif
