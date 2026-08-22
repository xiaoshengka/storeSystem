-- storeSystem v0.7.0 MySQL Cache-Aside schema.
-- Run this file with a database account that may create tables, then run the
-- service with a narrower SELECT/INSERT/UPDATE/DELETE account.

CREATE TABLE IF NOT EXISTS kv_schema_meta (
    singleton_id TINYINT UNSIGNED NOT NULL PRIMARY KEY,
    schema_version INT UNSIGNED NOT NULL,
    bootstrap_state VARCHAR(16) NOT NULL,
    active_writer_uuid BINARY(16) NULL,
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),
    CONSTRAINT chk_kv_schema_singleton CHECK (singleton_id = 1),
    CONSTRAINT chk_kv_bootstrap_state
        CHECK (bootstrap_state IN ('EMPTY', 'IN_PROGRESS', 'READY'))
) ENGINE=InnoDB;

INSERT INTO kv_schema_meta
    (singleton_id, schema_version, bootstrap_state, active_writer_uuid)
VALUES (1, 7, 'EMPTY', NULL)
ON DUPLICATE KEY UPDATE schema_version = schema_version;

CREATE TABLE IF NOT EXISTS kv_keys (
    key_hash BINARY(32) NOT NULL PRIMARY KEY,
    key_data MEDIUMBLOB NOT NULL,
    object_type TINYINT UNSIGNED NOT NULL,
    expire_at_ms BIGINT UNSIGNED NULL,
    object_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),
    CONSTRAINT chk_kv_object_type CHECK (object_type IN (0, 1, 2))
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS kv_strings (
    key_hash BINARY(32) NOT NULL PRIMARY KEY,
    value_data MEDIUMBLOB NOT NULL,
    CONSTRAINT fk_kv_strings_key FOREIGN KEY (key_hash)
        REFERENCES kv_keys(key_hash) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS kv_hash_fields (
    key_hash BINARY(32) NOT NULL,
    field_hash BINARY(32) NOT NULL,
    field_data MEDIUMBLOB NOT NULL,
    value_data MEDIUMBLOB NOT NULL,
    PRIMARY KEY (key_hash, field_hash),
    CONSTRAINT fk_kv_hash_fields_key FOREIGN KEY (key_hash)
        REFERENCES kv_keys(key_hash) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS kv_zset_members (
    key_hash BINARY(32) NOT NULL,
    member_hash BINARY(32) NOT NULL,
    member_data MEDIUMBLOB NOT NULL,
    score DOUBLE NOT NULL,
    PRIMARY KEY (key_hash, member_hash),
    KEY idx_kv_zset_score (key_hash, score, member_hash),
    CONSTRAINT fk_kv_zset_members_key FOREIGN KEY (key_hash)
        REFERENCES kv_keys(key_hash) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS kv_writer_state (
    writer_uuid BINARY(16) NOT NULL PRIMARY KEY,
    applied_sequence BIGINT UNSIGNED NOT NULL,
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS kv_change_log (
    change_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    writer_uuid BINARY(16) NOT NULL,
    mutation_sequence BIGINT UNSIGNED NOT NULL,
    key_hash BINARY(32) NOT NULL,
    key_data MEDIUMBLOB NOT NULL,
    operation_type TINYINT UNSIGNED NOT NULL,
    committed_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_kv_change_mutation (writer_uuid, mutation_sequence),
    KEY idx_kv_change_key (key_hash, change_id)
) ENGINE=InnoDB;
