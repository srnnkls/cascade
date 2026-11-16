#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::Path;

use cascade_engine::db::StateDb;
use cascade_engine::types::ItemId;
use rusqlite::Connection;

/// The complete schema a v8 daemon wrote, with one row in every table.
const V8_DATABASE: &str = "
    CREATE TABLE files (
        id TEXT PRIMARY KEY, backend_id TEXT NOT NULL, path TEXT UNIQUE NOT NULL,
        parent_id TEXT, name TEXT NOT NULL, is_dir BOOLEAN NOT NULL, size INTEGER,
        mime_type TEXT, mod_time INTEGER, remote_hash TEXT, local_hash TEXT,
        cache_state TEXT NOT NULL DEFAULT 'online', provenance TEXT NOT NULL DEFAULT 'cloud',
        disk_path TEXT, local_path TEXT, cached_at INTEGER, last_access INTEGER,
        dirty BOOLEAN NOT NULL DEFAULT FALSE, synced_at INTEGER,
        FOREIGN KEY (backend_id) REFERENCES backends(id)
    );
    CREATE TABLE backends (
        id TEXT PRIMARY KEY, backend_type TEXT NOT NULL, display_name TEXT NOT NULL,
        mount_path TEXT, config TEXT
    );
    CREATE TABLE pin_rules (
        id INTEGER PRIMARY KEY, path_glob TEXT NOT NULL,
        recursive BOOLEAN NOT NULL DEFAULT TRUE, conditions TEXT
    );
    CREATE UNIQUE INDEX idx_pin_rules_path ON pin_rules(path_glob);
    CREATE TABLE lifecycle_policies (
        id INTEGER PRIMARY KEY, path_glob TEXT NOT NULL, max_age INTEGER,
        max_file_size INTEGER, priority INTEGER NOT NULL DEFAULT 0, conditions TEXT
    );
    CREATE TABLE config_cache (dir_path TEXT PRIMARY KEY, modified_at INTEGER, config TEXT NOT NULL);
    CREATE TABLE sync_cursors (backend_id TEXT PRIMARY KEY, cursor TEXT NOT NULL);
    CREATE TABLE p2p_peers (
        device_id TEXT PRIMARY KEY, name TEXT, addresses TEXT, last_seen INTEGER,
        online BOOLEAN NOT NULL DEFAULT FALSE
    );
    CREATE TABLE p2p_block_index (
        file_id TEXT NOT NULL, block_index INTEGER NOT NULL, block_hash BLOB NOT NULL,
        PRIMARY KEY (file_id, block_index)
    );
    CREATE TABLE grants (
        id INTEGER PRIMARY KEY, grantee TEXT NOT NULL, capability TEXT NOT NULL,
        scope_kind TEXT NOT NULL, scope_path TEXT, granted_by TEXT NOT NULL, expires INTEGER
    );
    CREATE TABLE manage_audit (
        id INTEGER PRIMARY KEY, timestamp INTEGER NOT NULL, actor_device TEXT NOT NULL,
        capability TEXT NOT NULL, scope_kind TEXT NOT NULL, scope_path TEXT,
        command TEXT NOT NULL, outcome TEXT NOT NULL
    );
    CREATE TABLE capability_tokens (
        token_id TEXT PRIMARY KEY, issuer TEXT NOT NULL, bearer TEXT NOT NULL,
        capability TEXT NOT NULL, scope_kind TEXT NOT NULL, scope_path TEXT,
        expires INTEGER NOT NULL, issued_at INTEGER NOT NULL, token_json TEXT NOT NULL
    );
    CREATE TABLE token_revocations (token_id TEXT PRIMARY KEY, revoked_at INTEGER NOT NULL);
    CREATE TABLE data_receive_quarantine (
        folder_id TEXT NOT NULL, peer_device TEXT NOT NULL, path TEXT NOT NULL,
        file_json TEXT NOT NULL, observed_at INTEGER NOT NULL,
        PRIMARY KEY (folder_id, peer_device, path)
    );
    CREATE TABLE data_explicit_control (
        peer_device TEXT NOT NULL, folder_id TEXT NOT NULL, data_read BOOLEAN NOT NULL,
        data_write BOOLEAN NOT NULL, observed_at INTEGER NOT NULL,
        PRIMARY KEY (peer_device, folder_id)
    );
    CREATE TABLE auth_codes (
        code TEXT PRIMARY KEY, kind TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
        token_id TEXT, token_json TEXT, created_at TEXT NOT NULL, expires_at TEXT NOT NULL
    );
    CREATE TABLE daemon_secret (
        id INTEGER PRIMARY KEY CHECK (id = 1), secret TEXT NOT NULL, created_at TEXT NOT NULL
    );
    CREATE TABLE max_file_length_rules (
        id INTEGER PRIMARY KEY, path_glob TEXT NOT NULL, max_bytes INTEGER NOT NULL,
        priority INTEGER NOT NULL DEFAULT 0, conditions TEXT
    );
    CREATE TABLE exec_sessions (
        id INTEGER PRIMARY KEY, kind TEXT NOT NULL, owner_device TEXT NOT NULL,
        scope_kind TEXT NOT NULL, scope_path TEXT, command TEXT NOT NULL,
        started_at INTEGER NOT NULL, ended_at INTEGER, exit_code INTEGER, exit_signal INTEGER
    );
    CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
    INSERT INTO schema_meta VALUES ('version', '8');

    INSERT INTO backends VALUES ('nas', 'smb', 'NAS', 'nas', NULL);
    INSERT INTO files (id, backend_id, path, name, is_dir, size)
        VALUES ('nas:42', 'nas', 'nas/Documents/report.pdf', 'report.pdf', FALSE, 1234);
    INSERT INTO pin_rules (path_glob) VALUES ('nas/Documents/**');
    INSERT INTO lifecycle_policies (path_glob, max_age) VALUES ('nas/Archive/**', 86400);
    INSERT INTO config_cache VALUES ('nas', 1, '{}');
    INSERT INTO sync_cursors VALUES ('nas', 'cursor-1');
    INSERT INTO p2p_peers (device_id, name) VALUES ('PEER', 'laptop');
    INSERT INTO p2p_block_index VALUES ('nas:42', 0, X'00');
    INSERT INTO grants (grantee, capability, scope_kind, granted_by) VALUES ('PEER', 'status:read', 'node', 'SELF');
    INSERT INTO manage_audit (timestamp, actor_device, capability, scope_kind, command, outcome)
        VALUES (1, 'PEER', 'status:read', 'node', 'status', 'ok');
    INSERT INTO capability_tokens VALUES ('t1', 'SELF', 'PEER', 'status:read', 'node', NULL, 9, 1, '{}');
    INSERT INTO token_revocations VALUES ('t0', 1);
    INSERT INTO data_receive_quarantine VALUES ('f', 'PEER', 'a.txt', '{}', 1);
    INSERT INTO data_explicit_control VALUES ('PEER', 'f', TRUE, FALSE, 1);
    INSERT INTO auth_codes (code, kind, created_at, expires_at) VALUES ('c', 'pairing', 'now', 'later');
    INSERT INTO daemon_secret VALUES (1, 's', 'now');
    INSERT INTO max_file_length_rules (path_glob, max_bytes) VALUES ('**', 10);
    INSERT INTO exec_sessions (kind, owner_device, scope_kind, command, started_at)
        VALUES ('pty', 'PEER', 'node', 'sh', 1);
";

const V8_TABLES: [&str; 19] = [
    "files",
    "backends",
    "pin_rules",
    "lifecycle_policies",
    "config_cache",
    "sync_cursors",
    "p2p_peers",
    "p2p_block_index",
    "grants",
    "manage_audit",
    "capability_tokens",
    "token_revocations",
    "data_receive_quarantine",
    "data_explicit_control",
    "auth_codes",
    "daemon_secret",
    "max_file_length_rules",
    "exec_sessions",
    "schema_meta",
];

fn columns(conn: &Connection, table: &str) -> Vec<String> {
    let mut statement = conn
        .prepare(&format!("SELECT name FROM pragma_table_info('{table}')"))
        .unwrap();
    statement
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

fn assert_has_columns(conn: &Connection, table: &str, required: &[&str]) {
    let present = columns(conn, table);
    for column in required {
        assert!(
            present.iter().any(|name| name == column),
            "table {table} lacks column {column}; has {present:?}"
        );
    }
}

fn write_v8_database(path: &Path) {
    let conn = Connection::open(path).unwrap();
    conn.execute_batch(V8_DATABASE).unwrap();
}

#[test]
fn v8_database_migrates_to_v9_with_existing_rows_intact() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.db");
    write_v8_database(&path);

    let db = StateDb::open(&path).unwrap();
    assert_eq!(
        db.get_file_path(&ItemId::new("nas", "42"))
            .unwrap()
            .as_deref(),
        Some("nas/Documents/report.pdf")
    );
    assert_eq!(
        db.get_backend_mount("nas").unwrap(),
        Some(Some("nas".to_owned()))
    );
    drop(db);

    let conn = Connection::open(&path).unwrap();
    let version: String = conn
        .query_row(
            "SELECT value FROM schema_meta WHERE key = 'version'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, "9");

    for table in V8_TABLES {
        let rows: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(rows, 1, "rows of v8 table {table} after migration");
    }

    assert_has_columns(
        &conn,
        "replica_items",
        &[
            "uid",
            "backend_id",
            "stable_id",
            "native_id",
            "parent_uid",
            "name",
            "kind",
            "size",
            "btime",
            "mtime",
            "ctime",
            "content_version",
            "metadata_version",
            "uploaded",
            "listed_at",
            "pinned",
            "revision",
        ],
    );
    assert_has_columns(
        &conn,
        "replica_tombstones",
        &["uid", "parent_uid", "revision"],
    );
    assert_has_columns(&conn, "replica_meta", &["store_id", "revision", "floor"]);
    assert_has_columns(
        &conn,
        "content_objects",
        &[
            "object_id",
            "uid",
            "content_version",
            "size",
            "state",
            "dirty",
            "upload_temp_name",
            "upload_file_id",
            "last_access",
        ],
    );
    assert_has_columns(&conn, "content_ranges", &["object_id", "start", "end"]);
    assert_has_columns(&conn, "materialized_items", &["uid"]);
}
