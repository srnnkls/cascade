use anyhow::Result;
use rusqlite::Connection;

/// Schema version tracker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SchemaVersion(pub i32);

impl SchemaVersion {
    /// Current schema version.
    #[must_use]
    pub const fn current() -> Self {
        Self(9)
    }
}

/// Run all migrations from `from` to `to`.
pub fn migrate(conn: &Connection, from: SchemaVersion, _to: SchemaVersion) -> Result<()> {
    if from < SchemaVersion(1) {
        v1_init(conn)?;
    }
    if from < SchemaVersion(2) {
        v2_manage_plane(conn)?;
    }
    if from < SchemaVersion(3) {
        v3_capability_tokens(conn)?;
    }
    if from < SchemaVersion(4) {
        v4_data_receive_quarantine(conn)?;
    }
    if from < SchemaVersion(5) {
        v5_data_explicit_control(conn)?;
    }
    if from < SchemaVersion(6) {
        v6_auth_codes(conn)?;
    }
    if from < SchemaVersion(7) {
        v7_max_file_length_rules(conn)?;
    }
    if from < SchemaVersion(8) {
        v8_exec_sessions(conn)?;
    }
    if from < SchemaVersion(9) {
        v9_replica_and_content(conn)?;
    }

    Ok(())
}

/// Initial schema — all tables for Phase 1.
fn v1_init(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS files (
            id            TEXT PRIMARY KEY,
            backend_id    TEXT NOT NULL,
            path          TEXT UNIQUE NOT NULL,
            parent_id     TEXT,
            name          TEXT NOT NULL,
            is_dir        BOOLEAN NOT NULL,
            size          INTEGER,
            mime_type     TEXT,
            mod_time      INTEGER,
            remote_hash   TEXT,
            local_hash    TEXT,

            cache_state   TEXT NOT NULL DEFAULT 'online',
            provenance    TEXT NOT NULL DEFAULT 'cloud',
            disk_path     TEXT,
            local_path    TEXT,
            cached_at     INTEGER,
            last_access   INTEGER,
            dirty         BOOLEAN NOT NULL DEFAULT FALSE,
            synced_at     INTEGER,

            FOREIGN KEY (backend_id) REFERENCES backends(id)
        );

        CREATE INDEX IF NOT EXISTS idx_files_path ON files(path);
        CREATE INDEX IF NOT EXISTS idx_files_backend ON files(backend_id);
        CREATE INDEX IF NOT EXISTS idx_files_cache_state ON files(cache_state);
        CREATE INDEX IF NOT EXISTS idx_files_last_access ON files(last_access);

        CREATE TABLE IF NOT EXISTS backends (
            id            TEXT PRIMARY KEY,
            backend_type  TEXT NOT NULL,
            display_name  TEXT NOT NULL,
            mount_path    TEXT,
            config        TEXT
        );

        CREATE TABLE IF NOT EXISTS pin_rules (
            id            INTEGER PRIMARY KEY,
            path_glob     TEXT NOT NULL,
            recursive     BOOLEAN NOT NULL DEFAULT TRUE,
            conditions    TEXT
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_pin_rules_path ON pin_rules(path_glob);

        CREATE TABLE IF NOT EXISTS lifecycle_policies (
            id            INTEGER PRIMARY KEY,
            path_glob     TEXT NOT NULL,
            max_age       INTEGER,
            max_file_size INTEGER,
            priority      INTEGER NOT NULL DEFAULT 0,
            conditions    TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_lifecycle_priority ON lifecycle_policies(priority DESC);

        CREATE TABLE IF NOT EXISTS config_cache (
            dir_path      TEXT PRIMARY KEY,
            modified_at   INTEGER,
            config        TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS sync_cursors (
            backend_id    TEXT PRIMARY KEY,
            cursor        TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS p2p_peers (
            device_id     TEXT PRIMARY KEY,
            name          TEXT,
            addresses     TEXT,
            last_seen     INTEGER,
            online        BOOLEAN NOT NULL DEFAULT FALSE
        );

        CREATE TABLE IF NOT EXISTS p2p_block_index (
            file_id       TEXT NOT NULL,
            block_index   INTEGER NOT NULL,
            block_hash    BLOB NOT NULL,
            PRIMARY KEY (file_id, block_index)
        );

        CREATE INDEX IF NOT EXISTS idx_block_hash ON p2p_block_index(block_hash);
        ",
    )?;

    Ok(())
}

/// Schema v2 — the node management plane.
///
/// Two tables back the capability model in [`crate::manage`]: `grants` holds
/// the capability grants resolved at authorisation time, and `manage_audit` is
/// an append-only log of every management command the node processed. The
/// audit table has no `UPDATE` or `DELETE` path in the typed API so a
/// compromised manager cannot erase its tracks.
fn v2_manage_plane(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS grants (
            id            INTEGER PRIMARY KEY,
            grantee       TEXT NOT NULL,
            capability    TEXT NOT NULL,
            scope_kind    TEXT NOT NULL,
            scope_path    TEXT,
            granted_by    TEXT NOT NULL,
            expires       INTEGER
        );

        CREATE INDEX IF NOT EXISTS idx_grants_grantee ON grants(grantee);

        CREATE TABLE IF NOT EXISTS manage_audit (
            id            INTEGER PRIMARY KEY,
            timestamp     INTEGER NOT NULL,
            actor_device  TEXT NOT NULL,
            capability    TEXT NOT NULL,
            scope_kind    TEXT NOT NULL,
            scope_path    TEXT,
            command       TEXT NOT NULL,
            outcome       TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_manage_audit_ts ON manage_audit(timestamp);
        ",
    )?;

    Ok(())
}

/// Schema v3 — signed capability tokens.
///
/// Two tables back the portable-grant model in [`crate::manage::token`].
/// `capability_tokens` records every token this node issued, so the owner can
/// list and reprint them; the full signed token is stored as JSON because a
/// token is a self-contained credential the bearer carries offline.
/// `token_revocations` is the append-only revocation list the verify path
/// consults — a token id appearing here is a hard rejection at verify time.
/// Neither table has a typed `DELETE` path: an issued token is a historical
/// fact and a revocation is permanent, so a compromised manager cannot un-issue
/// or un-revoke to cover its tracks. Issue and revoke events are additionally
/// recorded in the existing `manage_audit` log.
fn v3_capability_tokens(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS capability_tokens (
            token_id      TEXT PRIMARY KEY,
            issuer        TEXT NOT NULL,
            bearer        TEXT NOT NULL,
            capability    TEXT NOT NULL,
            scope_kind    TEXT NOT NULL,
            scope_path    TEXT,
            expires       INTEGER NOT NULL,
            issued_at     INTEGER NOT NULL,
            token_json    TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_capability_tokens_bearer ON capability_tokens(bearer);

        CREATE TABLE IF NOT EXISTS token_revocations (
            token_id      TEXT PRIMARY KEY,
            revoked_at    INTEGER NOT NULL
        );
        ",
    )?;

    Ok(())
}

/// Schema v4 — directional data-sharing receive-only quarantine.
///
/// `data_receive_quarantine` holds proposed index rows that arrived from a
/// peer whose `data:write` grant for the folder was absent or expired. Rather
/// than silently discarding the frame (which would hide what the peer is
/// trying to push) or merging it into the authoritative index (which would
/// violate the grant), rejected rows are parked here, keyed by
/// `(folder_id, peer_device, path)`.
///
/// A newer proposal for the same path replaces the older (INSERT OR REPLACE),
/// so the table is bounded by the number of distinct paths per peer per
/// folder — it does not grow without bound. The rows are surfaced to the
/// operator as "rejected local additions from `<peer>`". If the operator later
/// grants `data:write` for that peer, quarantined rows become eligible to
/// merge on the next index exchange (the peer re-sends; the node does not
/// auto-replay the quarantine, so a stale proposal cannot resurrect deleted
/// content). Rows may also be pruned explicitly by the operator.
fn v4_data_receive_quarantine(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS data_receive_quarantine (
            folder_id     TEXT NOT NULL,
            peer_device   TEXT NOT NULL,
            path          TEXT NOT NULL,
            file_json     TEXT NOT NULL,
            observed_at   INTEGER NOT NULL,
            PRIMARY KEY (folder_id, peer_device, path)
        );

        CREATE INDEX IF NOT EXISTS idx_quarantine_folder_peer
            ON data_receive_quarantine(folder_id, peer_device);
        ",
    )?;

    Ok(())
}

/// Schema v5 — explicit-control bit for directional data sharing.
///
/// `data_explicit_control` is the durable backing for the F2 invariant: a
/// peer who has *ever* presented a verified data-verb token for a folder is
/// in explicit-control mode for that folder, even after the token has been
/// revoked or has expired. The runtime data-plane gate consults the bit so
/// the absent direction stays denied; a token-only restriction cannot be
/// widened back to the trusted-peer default by revoking or letting the
/// token lapse.
///
/// The bit is set on the first successful token verify (folded in by the
/// `DataAuthority` impl) and persists across restarts, so a stale
/// restart cannot re-introduce the F2 widening. An explicit
/// `clear_data_explicit_control` row operation — the only path that
/// removes a row — exists for the operator to return the peer to the
/// trusted-peer default after they have removed the underlying grant.
fn v5_data_explicit_control(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS data_explicit_control (
            peer_device   TEXT NOT NULL,
            folder_id     TEXT NOT NULL,
            data_read     BOOLEAN NOT NULL,
            data_write    BOOLEAN NOT NULL,
            observed_at   INTEGER NOT NULL,
            PRIMARY KEY (peer_device, folder_id)
        );
        ",
    )?;

    Ok(())
}

/// Schema v6 — PWA authentication codes and daemon shared secret.
///
/// `auth_codes` holds short-lived pending codes for two flows:
/// - *pairing*: the CLI generates a code, the PWA submits it to get a token.
/// - *device*: the PWA requests a code, the CLI authorises it, the PWA polls
///   until the token appears.
///
/// `daemon_secret` is a single-row table holding a hex-encoded secret the
/// operator shares with the PWA for password-style authentication.
fn v6_auth_codes(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS auth_codes (
            code        TEXT PRIMARY KEY,
            kind        TEXT NOT NULL,
            status      TEXT NOT NULL DEFAULT 'pending',
            token_id    TEXT,
            token_json  TEXT,
            created_at  TEXT NOT NULL,
            expires_at  TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_auth_codes_status
            ON auth_codes(status);

        CREATE TABLE IF NOT EXISTS daemon_secret (
            id          INTEGER PRIMARY KEY CHECK (id = 1),
            secret      TEXT NOT NULL,
            created_at  TEXT NOT NULL
        );
        ",
    )?;

    Ok(())
}

/// Schema v7 -- max file length rules.
///
/// `max_file_length_rules` holds per-path-glob constraints on the maximum
/// allowed file size. Rules are matched by priority (higher wins) and
/// optionally gated by a conditional expression string evaluated against
/// the engine's `EvalContext`.
fn v7_max_file_length_rules(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS max_file_length_rules (
            id            INTEGER PRIMARY KEY,
            path_glob     TEXT NOT NULL,
            max_bytes     INTEGER NOT NULL,
            priority      INTEGER NOT NULL DEFAULT 0,
            conditions    TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_max_file_length_priority
            ON max_file_length_rules(priority DESC);
        ",
    )?;

    Ok(())
}

/// Schema v8 -- exec sessions (PTY and headless process control).
///
/// `exec_sessions` tracks every PTY / process session the node spawns through
/// the management plane. The row is the durable, restart-survivable record the
/// dispatcher resolves a write/resize/kill/signal verb's authorisation scope
/// from: a verb that carries only a session id resolves its target scope from
/// `scope_kind` / `scope_path` here, never the caller-advertised wire scope, so
/// a caller holding `exec:pty` over `/work` cannot drive a session spawned under
/// `/personal`. The two scope columns reuse the exact `Scope::to_columns` /
/// `Scope::from_columns` encoding the `grants` and `manage_audit` tables use.
///
/// `owner_device` is the authenticated caller that spawned the session, and the
/// `command` summary backs both audit and the enumerate-for-authorised-peer
/// requirement. `ended_at` is NULL while a session is live and set on exit;
/// because exec rows are operational state rather than integrity state, a future
/// prune of ended rows is acceptable (unlike the audit and token tables, which
/// have no DELETE path).
fn v8_exec_sessions(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS exec_sessions (
            id            INTEGER PRIMARY KEY,
            kind          TEXT NOT NULL,
            owner_device  TEXT NOT NULL,
            scope_kind    TEXT NOT NULL,
            scope_path    TEXT,
            command       TEXT NOT NULL,
            started_at    INTEGER NOT NULL,
            ended_at      INTEGER,
            exit_code     INTEGER,
            exit_signal   INTEGER
        );

        CREATE INDEX IF NOT EXISTS idx_exec_sessions_owner ON exec_sessions(owner_device);
        CREATE INDEX IF NOT EXISTS idx_exec_sessions_live ON exec_sessions(ended_at);
        ",
    )?;

    Ok(())
}

/// Schema v9 -- revisioned replica and content cache index.
///
/// `replica_items` maps a daemon-assigned `uid` to the backend's stable id and
/// keeps the last remote revision seen (`remote_size`, `remote_mtime`,
/// `remote_ctime`) beside the presented attributes. `replica_tombstones` and
/// the single-row `replica_meta` back change enumeration from a sync anchor.
///
/// `content_objects` holds one cached sparse file per row with its
/// `source_revision` (`source_*` columns) and write-back state; the upload's
/// temporary name and file id are recorded before the rename so a restart
/// recognises a finished rename. `content_ranges` is the authority for the
/// fetched byte ranges of an object, kept merged. `materialized_items` is the
/// set of items the system reports as on disk. Times are nanoseconds since the
/// Unix epoch.
fn v9_replica_and_content(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS replica_items (
            uid               INTEGER PRIMARY KEY,
            backend_id        TEXT NOT NULL,
            stable_id         TEXT NOT NULL,
            native_id         TEXT NOT NULL,
            parent_uid        INTEGER,
            name              TEXT NOT NULL,
            kind              TEXT NOT NULL,
            size              INTEGER,
            btime             INTEGER,
            mtime             INTEGER,
            ctime             INTEGER,
            remote_size       INTEGER,
            remote_mtime      INTEGER,
            remote_ctime      INTEGER,
            content_version   TEXT NOT NULL,
            metadata_version  TEXT NOT NULL,
            uploaded          BOOLEAN NOT NULL DEFAULT TRUE,
            listed_at         INTEGER,
            pinned            BOOLEAN NOT NULL DEFAULT FALSE,
            revision          INTEGER NOT NULL,
            UNIQUE (backend_id, stable_id)
        );

        CREATE INDEX IF NOT EXISTS idx_replica_items_parent ON replica_items(parent_uid, name);
        CREATE INDEX IF NOT EXISTS idx_replica_items_revision ON replica_items(revision);

        CREATE TABLE IF NOT EXISTS replica_tombstones (
            uid           INTEGER PRIMARY KEY,
            parent_uid    INTEGER,
            revision      INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_replica_tombstones_revision ON replica_tombstones(revision);

        CREATE TABLE IF NOT EXISTS replica_meta (
            id            INTEGER PRIMARY KEY CHECK (id = 1),
            store_id      TEXT NOT NULL,
            revision      INTEGER NOT NULL DEFAULT 0,
            floor         INTEGER NOT NULL DEFAULT 0
        );

        INSERT OR IGNORE INTO replica_meta (id, store_id) VALUES (1, lower(hex(randomblob(16))));

        CREATE TABLE IF NOT EXISTS content_objects (
            object_id         INTEGER PRIMARY KEY,
            uid               INTEGER NOT NULL,
            content_version   TEXT NOT NULL,
            source_stable_id  TEXT NOT NULL,
            source_size       INTEGER,
            source_mtime      INTEGER,
            source_ctime      INTEGER,
            size              INTEGER NOT NULL,
            state             TEXT NOT NULL DEFAULT 'partial',
            dirty             BOOLEAN NOT NULL DEFAULT FALSE,
            upload_temp_name  TEXT,
            upload_file_id    TEXT,
            last_access       INTEGER
        );

        CREATE INDEX IF NOT EXISTS idx_content_objects_uid ON content_objects(uid);
        CREATE INDEX IF NOT EXISTS idx_content_objects_last_access ON content_objects(last_access);

        CREATE TABLE IF NOT EXISTS content_ranges (
            object_id     INTEGER NOT NULL REFERENCES content_objects(object_id) ON DELETE CASCADE,
            start         INTEGER NOT NULL,
            end           INTEGER NOT NULL,
            PRIMARY KEY (object_id, start)
        );

        CREATE TABLE IF NOT EXISTS materialized_items (
            uid           INTEGER PRIMARY KEY
        );
        ",
    )?;

    Ok(())
}
