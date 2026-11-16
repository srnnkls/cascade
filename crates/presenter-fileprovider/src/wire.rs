//! Wire types for inbound File Provider RPC calls.
//!
//! The Swift File Provider extension sends each call as a JSON `Request`
//! and expects a JSON `Response`. The request shape matches the engine's
//! shared [`cascade_engine::protocol::Request`]. The response shape extends
//! it with a structured `error` object so the Swift side can map errors
//! to specific `NSFileProviderError` cases.
//!
//! Each handler module defines its own params/result types and re-exports
//! them here.

use cascade_engine::types::SyncCursor;
use serde::{Deserialize, Serialize};

use crate::items::{DomainItem, DomainItemKind, FileProviderItem};

/// Inbound RPC method names.
///
/// The Swift side issues calls keyed by these strings; the server dispatches
/// on them. Kept as `&'static str` constants to ensure both sides use the
/// same spelling — adding a new method means adding a constant here and a
/// branch in the dispatcher.
pub mod methods {
    pub const GET_ITEM: &str = "getItem";
    pub const ENUMERATE_ITEMS: &str = "enumerateItems";
    pub const FETCH_CONTENTS: &str = "fetchContents";
    pub const IMPORT_DOCUMENT: &str = "importDocument";
    pub const CREATE_DIRECTORY: &str = "createDirectory";
    pub const DELETE_ITEM: &str = "deleteItem";
    pub const MOVE_ITEM: &str = "moveItem";
    pub const CURRENT_SYNC_CURSOR: &str = "currentSyncCursor";
    pub const ENUMERATE_CHANGES: &str = "enumerateChanges";
}

/// Structured error returned in an [`RpcResponse`].
///
/// `code` is one of the well-known machine-readable strings defined by
/// [`crate::handlers::ErrorCode`]; `message` is a free-form human-readable
/// string suitable for logging on the Swift side.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RpcError {
    pub code: String,
    pub message: String,
}

/// Inbound RPC response envelope.
///
/// Mirrors [`cascade_engine::protocol::Response`] but uses a structured
/// `error` object so the Swift side can switch on `code`. Successful
/// responses carry `result`; failed responses carry `error`. Exactly one
/// of the two is present.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcResponse {
    pub id: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

impl RpcResponse {
    #[must_use]
    pub const fn ok(id: u32, result: serde_json::Value) -> Self {
        Self {
            id,
            result: Some(result),
            error: None,
        }
    }

    #[must_use]
    pub const fn err(id: u32, error: RpcError) -> Self {
        Self {
            id,
            result: None,
            error: Some(error),
        }
    }
}

// ---------------------------------------------------------------------------
// getItem
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct GetItemParams {
    pub id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct GetItemResult {
    pub item: FileProviderItem,
}

// ---------------------------------------------------------------------------
// enumerateItems
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct EnumerateItemsParams {
    pub parent_id: String,
    #[serde(default)]
    pub page: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EnumerateItemsResult {
    pub items: Vec<FileProviderItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page: Option<String>,
}

// ---------------------------------------------------------------------------
// fetchContents
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct FetchContentsParams {
    pub id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct FetchContentsResult {
    pub path: String,
}

// ---------------------------------------------------------------------------
// importDocument
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct ImportDocumentParams {
    pub source_url: String,
    pub parent_id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub existing_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ImportDocumentResult {
    pub item: FileProviderItem,
}

// ---------------------------------------------------------------------------
// createDirectory
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct CreateDirectoryParams {
    pub name: String,
    pub parent_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CreateDirectoryResult {
    pub item: FileProviderItem,
}

// ---------------------------------------------------------------------------
// deleteItem
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct DeleteItemParams {
    pub id: String,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct DeleteItemResult {}

// ---------------------------------------------------------------------------
// moveItem
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct MoveItemParams {
    pub id: String,
    pub new_parent_id: String,
    pub new_name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct MoveItemResult {
    pub item: FileProviderItem,
}

// ---------------------------------------------------------------------------
// currentSyncCursor
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct CurrentSyncCursorParams {
    pub parent_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurrentSyncCursorResult {
    pub cursor: SyncCursor,
}

// ---------------------------------------------------------------------------
// enumerateChanges
// ---------------------------------------------------------------------------

/// Parameters for the `enumerateChanges` RPC.
///
/// `since_cursor` is the opaque cursor the Swift File Provider extension
/// last persisted (the one the engine handed back via either
/// `currentSyncCursor` or a prior `enumerateChanges` call). `None` — or a
/// cursor the engine no longer recognises — means "start fresh": the
/// engine returns every current child as `added_or_modified` and emits a
/// new cursor describing the snapshot it just took.
#[derive(Debug, Clone, Deserialize)]
pub struct EnumerateChangesParams {
    pub parent_id: String,
    #[serde(default)]
    pub since_cursor: Option<SyncCursor>,
}

/// Result of the `enumerateChanges` RPC.
///
/// `added_or_modified` carries items that are present in the current view
/// but were either absent from the prior snapshot or have a different
/// metadata tuple. `deleted` carries the IDs of items present in the
/// prior snapshot that no longer exist. `new_cursor` is the cursor the
/// caller must echo on the next `enumerateChanges` call to receive an
/// incremental delta — passing back a stale or unknown cursor falls
/// through to the "first call" behaviour.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnumerateChangesResult {
    pub added_or_modified: Vec<FileProviderItem>,
    pub deleted: Vec<String>,
    pub new_cursor: SyncCursor,
}

// ---------------------------------------------------------------------------
// Protocol v2
// ---------------------------------------------------------------------------

/// Protocol version the [`crate::server::ContentServer`] speaks.
pub const PROTOCOL_VERSION: u32 = 2;

/// Method names of protocol v2.
pub mod methods_v2 {
    pub const HELLO: &str = "hello";
    pub const ITEM: &str = "item";
    pub const LIST: &str = "list";
    pub const WORKING_SET: &str = "workingSet";
    pub const CURRENT_ANCHOR: &str = "currentAnchor";
    pub const CHANGES: &str = "changes";
    pub const FETCH: &str = "fetch";
    pub const FETCH_RANGE: &str = "fetchRange";
    pub const CREATE: &str = "create";
    pub const MODIFY: &str = "modify";
    pub const DELETE: &str = "delete";
    pub const SET_PINNED: &str = "setPinned";
    pub const EVICT: &str = "evict";
    pub const MATERIALIZED_SET: &str = "materializedSet";
    pub const CANCEL: &str = "cancel";
    pub const PROGRESS: &str = "progress";
    pub const SUBSCRIBE: &str = "subscribe";
}

/// Error code of protocol v2: the Swift case name of an `NSFileProviderError`
/// code or of an `NSCocoaErrorDomain` code, so the extension maps each 1:1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FileProviderErrorCode {
    NotAuthenticated,
    FilenameCollision,
    SyncAnchorExpired,
    PageExpired,
    ServerUnreachable,
    NoSuchItem,
    DeletionRejected,
    DirectoryNotEmpty,
    CannotSynchronize,
    NonEvictable,
    VersionNoLongerAvailable,
    ExcludedFromSync,
    OlderExtensionVersionRunning,
    NewerExtensionVersionFound,
    FileWriteOutOfSpace,
    UserCancelled,
    FeatureUnsupported,
}

impl FileProviderErrorCode {
    /// Wire identifier.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotAuthenticated => "notAuthenticated",
            Self::FilenameCollision => "filenameCollision",
            Self::SyncAnchorExpired => "syncAnchorExpired",
            Self::PageExpired => "pageExpired",
            Self::ServerUnreachable => "serverUnreachable",
            Self::NoSuchItem => "noSuchItem",
            Self::DeletionRejected => "deletionRejected",
            Self::DirectoryNotEmpty => "directoryNotEmpty",
            Self::CannotSynchronize => "cannotSynchronize",
            Self::NonEvictable => "nonEvictable",
            Self::VersionNoLongerAvailable => "versionNoLongerAvailable",
            Self::ExcludedFromSync => "excludedFromSync",
            Self::OlderExtensionVersionRunning => "olderExtensionVersionRunning",
            Self::NewerExtensionVersionFound => "newerExtensionVersionFound",
            Self::FileWriteOutOfSpace => "fileWriteOutOfSpace",
            Self::UserCancelled => "userCancelled",
            Self::FeatureUnsupported => "featureUnsupported",
        }
    }

    /// Error domain of the code on the Swift side.
    #[must_use]
    pub const fn domain(self) -> &'static str {
        match self {
            Self::FileWriteOutOfSpace | Self::UserCancelled | Self::FeatureUnsupported => {
                "NSCocoaErrorDomain"
            }
            _ => "NSFileProviderErrorDomain",
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct HelloParams {
    pub version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloResult {
    pub version: u32,
    pub root_uid: String,
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ItemParams {
    pub uid: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItemResult {
    pub item: DomainItem,
}

/// A page of items; `page` is the `next_page` of the previous reply.
#[derive(Debug, Clone, Deserialize)]
pub struct ListParams {
    pub uid: String,
    #[serde(default)]
    pub page: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WorkingSetParams {
    #[serde(default)]
    pub page: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageResult {
    pub items: Vec<DomainItem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_page: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnchorResult {
    pub anchor: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChangesParams {
    pub anchor: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangesResult {
    pub updated: Vec<DomainItem>,
    pub deleted: Vec<String>,
    pub anchor: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FetchParams {
    pub op_id: String,
    pub uid: String,
    #[serde(default)]
    pub requested_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchResult {
    pub path: String,
    pub item: DomainItem,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FetchRangeParams {
    pub op_id: String,
    pub uid: String,
    #[serde(default)]
    pub requested_version: Option<String>,
    #[serde(default)]
    pub strict: bool,
    pub offset: u64,
    pub length: u64,
    pub alignment: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchRangeResult {
    pub path: String,
    pub retrieved_offset: u64,
    pub retrieved_length: u64,
    pub item: DomainItem,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateParams {
    pub op_id: String,
    pub parent_uid: String,
    pub name: String,
    pub kind: DomainItemKind,
    #[serde(default)]
    pub attributes: serde_json::Value,
    #[serde(default)]
    pub contents_path: Option<String>,
    #[serde(default)]
    pub may_already_exist: bool,
}

/// Reply of `create` and `modify`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MutationResult {
    pub item: DomainItem,
    pub fetch_content: bool,
}

/// A field `modify` changes; attribute fields are accepted and not stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangedField {
    Name,
    ParentUid,
    Contents,
    #[serde(other)]
    Attribute,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ModifyParams {
    pub op_id: String,
    pub uid: String,
    #[serde(default)]
    pub changed_fields: Vec<ChangedField>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub parent_uid: Option<String>,
    #[serde(default)]
    pub attributes: serde_json::Value,
    #[serde(default)]
    pub contents_path: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeleteParams {
    pub op_id: String,
    pub uid: String,
    #[serde(default)]
    pub recursive: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SetPinnedParams {
    pub uids: Vec<String>,
    pub pinned: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EvictParams {
    pub uid: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MaterializedSetParams {
    pub uids: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CancelParams {
    pub op_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProgressParams {
    pub op_id: String,
}

/// A frame of the `progress` stream; the last one is `finished`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgressFrame {
    pub op_id: String,
    pub done: u64,
    pub total: u64,
    pub finished: bool,
}

/// A frame of the `subscribe` stream after its acknowledgement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "signal", rename_all = "camelCase")]
pub enum Signal {
    WorkingSet,
    ErrorResolved,
    Evict { uid: String },
    RequestDownload { uid: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn rpc_response_ok_serialises_without_error_field() {
        let response = RpcResponse::ok(1, json!({"a": 1}));
        let encoded = serde_json::to_value(&response).unwrap();
        assert_eq!(encoded["id"], 1);
        assert_eq!(encoded["result"], json!({"a": 1}));
        assert!(encoded.get("error").is_none());
    }

    #[test]
    fn rpc_response_err_serialises_without_result_field() {
        let response = RpcResponse::err(
            7,
            RpcError {
                code: "not_found".to_string(),
                message: "missing".to_string(),
            },
        );
        let encoded = serde_json::to_value(&response).unwrap();
        assert_eq!(encoded["id"], 7);
        assert_eq!(encoded["error"]["code"], "not_found");
        assert_eq!(encoded["error"]["message"], "missing");
        assert!(encoded.get("result").is_none());
    }

    #[test]
    fn enumerate_items_params_accepts_missing_page() {
        let params: EnumerateItemsParams =
            serde_json::from_value(json!({"parent_id": "gdrive:root"})).unwrap();
        assert_eq!(params.parent_id, "gdrive:root");
        assert!(params.page.is_none());
    }

    #[test]
    fn current_sync_cursor_params_deserialises() {
        let params: CurrentSyncCursorParams =
            serde_json::from_value(json!({"parent_id": "gdrive:root"})).unwrap();
        assert_eq!(params.parent_id, "gdrive:root");
    }

    #[test]
    fn current_sync_cursor_result_round_trips() {
        let result = CurrentSyncCursorResult {
            cursor: SyncCursor::new(vec![1, 2, 3, 4]),
        };
        let encoded = serde_json::to_value(&result).unwrap();
        // SyncCursor encodes as base64url-no-pad string.
        assert!(encoded["cursor"].is_string());
        let decoded: CurrentSyncCursorResult = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded.cursor.as_bytes(), &[1, 2, 3, 4]);
    }
}
