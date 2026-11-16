//! Unix domain socket servers of the File Provider extension.
//!
//! [`FileProviderServer`] serves protocol v1 to the Swift `ActionHandler`,
//! which opens a fresh socket per call, writes one length-prefixed JSON
//! [`cascade_engine::protocol::Request`], reads back one length-prefixed
//! [`crate::wire::RpcResponse`], and closes; it dispatches to a
//! [`crate::handlers::FileProviderHandlers`] implementation.
//!
//! [`ContentServer`] serves protocol v2 from the content cache over
//! persistent connections with the same framing.
//!
//! Each server task runs until the cancel signal fires or the listener is
//! dropped.

use std::collections::HashMap;
use std::io::ErrorKind;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError};
use std::time::Duration;

use anyhow::{Context, Result};
use cascade_engine::backend::{BackendError, TransientError};
use cascade_engine::content::anchor::SyncAnchor;
use cascade_engine::content::cache::{ContentCache, Handoff, RangeRequest};
use cascade_engine::content::domain::{CachedBytes, DomainError, Mount};
use cascade_engine::content::evict::EvictError;
use cascade_engine::content::names::NameError;
use cascade_engine::content::replica::{Changes, ItemKind, ReplicaItem};
use cascade_engine::content::transfer::{CacheSignal, TransferError};
use cascade_engine::protocol::{Request, encode_message};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::unix::OwnedReadHalf;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{broadcast, mpsc, watch};

/// File mode applied to the RPC socket after binding.
///
/// `0o600` — owner read/write only. The socket carries privileged engine
/// RPCs (full filesystem read/write); anyone with connect access can issue
/// them, so the socket must never be readable by other local users. The
/// daemon's process umask is not relied upon — we set the mode explicitly
/// after `bind`.
const SOCKET_MODE: u32 = 0o600;

use crate::handlers::{FileProviderHandlers, HandlerError, HandlerResult};
use crate::items::{DomainItem, DomainItemKind, EXTENSION_ROOT_ID, FileProviderItem};
use crate::wire::{
    AnchorResult, CancelParams, ChangedField, ChangesParams, ChangesResult, CreateDirectoryParams,
    CreateDirectoryResult, CreateParams, CurrentSyncCursorParams, CurrentSyncCursorResult,
    DeleteItemParams, DeleteItemResult, DeleteParams, EnumerateChangesParams,
    EnumerateChangesResult, EnumerateItemsParams, EnumerateItemsResult, EvictParams,
    FetchContentsParams, FetchContentsResult, FetchParams, FetchRangeParams, FetchRangeResult,
    FetchResult, FileProviderErrorCode, GetItemParams, GetItemResult, HelloParams, HelloResult,
    ImportDocumentParams, ImportDocumentResult, ItemParams, ItemResult, ListParams,
    MaterializedSetParams, ModifyParams, MoveItemParams, MoveItemResult, MutationResult,
    PROTOCOL_VERSION, PageResult, ProgressFrame, ProgressParams, RpcError, RpcResponse,
    SetPinnedParams, Signal, WorkingSetParams, methods, methods_v2,
};

/// Server that listens on a Unix domain socket and dispatches each
/// incoming [`Request`] to a [`FileProviderHandlers`].
#[derive(Debug)]
pub struct FileProviderServer {
    socket_path: PathBuf,
    handlers: Arc<dyn FileProviderHandlers>,
}

impl FileProviderServer {
    pub fn new(socket_path: impl Into<PathBuf>, handlers: Arc<dyn FileProviderHandlers>) -> Self {
        Self {
            socket_path: socket_path.into(),
            handlers,
        }
    }

    /// Socket path the server listens on.
    #[must_use]
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Bind the listener and serve until `cancel` fires.
    ///
    /// The socket path is removed before binding if a stale file exists, so
    /// the daemon can restart cleanly after a crash. The parent directory is
    /// created on demand.
    ///
    /// # Security
    ///
    /// The socket file is chmoded to `0o600` (owner read/write only)
    /// immediately after binding. The socket carries privileged engine RPCs
    /// — any connecting client can issue filesystem operations — so the
    /// permissions must restrict access to the owning user. Any future
    /// change to the bind sequence must preserve this invariant; rely on
    /// explicit `set_permissions`, not the process umask.
    pub async fn serve(&self, cancel: watch::Receiver<bool>) -> Result<()> {
        let listener = bind(&self.socket_path).await?;
        accept_until(listener, cancel, |stream| {
            let handlers = Arc::clone(&self.handlers);
            tokio::spawn(async move {
                if let Err(error) = serve_connection(stream, handlers).await {
                    tracing::warn!(%error, "file provider connection failed");
                }
            });
        })
        .await
    }
}

/// Binds the socket at `socket_path` with mode [`SOCKET_MODE`], creating its
/// parent and removing a stale socket first; a socket another listener
/// still accepts on is refused.
async fn bind(socket_path: &Path) -> Result<UnixListener> {
    if let Some(parent) = socket_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create socket parent directory {}", parent.display()))?;
    }
    if socket_path.exists() {
        if UnixStream::connect(socket_path).await.is_ok() {
            anyhow::bail!(
                "socket {} is held by another listener",
                socket_path.display()
            );
        }
        tokio::fs::remove_file(socket_path)
            .await
            .with_context(|| format!("remove stale socket {}", socket_path.display()))?;
    }

    let listener = UnixListener::bind(socket_path)
        .with_context(|| format!("bind socket {}", socket_path.display()))?;
    tokio::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(SOCKET_MODE))
        .await
        .with_context(|| format!("chmod socket {} to {SOCKET_MODE:#o}", socket_path.display()))?;
    tracing::info!(socket = %socket_path.display(), "file provider server listening");
    Ok(listener)
}

/// Hands every accepted connection to `on_stream` until `cancel` fires.
async fn accept_until(
    listener: UnixListener,
    mut cancel: watch::Receiver<bool>,
    on_stream: impl Fn(UnixStream),
) -> Result<()> {
    loop {
        tokio::select! {
            accept = listener.accept() => {
                match accept {
                    Ok((stream, _)) => on_stream(stream),
                    Err(error) => {
                        tracing::warn!(%error, "file provider accept failed");
                    }
                }
            }
            _ = cancel.changed() => {
                if *cancel.borrow() {
                    tracing::info!("file provider server shutting down");
                    return Ok(());
                }
            }
        }
    }
}

/// Read one length-prefixed request, dispatch, and write one response.
async fn serve_connection(
    mut stream: UnixStream,
    handlers: Arc<dyn FileProviderHandlers>,
) -> Result<()> {
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .context("read request length")?;
    let body_len = u32::from_be_bytes(len_buf) as usize;
    let mut body = vec![0u8; body_len];
    stream
        .read_exact(&mut body)
        .await
        .context("read request body")?;

    let request: Request = serde_json::from_slice(&body).context("decode request body")?;
    let response = dispatch(handlers.as_ref(), &request).await;

    let encoded = encode_message(&response).context("encode response")?;
    stream.write_all(&encoded).await.context("write response")?;
    stream.flush().await.context("flush response")?;
    Ok(())
}

/// Route a request to the appropriate handler and build a [`RpcResponse`].
pub async fn dispatch(handlers: &dyn FileProviderHandlers, request: &Request) -> RpcResponse {
    let id = request.id;
    let method = request.method.as_str();
    let params = request.params.clone();

    let outcome: HandlerResult<serde_json::Value> = match method {
        methods::GET_ITEM => handle_get_item(handlers, params).await,
        methods::ENUMERATE_ITEMS => handle_enumerate_items(handlers, params).await,
        methods::FETCH_CONTENTS => handle_fetch_contents(handlers, params).await,
        methods::IMPORT_DOCUMENT => handle_import_document(handlers, params).await,
        methods::CREATE_DIRECTORY => handle_create_directory(handlers, params).await,
        methods::DELETE_ITEM => handle_delete_item(handlers, params).await,
        methods::MOVE_ITEM => handle_move_item(handlers, params).await,
        methods::CURRENT_SYNC_CURSOR => handle_current_sync_cursor(handlers, params).await,
        methods::ENUMERATE_CHANGES => handle_enumerate_changes(handlers, params).await,
        other => Err(HandlerError::internal(format!(
            "unknown File Provider RPC method: {other}"
        ))),
    };

    match outcome {
        Ok(value) => RpcResponse::ok(id, value),
        Err(error) => RpcResponse::err(
            id,
            RpcError {
                code: error.code.as_str().to_string(),
                message: error.message,
            },
        ),
    }
}

fn parse_params<T: serde::de::DeserializeOwned>(
    method: &str,
    params: serde_json::Value,
) -> HandlerResult<T> {
    serde_json::from_value(params)
        .map_err(|error| HandlerError::internal(format!("invalid params for {method}: {error}")))
}

fn serialise_result<T: serde::Serialize>(
    method: &str,
    value: &T,
) -> HandlerResult<serde_json::Value> {
    serde_json::to_value(value).map_err(|error| {
        HandlerError::internal(format!("failed to encode {method} result: {error}"))
    })
}

async fn handle_get_item(
    handlers: &dyn FileProviderHandlers,
    params: serde_json::Value,
) -> HandlerResult<serde_json::Value> {
    let params: GetItemParams = parse_params(methods::GET_ITEM, params)?;
    let item: FileProviderItem = handlers.get_item(&params.id).await?;
    serialise_result(methods::GET_ITEM, &GetItemResult { item })
}

async fn handle_enumerate_items(
    handlers: &dyn FileProviderHandlers,
    params: serde_json::Value,
) -> HandlerResult<serde_json::Value> {
    let params: EnumerateItemsParams = parse_params(methods::ENUMERATE_ITEMS, params)?;
    let output = handlers
        .enumerate_items(&params.parent_id, params.page.as_deref())
        .await?;
    serialise_result(
        methods::ENUMERATE_ITEMS,
        &EnumerateItemsResult {
            items: output.items,
            next_page: output.next_page,
        },
    )
}

async fn handle_fetch_contents(
    handlers: &dyn FileProviderHandlers,
    params: serde_json::Value,
) -> HandlerResult<serde_json::Value> {
    let params: FetchContentsParams = parse_params(methods::FETCH_CONTENTS, params)?;
    let path = handlers.fetch_contents(&params.id).await?;
    serialise_result(
        methods::FETCH_CONTENTS,
        &FetchContentsResult {
            path: path.to_string_lossy().into_owned(),
        },
    )
}

async fn handle_import_document(
    handlers: &dyn FileProviderHandlers,
    params: serde_json::Value,
) -> HandlerResult<serde_json::Value> {
    let params: ImportDocumentParams = parse_params(methods::IMPORT_DOCUMENT, params)?;
    let item = handlers
        .import_document(
            &params.source_url,
            &params.parent_id,
            params.name.as_deref(),
            params.existing_id.as_deref(),
        )
        .await?;
    serialise_result(methods::IMPORT_DOCUMENT, &ImportDocumentResult { item })
}

async fn handle_create_directory(
    handlers: &dyn FileProviderHandlers,
    params: serde_json::Value,
) -> HandlerResult<serde_json::Value> {
    let params: CreateDirectoryParams = parse_params(methods::CREATE_DIRECTORY, params)?;
    let item = handlers
        .create_directory(&params.name, &params.parent_id)
        .await?;
    serialise_result(methods::CREATE_DIRECTORY, &CreateDirectoryResult { item })
}

async fn handle_delete_item(
    handlers: &dyn FileProviderHandlers,
    params: serde_json::Value,
) -> HandlerResult<serde_json::Value> {
    let params: DeleteItemParams = parse_params(methods::DELETE_ITEM, params)?;
    handlers.delete_item(&params.id).await?;
    serialise_result(methods::DELETE_ITEM, &DeleteItemResult {})
}

async fn handle_move_item(
    handlers: &dyn FileProviderHandlers,
    params: serde_json::Value,
) -> HandlerResult<serde_json::Value> {
    let params: MoveItemParams = parse_params(methods::MOVE_ITEM, params)?;
    let item = handlers
        .move_item(&params.id, &params.new_parent_id, &params.new_name)
        .await?;
    serialise_result(methods::MOVE_ITEM, &MoveItemResult { item })
}

async fn handle_current_sync_cursor(
    handlers: &dyn FileProviderHandlers,
    params: serde_json::Value,
) -> HandlerResult<serde_json::Value> {
    let params: CurrentSyncCursorParams = parse_params(methods::CURRENT_SYNC_CURSOR, params)?;
    let cursor = handlers.current_sync_cursor(&params.parent_id).await?;
    serialise_result(
        methods::CURRENT_SYNC_CURSOR,
        &CurrentSyncCursorResult { cursor },
    )
}

async fn handle_enumerate_changes(
    handlers: &dyn FileProviderHandlers,
    params: serde_json::Value,
) -> HandlerResult<serde_json::Value> {
    let params: EnumerateChangesParams = parse_params(methods::ENUMERATE_CHANGES, params)?;
    let output = handlers
        .enumerate_changes(&params.parent_id, params.since_cursor.as_ref())
        .await?;
    serialise_result(
        methods::ENUMERATE_CHANGES,
        &EnumerateChangesResult {
            added_or_modified: output.added_or_modified,
            deleted: output.deleted,
            new_cursor: output.new_cursor,
        },
    )
}

/// Methods that start work and carry an `op_id`.
const OPERATIONS: [&str; 5] = [
    methods_v2::FETCH,
    methods_v2::FETCH_RANGE,
    methods_v2::CREATE,
    methods_v2::MODIFY,
    methods_v2::DELETE,
];

/// Operations `cancel` interrupts; mutations run to their end.
const CANCELLABLE: [&str; 2] = [methods_v2::FETCH, methods_v2::FETCH_RANGE];

/// Capabilities announced by `hello`.
const CAPABILITIES: [&str; 6] = [
    "fetchRange",
    "pin",
    "evict",
    "cancel",
    "progress",
    "subscribe",
];

/// Items per page of `list` and `workingSet`.
const PAGE_SIZE: usize = 256;

/// Interval between samples of the `progress` stream.
const PROGRESS_INTERVAL: Duration = Duration::from_millis(100);

type Frames = mpsc::UnboundedSender<RpcResponse>;
type CallResult<T> = std::result::Result<T, CallError>;

/// Server of protocol v2: serves the File Provider domain from a
/// [`ContentCache`].
///
/// A connection carries any number of requests, each
/// answered under its `id` once it finishes; `subscribe` and `progress`
/// answer with a stream of frames under their request's `id`.
#[derive(Debug)]
pub struct ContentServer {
    socket_path: PathBuf,
    domain: Arc<Domain>,
}

impl ContentServer {
    pub fn new(socket_path: impl Into<PathBuf>, cache: ContentCache) -> Self {
        Self {
            socket_path: socket_path.into(),
            domain: Arc::new(Domain {
                cache,
                ops: Arc::default(),
            }),
        }
    }

    /// Socket path the server listens on.
    #[must_use]
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Bind the listener as [`FileProviderServer::serve`] does and serve
    /// until `cancel` fires.
    ///
    /// # Errors
    ///
    /// Fails when the socket cannot be bound.
    pub async fn serve(&self, cancel: watch::Receiver<bool>) -> Result<()> {
        let listener = self.listen().await?;
        self.serve_on(listener, cancel).await
    }

    /// Bind the listener as [`FileProviderServer::serve`] does.
    ///
    /// # Errors
    ///
    /// Fails when the socket cannot be bound or another listener holds it.
    pub async fn listen(&self) -> Result<UnixListener> {
        bind(&self.socket_path).await
    }

    /// Serve connections accepted on `listener` until `cancel` fires.
    ///
    /// # Errors
    ///
    /// Never fails once the listener is bound.
    pub async fn serve_on(
        &self,
        listener: UnixListener,
        cancel: watch::Receiver<bool>,
    ) -> Result<()> {
        accept_until(listener, cancel, |stream| {
            let domain = Arc::clone(&self.domain);
            tokio::spawn(async move {
                if let Err(error) = serve_domain_connection(stream, domain).await {
                    tracing::warn!(error = %format!("{error:#}"), "file provider connection failed");
                }
            });
        })
        .await
    }
}

/// Reads requests until the peer closes and runs each in its own task;
/// one writer task sends the frames they produce.
async fn serve_domain_connection(stream: UnixStream, domain: Arc<Domain>) -> Result<()> {
    let (mut reader, mut writer) = stream.into_split();
    let (frames, mut outgoing) = mpsc::unbounded_channel::<RpcResponse>();
    let (hangup, hung_up) = watch::channel(false);
    let writing = tokio::spawn(async move {
        while let Some(frame) = outgoing.recv().await {
            writer
                .write_all(&encode_message(&frame).context("encode response")?)
                .await
                .context("write response")?;
        }
        anyhow::Ok(())
    });
    let read = loop {
        match read_request(&mut reader).await {
            Ok(Some(request)) => {
                let op = domain.ops.begin(&request);
                let domain = Arc::clone(&domain);
                let frames = frames.clone();
                let hung_up = hung_up.clone();
                tokio::spawn(async move { domain.handle(request, op, &frames, hung_up).await });
            }
            Ok(None) => break Ok(()),
            Err(error) => break Err(error),
        }
    };
    hangup.send_replace(true);
    drop(frames);
    writing.await.context("join response writer")??;
    read
}

/// One length-prefixed request, or `None` when the peer closed.
async fn read_request(reader: &mut OwnedReadHalf) -> Result<Option<Request>> {
    let mut len = [0u8; 4];
    match reader.read_exact(&mut len).await {
        Ok(_) => {}
        Err(error) if error.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error).context("read request length"),
    }
    let mut body = vec![0u8; usize::try_from(u32::from_be_bytes(len))?];
    reader
        .read_exact(&mut body)
        .await
        .context("read request body")?;
    Ok(Some(
        serde_json::from_slice(&body).context("decode request body")?,
    ))
}

/// A failed call with its File Provider error code.
#[derive(Debug)]
struct CallError {
    code: FileProviderErrorCode,
    message: String,
}

impl CallError {
    fn new(code: FileProviderErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl From<anyhow::Error> for CallError {
    fn from(error: anyhow::Error) -> Self {
        Self::new(code_of(&error), format!("{error:#}"))
    }
}

/// The File Provider error code of an engine failure.
fn code_of(error: &anyhow::Error) -> FileProviderErrorCode {
    use FileProviderErrorCode as Code;
    for cause in error.chain() {
        if cause.is::<TransferError>() {
            return Code::VersionNoLongerAvailable;
        }
        if cause.is::<NameError>() {
            return Code::ExcludedFromSync;
        }
        if let Some(transient) = cause.downcast_ref::<TransientError>() {
            return match transient {
                TransientError::Unreachable(_) => Code::ServerUnreachable,
                TransientError::Busy(_) => Code::CannotSynchronize,
            };
        }
        if let Some(evict) = cause.downcast_ref::<EvictError>() {
            return match evict {
                EvictError::OutOfSpace => Code::FileWriteOutOfSpace,
                EvictError::Busy => Code::NonEvictable,
            };
        }
        if matches!(
            cause.downcast_ref::<DomainError>(),
            Some(DomainError::DirectoryNotEmpty)
        ) {
            return Code::DirectoryNotEmpty;
        }
        if let Some(backend) = cause.downcast_ref::<BackendError>() {
            return match backend {
                BackendError::NotFound(_) => Code::NoSuchItem,
                BackendError::Forbidden(_) | BackendError::ReadOnly(_) => Code::NotAuthenticated,
                BackendError::Conflict(_) => Code::FilenameCollision,
            };
        }
    }
    Code::CannotSynchronize
}

fn reply<T: serde::Serialize>(value: &T) -> CallResult<serde_json::Value> {
    serde_json::to_value(value).map_err(|error| {
        CallError::new(FileProviderErrorCode::CannotSynchronize, error.to_string())
    })
}

fn parse<T: serde::de::DeserializeOwned>(params: serde_json::Value) -> CallResult<T> {
    serde_json::from_value(params).map_err(|error| {
        CallError::new(
            FileProviderErrorCode::FeatureUnsupported,
            format!("invalid params: {error}"),
        )
    })
}

fn response(id: u32, outcome: CallResult<serde_json::Value>) -> RpcResponse {
    match outcome {
        Ok(value) => RpcResponse::ok(id, value),
        Err(error) => RpcResponse::err(
            id,
            RpcError {
                code: error.code.as_str().to_owned(),
                message: error.message,
            },
        ),
    }
}

/// Queues `frame`; `false` once the connection's writer is gone.
fn send(frames: &Frames, frame: RpcResponse) -> bool {
    frames.send(frame).is_ok()
}

/// An item named on the wire.
enum Target {
    Root,
    Item(u64),
}

fn target(uid: &str) -> CallResult<Target> {
    if uid == EXTENSION_ROOT_ID {
        return Ok(Target::Root);
    }
    uid.parse()
        .map(Target::Item)
        .map_err(|_| CallError::new(FileProviderErrorCode::NoSuchItem, format!("no item {uid}")))
}

/// The uid of an item below the neutral root.
fn item_uid(uid: &str) -> CallResult<u64> {
    match target(uid)? {
        Target::Item(uid) => Ok(uid),
        Target::Root => Err(CallError::new(
            FileProviderErrorCode::FeatureUnsupported,
            "the neutral root holds only the mounted backends",
        )),
    }
}

fn anchor_string(anchor: &SyncAnchor) -> CallResult<String> {
    String::from_utf8(anchor.to_bytes()).map_err(|error| {
        CallError::new(FileProviderErrorCode::CannotSynchronize, error.to_string())
    })
}

/// Operations in flight by `op_id`, for `cancel` and `progress`.
#[derive(Debug, Default)]
struct Ops {
    running: std::sync::Mutex<HashMap<String, Running>>,
    serial: AtomicU64,
}

#[derive(Debug)]
struct Running {
    serial: u64,
    uid: Option<u64>,
    cancel: watch::Sender<bool>,
}

/// Registration of a running operation, removed on drop.
struct OpGuard {
    ops: Arc<Ops>,
    op_id: String,
    serial: u64,
    cancelled: watch::Receiver<bool>,
}

impl Ops {
    fn running(&self) -> std::sync::MutexGuard<'_, HashMap<String, Running>> {
        self.running.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Registers the operation `request` starts, before it is spawned, so a
    /// later `cancel` or `progress` on the connection finds it.
    fn begin(self: &Arc<Self>, request: &Request) -> Option<OpGuard> {
        if !OPERATIONS.contains(&request.method.as_str()) {
            return None;
        }
        let op_id = request.params.get("op_id")?.as_str()?.to_owned();
        let uid = request
            .params
            .get("uid")
            .and_then(serde_json::Value::as_str)
            .and_then(|uid| uid.parse().ok());
        let serial = self.serial.fetch_add(1, Ordering::Relaxed);
        let (cancel, cancelled) = watch::channel(false);
        self.running().insert(
            op_id.clone(),
            Running {
                serial,
                uid,
                cancel,
            },
        );
        Some(OpGuard {
            ops: Arc::clone(self),
            op_id,
            serial,
            cancelled,
        })
    }

    fn cancel(&self, op_id: &str) {
        if let Some(running) = self.running().get(op_id) {
            running.cancel.send_replace(true);
        }
    }

    fn is_running(&self, op_id: &str) -> bool {
        self.running().contains_key(op_id)
    }

    /// The item of a running operation that names one.
    fn uid_of(&self, op_id: &str) -> Option<u64> {
        self.running().get(op_id).and_then(|running| running.uid)
    }
}

impl Drop for OpGuard {
    fn drop(&mut self) {
        let mut running = self.ops.running();
        if running
            .get(&self.op_id)
            .is_some_and(|op| op.serial == self.serial)
        {
            running.remove(&self.op_id);
        }
    }
}

impl OpGuard {
    async fn cancellable(
        &self,
        call: impl Future<Output = CallResult<serde_json::Value>>,
    ) -> CallResult<serde_json::Value> {
        let mut cancelled = self.cancelled.clone();
        tokio::select! {
            outcome = call => outcome,
            Ok(_) = cancelled.wait_for(|cancelled| *cancelled) => Err(CallError::new(
                FileProviderErrorCode::UserCancelled,
                format!("operation {} cancelled", self.op_id),
            )),
        }
    }
}

/// The domain a [`ContentServer`] serves.
#[derive(Debug)]
struct Domain {
    cache: ContentCache,
    ops: Arc<Ops>,
}

impl Domain {
    async fn handle(
        &self,
        request: Request,
        op: Option<OpGuard>,
        frames: &Frames,
        hung_up: watch::Receiver<bool>,
    ) {
        let id = request.id;
        match request.method.as_str() {
            methods_v2::SUBSCRIBE => self.subscribe(id, frames, hung_up).await,
            methods_v2::PROGRESS => self.progress(id, request.params, frames, hung_up).await,
            method => {
                let call = self.call(method, request.params);
                let outcome = match &op {
                    Some(op) if CANCELLABLE.contains(&method) => op.cancellable(call).await,
                    _ => call.await,
                };
                drop(op);
                send(frames, response(id, outcome));
            }
        }
    }

    async fn call(&self, method: &str, params: serde_json::Value) -> CallResult<serde_json::Value> {
        match method {
            methods_v2::HELLO => Self::hello(parse(params)?),
            methods_v2::ITEM => self.item(&parse(params)?),
            methods_v2::LIST => self.list(parse(params)?).await,
            methods_v2::WORKING_SET => self.working_set(&parse(params)?),
            methods_v2::CURRENT_ANCHOR => reply(&AnchorResult {
                anchor: anchor_string(&self.cache.current_anchor()?)?,
            }),
            methods_v2::CHANGES => self.changes(&parse(params)?),
            methods_v2::FETCH => self.fetch(parse(params)?).await,
            methods_v2::FETCH_RANGE => self.fetch_range(parse(params)?).await,
            methods_v2::CREATE => self.create(parse(params)?).await,
            methods_v2::MODIFY => self.modify(parse(params)?).await,
            methods_v2::DELETE => self.delete(parse(params)?).await,
            methods_v2::SET_PINNED => self.set_pinned(parse(params)?).await,
            methods_v2::EVICT => {
                let params: EvictParams = parse(params)?;
                self.cache.evict(item_uid(&params.uid)?).await?;
                reply(&serde_json::json!({}))
            }
            methods_v2::MATERIALIZED_SET => {
                let params: MaterializedSetParams = parse(params)?;
                let uids: Vec<u64> = params
                    .uids
                    .iter()
                    .filter_map(|uid| uid.parse().ok())
                    .collect();
                self.cache.set_materialized(&uids).await?;
                reply(&serde_json::json!({}))
            }
            methods_v2::CANCEL => {
                let params: CancelParams = parse(params)?;
                self.ops.cancel(&params.op_id);
                reply(&serde_json::json!({}))
            }
            other => Err(CallError::new(
                FileProviderErrorCode::FeatureUnsupported,
                format!("unknown File Provider RPC method: {other}"),
            )),
        }
    }

    fn known(&self, uid: u64) -> CallResult<ReplicaItem> {
        self.cache.item(uid)?.ok_or_else(|| {
            CallError::new(FileProviderErrorCode::NoSuchItem, format!("no item {uid}"))
        })
    }

    fn present(item: &ReplicaItem, mounts: &[Mount]) -> DomainItem {
        let mount = mounts
            .iter()
            .find(|mount| mount.root_uid == item.uid)
            .map(|mount| mount.backend_id.as_str());
        DomainItem::from_replica(item, mount)
    }

    fn present_one(&self, item: &ReplicaItem) -> DomainItem {
        Self::present(item, &self.cache.mounts())
    }

    fn is_mount_root(&self, uid: u64) -> CallResult<bool> {
        Ok(self
            .cache
            .item(uid)?
            .is_some_and(|item| item.parent_uid.is_none()))
    }

    fn hello(params: HelloParams) -> CallResult<serde_json::Value> {
        let version = params.version;
        match version.cmp(&PROTOCOL_VERSION) {
            std::cmp::Ordering::Less => Err(CallError::new(
                FileProviderErrorCode::OlderExtensionVersionRunning,
                format!("protocol {version} is older than {PROTOCOL_VERSION}"),
            )),
            std::cmp::Ordering::Greater => Err(CallError::new(
                FileProviderErrorCode::NewerExtensionVersionFound,
                format!("protocol {version} is newer than {PROTOCOL_VERSION}"),
            )),
            std::cmp::Ordering::Equal => reply(&HelloResult {
                version,
                root_uid: EXTENSION_ROOT_ID.to_owned(),
                capabilities: CAPABILITIES.map(str::to_owned).to_vec(),
            }),
        }
    }

    fn item(&self, params: &ItemParams) -> CallResult<serde_json::Value> {
        let item = match target(&params.uid)? {
            Target::Root => DomainItem::root(),
            Target::Item(uid) => self.present_one(&self.known(uid)?),
        };
        reply(&ItemResult { item })
    }

    /// The first page lists the folder from the backend; later pages read
    /// the replica.
    async fn list(&self, params: ListParams) -> CallResult<serde_json::Value> {
        let mounts = self.cache.mounts();
        let items = match target(&params.uid)? {
            Target::Root => mounts
                .iter()
                .map(|mount| self.known(mount.root_uid))
                .collect::<CallResult<Vec<_>>>()?,
            Target::Item(uid) if params.page.is_none() => self.cache.list(uid).await?,
            Target::Item(uid) => self.cache.children(uid)?,
        };
        reply(&page(items, params.page.as_deref(), &mounts)?)
    }

    fn working_set(&self, params: &WorkingSetParams) -> CallResult<serde_json::Value> {
        let items = self.cache.working_set()?;
        reply(&page(items, params.page.as_deref(), &self.cache.mounts())?)
    }

    fn changes(&self, params: &ChangesParams) -> CallResult<serde_json::Value> {
        let expired =
            |message: String| CallError::new(FileProviderErrorCode::SyncAnchorExpired, message);
        let anchor = SyncAnchor::from_bytes(params.anchor.as_bytes())
            .map_err(|error| expired(format!("{error:#}")))?;
        let Changes::Since(changes) = self.cache.changes(&anchor)? else {
            return Err(expired(format!("anchor {} expired", params.anchor)));
        };
        let mounts = self.cache.mounts();
        reply(&ChangesResult {
            updated: changes
                .updated
                .iter()
                .map(|item| Self::present(item, &mounts))
                .collect(),
            deleted: changes.deleted.iter().map(u64::to_string).collect(),
            anchor: anchor_string(&changes.anchor)?,
        })
    }

    /// The item as handed out: its current metadata at the handed-out
    /// content version.
    fn handed_out(&self, uid: u64, handoff: &Handoff) -> CallResult<DomainItem> {
        let mut item = self.present_one(&self.known(uid)?);
        item.content_version.clone_from(&handoff.content_version);
        Ok(item)
    }

    async fn fetch(&self, params: FetchParams) -> CallResult<serde_json::Value> {
        let uid = item_uid(&params.uid)?;
        let handoff = self
            .cache
            .fetch(uid, params.requested_version.as_deref())
            .await?;
        reply(&FetchResult {
            item: self.handed_out(uid, &handoff)?,
            path: handoff.path.to_string_lossy().into_owned(),
        })
    }

    async fn fetch_range(&self, params: FetchRangeParams) -> CallResult<serde_json::Value> {
        let uid = item_uid(&params.uid)?;
        let handoff = self
            .cache
            .fetch_range(&RangeRequest {
                uid,
                requested_version: params.requested_version,
                strict: params.strict,
                offset: params.offset,
                length: params.length,
                alignment: params.alignment,
            })
            .await?;
        reply(&FetchRangeResult {
            item: self.handed_out(uid, &handoff)?,
            path: handoff.path.to_string_lossy().into_owned(),
            retrieved_offset: handoff.range.start,
            retrieved_length: handoff.range.end - handoff.range.start,
        })
    }

    /// Creates the item; with `may_already_exist` an item of the same name
    /// and kind is taken over, receiving the contents of a file.
    async fn create(&self, params: CreateParams) -> CallResult<serde_json::Value> {
        let parent = item_uid(&params.parent_uid)?;
        let contents = params.contents_path.as_deref().map(Path::new);
        if let Some(existing) = self.cache.existing_child(parent, &params.name).await? {
            let same_kind = matches!(
                (existing.kind, params.kind),
                (ItemKind::File, DomainItemKind::File) | (ItemKind::Folder, DomainItemKind::Folder)
            );
            if !params.may_already_exist || !same_kind {
                return Err(CallError::new(
                    FileProviderErrorCode::FilenameCollision,
                    format!("{:?} exists in item {parent}", params.name),
                ));
            }
            let item = match (contents, existing.kind) {
                (Some(contents), ItemKind::File) => self.cache.save(existing.uid, contents).await?,
                _ => existing,
            };
            return self.mutated(&item);
        }
        let item = match params.kind {
            DomainItemKind::Folder => self.cache.create_folder(parent, &params.name).await?,
            DomainItemKind::File => {
                self.cache
                    .create_file(parent, &params.name, contents)
                    .await?
            }
        };
        self.mutated(&item)
    }

    /// Applies a rename or reparent, then new contents. A name the backend
    /// cannot store leaves the item under its current name (AD-18).
    async fn modify(&self, params: ModifyParams) -> CallResult<serde_json::Value> {
        let uid = item_uid(&params.uid)?;
        let mut item = self.known(uid)?;
        let moves = params
            .changed_fields
            .iter()
            .any(|field| matches!(field, ChangedField::Name | ChangedField::ParentUid));
        if moves {
            let Some(current_parent) = item.parent_uid else {
                return Err(CallError::new(
                    FileProviderErrorCode::FeatureUnsupported,
                    "a mounted backend cannot be renamed or moved",
                ));
            };
            let parent = params
                .parent_uid
                .as_deref()
                .map_or(Ok(current_parent), item_uid)?;
            let name = params.name.clone().unwrap_or_else(|| item.name.clone());
            if parent != current_parent || name != item.name {
                match self.cache.rename(uid, parent, &name).await {
                    Ok(renamed) => item = renamed,
                    Err(error) if error.is::<NameError>() => {
                        tracing::info!(
                            uid,
                            name,
                            "rename to a name the backend cannot store left unapplied"
                        );
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        }
        if let Some(contents) = &params.contents_path {
            item = self.cache.save(uid, Path::new(contents)).await?;
        }
        self.mutated(&item)
    }

    fn mutated(&self, item: &ReplicaItem) -> CallResult<serde_json::Value> {
        reply(&MutationResult {
            item: self.present_one(item),
            fetch_content: false,
        })
    }

    async fn delete(&self, params: DeleteParams) -> CallResult<serde_json::Value> {
        let rejected = || {
            CallError::new(
                FileProviderErrorCode::DeletionRejected,
                "the neutral root and mounted backends cannot be deleted",
            )
        };
        match target(&params.uid) {
            Ok(Target::Root) => return Err(rejected()),
            Ok(Target::Item(uid)) => {
                if self.is_mount_root(uid)? {
                    return Err(rejected());
                }
                self.cache.delete_item(uid, params.recursive).await?;
            }
            Err(_) => {}
        }
        reply(&serde_json::json!({}))
    }

    async fn set_pinned(&self, params: SetPinnedParams) -> CallResult<serde_json::Value> {
        for uid in &params.uids {
            if let Target::Item(uid) = target(uid)? {
                self.cache.keep_downloaded(uid, params.pinned).await?;
            }
        }
        reply(&serde_json::json!({}))
    }

    /// Acknowledges, then sends a frame per daemon signal until the peer
    /// closes.
    async fn subscribe(&self, id: u32, frames: &Frames, mut hung_up: watch::Receiver<bool>) {
        let mut signals = self.cache.subscribe();
        let mut working_set = self.cache.working_set_changes();
        if !send(frames, RpcResponse::ok(id, serde_json::json!({}))) {
            return;
        }
        loop {
            let signal = tokio::select! {
                received = signals.recv() => match received {
                    Ok(CacheSignal::ErrorResolved) => Signal::ErrorResolved,
                    Ok(CacheSignal::Evict(uid)) => Signal::Evict { uid: uid.to_string() },
                    Ok(CacheSignal::RequestDownload(uid)) => {
                        Signal::RequestDownload { uid: uid.to_string() }
                    }
                    Err(broadcast::error::RecvError::Lagged(missed)) => {
                        tracing::warn!(missed, "signals dropped for a slow subscriber");
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                },
                changed = working_set.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    Signal::WorkingSet
                }
                _ = hung_up.wait_for(|hung_up| *hung_up) => return,
            };
            let frame = response(id, reply(&signal));
            if !send(frames, frame) {
                return;
            }
        }
    }

    /// Acknowledges, then samples the cached bytes of the operation's item
    /// and sends each change until the operation finished.
    async fn progress(
        &self,
        id: u32,
        params: serde_json::Value,
        frames: &Frames,
        mut hung_up: watch::Receiver<bool>,
    ) {
        let params: ProgressParams = match parse(params) {
            Ok(params) => params,
            Err(error) => {
                send(frames, response(id, Err(error)));
                return;
            }
        };
        if !send(frames, RpcResponse::ok(id, serde_json::json!({}))) {
            return;
        }
        let uid = self.ops.uid_of(&params.op_id);
        let mut ticks = tokio::time::interval(PROGRESS_INTERVAL);
        let mut last = None;
        loop {
            tokio::select! {
                _ = ticks.tick() => {}
                _ = hung_up.wait_for(|hung_up| *hung_up) => return,
            }
            let running = self.ops.is_running(&params.op_id);
            let bytes = uid
                .and_then(|uid| self.cache.cached_bytes(uid).ok())
                .unwrap_or(CachedBytes { done: 0, total: 0 });
            let frame = ProgressFrame {
                op_id: params.op_id.clone(),
                done: bytes.done,
                total: bytes.total,
                finished: !running,
            };
            if last.as_ref() != Some(&frame) && !send(frames, response(id, reply(&frame))) {
                return;
            }
            if !running {
                return;
            }
            last = Some(frame);
        }
    }
}

/// The page of `items`, in uid order, after the uid `after` names.
fn page(
    mut items: Vec<ReplicaItem>,
    after: Option<&str>,
    mounts: &[Mount],
) -> CallResult<PageResult> {
    let after = after
        .map(|after| {
            after.parse::<u64>().map_err(|_| {
                CallError::new(
                    FileProviderErrorCode::PageExpired,
                    format!("page {after:?}"),
                )
            })
        })
        .transpose()?;
    items.sort_by_key(|item| item.uid);
    items.retain(|item| after.is_none_or(|after| item.uid > after));
    let next_page = (items.len() > PAGE_SIZE).then(|| {
        items.truncate(PAGE_SIZE);
        items.last().map(|item| item.uid.to_string())
    });
    Ok(PageResult {
        items: items
            .iter()
            .map(|item| Domain::present(item, mounts))
            .collect(),
        next_page: next_page.flatten(),
    })
}

// Re-export so callers can build a server without pulling the
// `wire::methods` constants individually.
#[doc(inline)]
pub use crate::wire::methods as method_names;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::{EnumerateChangesOutput, EnumerateOutput, ErrorCode};
    use async_trait::async_trait;
    use cascade_engine::protocol::Request;
    use cascade_engine::types::{CacheState, SyncCursor};
    use serde_json::json;
    use std::sync::Mutex;

    /// In-memory stub handler used to exercise the dispatcher without an
    /// engine. Each method records its invocation so tests can assert it
    /// was reached with the expected parameters.
    #[derive(Debug, Default)]
    struct StubHandlers {
        calls: Mutex<Vec<String>>,
        force_not_found: bool,
    }

    impl StubHandlers {
        fn record(&self, method: &str) {
            self.calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(method.to_string());
        }
    }

    fn sample_item(id: &str, parent_id: &str, name: &str) -> FileProviderItem {
        FileProviderItem {
            id: id.to_string(),
            parent_id: parent_id.to_string(),
            filename: name.to_string(),
            is_directory: false,
            size: Some(0),
            content_type: None,
            last_modified: None,
            cache_state: CacheState::Online,
        }
    }

    #[async_trait]
    impl FileProviderHandlers for StubHandlers {
        async fn get_item(&self, id: &str) -> HandlerResult<FileProviderItem> {
            self.record("get_item");
            if self.force_not_found {
                return Err(HandlerError::not_found(id.to_string()));
            }
            Ok(sample_item(id, "gdrive:root", "sample.txt"))
        }

        async fn enumerate_items(
            &self,
            parent_id: &str,
            _page: Option<&str>,
        ) -> HandlerResult<EnumerateOutput> {
            self.record("enumerate_items");
            Ok(EnumerateOutput {
                items: vec![sample_item("gdrive:a", parent_id, "a.txt")],
                next_page: None,
            })
        }

        async fn fetch_contents(&self, id: &str) -> HandlerResult<PathBuf> {
            self.record("fetch_contents");
            Ok(PathBuf::from(format!("/tmp/{id}")))
        }

        async fn import_document(
            &self,
            _source_url: &str,
            parent_id: &str,
            name: Option<&str>,
            _existing_id: Option<&str>,
        ) -> HandlerResult<FileProviderItem> {
            self.record("import_document");
            Ok(sample_item(
                "gdrive:new",
                parent_id,
                name.unwrap_or("imported.bin"),
            ))
        }

        async fn create_directory(
            &self,
            name: &str,
            parent_id: &str,
        ) -> HandlerResult<FileProviderItem> {
            self.record("create_directory");
            let mut item = sample_item("gdrive:dir", parent_id, name);
            item.is_directory = true;
            item.size = None;
            Ok(item)
        }

        async fn delete_item(&self, _id: &str) -> HandlerResult<()> {
            self.record("delete_item");
            Ok(())
        }

        async fn move_item(
            &self,
            id: &str,
            new_parent_id: &str,
            new_name: &str,
        ) -> HandlerResult<FileProviderItem> {
            self.record("move_item");
            Ok(sample_item(id, new_parent_id, new_name))
        }

        async fn current_sync_cursor(&self, _parent_id: &str) -> HandlerResult<SyncCursor> {
            self.record("current_sync_cursor");
            Ok(SyncCursor::new(vec![0xab, 0xcd, 0xef]))
        }

        async fn enumerate_changes(
            &self,
            parent_id: &str,
            _since_cursor: Option<&SyncCursor>,
        ) -> HandlerResult<EnumerateChangesOutput> {
            self.record("enumerate_changes");
            Ok(EnumerateChangesOutput {
                added_or_modified: vec![sample_item("gdrive:a", parent_id, "a.txt")],
                deleted: vec!["gdrive:gone".to_string()],
                new_cursor: SyncCursor::new(vec![0x12, 0x34]),
            })
        }
    }

    fn make_request(id: u32, method: &str, params: serde_json::Value) -> Request {
        Request {
            id,
            method: method.to_string(),
            params,
        }
    }

    #[tokio::test]
    async fn dispatch_get_item_routes_to_handler() {
        let handlers = StubHandlers::default();
        let request = make_request(1, methods::GET_ITEM, json!({"id": "gdrive:abc"}));

        let response = dispatch(&handlers, &request).await;

        assert_eq!(response.id, 1);
        assert!(response.error.is_none());
        let result = response.result.unwrap();
        assert_eq!(result["item"]["id"], "gdrive:abc");
        assert!(
            handlers
                .calls
                .lock()
                .unwrap()
                .contains(&"get_item".to_string())
        );
    }

    #[tokio::test]
    async fn dispatch_enumerate_items_returns_array() {
        let handlers = StubHandlers::default();
        let request = make_request(
            2,
            methods::ENUMERATE_ITEMS,
            json!({"parent_id": "gdrive:root"}),
        );

        let response = dispatch(&handlers, &request).await;

        let result = response.result.unwrap();
        let items = result["items"].as_array().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["parent_id"], "gdrive:root");
    }

    #[tokio::test]
    async fn dispatch_fetch_contents_returns_path() {
        let handlers = StubHandlers::default();
        let request = make_request(3, methods::FETCH_CONTENTS, json!({"id": "gdrive:f"}));

        let response = dispatch(&handlers, &request).await;

        let result = response.result.unwrap();
        assert_eq!(result["path"], "/tmp/gdrive:f");
    }

    #[tokio::test]
    async fn dispatch_import_document_returns_item() {
        let handlers = StubHandlers::default();
        let request = make_request(
            4,
            methods::IMPORT_DOCUMENT,
            json!({
                "source_url": "/tmp/source.txt",
                "parent_id": "gdrive:root",
                "name": "imported.txt"
            }),
        );

        let response = dispatch(&handlers, &request).await;

        let result = response.result.unwrap();
        assert_eq!(result["item"]["filename"], "imported.txt");
    }

    #[tokio::test]
    async fn dispatch_create_directory_returns_directory_item() {
        let handlers = StubHandlers::default();
        let request = make_request(
            5,
            methods::CREATE_DIRECTORY,
            json!({"name": "Photos", "parent_id": "gdrive:root"}),
        );

        let response = dispatch(&handlers, &request).await;

        let result = response.result.unwrap();
        assert_eq!(result["item"]["filename"], "Photos");
        assert_eq!(result["item"]["is_directory"], true);
    }

    #[tokio::test]
    async fn dispatch_delete_item_returns_empty_object() {
        let handlers = StubHandlers::default();
        let request = make_request(6, methods::DELETE_ITEM, json!({"id": "gdrive:x"}));

        let response = dispatch(&handlers, &request).await;

        let result = response.result.unwrap();
        assert_eq!(result, json!({}));
    }

    #[tokio::test]
    async fn dispatch_enumerate_changes_returns_delta() {
        let handlers = StubHandlers::default();
        let request = make_request(
            13,
            methods::ENUMERATE_CHANGES,
            json!({"parent_id": "gdrive:root"}),
        );

        let response = dispatch(&handlers, &request).await;

        assert!(response.error.is_none());
        let result = response.result.unwrap();
        let added = result["added_or_modified"].as_array().unwrap();
        assert_eq!(added.len(), 1);
        assert_eq!(added[0]["id"], "gdrive:a");
        let deleted = result["deleted"].as_array().unwrap();
        assert_eq!(deleted.len(), 1);
        assert_eq!(deleted[0], "gdrive:gone");
        // new_cursor is base64url-no-pad of [0x12, 0x34] = "EjQ".
        assert_eq!(result["new_cursor"], "EjQ");
    }

    #[tokio::test]
    async fn dispatch_enumerate_changes_accepts_since_cursor() {
        let handlers = StubHandlers::default();
        // `EjQ` decodes to [0x12, 0x34].
        let request = make_request(
            14,
            methods::ENUMERATE_CHANGES,
            json!({"parent_id": "gdrive:root", "since_cursor": "EjQ"}),
        );

        let response = dispatch(&handlers, &request).await;
        assert!(response.error.is_none());
    }

    #[tokio::test]
    async fn dispatch_current_sync_cursor_returns_cursor_string() {
        let handlers = StubHandlers::default();
        let request = make_request(
            12,
            methods::CURRENT_SYNC_CURSOR,
            json!({"parent_id": "gdrive:root"}),
        );

        let response = dispatch(&handlers, &request).await;

        assert!(response.error.is_none());
        let result = response.result.unwrap();
        // Cursor encodes as base64url-no-pad of [0xab, 0xcd, 0xef] = "q83v".
        assert_eq!(result["cursor"], "q83v");
    }

    #[tokio::test]
    async fn dispatch_move_item_returns_updated_item() {
        let handlers = StubHandlers::default();
        let request = make_request(
            7,
            methods::MOVE_ITEM,
            json!({
                "id": "gdrive:f",
                "new_parent_id": "gdrive:other",
                "new_name": "renamed.txt"
            }),
        );

        let response = dispatch(&handlers, &request).await;

        let result = response.result.unwrap();
        assert_eq!(result["item"]["parent_id"], "gdrive:other");
        assert_eq!(result["item"]["filename"], "renamed.txt");
    }

    #[tokio::test]
    async fn dispatch_unknown_method_returns_internal_error() {
        let handlers = StubHandlers::default();
        let request = make_request(8, "doesNotExist", json!({}));

        let response = dispatch(&handlers, &request).await;

        let error = response.error.unwrap();
        assert_eq!(error.code, ErrorCode::Internal.as_str());
        assert!(error.message.contains("doesNotExist"));
    }

    #[tokio::test]
    async fn dispatch_not_found_propagates_code() {
        let handlers = StubHandlers {
            force_not_found: true,
            ..StubHandlers::default()
        };
        let request = make_request(9, methods::GET_ITEM, json!({"id": "gdrive:missing"}));

        let response = dispatch(&handlers, &request).await;

        let error = response.error.unwrap();
        assert_eq!(error.code, ErrorCode::NotFound.as_str());
        assert_eq!(error.message, "gdrive:missing");
    }

    #[tokio::test]
    async fn dispatch_malformed_params_returns_internal_error() {
        let handlers = StubHandlers::default();
        let request = make_request(10, methods::GET_ITEM, json!({"wrong": "field"}));

        let response = dispatch(&handlers, &request).await;

        let error = response.error.unwrap();
        assert_eq!(error.code, ErrorCode::Internal.as_str());
        assert!(error.message.contains("invalid params"));
    }

    #[tokio::test]
    async fn server_chmods_socket_to_owner_only() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("perms.sock");

        let handlers: Arc<dyn FileProviderHandlers> = Arc::new(StubHandlers::default());
        let server = FileProviderServer::new(&socket_path, handlers);

        let (cancel_tx, cancel_rx) = watch::channel(false);
        let probe_path = socket_path.clone();
        let handle = tokio::spawn(async move { server.serve(cancel_rx).await });

        // Wait for the socket to appear.
        for _ in 0..50 {
            if probe_path.exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(probe_path.exists(), "server failed to bind socket");

        let metadata = tokio::fs::metadata(&probe_path).await.unwrap();
        let mode = metadata.permissions().mode() & 0o777;
        assert_eq!(
            mode, 0o600,
            "socket permission bits {mode:#o} should be 0o600 (owner read/write only)"
        );

        cancel_tx.send(true).unwrap();
        handle.abort();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn server_round_trips_a_request_over_a_socket() {
        use tokio::net::UnixStream as ClientStream;

        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("server.sock");

        let handlers: Arc<dyn FileProviderHandlers> = Arc::new(StubHandlers::default());
        let server = FileProviderServer::new(&socket_path, handlers);

        let (cancel_tx, cancel_rx) = watch::channel(false);
        let server_socket = socket_path.clone();
        let handle = tokio::spawn(async move { server.serve(cancel_rx).await });

        // Wait for the socket to appear.
        for _ in 0..50 {
            if server_socket.exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(server_socket.exists(), "server failed to bind socket");

        // Connect and send a real request.
        let mut stream = ClientStream::connect(&server_socket).await.unwrap();
        let request = make_request(11, methods::GET_ITEM, json!({"id": "gdrive:hello"}));
        let encoded = encode_message(&request).unwrap();
        stream.write_all(&encoded).await.unwrap();

        // Read the response.
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await.unwrap();
        let body_len = u32::from_be_bytes(len_buf) as usize;
        let mut body = vec![0u8; body_len];
        stream.read_exact(&mut body).await.unwrap();
        let response: RpcResponse = serde_json::from_slice(&body).unwrap();

        assert_eq!(response.id, 11);
        let result = response.result.unwrap();
        assert_eq!(result["item"]["id"], "gdrive:hello");

        // Shut down the server.
        cancel_tx.send(true).unwrap();
        handle.abort();
        let _ = handle.await;
    }
}
