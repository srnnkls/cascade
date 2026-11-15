//! Swappable SMB client: deadlines, liveness probe and one reconnect per operation.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use cascade_engine::backend::TransientError;
use smb::{
    Client, ClientConfig, ConnectionConfig, DirAccessMask, FileCreateArgs, Resource, UncPath,
};
use tokio::time::{Instant, timeout_at};

use crate::config::SmbConfig;

/// Deadline for establishing a session and connecting the share.
const CONNECT_DEADLINE: Duration = Duration::from_secs(5);
/// Deadline for a list, a metadata call and each read or write request.
pub const REQUEST_DEADLINE: Duration = Duration::from_secs(10);
/// Time after which an operation, including its reconnect and retry, has failed.
/// Kept below 15 s so the failure is delivered within 15 s of the call.
const OPERATION_BUDGET: Duration = Duration::from_millis(14_500);
/// Largest read or write a single request carries.
pub const MAX_REQUEST: usize = 1024 * 1024;

/// How an error bears on the health of the connection that produced it.
#[derive(PartialEq, Eq)]
enum ErrorKind {
    /// The connection is gone or did not answer.
    ConnectionLost,
    /// The server answered with a status; the connection is alive.
    ServerReply,
    /// Neither; a liveness probe decides.
    Undetermined,
}

fn classify(err: &anyhow::Error) -> ErrorKind {
    match err.chain().find_map(|e| e.downcast_ref::<smb::Error>()) {
        Some(
            smb::Error::ConnectionStopped
            | smb::Error::MessageProcessingError(_)
            | smb::Error::OperationTimeout(..)
            | smb::Error::TransportError(_),
        ) => ErrorKind::ConnectionLost,
        Some(smb::Error::ReceivedErrorMessage(..) | smb::Error::UnexpectedMessageStatus(_)) => {
            ErrorKind::ServerReply
        }
        _ => ErrorKind::Undetermined,
    }
}

enum Failure {
    /// The client was discarded; the operation may be retried on a new one.
    ConnectionLost(anyhow::Error),
    /// Final for this operation.
    Other(anyhow::Error),
}

/// An SMB client that is rebuilt after the connection is lost.
pub struct SmbClient {
    config: SmbConfig,
    share_path: UncPath,
    current: Mutex<Option<Arc<Client>>>,
    connecting: tokio::sync::Mutex<()>,
}

impl SmbClient {
    pub const fn new(config: SmbConfig, share_path: UncPath) -> Self {
        Self {
            config,
            share_path,
            current: Mutex::new(None),
            connecting: tokio::sync::Mutex::const_new(()),
        }
    }

    /// Run `op` on a connected client under the request deadline.
    ///
    /// When the connection is lost the client is rebuilt and `op` runs once
    /// more; the call fails within [`OPERATION_BUDGET`], and a lost connection
    /// is reported as [`TransientError::Unreachable`].
    pub async fn run<T, F, Fut>(&self, op: F) -> anyhow::Result<T>
    where
        F: Fn(Arc<Client>) -> Fut + Sync,
        Fut: Future<Output = anyhow::Result<T>> + Send,
        T: Send,
    {
        let deadline = Instant::now() + OPERATION_BUDGET;
        match self.attempt(&op, deadline).await {
            Ok(value) => return Ok(value),
            Err(Failure::Other(err)) => return Err(err),
            Err(Failure::ConnectionLost(err)) => {
                tracing::debug!(share = %self.share_path, error = %format!("{err:#}"), "SMB connection lost; reconnecting");
            }
        }
        match self.attempt(&op, deadline).await {
            Ok(value) => Ok(value),
            Err(Failure::Other(err)) => Err(err),
            Err(Failure::ConnectionLost(err)) => {
                Err(TransientError::Unreachable(format!("{}: {err:#}", self.share_path)).into())
            }
        }
    }

    async fn attempt<T, F, Fut>(&self, op: &F, deadline: Instant) -> Result<T, Failure>
    where
        F: Fn(Arc<Client>) -> Fut + Sync,
        Fut: Future<Output = anyhow::Result<T>> + Send,
        T: Send,
    {
        let client = self.connected(deadline).await?;
        let request_deadline = deadline.min(Instant::now() + REQUEST_DEADLINE);
        let lost = match timeout_at(request_deadline, op(Arc::clone(&client))).await {
            Ok(Ok(value)) => return Ok(value),
            Ok(Err(err)) => match classify(&err) {
                ErrorKind::ConnectionLost => err,
                ErrorKind::ServerReply => return Err(Failure::Other(err)),
                ErrorKind::Undetermined => {
                    if self.probe(&client, deadline).await {
                        return Err(Failure::Other(err));
                    }
                    err
                }
            },
            Err(_) => anyhow::anyhow!("SMB request timed out"),
        };
        self.discard(&client);
        Err(Failure::ConnectionLost(lost))
    }

    fn slot(&self) -> std::sync::MutexGuard<'_, Option<Arc<Client>>> {
        self.current.lock().unwrap_or_else(PoisonError::into_inner)
    }

    async fn connected(&self, deadline: Instant) -> Result<Arc<Client>, Failure> {
        if let Some(client) = self.slot().as_ref() {
            return Ok(Arc::clone(client));
        }
        let Ok(_connecting) = timeout_at(deadline, self.connecting.lock()).await else {
            return Err(Failure::ConnectionLost(anyhow::anyhow!(
                "timed out waiting for the SMB connection"
            )));
        };
        if let Some(client) = self.slot().as_ref() {
            return Ok(Arc::clone(client));
        }
        let client = Client::new(ClientConfig {
            dfs: false,
            connection: ConnectionConfig {
                port: self.config.port,
                timeout: Some(REQUEST_DEADLINE),
                ..Default::default()
            },
            ..Default::default()
        });
        let connect = client.share_connect(
            &self.share_path,
            &self.config.username,
            self.config.password.clone(),
        );
        match timeout_at(deadline.min(Instant::now() + CONNECT_DEADLINE), connect).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                let err = anyhow::Error::from(err);
                return Err(if classify(&err) == ErrorKind::ServerReply {
                    Failure::Other(err)
                } else {
                    Failure::ConnectionLost(err)
                });
            }
            Err(_) => {
                return Err(Failure::ConnectionLost(anyhow::anyhow!(
                    "connecting to SMB share timed out"
                )));
            }
        }
        tracing::debug!(share = %self.share_path, "connected to SMB share");
        let client = Arc::new(client);
        *self.slot() = Some(Arc::clone(&client));
        Ok(client)
    }

    /// Whether the share root still opens on `client`.
    async fn probe(&self, client: &Client, deadline: Instant) -> bool {
        let access = DirAccessMask::new()
            .with_read_attributes(true)
            .with_synchronize(true)
            .into();
        let probe = async {
            let resource = client
                .create_file(
                    &self.share_path,
                    &FileCreateArgs::make_open_existing(access),
                )
                .await?;
            if let Resource::Directory(dir) = resource {
                dir.close().await?;
            }
            smb::Result::Ok(())
        };
        let probe_deadline = deadline.min(Instant::now() + CONNECT_DEADLINE);
        matches!(timeout_at(probe_deadline, probe).await, Ok(Ok(())))
    }

    /// Drop `client` if it is still the current one, closing it in the background.
    fn discard(&self, client: &Arc<Client>) {
        let removed = {
            let mut slot = self.slot();
            if slot.as_ref().is_some_and(|c| Arc::ptr_eq(c, client)) {
                slot.take()
            } else {
                None
            }
        };
        if let Some(client) = removed {
            tokio::spawn(async move {
                let _ = tokio::time::timeout(CONNECT_DEADLINE, client.close()).await;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ErrorKind, classify};

    #[test]
    fn io_error_is_left_to_the_liveness_probe() {
        let err = anyhow::Error::from(smb::Error::IoError(std::io::Error::other(
            "STATUS_FILE_LOCK_CONFLICT",
        )));
        assert!(classify(&err) == ErrorKind::Undetermined);
    }

    #[test]
    fn stopped_connection_is_lost() {
        let err = anyhow::Error::from(smb::Error::ConnectionStopped);
        assert!(classify(&err) == ErrorKind::ConnectionLost);
    }
}
