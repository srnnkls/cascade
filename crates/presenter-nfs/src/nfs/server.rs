//! `NFSv3` and MOUNT v3 on one loopback port, served by `nfs3_server`.
//!
//! The crate's listener runs on a private loopback port and every
//! connection to the public port is relayed to it, so stopping the server
//! closes the connections the crate would otherwise keep serving.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use nfs3_server::tcp::{NFSTcp, NFSTcpListener};
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinHandle, JoinSet};

use super::context::NfsContext;
use super::fs::{CascadeFs, TRANSFER_SIZE};

/// Buffer of each direction of a relayed connection, a 1 MiB transfer in
/// few reads.
const RELAY_BUFFER: usize = 256 * 1024;

/// The VFS cache mode for an NFS export.
///
/// Controls write support and how eagerly content is cached on disk. `Off`
/// exports the tree read-only; `Minimal` and `Full` are write-capable, trading
/// disk usage for caching eagerness. `Minimal` is the default — on-demand
/// reads, reliable writes, minimal disk usage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NfsCacheMode {
    /// Read-only export. Writes are refused.
    Off,
    /// Write-capable with minimal on-disk caching.
    #[default]
    Minimal,
    /// Write-capable, caching everything eagerly.
    Full,
}

impl NfsCacheMode {
    /// Whether this mode permits write operations on the export.
    #[must_use]
    pub const fn writes_permitted(self) -> bool {
        match self {
            Self::Off => false,
            Self::Minimal | Self::Full => true,
        }
    }
}

/// The `mount_nfs` options for the server on `port`: `NFSv3` over TCP with
/// MOUNT on the same port, so no portmapper and no reserved port is needed,
/// locks kept by the client, and 1 MiB transfers.
#[must_use]
pub fn macos_mount_options(port: u16) -> String {
    format!(
        "vers=3,tcp,port={port},mountport={port},locallocks,rsize={TRANSFER_SIZE},wsize={TRANSFER_SIZE}"
    )
}

/// NFS server configuration.
#[derive(Debug)]
pub struct NfsServerConfig {
    /// Address to bind (typically 127.0.0.1:0 for OS-assigned port).
    pub bind_addr: SocketAddr,
    /// Export path for the mount protocol.
    pub export_path: String,
    /// The VFS cache mode governing write support for this export.
    pub cache_mode: NfsCacheMode,
}

impl Default for NfsServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            export_path: "/".to_string(),
            cache_mode: NfsCacheMode::default(),
        }
    }
}

/// Running NFS server handle.
#[derive(Debug)]
pub struct NfsServer {
    /// The actual address the server bound to (useful when port is 0).
    pub local_addr: SocketAddr,
    task: JoinHandle<()>,
}

impl NfsServer {
    /// Start the NFS server. Returns a handle with the bound address.
    ///
    /// With a content cache, file handles carry the generation of its
    /// replica store, so they stay valid across servers on one state
    /// database.
    ///
    /// # Errors
    ///
    /// Returns an error if a listener cannot bind or the export root cannot
    /// be resolved.
    pub async fn start(config: NfsServerConfig, ctx: Arc<NfsContext>) -> anyhow::Result<Self> {
        let cache_mode = config.cache_mode;
        let fs = CascadeFs::new(ctx, cache_mode).await?;
        let private = "127.0.0.1:0";
        let mut nfs = match fs.generation()? {
            Some(generation) => {
                NFSTcpListener::bind_with_generation(private, fs, generation).await?
            }
            None => NFSTcpListener::bind(private, fs).await?,
        };
        nfs.with_export_name(&config.export_path);
        let upstream = SocketAddr::from((Ipv4Addr::LOCALHOST, nfs.get_listen_port()));
        let listener = TcpListener::bind(config.bind_addr).await?;
        let local_addr = listener.local_addr()?;
        let task = tokio::spawn(serve(listener, nfs, upstream));
        tracing::info!(addr = %local_addr, ?cache_mode, "NFS server started");
        Ok(Self { local_addr, task })
    }

    /// Stop the NFS server: the listener and every connection close, and
    /// no later request is served.
    ///
    /// # Errors
    ///
    /// This function currently always succeeds but returns `Result` for API
    /// consistency with other presenter stop methods.
    pub fn stop(self) -> anyhow::Result<()> {
        self.task.abort();
        tracing::info!("NFS server stopped");
        Ok(())
    }
}

/// Relays every connection to `listener` to the crate's listener `nfs` at
/// `upstream` until aborted; aborting drops both listeners and every relay.
async fn serve(listener: TcpListener, nfs: NFSTcpListener<CascadeFs>, upstream: SocketAddr) {
    let mut relays = JoinSet::new();
    let crate_loop = nfs.handle_forever();
    tokio::pin!(crate_loop);
    loop {
        tokio::select! {
            result = &mut crate_loop => {
                if let Err(error) = result {
                    tracing::error!(%error, "NFS listener failed");
                }
                return;
            }
            accepted = listener.accept() => match accepted {
                Ok((client, peer)) => {
                    tracing::debug!(%peer, "NFS connection accepted");
                    relays.spawn(relay(client, upstream));
                }
                Err(error) => tracing::warn!(%error, "NFS accept failed"),
            },
            Some(_) = relays.join_next(), if !relays.is_empty() => {}
        }
    }
}

async fn relay(mut client: TcpStream, upstream: SocketAddr) {
    let result = async {
        let mut server = TcpStream::connect(upstream).await?;
        client.set_nodelay(true)?;
        server.set_nodelay(true)?;
        tokio::io::copy_bidirectional_with_sizes(
            &mut client,
            &mut server,
            RELAY_BUFFER,
            RELAY_BUFFER,
        )
        .await
    }
    .await;
    if let Err(error) = result {
        tracing::debug!(%error, "NFS connection closed");
    }
}

#[cfg(test)]
mod tests {
    use super::macos_mount_options;

    #[test]
    fn macos_mount_options_name_the_port_for_nfs_and_mount_with_local_locks_and_1_mib_transfers() {
        assert_eq!(
            macos_mount_options(12345),
            "vers=3,tcp,port=12345,mountport=12345,locallocks,rsize=1048576,wsize=1048576"
        );
    }
}
