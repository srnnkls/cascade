//! `NFSv3` and MOUNT v3 over TCP through `nfs3_server`.
//!
//! [`fs::CascadeFs`] implements the crate's file system traits over the
//! [`context::NfsContext`], with the write rules of [`mod@write`];
//! [`server::NfsServer`] serves it on a loopback port.

pub mod context;
pub mod fs;
pub mod server;
pub mod sidecar;
pub mod write;
