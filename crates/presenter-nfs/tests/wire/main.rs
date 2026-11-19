//! The NFS presenter started as the daemon starts it, driven over TCP by an
//! `NFSv3` client, and in the ignored `mount` tests by the macOS kernel client.

mod artefacts;
mod cache;
mod identity;
mod listing;
mod mount;
mod named;
mod status;
mod support;
mod vfs;
mod writes;
