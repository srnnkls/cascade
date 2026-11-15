//! The sync types of the content cache.
//!
//! `std::sync` in the shipping build, `loom::sync` under `--cfg cascade_loom`.
//! loom explores interleavings only over its own types, so a cache primitive
//! that a loom model covers takes its locks and atomics from here.

pub use std::sync::atomic::Ordering;

#[cfg(not(cascade_loom))]
pub use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
#[cfg(not(cascade_loom))]
pub use std::sync::{Arc, Condvar, Mutex, MutexGuard};

#[cfg(cascade_loom)]
pub use loom::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
#[cfg(cascade_loom)]
pub use loom::sync::{Arc, Condvar, Mutex, MutexGuard};
