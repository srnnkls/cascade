//! Test seams for the transfer, write-back and eviction suites.

#[cfg(unix)]
pub mod harness;
pub mod mock_backend;
pub mod recording;

#[cfg(unix)]
pub use harness::{Daemon, Disk};
pub use mock_backend::{Fault, MockBackend, MockOp, StopPoint};
pub use recording::{BackendLog, Hold, Placed, RecordingBackend, Upload};
