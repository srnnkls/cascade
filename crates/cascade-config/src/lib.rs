#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::string_slice
    )
)]
//! `.cascade` config parser — config files, merge, directory walk.
//!
//! A directory's gitignore-style `.cascade` and its shared and local TOML
//! configs all deserialise to [`CascadeConfig`]. Directory walk produces
//! [`ResolvedConfig`] with child-overrides-parent precedence.

pub mod matcher;
pub mod merge;
pub mod parse;
pub mod types;

pub use matcher::IgnoreMatcher;
pub use types::{
    AttributeCache, CacheConfig, CacheStatePosture, CascadeConfig, DeviceConfig, GrantConfig,
    IgnoreRule, MaxAge, MaxSize, NfsConfig, ResolvedConfig, ScopeConfig, WholeSeconds,
};
