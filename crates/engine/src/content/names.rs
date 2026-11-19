//! Names a backend cannot store (TR-10), and paths `.cascade` ignore rules
//! keep off the backend.

/// Characters a name stored on the backend cannot hold, and the path
/// separator.
const REJECTED_NAME_CHARS: [char; 9] = ['?', '*', '<', '>', '"', '\\', '|', ':', '/'];

/// A name the backend cannot store, or a path an ignore rule keeps off it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NameError {
    #[error("name {0:?} cannot be stored on the backend")]
    Rejected(String),
    #[error("path {0:?} is ignored by a .cascade rule")]
    Ignored(String),
}

/// Refuses names rejected by TR-10, before anything is applied.
///
/// # Errors
///
/// Fails with [`NameError::Rejected`] for an empty name, `.`, `..` or a name
/// holding one of `? * < > " \ | : /`.
pub fn check_name(name: &str) -> Result<(), NameError> {
    if name.is_empty() || name == "." || name == ".." || name.contains(REJECTED_NAME_CHARS) {
        return Err(NameError::Rejected(name.to_owned()));
    }
    Ok(())
}
