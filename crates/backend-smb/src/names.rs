//! Names an SMB share cannot hold.

/// Characters SMB rejects in a file or folder name.
const FORBIDDEN: [char; 8] = ['?', '*', '<', '>', '"', '\\', '|', ':'];

/// Reject the last component of `native_id` when SMB cannot hold it.
///
/// # Errors
///
/// Returns an error naming the first forbidden character.
pub fn check(native_id: &str) -> anyhow::Result<()> {
    let name = native_id
        .rsplit_once('/')
        .map_or(native_id, |(_, name)| name);
    if let Some(c) = name.chars().find(|c| FORBIDDEN.contains(c)) {
        anyhow::bail!("SMB cannot hold the name {name:?}: it contains {c:?}");
    }
    Ok(())
}
