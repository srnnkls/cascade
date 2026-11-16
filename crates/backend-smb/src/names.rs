//! Names an SMB share cannot hold.

use cascade_engine::content::names::check_name;

/// Reject the last component of `native_id` when the backend cannot store it.
///
/// # Errors
///
/// Returns the [`NameError`](cascade_engine::content::names::NameError) of
/// the last component.
pub fn check(native_id: &str) -> anyhow::Result<()> {
    let name = native_id
        .rsplit_once('/')
        .map_or(native_id, |(_, name)| name);
    check_name(name)?;
    Ok(())
}
