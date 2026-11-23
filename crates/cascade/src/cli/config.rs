use anyhow::Result;
use cascade_config::merge;
use std::path::Path;

/// Show the resolved .cascade config for a directory.
pub fn show(path: &str) -> Result<()> {
    let dir = Path::new(path);
    if !dir.exists() {
        anyhow::bail!("directory does not exist: {path}");
    }

    let resolved = merge::resolve(dir, dir);

    println!("Resolved config for: {path}");
    println!();

    if resolved.ignores.is_empty() {
        println!("  No ignore rules.");
    } else {
        println!("  Ignore rules:");
        for rule in &resolved.ignores {
            let neg = if rule.negated { "!" } else { "" };
            let d = if rule.dir_only { "/" } else { "" };
            println!("    {}{}{}", neg, rule.pattern, d);
        }
    }

    if resolved.pins.is_empty() {
        println!("  No pin rules.");
    } else {
        println!("  Pin rules:");
        for pin in &resolved.pins {
            println!("    {}", pin.path);
        }
    }

    if let Some(cache) = &resolved.cache {
        println!("  Cache:");
        if let Some(max) = &cache.max_size {
            println!("    Max size: {max}");
        }
        if let Some(age) = &cache.max_age {
            println!("    Max age: {age}");
        }
        if let Some(state) = &cache.default_state {
            println!("    Default state: {state:?}");
        }
    }

    Ok(())
}

/// Validate all .cascade files in the current tree.
pub fn validate() -> Result<()> {
    let current = std::env::current_dir()?;

    println!("Validating .cascade files under: {}", current.display());

    walk_and_validate(&current)?;

    println!("Validation complete.");
    Ok(())
}

fn walk_and_validate(dir: &Path) -> Result<()> {
    if let Some(config) = cascade_config::parse::load_dir_strict(dir)? {
        for rule in &config.ignore {
            if rule.pattern.is_empty() {
                anyhow::bail!("empty ignore pattern in {}", dir.join(".cascade").display());
            }
        }
        println!("  OK: {}", dir.display());
    }

    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            if entry.file_type()?.is_dir() {
                walk_and_validate(&entry.path())?;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn validate_rejects_a_child_config_the_daemon_rejects() {
        let root = TempDir::new().unwrap();
        let child = root.path().join("child");
        std::fs::create_dir(&child).unwrap();
        std::fs::write(child.join("cascade.toml"), "[nfs]\nsoft = true\n").unwrap();

        let error = walk_and_validate(root.path()).expect_err("strict loader rejects the key");

        let message = format!("{error:#}");
        assert!(message.contains("cascade.toml"), "{message}");
        assert!(message.contains("soft"), "{message}");
    }
}
