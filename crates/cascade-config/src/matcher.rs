//! Gitignore matching of ignore rules, with ripgrep's `ignore` crate.

use std::borrow::Cow;
use std::path::Path;

use ignore::gitignore::{Gitignore, GitignoreBuilder};

use crate::types::IgnoreRule;

/// Compiled ignore rules, matched with gitignore semantics and without
/// regard to case against a path relative to the mount root.
#[derive(Debug, Clone)]
pub struct IgnoreMatcher {
    gitignore: Gitignore,
}

impl IgnoreMatcher {
    /// Compiles `rules` in order; a rule whose glob does not compile is
    /// logged and left out.
    #[must_use]
    pub fn new(rules: &[IgnoreRule]) -> Self {
        let mut builder = GitignoreBuilder::new("");
        if let Err(error) = builder.case_insensitive(true) {
            tracing::warn!(%error, "ignore rules match case-sensitively");
        }
        for rule in rules {
            let line = fold_non_ascii(&gitignore_line(rule)).into_owned();
            if let Err(error) = builder.add_line(None, &line) {
                tracing::warn!(rule = %line, %error, "ignore rule left out");
            }
        }
        let gitignore = builder.build().unwrap_or_else(|error| {
            tracing::warn!(%error, "ignore rules left out");
            Gitignore::empty()
        });
        Self { gitignore }
    }

    /// Whether no rule is present.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.gitignore.is_empty()
    }

    /// Whether `path`, relative to the mount root, is ignored: the last rule
    /// matching it decides, and every path beneath an ignored folder is
    /// ignored without a rule re-including it.
    #[must_use]
    pub fn is_ignored(&self, path: &str, is_dir: bool) -> bool {
        if self.is_empty() {
            return false;
        }
        let path = fold_non_ascii(path.trim_start_matches('/'));
        let path = Path::new(path.as_ref());
        path.ancestors()
            .skip(1)
            .filter(|folder| !folder.as_os_str().is_empty())
            .any(|folder| self.gitignore.matched(folder, true).is_ignore())
            || self.gitignore.matched(path, is_dir).is_ignore()
    }
}

/// The gitignore line of `rule`: `!` for a negated rule, a trailing `/` for
/// a folder-only rule.
fn gitignore_line(rule: &IgnoreRule) -> String {
    let (negation, pattern) = if rule.negated {
        ("!", rule.pattern.strip_prefix('!').unwrap_or(&rule.pattern))
    } else {
        ("", rule.pattern.as_str())
    };
    let folder = if rule.dir_only && !pattern.ends_with('/') {
        "/"
    } else {
        ""
    };
    format!("{negation}{pattern}{folder}")
}

/// `text` with its non-ASCII characters lower-cased. The `ignore` crate folds
/// only ASCII case: it compiles a non-ASCII literal to byte escapes, which a
/// case-insensitive regex leaves as they are.
fn fold_non_ascii(text: &str) -> Cow<'_, str> {
    if text.is_ascii() {
        return Cow::Borrowed(text);
    }
    let mut folded = String::with_capacity(text.len());
    for character in text.chars() {
        if character.is_ascii() {
            folded.push(character);
        } else {
            folded.extend(character.to_lowercase());
        }
    }
    Cow::Owned(folded)
}
