//! `ResolvedConfig::is_ignored` follows gitignore semantics over the item's
//! full VFS path relative to the mount root, the backend's mount prefix
//! included.

use cascade_config::ResolvedConfig;
use cascade_config::merge::resolve_from_configs;
use cascade_config::parse::{gitignore, toml};

/// One expectation: the `.cascade` text, the matched path, whether it names
/// a folder, whether it is ignored, and the clause it exercises.
struct Case {
    rules: &'static str,
    path: &'static str,
    is_dir: bool,
    ignored: bool,
    clause: &'static str,
}

const fn case(
    rules: &'static str,
    path: &'static str,
    is_dir: bool,
    ignored: bool,
    clause: &'static str,
) -> Case {
    Case {
        rules,
        path,
        is_dir,
        ignored,
        clause,
    }
}

const FILE: bool = false;
const DIR: bool = true;

const CASES: &[Case] = &[
    case(
        "Mutagen",
        "Mutagen",
        DIR,
        true,
        "unanchored name at the root",
    ),
    case(
        "Mutagen",
        "nas/Mutagen",
        DIR,
        true,
        "unanchored name below the mount prefix",
    ),
    case(
        "Mutagen",
        "nas/a/b/Mutagen",
        FILE,
        true,
        "unanchored name at any depth",
    ),
    case(
        "Mutagen",
        "nas/Mutagenic",
        DIR,
        false,
        "unanchored name matches whole names only",
    ),
    case(
        "Mutagen",
        "nas/xMutagen",
        FILE,
        false,
        "unanchored name matches whole names only",
    ),
    case(
        "/nas/build",
        "nas/build",
        DIR,
        true,
        "leading slash anchors to the mount root",
    ),
    case(
        "/nas/build",
        "nas/sub/nas/build",
        DIR,
        false,
        "leading slash anchors to the mount root",
    ),
    case(
        "/build",
        "nas/build",
        DIR,
        false,
        "anchored pattern without the mount prefix",
    ),
    case(
        "/build",
        "build",
        FILE,
        true,
        "anchored pattern at the mount root",
    ),
    case(
        "nas/docs/tmp",
        "nas/docs/tmp",
        FILE,
        true,
        "inner slash anchors to the mount root",
    ),
    case(
        "nas/docs/tmp",
        "x/nas/docs/tmp",
        FILE,
        false,
        "inner slash anchors to the mount root",
    ),
    case(
        "docs/tmp",
        "nas/docs/tmp",
        FILE,
        false,
        "inner slash anchors without the mount prefix",
    ),
    case(
        "cache/",
        "nas/cache",
        DIR,
        true,
        "trailing slash matches a folder",
    ),
    case(
        "cache/",
        "nas/cache",
        FILE,
        false,
        "trailing slash does not match a file",
    ),
    case(
        "cache/",
        "nas/deep/cache",
        DIR,
        true,
        "trailing slash alone does not anchor",
    ),
    case("*.log", "nas/a/debug.log", FILE, true, "star at any depth"),
    case(
        "*.log",
        "nas/a/debug.logs",
        FILE,
        false,
        "star leaves the suffix fixed",
    ),
    case(
        "nas/*.tmp",
        "nas/x.tmp",
        FILE,
        true,
        "star within one segment",
    ),
    case(
        "nas/*.tmp",
        "nas/sub/x.tmp",
        FILE,
        false,
        "star does not cross a slash",
    ),
    case(
        "nas/**/scratch",
        "nas/scratch",
        DIR,
        true,
        "double star matches zero segments",
    ),
    case(
        "nas/**/scratch",
        "nas/a/b/scratch",
        DIR,
        true,
        "double star across segments",
    ),
    case(
        "nas/**/scratch",
        "other/a/scratch",
        DIR,
        false,
        "double star keeps the anchor",
    ),
    case(
        "**/node_modules",
        "nas/x/node_modules",
        DIR,
        true,
        "leading double star",
    ),
    case(
        "nas/logs/**",
        "nas/logs/a/b.txt",
        FILE,
        true,
        "trailing double star",
    ),
    case(
        "*.log\n!keep.log",
        "nas/keep.log",
        FILE,
        false,
        "negation re-includes",
    ),
    case(
        "*.log\n!keep.log",
        "nas/debug.log",
        FILE,
        true,
        "negation leaves the rest",
    ),
    case(
        "!keep.log\n*.log",
        "nas/keep.log",
        FILE,
        true,
        "last matching rule wins",
    ),
    case(
        "Mutagen/",
        "nas/Mutagen/sub/file.txt",
        FILE,
        true,
        "path under an ignored folder",
    ),
    case(
        "Mutagen/",
        "nas/Mutagen/sub",
        DIR,
        true,
        "folder under an ignored folder",
    ),
    case(
        "Mutagen/\n!keep.txt",
        "nas/Mutagen/keep.txt",
        FILE,
        true,
        "no re-include under an ignored folder",
    ),
    case(
        "Mutagen/\n!keep.txt",
        "nas/other/keep.txt",
        FILE,
        false,
        "negation outside the ignored folder",
    ),
    case(
        "/nas/Mutagen",
        "nas/Mutagen/x/y.bin",
        FILE,
        true,
        "anchored folder ignores its subtree",
    ),
];

/// SMB shares and APFS hold names case-insensitively, so a rule matches a
/// path whose case differs from it; anchoring and negation are unchanged.
const CASE_FOLDED: &[Case] = &[
    case(
        "Mutagen",
        "nas/mutagen",
        DIR,
        true,
        "unanchored name in another case",
    ),
    case(
        "mutagen",
        "nas/a/MUTAGEN",
        FILE,
        true,
        "unanchored name in another case at any depth",
    ),
    case(
        "Mutagen",
        "nas/mutagenic",
        DIR,
        false,
        "unanchored name in another case matches whole names only",
    ),
    case(
        "/nas/Build",
        "NAS/build",
        DIR,
        true,
        "anchored rule in another case",
    ),
    case(
        "/nas/Build",
        "nas/sub/NAS/build",
        DIR,
        false,
        "anchored rule in another case keeps its anchor",
    ),
    case(
        "Mutagen/",
        "nas/mutagen",
        DIR,
        true,
        "folder rule in another case",
    ),
    case(
        "Mutagen/",
        "nas/MUTAGEN/sub/x.bin",
        FILE,
        true,
        "path under a folder ignored in another case",
    ),
    case(
        "Mutagen/",
        "nas/mutagen",
        FILE,
        false,
        "folder rule in another case does not match a file",
    ),
    case(
        "*.log",
        "nas/a/debug.LOG",
        FILE,
        true,
        "star pattern in another case",
    ),
    case(
        "Über/",
        "nas/über/a.txt",
        FILE,
        true,
        "non-ASCII name in another case",
    ),
    case(
        "*.log\n!Keep.log",
        "nas/keep.LOG",
        FILE,
        false,
        "negation in another case re-includes",
    ),
    case(
        "*.log\n!Keep.log",
        "nas/DEBUG.log",
        FILE,
        true,
        "negation in another case leaves the rest",
    ),
    case(
        "Mutagen/\n!keep.txt",
        "nas/mutagen/KEEP.txt",
        FILE,
        true,
        "no re-include under a folder ignored in another case",
    ),
];

fn resolved(rules: &str) -> ResolvedConfig {
    resolve_from_configs(vec![gitignore::parse(rules)])
}

fn disagreements(cases: &[Case]) -> Vec<String> {
    cases
        .iter()
        .filter(|case| resolved(case.rules).is_ignored(case.path, case.is_dir) != case.ignored)
        .map(|case| {
            format!(
                "{}: rules {:?}, {} {:?} expected ignored={}",
                case.clause,
                case.rules,
                if case.is_dir { "folder" } else { "file" },
                case.path,
                case.ignored
            )
        })
        .collect()
}

#[test]
fn is_ignored_follows_gitignore_semantics_over_the_mount_prefixed_path() {
    let wrong = disagreements(CASES);
    assert!(
        wrong.is_empty(),
        "{} of {} cases disagree with gitignore semantics:\n{}",
        wrong.len(),
        CASES.len(),
        wrong.join("\n")
    );
}

#[test]
fn is_ignored_matches_a_path_whose_case_differs_from_the_rule() {
    let wrong = disagreements(CASE_FOLDED);
    assert!(
        wrong.is_empty(),
        "{} of {} cases disagree with case-insensitive matching:\n{}",
        wrong.len(),
        CASE_FOLDED.len(),
        wrong.join("\n")
    );
}

#[test]
fn a_config_without_ignore_rules_ignores_nothing() -> anyhow::Result<()> {
    let config = resolve_from_configs(vec![toml::parse(
        "[[pin]]\npath = \"nas/docs\"\n\n[cache]\nmax_size = \"1GB\"\n",
    )?]);

    for (path, is_dir) in [("nas", DIR), ("nas/docs", DIR), ("nas/docs/a.txt", FILE)] {
        assert!(
            !config.is_ignored(path, is_dir),
            "{path} ignored without an ignore rule"
        );
    }
    Ok(())
}
