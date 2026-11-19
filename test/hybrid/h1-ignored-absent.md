---
append: [shared/teardown.md]
---

# H1: an ignored path is absent from the Cascade volume

Run: `test/hybrid/run.sh test/hybrid/h1-ignored-absent.md` from the repository root.

Prerequisites: docker with the `cascade-smb` container of `test/smb/compose.yml` running, mutagen, sqlite3, scrut, timeout.

The mount point's `.cascade` ignores `/nas/$RUN/mirror/` (anchored) and the
name `scratch`; the share layout is in `lib.sh`.

```scrut
$ source "$TESTDIR/lib.sh" && { hybrid_require && hybrid_setup && hybrid_start; } 2>&1
```

The run's directory lists only what is not ignored.

```scrut
$ vol ls "$VOL"
deep
keep
move-a.txt
move-b.txt
plain.txt
```

A folder named `scratch` deeper down is ignored by the name rule.

```scrut
$ vol ls "$VOL/deep"
kept.txt
```

A folder named `mirror` below another folder is outside the anchored rule.

```scrut
$ vol ls "$VOL/keep/mirror"
kept.txt
```

Ignored paths are not found by name.

```scrut
$ for path in mirror mirror/seed.txt scratch deep/scratch deep/scratch/inner.txt; do vol test -e "$VOL/$path" && echo "found $path"; done; true
```

The replica holds rows for exactly the served paths that were listed.

```scrut
$ replica_paths
deep
deep/kept.txt
keep
keep/mirror
keep/mirror/kept.txt
move-a.txt
move-b.txt
plain.txt
```
