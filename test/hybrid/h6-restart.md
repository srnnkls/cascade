---
append: [shared/teardown.md]
---

# H6: after a daemon restart the ignored subtree stays absent and Mutagen still syncs

Run: `test/hybrid/run.sh test/hybrid/h6-restart.md` from the repository root.

Prerequisites: docker with the `cascade-smb` container of `test/smb/compose.yml` running, mutagen, sqlite3, scrut, timeout.

The mount point's `.cascade` ignores `/nas/$RUN/mirror/` (anchored) and the
name `scratch`; the share layout is in `lib.sh`. Mutagen session `$SESSION`
replicates `$LOCAL` with `/storage/$RUN/mirror`.

```scrut
$ source "$TESTDIR/lib.sh" && { hybrid_require && hybrid_setup && hybrid_start && vol ls "$VOL" >/dev/null && sync_create && sync_flush; } 2>&1
```

The daemon stops, unmounting the volume, and starts again with the same
container and `.cascade`.

```scrut
$ { hybrid_stop && ! hybrid_mounted && hybrid_start; } 2>&1
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

Ignored paths are not found by name.

```scrut
$ for path in mirror mirror/seed.txt scratch deep/scratch; do vol test -e "$VOL/$path" && echo "found $path"; done; true
```

The replica holds no row for an ignored path.

```scrut
$ ignored_rows
```

The session still replicates both ways.

```scrut
$ printf 'after restart\n' > "$LOCAL/after-restart.txt" && nas "printf 'nas after restart\n' > /storage/$RUN/mirror/nas-after-restart.txt" && sync_flush && nas "cat /storage/$RUN/mirror/after-restart.txt" && cat "$LOCAL/nas-after-restart.txt"
after restart
nas after restart
```

```scrut
$ sync_health
conflicts=0 alpha-scan=0 alpha-transition=0 beta-scan=0 beta-transition=0 error=
```
