---
append: [shared/teardown.md]
---

# H4: Mutagen replicates the ignored subtree, which Cascade never shows

Run: `test/hybrid/run.sh test/hybrid/h4-mutagen-replicates-ignored.md` from the repository root.

Prerequisites: docker with the `cascade-smb` container of `test/smb/compose.yml` running, mutagen, sqlite3, scrut, timeout.

The mount point's `.cascade` ignores `/nas/$RUN/mirror/` (anchored) and the
name `scratch`; the share layout is in `lib.sh`. Mutagen session `$SESSION`
replicates `$LOCAL` with `/storage/$RUN/mirror` while the daemon runs.

```scrut
$ source "$TESTDIR/lib.sh" && { hybrid_require && hybrid_setup && hybrid_start && vol ls "$VOL" >/dev/null && sync_create && sync_flush; } 2>&1
```

The session brings the share's content to the local directory.

```scrut
$ local_tree "$LOCAL"; cat "$LOCAL/seed.txt"
seed.txt
seed
```

A file created locally reaches the share, owned by the share's user.

```scrut
$ mkdir "$LOCAL/sub" && printf 'from local\n' > "$LOCAL/sub/from-local.txt" && sync_flush && nas "stat -c '%u:%g' /storage/$RUN/mirror/sub/from-local.txt && cat /storage/$RUN/mirror/sub/from-local.txt"
1000:1000
from local
```

A file created on the share reaches the local directory.

```scrut
$ nas "printf 'from nas\n' > /storage/$RUN/mirror/from-nas.txt" && sync_flush && cat "$LOCAL/from-nas.txt"
from nas
```

An edit on either side reaches the other.

```scrut
$ printf 'edited locally\n' > "$LOCAL/from-nas.txt" && nas "printf 'edited on nas\n' > /storage/$RUN/mirror/sub/from-local.txt" && sync_flush && nas "cat /storage/$RUN/mirror/from-nas.txt" && cat "$LOCAL/sub/from-local.txt"
edited locally
edited on nas
```

A delete on either side reaches the other.

```scrut
$ rm "$LOCAL/from-nas.txt" && nas "rm /storage/$RUN/mirror/seed.txt" && sync_flush && nas "cd /storage/$RUN/mirror && find . -mindepth 1 | sort" && local_tree "$LOCAL"
./sub
./sub/from-local.txt
sub
sub/from-local.txt
```

After that activity, a listing of the run's directory and a change scan of
the folders it listed, the volume still shows nothing of the replicated
folder.

```scrut
$ vol ls "$VOL" >/dev/null && wait_scan_interval && vol ls "$VOL" | grep -xE 'mirror|scratch'; for path in mirror mirror/sub mirror/sub/from-local.txt; do vol test -e "$VOL/$path" && echo "found $path"; done; true
```

The replica holds no row for an ignored path.

```scrut
$ ignored_rows
```
