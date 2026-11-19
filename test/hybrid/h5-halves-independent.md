---
append: [shared/teardown.md]
---

# H5: Cascade and Mutagen do not disturb each other

Run: `test/hybrid/run.sh test/hybrid/h5-halves-independent.md` from the repository root.

Prerequisites: docker with the `cascade-smb` container of `test/smb/compose.yml` running, mutagen, sqlite3, scrut, timeout.

The mount point's `.cascade` ignores `/nas/$RUN/mirror/` (anchored) and the
name `scratch`; the share layout is in `lib.sh`. Mutagen session `$SESSION`
replicates `$LOCAL` with `/storage/$RUN/mirror` while the daemon serves the
rest of the run's directory.

```scrut
$ source "$TESTDIR/lib.sh" && { hybrid_require && hybrid_setup && hybrid_start && vol ls "$VOL" >/dev/null && sync_create && sync_flush; } 2>&1
```

Mutagen activity in the replicated folder and Cascade writes outside it,
interleaved: a local create and edit, a create on the share, a write, a
folder with a file and a rename through the volume.

```scrut
$ printf 'm local\n' > "$LOCAL/m-local.txt" && printf 'seed edited\n' > "$LOCAL/seed.txt" && nas "printf 'm nas\n' > /storage/$RUN/mirror/m-nas.txt" && printf 'c new\n' > "$VOL/c-new.txt" && mkdir "$VOL/c-dir" && printf 'c inner\n' > "$VOL/c-dir/inner.txt" && printf 'kept more\n' > "$VOL/keep/mirror/kept.txt" && mv "$VOL/move-a.txt" "$VOL/moved-a.txt" && sync_flush
```

Both halves settle: Cascade's writes reach the share and no write-back is
pending, then Mutagen completes a cycle.

```scrut
$ poll 30 nas_is c-dir/inner.txt 'c inner' && poll 30 nas_is c-new.txt 'c new' && poll 30 nas_is keep/mirror/kept.txt 'kept more' && poll 30 writeback_idle && sync_flush
```

The session reports no conflict and no problem.

```scrut
$ sync_health
conflicts=0 alpha-scan=0 alpha-transition=0 beta-scan=0 beta-transition=0 error=
```

The share holds exactly the expected files in both halves: no AppleDouble
companion, no temporary upload, nothing of Cascade's in the replicated folder.

```scrut
$ nas_tree
c-dir
c-dir/inner.txt
c-new.txt
deep
deep/kept.txt
deep/scratch
deep/scratch/inner.txt
keep
keep/mirror
keep/mirror/kept.txt
mirror
mirror/m-local.txt
mirror/m-nas.txt
mirror/seed.txt
move-b.txt
moved-a.txt
plain.txt
scratch
```

Their content is each writer's.

```scrut
$ nas "cd /storage/$RUN && cat c-new.txt c-dir/inner.txt keep/mirror/kept.txt moved-a.txt mirror/m-local.txt mirror/m-nas.txt mirror/seed.txt"
c new
c inner
kept more
move a
m local
m nas
seed edited
```

The local directory mirrors the replicated folder exactly.

```scrut
$ local_tree "$LOCAL"
m-local.txt
m-nas.txt
seed.txt
```

No temporary upload name is left anywhere on the share.

```scrut
$ timeout 30 docker exec "$HYBRID_CONTAINER" find /storage -name '.cascade-tmp-*'
```
