---
append: [shared/teardown.md]
---

# H2: Cascade never writes into an ignored path

Run: `test/hybrid/run.sh test/hybrid/h2-ignored-never-written.md` from the repository root.

Prerequisites: docker with the `cascade-smb` container of `test/smb/compose.yml` running, mutagen, sqlite3, scrut, timeout.

The mount point's `.cascade` ignores `/nas/$RUN/mirror/` (anchored) and the
name `scratch`; the share layout is in `lib.sh`. The share already holds a
file `scratch` and a folder `deep/scratch` that the volume hides.

```scrut
$ source "$TESTDIR/lib.sh" && { hybrid_require && hybrid_setup && hybrid_start; } 2>&1
```

Creating a file or a folder under an ignored name is refused with
NFS3ERR_ACCES.

```scrut
$ attempt touch "$VOL/scratch"; attempt mkdir "$VOL/deep/scratch"; attempt mkdir "$VOL/mirror"; attempt touch "$VOL/keep/scratch"
refused: Permission denied
refused: Permission denied
refused: Permission denied
refused: Permission denied
```

Renaming a file onto an ignored name is refused with NFS3ERR_ACCES.

```scrut
$ attempt mv "$VOL/move-a.txt" "$VOL/scratch"; attempt mv "$VOL/move-b.txt" "$VOL/deep/scratch"
refused: Permission denied
refused: Permission denied
```

Renaming a folder whose share content holds an ignored entry is refused with
NFS3ERR_ACCES, and the ignored entry stays where it was.

```scrut
$ attempt mv "$VOL/deep" "$VOL/deep2"; nas "test -f /storage/$RUN/deep/scratch/inner.txt" && echo kept
refused: Permission denied
kept
```

Creating inside or moving into an ignored folder fails.

```scrut
$ for op in "touch $VOL/mirror/new.txt" "mkdir $VOL/mirror/new" "mv $VOL/plain.txt $VOL/mirror/plain.txt" "mv $VOL/plain.txt $VOL/deep/scratch/plain.txt"; do attempt $op | cut -d: -f1; done
refused
refused
refused
refused
```

The served files stay where they were.

```scrut
$ vol ls "$VOL"
deep
keep
move-a.txt
move-b.txt
plain.txt
```

A write to a served path after the refused ones reaches the share, and then
no write-back is pending.

```scrut
$ printf 'marker\n' > "$VOL/marker.txt" && poll 30 nas_is marker.txt marker && poll 30 writeback_idle
```

The share holds exactly its seeded layout and the marker, and the hidden
`scratch` file keeps its content.

```scrut
$ nas_tree; nas "cat /storage/$RUN/scratch"
deep
deep/kept.txt
deep/scratch
deep/scratch/inner.txt
keep
keep/mirror
keep/mirror/kept.txt
marker.txt
mirror
mirror/seed.txt
move-a.txt
move-b.txt
plain.txt
scratch
nas scratch
```
