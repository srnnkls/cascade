---
append: [shared/teardown.md]
---

# H3: paths that are not ignored are served as before

Run: `test/hybrid/run.sh test/hybrid/h3-served-paths.md` from the repository root.

Prerequisites: docker with the `cascade-smb` container of `test/smb/compose.yml` running, mutagen, sqlite3, scrut, timeout.

The mount point's `.cascade` ignores `/nas/$RUN/mirror/` (anchored) and the
name `scratch`; the share layout is in `lib.sh`, plus a 64 MiB `large.bin`.

```scrut
$ source "$TESTDIR/lib.sh" && { hybrid_require && hybrid_setup && seed_large && hybrid_start; } 2>&1
```

Served files are listed and read with the share's content, including one
below a folder named `mirror` that the anchored rule does not reach. The
absence of ignored names is H1's concern.

```scrut
$ vol ls "$VOL" | grep -vxE 'mirror|scratch'; vol cat "$VOL/plain.txt" "$VOL/deep/kept.txt" "$VOL/keep/mirror/kept.txt"
deep
keep
large.bin
move-a.txt
move-b.txt
plain.txt
plain
kept
kept
```

The first 64 KiB of `large.bin` read through the volume equal the share's.

```scrut
$ cmp <(vol head -c 65536 "$VOL/large.bin") <(nas "head -c 65536 /storage/$RUN/large.bin") && echo equal
equal
```

That read fetched only part of the file into the content cache.

```scrut
$ bytes=$(large_cached_bytes); ((bytes > 0 && bytes < 64 * 1024 * 1024)) && echo partial || echo "cached $bytes bytes"
partial
```

A file written through the volume reaches the share after the write-back
delay.

```scrut
$ printf 'written through cascade\n' > "$VOL/written.txt" && poll 30 nas_is written.txt 'written through cascade' && poll 30 writeback_idle && echo uploaded
uploaded
```

A file given an extended attribute on the volume, which macOS stores in the
AppleDouble companion `._tagged.txt`, reaches the share and keeps the
attribute on the volume.

```scrut
$ printf 'tagged\n' > "$VOL/tagged.txt" && vol xattr -w org.cascade.hybrid yes "$VOL/tagged.txt" && poll 30 nas_is tagged.txt tagged && poll 30 writeback_idle && vol xattr -p org.cascade.hybrid "$VOL/tagged.txt"
yes
```

The share holds the seeded layout and the two written files, and no
AppleDouble or temporary upload name.

```scrut
$ nas_tree
deep
deep/kept.txt
deep/scratch
deep/scratch/inner.txt
keep
keep/mirror
keep/mirror/kept.txt
large.bin
mirror
mirror/seed.txt
move-a.txt
move-b.txt
plain.txt
scratch
tagged.txt
written.txt
```
