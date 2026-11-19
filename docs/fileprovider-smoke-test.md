# macOS File Provider smoke test

This checklist runs the signed host app, its File Provider extension and its embedded
daemon against the Docker Samba share and records one result per acceptance criterion:
AC-1 to AC-15 and AC-22 to AC-29. It also records the byte ranges QuickTime and VLC
request when they open a large video, and repeats the live SMB identity suite against
the Proxmox NAS.

The host app is a menu-bar agent. It registers itself as a login item, registers the
Cascade File Provider domain and runs the `cascade` binary from its own bundle as
`cascade start --app-group <container>`. That daemon serves File Provider protocol v2
on `fp.sock` in the App Group container and mounts the NFS volume, both from one
content cache. Nothing is started by hand. `cascade start --file-provider` without
`--app-group` still serves protocol v1, which the current extension does not speak.

## Facts that shape the run

- The Samba share from `test/smb/compose.yml` binds `127.0.0.1:1445` on the machine
  that runs Docker.
- The Mac mini that holds the signing identity has no Docker installed. The share
  either runs on the Mac that runs the extension, or it runs elsewhere and is reachable
  from that Mac at the host and port given in the backend configuration (step 1 shows
  an ssh tunnel that keeps `127.0.0.1:1445`).
- Signing over ssh does not work: `codesign` fails with `errSecInternalComponent`
  because the keychain holding the key is locked. The signed build runs in a local GUI
  session on the Mac that runs the extension.
- A free Personal Team gives seven-day provisioning profiles. Rebuild and reinstall
  when they expire. A Personal Team cannot use File Provider testing mode, so the
  domain is enabled by hand in System Settings.
- An SMB backend without an `id` key takes the name it is configured under in
  `config.toml`; the configuration below uses `nas`, so items appear under `nas/`.
- `cache.max_size` and pin rules are read at daemon start only. Change them with the
  daemon stopped. They come from the `.cascade` files in the mount-point directory
  (`~/Cloud` by default), which the daemon reads before it mounts the NFS volume over
  that directory.
- An eviction sweep of the content cache runs every five minutes, the first one five
  minutes after the daemon starts. `cascade cache evict` does not reach the content
  cache; the checks below wait for the daemon's own sweep.
- The daemon log at its default level records no line per content read, fetch or
  upload. Content reads are decided from the `content_objects` and `content_ranges`
  tables of the state database.
- The extension needs the App Group entitlement `<team id>.group.io.cascade.shared`,
  which only a team-signed build carries. The embedded `cascade` binary is signed with
  the team identity and the hardened runtime but carries no App Group entitlement of
  its own.

## Prerequisites

- macOS 26 or later on the Mac that runs the extension.
- Xcode 26 or later with an Apple account signed in under Xcode > Settings > Accounts,
  and the ten-character team id of that account.
- rustup: the build phase compiles `cascade` with cargo at the toolchain pinned in
  `rust-toolchain.toml`.
- Docker with Compose on the machine that runs the share.
- About 30 GiB free on the machine that runs Docker and on the Mac: the fixtures hold
  about 23 GiB.
- `sqlite3` and `python3`, both shipped with macOS and the Command Line Tools.
- VLC for the range log in [Ranges requested by QuickTime and VLC](#ranges-requested-by-quicktime-and-vlc).

## 1. Start the Samba share

On the machine that runs Docker, in a checkout of this repository:

```bash
docker compose -f test/smb/compose.yml up -d --wait
```

The share `data` (user `cascade`, password `cascade`) holds `big.bin` and
`sub/hello.txt`; the container is named `cascade-smb`.

When Docker runs on another machine, forward the port on the Mac that runs the
extension and keep the tunnel open for the whole run:

```bash
ssh -N -L 1445:127.0.0.1:1445 <docker host>
```

## 2. Build and sign the host app

In a local GUI session on the Mac that runs the extension, from the repository root:

```bash
TEAM=<team id>
make fileprovider-build TEAM=$TEAM
```

The build passes `-allowProvisioningUpdates`, which lets Xcode register the bundle
identifiers `io.cascade.CascadeFileProviderHost` and
`io.cascade.CascadeFileProviderHost.FileProvider` and the App Group, and create the
development profiles. The script phase
`swift/CascadeFileProvider/Scripts/embed-cascade.sh` compiles `cascade` with cargo,
places it in `Contents/MacOS` and signs it with the app's identity.

Locate the app and check the signatures:

```bash
APP="$(make -s fileprovider-app-path TEAM=$TEAM)"
codesign -dv --entitlements - "$APP/Contents/PlugIns/CascadeFileProvider.appex"
codesign -dv "$APP/Contents/MacOS/cascade"
```

The extension's entitlements list `com.apple.security.application-groups` with
`<team id>.group.io.cascade.shared`. The `cascade` signature shows
`TeamIdentifier=<team id>` and `flags=0x10000(runtime)`.

| Field | Value |
|-------|-------|
| Date | |
| macOS version | |
| Xcode version | |
| Team type (Personal Team or paid Apple Developer Program) | |
| Team id | |
| Extension entitlements list the App Group (yes/no) | |
| `cascade` shows the team identity and the runtime flag (yes/no) | |

## 3. Configure the backend

The Samba share is the only backend. No other `cascade start` may run: the host app
adopts a daemon that already serves the socket instead of starting its own.

```bash
mkdir -p ~/.config/cascade
cat > ~/.config/cascade/config.toml <<'EOF'
[backends.nas]
type = "smb"
EOF
cat > ~/.config/cascade/nas.toml <<'EOF'
type = "smb"
host = "127.0.0.1"
port = 1445
share = "data"
username = "cascade"
password = "cascade"
EOF
pgrep -fl 'cascade start'
```

`password = "cascade"` keeps the share's password in the file in plain text. To keep it
in the login keychain instead, omit the `password` line and store the password with
`cascade backend-add smb --name nas` (answers `127.0.0.1`, `data`, `1445`, `cascade`,
`cascade`) before writing the two files; the host app's daemon then shows the keychain's
access prompt on its first read, answered with Always Allow.

`pgrep` prints nothing. The NFS volume mounts at `~/Cloud` unless `config.toml` sets
`[mount] point`.

## 4. Install and launch the host app

```bash
ditto "$APP" /Applications/CascadeFileProviderHost.app
open /Applications/CascadeFileProviderHost.app
```

1. Approve the login item when macOS asks, and any prompt for access to the Group
   Container. Record every prompt and which process it names.
2. Open the menu-bar item (an external-drive icon). The status reads
   "Cascade is running."
3. Check the container:

   ```bash
   GROUP=~/Library/Group\ Containers/$TEAM.group.io.cascade.shared
   ls -l "$GROUP"
   tail -n 20 "$GROUP/daemon.log"
   ```

   The container holds `daemon.log`, `fp.sock`, `cascade.pid`, `state.db`, `cache/` and
   `staging/`. The log shows `Cascade started (File Provider and NFS).` with the socket
   and the mount point. Terminal may ask for access to another app's data the first
   time it reads the container; allow it.
4. If the Cascade location shows as disabled under System Settings > General >
   Login Items & Extensions > File Providers, enable it.

| Field | Value |
|-------|-------|
| Prompts shown, each with the process it names | |
| Embedded daemon reached the Group Container without a prompt (yes/no) | |
| Administrator prompt for the NFS mount (yes/no) | |
| Menu-bar status | |
| `daemon.log` and `fp.sock` present (yes/no) | |
| Domain had to be enabled by hand (yes/no) | |

## 5. Session variables and helpers

Every check below runs in a shell with these definitions. Define `d` for the machine
that runs Docker.

```bash
GROUP=~/Library/Group\ Containers/$TEAM.group.io.cascade.shared
DB="$GROUP/state.db"
FP="$(ls -d ~/Library/CloudStorage/*Cascade* | head -n 1)"
NFS=~/Cloud

d() { docker "$@"; }                       # Docker on this Mac
# d() { ssh <docker host> docker "$@"; }   # Docker on another machine

# Run a shell script in the share root as the share's owner, outside SMB.
share() { printf '%s\n' "$1" | d exec -i -u 1000:1000 -e TZ=UTC -w /storage cascade-smb sh; }

q() { sqlite3 -readonly -header -column "$DB" "$1"; }
uid_of() { sqlite3 -readonly "$DB" "select uid from replica_items where name = '$1';"; }
objects() {
    q "select o.object_id, r.uid, r.name, o.state, o.dirty, r.pinned, o.size,
           (select coalesce(sum(c.\"end\" - c.start), 0) from content_ranges c
            where c.object_id = o.object_id) as fetched
       from content_objects o join replica_items r on r.uid = o.uid
       order by o.object_id;"
}
populated() { q "select coalesce(sum(\"end\" - start), 0) as populated from content_ranges;"; }

# Print the inode and flags of a file whenever they change; stop with Ctrl-C.
watch_flags() {
    local prev now
    while :; do
        now=$(stat -f '%i %Sf' "$1" 2>&1)
        [ "$now" != "$prev" ] && echo "$(date +%T) $now"
        prev=$now
        sleep 1
    done
}
```

`ls "$FP"` lists `nas`. `stat -f %Sf` prints `dataless` for a placeholder and `-` for
a downloaded file.

Two read helpers:

```bash
cat > /tmp/cascade-pread.py <<'EOF'
import hashlib, os, sys, time
path, offset, length = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
start = time.monotonic()
try:
    fd = os.open(path, os.O_RDONLY)
    data = os.pread(fd, length, offset)
    os.close(fd)
except OSError as error:
    print(f"failed after {time.monotonic() - start:.1f} s: {error}")
    sys.exit(1)
print(f"{len(data)} bytes at {offset} in {time.monotonic() - start:.1f} s "
      f"sha256 {hashlib.sha256(data).hexdigest()}")
EOF

cat > /tmp/cascade-stream.py <<'EOF'
import os, sys, time
path, chunk = sys.argv[1], 1 << 20
offset, worst = 0, 0.0
last = time.monotonic()
try:
    fd = os.open(path, os.O_RDONLY)
    while data := os.pread(fd, chunk, offset):
        now = time.monotonic()
        worst, last = max(worst, now - last), now
        offset += len(data)
    print(f"finished at {offset} bytes; longest stall {worst:.1f} s")
except OSError as error:
    print(f"failed at {offset} bytes {time.monotonic() - last:.1f} s after the last chunk: "
          f"{error}; longest stall before it {worst:.1f} s")
    sys.exit(1)
EOF
```

The reference hash of a range on the share comes from the share itself, for example
64 KiB at 512 MiB:

```bash
share 'dd if=smoke/media/half-read.bin bs=65536 skip=8192 count=1 2>/dev/null' | shasum -a 256
```

## 6. Seed the smoke fixtures

Run AC-1 before anything opens a file. The fixtures can be created before or after it;
creating them reads no content on the Mac.

```bash
share '
set -e
mkdir -p smoke/media smoke/never-listed smoke/keep smoke/evict smoke/edit smoke/remote smoke/offline
dd if=/dev/urandom of=smoke/media/one-gib.bin bs=1M count=1024
dd if=/dev/urandom of=smoke/media/half-read.bin bs=1M count=1024
dd if=/dev/urandom of=smoke/media/version.bin bs=1M count=2048
dd if=/dev/urandom of=smoke/media/eight-gib.bin bs=1M count=8192
dd if=/dev/urandom of=smoke/never-listed/eight-gib.bin bs=1M count=8192
dd if=/dev/urandom of=smoke/keep/keep-me.bin bs=1M count=64
for n in 1 2 3 4; do dd if=/dev/urandom of=smoke/evict/part-$n.bin bs=1M count=768; done
printf "pinned\n" > smoke/evict/pinned.txt
printf "dirty\n" > smoke/evict/dirty.txt
printf "remove me\n" > smoke/evict/remove-me.txt
dd if=/dev/urandom of=smoke/edit/upload.bin bs=1M count=1024
for f in offline-save local-wins twice rename-offline rename-colon delete-changed; do
    printf "%s\n" "$f" > smoke/edit/$f.txt
done
printf "rename me\n" > smoke/remote/rename-me.txt
printf "delete me\n" > smoke/remote/delete-me.txt
printf "version one\n" > smoke/remote/same-size.txt
touch -d "2020-01-02 03:04:05" smoke/remote/same-size.txt
printf "materialised\n" > smoke/offline/materialised.txt
dd if=/dev/urandom of=smoke/offline/daemon-only.bin bs=1M count=16
dd if=/dev/urandom of=smoke/offline/uncached.bin bs=1M count=16
'
```

Do not open `smoke/never-listed` in Finder or list it on the NFS volume before AC-5.

## 7. Acceptance checklist

Each item names its fixture, the steps, the observation that decides it and a result
field. Paths are relative to the share root: `smoke/media/one-gib.bin` is
`$FP/nas/smoke/media/one-gib.bin` in the File Provider folder,
`$NFS/nas/smoke/media/one-gib.bin` on the NFS volume and `/storage/smoke/media/one-gib.bin`
in the container.

### AC-1: a listing is dataless and reads no content

Walking-skeleton check; run it first.

1. In Finder, open Cascade under Locations, then `nas`, then `sub`. The root shows
   `nas`; `nas` shows `big.bin` and `sub`; `sub` shows `hello.txt`.
2. Without opening any file:

   ```bash
   ls "$FP/nas/sub"
   stat -f %Sf "$FP/nas/sub/hello.txt" "$FP/nas/big.bin"
   q "select count(*) as objects from content_objects;"
   ls -A "$GROUP/cache"
   ```

Decides: both `stat` lines print `dataless`, `content_objects` holds 0 rows and
`cache/` is empty.

| Field | Value |
|-------|-------|
| Root listed `nas` (yes/no) | |
| `stat -f %Sf` output | |
| `content_objects` rows | |
| Result (pass/fail) | |

### AC-2: opening a 1 GiB file costs about 1 GiB

Fixture: `smoke/media/one-gib.bin`.

```bash
stat -f %Sf "$FP/nas/smoke/media/one-gib.bin"
df -k /System/Volumes/Data
shasum -a 256 "$FP/nas/smoke/media/one-gib.bin"
share 'cat smoke/media/one-gib.bin' | shasum -a 256
df -k /System/Volumes/Data
```

Decides: the file was `dataless` before, both hashes match, and the `Used` column grew
by about 1 048 576 KiB (between 0.9 and 1.5 GiB), not about 2 GiB.

| Field | Value |
|-------|-------|
| `Used` before and after (KiB) | |
| Growth (GiB) | |
| Hashes match (yes/no) | |
| Result (pass/fail) | |

### AC-3: a `pread` of a dataless file returns the right bytes and starts one download

Fixture: `smoke/media/half-read.bin`.

```bash
stat -f %Sf "$FP/nas/smoke/media/half-read.bin"
python3 /tmp/cascade-pread.py "$FP/nas/smoke/media/half-read.bin" $((512 << 20)) 65536
share 'dd if=smoke/media/half-read.bin bs=65536 skip=8192 count=1 2>/dev/null' | shasum -a 256
objects | grep half-read.bin; sleep 2; objects | grep half-read.bin
```

Decides: the two hashes match; `half-read.bin` has exactly one `content_objects` row,
its `fetched` grows between the two queries and reaches `size` with state `complete`
when queried again later.

| Field | Value |
|-------|-------|
| Hashes match (yes/no) | |
| Object rows for the item | |
| `fetched` in the two queries | |
| Result (pass/fail) | |

### AC-4: a strict partial fetch of an old version is refused

Fixture: `smoke/media/version.bin` (2 GiB, so the download is still running at step 3).

1. Read the start of the dataless file:

   ```bash
   python3 /tmp/cascade-pread.py "$FP/nas/smoke/media/version.bin" 0 65536
   ```

2. At once overwrite 1 MiB of it in place on the share, which keeps its file id and
   moves its change time:

   ```bash
   share 'dd if=/dev/urandom of=smoke/media/version.bin bs=1M count=1 conv=notrunc 2>/dev/null'
   ```

3. Read another range of the partially materialised file, then look for the error:

   ```bash
   python3 /tmp/cascade-pread.py "$FP/nas/smoke/media/version.bin" $((1536 << 20)) 65536
   log show --last 5m --style compact | grep -e 'Code=-2009' -e 'versionNoLongerAvailable'
   ```

Decides: the fetch that names the old version fails with `versionNoLongerAvailable`
(`NSFileProviderErrorDomain` code -2009), seen in the system log or as the error of
the second read. When the system fetches the new version without a strict request,
record that: the outcome then says the system did not issue a strict fetch.

| Field | Value |
|-------|-------|
| Second read output | |
| Log lines with -2009 | |
| System issued a strict fetch (yes/no) | |
| Result (pass/fail) | |

### AC-5: an NFS read deep in a never-listed folder fetches only what it needs

Fixture: `smoke/never-listed/eight-gib.bin`, never listed in Finder or on the NFS volume.

```bash
python3 /tmp/cascade-pread.py "$NFS/nas/smoke/never-listed/eight-gib.bin" $((6 << 30)) 1048576
share 'dd if=smoke/never-listed/eight-gib.bin bs=1048576 skip=6144 count=1 2>/dev/null' | shasum -a 256
objects | grep eight-gib.bin
```

Decides: the hashes match and the object's `fetched` is below 67 108 864 bytes (64 MiB).

| Field | Value |
|-------|-------|
| Hashes match (yes/no) | |
| `fetched` (bytes) | |
| Result (pass/fail) | |

### AC-6: Keep Downloaded survives a sweep at a zero limit

Fixture: `smoke/keep/keep-me.bin`, never opened.

1. In Finder, right-click `keep-me.bin` and choose Keep Downloaded. Within a minute:

   ```bash
   stat -f %Sf "$FP/nas/smoke/keep/keep-me.bin"
   objects | grep keep-me.bin
   ```

   The flags print `-`; the row shows `pinned` 1 and state `complete`.
2. Choose Quit Cascade in the menu bar, then set a zero limit while the volume is
   unmounted:

   ```bash
   mount | grep "$NFS"
   printf '[cache]\nmax_size = "0B"\n' > "$NFS/.cascade.toml"
   open /Applications/CascadeFileProviderHost.app
   ```

   `mount` prints nothing before the file is written.
3. Wait six minutes after the relaunch, so the first sweep has run, then repeat the
   checks of step 1.

Decides: after the sweep `keep-me.bin` is still not dataless and its object is still
`complete` with `fetched` equal to `size`.

| Field | Value |
|-------|-------|
| Downloaded without opening (yes/no) | |
| Flags and object after the sweep | |
| Result (pass/fail) | |

### AC-7: a sweep removes least recently used objects down to the limit

Fixtures: `smoke/evict/part-1.bin` to `part-4.bin` (768 MiB each), `pinned.txt`, `dirty.txt`.

1. Quit Cascade, set a 1 GiB limit and relaunch:

   ```bash
   printf '[cache]\nmax_size = "1GiB"\n' > "$NFS/.cascade.toml"
   open /Applications/CascadeFileProviderHost.app
   ```

2. Check that no other large object remains (`objects`); remove leftovers of earlier
   items with Remove Download in Finder.
3. Read the four parts over NFS in order, so `part-1.bin` is the least recently used,
   then pin `pinned.txt` and cache `dirty.txt`:

   ```bash
   for n in 1 2 3 4; do cat "$NFS/nas/smoke/evict/part-$n.bin" > /dev/null; done
   cat "$NFS/nas/smoke/evict/dirty.txt"
   ```

   In Finder, choose Keep Downloaded on `smoke/evict/pinned.txt`.
4. Make `dirty.txt` dirty with the share paused, so its upload cannot finish:

   ```bash
   d pause cascade-smb
   printf 'unsent\n' >> "$NFS/nas/smoke/evict/dirty.txt"
   ```

5. Wait five minutes after step 4 with the share still paused, then:

   ```bash
   objects; populated
   d unpause cascade-smb
   ```

Decides: `populated` is at most 1 073 741 824; `part-1.bin`, `part-2.bin` and
`part-3.bin` have no row; `part-4.bin` is still there; `pinned.txt` (pinned 1) and
`dirty.txt` (dirty 1) are still there.

Then remove the limit for the rest of the run: Quit Cascade,
`: > "$NFS/.cascade.toml"`, relaunch.

| Field | Value |
|-------|-------|
| `objects` after the sweep | |
| `populated` (bytes) | |
| Result (pass/fail) | |

### AC-8: an edit saved while the NAS is paused reaches it after a restart

Fixture: `smoke/edit/offline-save.txt`.

```bash
cat "$FP/nas/smoke/edit/offline-save.txt"
d pause cascade-smb
printf 'saved offline\n' > "$FP/nas/smoke/edit/offline-save.txt"
sleep 10; objects | grep offline-save
```

The row shows `dirty` 1. Quit Cascade, relaunch it, then:

```bash
d unpause cascade-smb
share 'cat smoke/edit/offline-save.txt'
```

Repeat the `share` line every minute; uploads retry with a delay that doubles up to
five minutes.

Decides: within six minutes of the unpause the share holds `saved offline`, without
any action in Finder.

| Field | Value |
|-------|-------|
| Minutes from unpause to the edit on the share | |
| Result (pass/fail) | |

### AC-9: the local edit wins over a change on the NAS

Fixture: `smoke/edit/local-wins.txt`.

```bash
cat "$FP/nas/smoke/edit/local-wins.txt"
share 'printf "nas edit\n" > smoke/edit/local-wins.txt'
printf 'mac edit\n' > "$FP/nas/smoke/edit/local-wins.txt"
sleep 15
share 'cat smoke/edit/local-wins.txt; ls -A smoke/edit'
```

Decides: the share holds `mac edit` under `local-wins.txt` and `ls -A` shows the
fixture names only: no conflict copy and no `.cascade-tmp-*` file.

| Field | Value |
|-------|-------|
| Content on the share | |
| Listing of `smoke/edit` | |
| Result (pass/fail) | |

### AC-10: a rename on the NAS keeps the item and its content

Fixture: `smoke/remote/rename-me.txt`.

```bash
cat "$FP/nas/smoke/remote/rename-me.txt"
uid_of rename-me.txt; objects | grep rename-me
share 'mv smoke/remote/rename-me.txt smoke/remote/renamed.txt'; date +%T
```

Watch `smoke/remote` in Finder and note when `renamed.txt` appears, then:

```bash
uid_of renamed.txt; objects | grep renamed
```

Decides: Finder shows `renamed.txt` within 90 seconds; its uid (the item identifier)
equals the uid of `rename-me.txt`; the object keeps its `object_id` and `fetched`.

| Field | Value |
|-------|-------|
| Seconds until Finder showed the new name | |
| uid before and after | |
| `object_id` before and after | |
| Result (pass/fail) | |

### AC-11: a delete on the NAS disappears from Finder

Fixture: `smoke/remote/delete-me.txt`, listed in Finder.

```bash
ls "$FP/nas/smoke/remote"
share 'rm smoke/remote/delete-me.txt'; date +%T
```

Decides: `delete-me.txt` disappears from the Finder window within 90 seconds.

| Field | Value |
|-------|-------|
| Seconds until it disappeared | |
| Result (pass/fail) | |

### AC-12: with the NAS paused, cached files open and uncached ones fail fast

Fixtures: `smoke/offline/materialised.txt`, `daemon-only.bin`, `uncached.bin`.

1. Prepare the three states:

   ```bash
   cat "$FP/nas/smoke/offline/materialised.txt"
   cat "$NFS/nas/smoke/offline/daemon-only.bin" > /dev/null
   stat -f %Sf "$FP/nas/smoke/offline/"*
   objects | grep -e daemon-only -e uncached
   ```

   `materialised.txt` prints `-`; the other two print `dataless`; `daemon-only.bin`
   has a `complete` object and `uncached.bin` none.
2. Pause the share and read all three through the File Provider folder:

   ```bash
   d pause cascade-smb
   time cat "$FP/nas/smoke/offline/materialised.txt"
   time shasum -a 256 "$FP/nas/smoke/offline/daemon-only.bin"
   time cat "$FP/nas/smoke/offline/uncached.bin" > /dev/null
   log show --last 2m --style compact | grep -e 'Code=-1004' -e 'serverUnreachable'
   ```

3. Unpause and open `uncached.bin` again without restarting anything; the daemon
   signals the system once a backend call succeeds, at the latest after the next scan:

   ```bash
   d unpause cascade-smb
   sleep 90; shasum -a 256 "$FP/nas/smoke/offline/uncached.bin"
   share 'cat smoke/offline/uncached.bin' | shasum -a 256
   ```

Decides: the first two reads succeed; the third fails within 20 seconds with
`serverUnreachable` (`NSFileProviderErrorDomain` code -1004); after the unpause the
hashes of `uncached.bin` match.

| Field | Value |
|-------|-------|
| Times of the three reads | |
| Error of the third read and log lines | |
| Opened after unpause (yes/no) | |
| Result (pass/fail) | |

### AC-13: the host app's daemon serves the socket and the NFS volume

```bash
ps -o args= -p "$(cat "$GROUP/cascade.pid")"
ls -l "$GROUP/fp.sock"
printf %s "$GROUP/fp.sock" | wc -c
mount | grep "$NFS"
ls "$NFS/nas"
```

Decides: the process runs `cascade start --app-group <container>`; `fp.sock` is a
socket; its path is at most 103 bytes, which with the terminating NUL fits the
104-byte `sun_path`; the NFS volume is mounted and `ls` lists `big.bin`, `smoke` and
`sub`.

| Field | Value |
|-------|-------|
| Daemon command line | |
| Socket path length (bytes) | |
| NFS listing | |
| Result (pass/fail) | |

### AC-14: Remove Download makes the file dataless and drops its object

Fixture: `smoke/evict/remove-me.txt`; no cache limit is set.

```bash
cat "$FP/nas/smoke/evict/remove-me.txt"
stat -f %Sf "$FP/nas/smoke/evict/remove-me.txt"
```

The flags print `-`. In Finder, right-click `remove-me.txt` and record which download
actions the menu offers. The extension's Remove Download activates for an item that
is downloaded (`isDownloaded`) or pinned. Choose it, then:

```bash
stat -f %Sf "$FP/nas/smoke/evict/remove-me.txt"
q "select count(*) as objects from content_objects where uid = $(uid_of remove-me.txt);"
```

Decides: the flags print `dataless` and the count is 0.

| Field | Value |
|-------|-------|
| Finder offered Remove Download for the downloaded file (yes/no) | |
| Download actions in the menu | |
| Flags and object count after it | |
| Result (pass/fail) | |

### AC-15: a read survives a Samba restart

Fixture: `smoke/never-listed/eight-gib.bin`, read over NFS.

```bash
python3 /tmp/cascade-stream.py "$NFS/nas/smoke/never-listed/eight-gib.bin"
```

While it runs, in a second shell: `d restart cascade-smb`. When the stream ends:

```bash
python3 /tmp/cascade-pread.py "$NFS/nas/smoke/never-listed/eight-gib.bin" $((7 << 30)) 1048576
```

Decides: the stream either finishes with a longest stall of at most 15 seconds or
fails at most 15 seconds after its last chunk; the following read succeeds without
restarting the daemon.

| Field | Value |
|-------|-------|
| Stream output | |
| Following read output | |
| Result (pass/fail) | |

### AC-22: an edit that keeps size and last-write time is noticed

Fixture: `smoke/remote/same-size.txt` (`version one`, last-write time 2020-01-02 03:04:05).

```bash
cat "$NFS/nas/smoke/remote/same-size.txt"
q "select content_version from replica_items where name = 'same-size.txt';"
share 'printf "version two\n" > smoke/remote/same-size.txt && touch -d "2020-01-02 03:04:05" smoke/remote/same-size.txt'
sleep 90
q "select content_version from replica_items where name = 'same-size.txt';"
cat "$FP/nas/smoke/remote/same-size.txt"
```

Decides: the content version changed after the scan, and the read through the File
Provider folder returns `version two`. The read uses the File Provider folder because
the NFS client may answer from its own page cache when size and modification time are
unchanged.

| Field | Value |
|-------|-------|
| Content version before and after | |
| Content read | |
| Result (pass/fail) | |

### AC-23: two readers far apart in a dataless file are both served

Fixture: `smoke/media/eight-gib.bin`, dataless and never read.

```bash
python3 /tmp/cascade-pread.py "$FP/nas/smoke/media/eight-gib.bin" $((7 << 30)) 1048576 &
python3 /tmp/cascade-pread.py "$FP/nas/smoke/media/eight-gib.bin" $((1 << 30)) 1048576 &
wait
```

Decides: both reads print a time of at most 15 seconds; their hashes match
`share 'dd if=smoke/media/eight-gib.bin bs=1048576 skip=7168 count=1 2>/dev/null' | shasum -a 256`
and the same with `skip=1024`.

| Field | Value |
|-------|-------|
| Times of the two reads | |
| Hashes match (yes/no) | |
| Result (pass/fail) | |

### AC-24: names SMB cannot hold and symlinks stay on the Mac

```bash
printf 'x\n' > "$FP/nas/smoke/edit/a?b"
ln -s offline-save.txt "$FP/nas/smoke/edit/link"
sleep 15
ls -l "$FP/nas/smoke/edit"
share 'ls -A smoke/edit'
```

In Finder, record the sync status both items show.

Decides: both items remain in the File Provider folder, Finder shows them as excluded
from sync, and the share lists neither `a?b` nor `link`.

| Field | Value |
|-------|-------|
| Status Finder shows | |
| Share listing | |
| Result (pass/fail) | |

### AC-25: two saves while paused become one upload

Fixture: `smoke/edit/upload.bin` (1 GiB, so each upload lasts long enough to observe).

```bash
cat "$FP/nas/smoke/edit/upload.bin" > /dev/null
d pause cascade-smb
dd if=/dev/urandom of="$FP/nas/smoke/edit/upload.bin" bs=1m count=1024
sleep 15
dd if=/dev/urandom of="$FP/nas/smoke/edit/upload.bin" bs=1m count=1024
shasum -a 256 "$FP/nas/smoke/edit/upload.bin"
sleep 15
d unpause cascade-smb
share 'end=$(( $(date +%s) + 420 )); last=
while [ $(date +%s) -lt $end ]; do
    now="temp=$(ls -A smoke/edit | grep -c "^\.cascade-tmp-") inode=$(stat -c %i smoke/edit/upload.bin)"
    [ "$now" != "$last" ] && echo "$(date +%T) $now"
    last=$now; sleep 1
done'
share 'cat smoke/edit/upload.bin' | shasum -a 256
```

The loop runs seven minutes and prints each change of the temporary-file count and
of the target's inode; an upload writes `.cascade-tmp-<uid>` and renames it over the
target, which gives the target a new inode.

Decides: the loop shows one `temp=1` phase and one inode change; the share's hash
equals the hash of the second save.

| Field | Value |
|-------|-------|
| Loop output | |
| Hashes match (yes/no) | |
| Result (pass/fail) | |

### AC-26: two saves a minute apart upload without a download

Fixture: `smoke/edit/twice.txt`.

```bash
cat "$FP/nas/smoke/edit/twice.txt"
watch_flags "$FP/nas/smoke/edit/twice.txt"
```

Keep `watch_flags` running; in a second shell:

```bash
printf 'first save\n' > "$FP/nas/smoke/edit/twice.txt"
sleep 60
printf 'second save\n' > "$FP/nas/smoke/edit/twice.txt"
sleep 15
share 'cat smoke/edit/twice.txt'
```

Decides: the share holds `second save`; `watch_flags` never printed `dataless` and
the inode did not change, so macOS did not download the file again.

| Field | Value |
|-------|-------|
| Content on the share | |
| `watch_flags` output | |
| Result (pass/fail) | |

### AC-27: a rename to a name with `:` is not applied

Fixture: `smoke/edit/rename-colon.txt`.

In Finder, rename `rename-colon.txt` by typing `rename/colon.txt`; Finder shows `/` for
the `:` the file system name holds. Then:

```bash
sleep 10
ls "$FP/nas/smoke/edit"
share 'ls -A smoke/edit; cat smoke/edit/rename-colon.txt'
grep 'rename to a name the backend cannot store left unapplied' "$GROUP/daemon.log"
```

Decides: Finder shows `rename-colon.txt` again; the share still holds
`rename-colon.txt` with its content and no new name; `daemon.log` has the line.

| Field | Value |
|-------|-------|
| Name Finder shows | |
| Share listing | |
| Result (pass/fail) | |

### AC-28: deleting a file changed on the NAS removes it there

Fixture: `smoke/edit/delete-changed.txt`.

```bash
ls "$FP/nas/smoke/edit"
share 'printf "changed on the nas\n" > smoke/edit/delete-changed.txt'
```

In Finder, delete `delete-changed.txt` (Move to Trash, or Delete Immediately), then:

```bash
sleep 15
share 'ls -A smoke/edit'
```

Decides: the share no longer lists `delete-changed.txt`.

| Field | Value |
|-------|-------|
| Share listing | |
| Result (pass/fail) | |

### AC-29: a rename while paused reaches the NAS after unpause

Fixture: `smoke/edit/rename-offline.txt`, listed in Finder.

```bash
d pause cascade-smb
```

In Finder, rename `rename-offline.txt` to `renamed-offline.txt`. Then:

```bash
d unpause cascade-smb; date +%T
share 'ls -A smoke/edit'
```

Repeat the `share` line every 30 seconds for up to five minutes.

Decides: the share lists `renamed-offline.txt` and no `rename-offline.txt`, without
any action in Finder after the unpause.

| Field | Value |
|-------|-------|
| Time from unpause to the rename on the share | |
| Result (pass/fail) | |

## Ranges requested by QuickTime and VLC

Whether players read a File Provider file by ranges decides whether media can move off
the NFS volume. The daemon log does not record requested ranges; the player's reads
are captured with `fs_usage`, and the bytes the daemon fetched from `content_ranges`.

1. Put a large video (several GiB) on the share, for example:

   ```bash
   d cp ~/Movies/<large video>.mov cascade-smb:/storage/smoke/media/video.mov
   ```

   With Docker on another machine, copy the file there first and run `docker cp` there.
2. For each player and path, start from a file the daemon holds no object for (Remove
   Download in Finder, or a fresh copy under another name), then capture 60 seconds:

   ```bash
   sudo fs_usage -w -f filesys -t 60 "QuickTime Player" > /tmp/ranges-quicktime-nfs.txt &
   open -a "QuickTime Player" "$NFS/nas/smoke/media/video.mov"
   ```

   Use `"$FP/nas/smoke/media/video.mov"` for the File Provider folder and `VLC` as the
   process and application name for VLC. Play for a few seconds, then seek to the
   middle once.
3. Find the file descriptor in the `open` line of the video, then list its reads; `B=`
   is the length and `O=` the offset, both hexadecimal:

   ```bash
   grep video.mov /tmp/ranges-quicktime-nfs.txt | grep open
   grep -E ' (pread|read) +F=<fd> ' /tmp/ranges-quicktime-nfs.txt
   ```

4. Record what the daemon fetched, and repeat the query to see whether a whole-file
   download is running:

   ```bash
   objects | grep video.mov
   q "select start, \"end\" from content_ranges where object_id =
      (select object_id from content_objects where uid = $(uid_of video.mov));"
   ```

| Player | Path | First reads (offset, length) | Reads after the seek (offset, length) | Played before the whole file was fetched (yes/no) | Bytes fetched after 60 s | Whole-file download started (yes/no) |
|--------|------|------------------------------|---------------------------------------|---------------------------------------------------|--------------------------|--------------------------------------|
| QuickTime Player | NFS volume | | | | | |
| QuickTime Player | File Provider folder | | | | | |
| VLC | NFS volume | | | | | |
| VLC | File Provider folder | | | | | |

## Live identity suite on the Proxmox NAS

The SMB file ids that Cascade's item identity rests on were measured on Samba over a
virtiofs bind mount. On the NAS, repeat the live suite:

```bash
CASCADE_SMB_TEST=<host>:<port>/<share>/<user>/<password> \
    cargo test -p cascade-backend-smb --test smb_live -- --include-ignored
```

The share needs the suite's fixtures in its root: `big.bin` larger than 4 MiB and
`sub/hello.txt` holding `hello from samba` and a newline.

The tests in `mod container` of `crates/backend-smb/tests/smb_live.rs` change the share
outside SMB, pause it or restart it; they drive a Docker container named `cascade-smb`
on the machine that runs `cargo test`. They run against the NAS only when that
container serves the NAS's storage at `/storage` for uid 1000. Otherwise run the
share-only tests:

```bash
CASCADE_SMB_TEST=<host>:<port>/<share>/<user>/<password> \
    cargo test -p cascade-backend-smb --test smb_live -- --include-ignored --skip container::
```

and check identity through the daemon instead: configure a second backend for the NAS
(a `proxmox.toml` beside `nas.toml` and `[backends.proxmox]` in `config.toml`),
restart Cascade, list a test folder on the NFS volume, then in a shell on the NAS:

1. Rename a file in it.
2. Edit a file in place and restore its last-write time with `touch -d`.
3. Replace a file by renaming another file over it.
4. Reboot the NAS (or restart its SMB service).

After each step wait 90 seconds and read the stable ids:

```bash
q "select uid, name, stable_id, content_version from replica_items where backend_id = 'proxmox' order by name;"
```

A rename, an in-place edit and a reboot keep `stable_id` and `uid`; an in-place edit
mints a new `content_version`; replace-by-rename shows the other file's `stable_id`
under the target name. A `stable_id` that changes on rename, on in-place edit or across
the reboot means the NAS's file system does not keep SMB file ids: the daemon then sees
a delete and a create, gives the file a new uid, fetches its content again and loses
its pin.

| Check | Expected | `stable_id` before | `stable_id` after | uid kept (yes/no) | Result |
|-------|----------|--------------------|-------------------|-------------------|--------|
| Share-only tests | pass | | | | |
| Identity tests (container present) | pass | | | | |
| Rename | id kept | | | | |
| In-place edit, time restored | id kept, new content version | | | | |
| Replace by rename | id of the renamed file | | | | |
| Reboot | id kept | | | | |

| Field | Value |
|-------|-------|
| NAS file system (ZFS, ext4, other) | |
| SMB server and version | |

## Shut down

1. Choose Quit Cascade in the menu bar, then check that the daemon stopped:

   ```bash
   pgrep -fl 'cascade start'
   ls "$GROUP/fp.sock"
   mount | grep "$NFS"
   ```

   `pgrep` and `mount` print nothing and `fp.sock` no longer exists.
2. Stop the share and drop its volume on the machine that runs Docker:

   ```bash
   docker compose -f test/smb/compose.yml down -v
   ```

| Field | Value |
|-------|-------|
| Daemon stopped on Quit (yes/no) | |

## Run record

| Criterion | Result (pass/fail) | Notes |
|-----------|--------------------|-------|
| AC-1 | | |
| AC-2 | | |
| AC-3 | | |
| AC-4 | | |
| AC-5 | | |
| AC-6 | | |
| AC-7 | | |
| AC-8 | | |
| AC-9 | | |
| AC-10 | | |
| AC-11 | | |
| AC-12 | | |
| AC-13 | | |
| AC-14 | | |
| AC-15 | | |
| AC-22 | | |
| AC-23 | | |
| AC-24 | | |
| AC-25 | | |
| AC-26 | | |
| AC-27 | | |
| AC-28 | | |
| AC-29 | | |

## Troubleshooting

- The menu bar reads "No App Group container: the app is not signed with the team
  identity." The build was not team-signed; rebuild with `DEVELOPMENT_TEAM`.
- The menu bar reads "The daemon did not start: …" or "The daemon exited with status
  …; restarting." Read `daemon.log` in the container; a missing `config.toml` or
  backend file, or a socket path over 103 bytes, stops the daemon at start.
- `codesign` fails with `errSecInternalComponent`: the build runs over ssh. Build in a
  local GUI session.
- The extension fails to launch after a week: the Personal Team profile expired.
  Rebuild and copy the app to `/Applications` again.
- System-side File Provider errors: `log stream --predicate 'subsystem == "com.apple.FileProvider"'`.
  The host app logs login-item failures under the subsystem
  `io.cascade.CascadeFileProviderHost` and signal relay failures under
  `io.cascade.CascadeFileProviderKit`.
