# macOS File Provider smoke-test guide

This guide walks through building the Cascade File Provider host app and extension,
registering the domain with macOS, and exercising the full Finder workflow against a
local backend. All commands are copy-pasteable. No cloud account is required — the
local backend exposes an ordinary directory over the File Provider bridge so every
step is fully offline-testable.

The [walking-skeleton check](#walking-skeleton-check-ac-1) at the end records the
team-signed build against the Docker Samba share.

## Prerequisites

- macOS 26 or later — the deployment target of the host app and the extension.
- Xcode 26 or later, signed in to an Apple account under Xcode → Settings → Accounts.
- A development team id (the ten-character id shown in Xcode's account settings, or
  under Membership on developer.apple.com). Signing is automatic; the team id is the
  only setting you supply. The extension and the host share the App Group
  `<team id>.group.io.cascade.shared`, which only a team-signed build carries, and
  the extension finds the daemon socket in that group's container. An ad-hoc or
  unsigned build compiles but cannot reach the daemon.

- The Rust toolchain at the version pinned in `rust-toolchain.toml` — `rustup` installs
  it on first use.

## Step 1 — Build the Rust engine

Build the File Provider presenter alongside the rest of the workspace:

```bash
cargo build --release
```

This produces `target/release/cascade`. The presenter code lives in
`crates/presenter-fileprovider/` and is compiled into the binary as a feature; no
separate build step is needed.

## Step 2 — Build the host app and extension

Run the build in a local session on the Mac (not over `ssh`, where the keychain holding
the signing key is locked):

```bash
xcodebuild \
    -project swift/CascadeFileProvider.xcodeproj \
    -scheme CascadeFileProviderHost \
    -configuration Debug \
    -destination "platform=macOS" \
    -allowProvisioningUpdates \
    DEVELOPMENT_TEAM=<your-team-id> \
    build
```

`-allowProvisioningUpdates` lets Xcode register the bundle ids and the App Group and
create the development profiles on first use.

The artefacts land in Xcode's standard DerivedData tree:

```
~/Library/Developer/Xcode/DerivedData/CascadeFileProvider-*/Build/Products/Debug/
    CascadeFileProviderHost.app/
        Contents/
            PlugIns/
                CascadeFileProvider.appex/
```

Confirm the App Group made it into the extension's signature:

```bash
codesign -d --entitlements - \
    ~/Library/Developer/Xcode/DerivedData/CascadeFileProvider-*/Build/Products/Debug/\
CascadeFileProviderHost.app/Contents/PlugIns/CascadeFileProvider.appex
```

The output lists `com.apple.security.application-groups` with
`<your-team-id>.group.io.cascade.shared`.

## Step 3 — Configure a local backend

Create a scratch directory to act as the backend root, then write a minimal backend
config. This avoids any OAuth flow and works entirely offline.

```bash
mkdir -p ~/CascadeSmokeTest
mkdir -p ~/.config/cascade

cat > ~/.config/cascade/local-smoke.toml <<'EOF'
type = "local"
root_path = "/Users/$USER/CascadeSmokeTest"
EOF
```

Add the backend to `config.toml` so `cascade start` picks it up:

```bash
cat > ~/.config/cascade/config.toml <<'EOF'
[backends.local-smoke]
type = "local"
EOF
```

## Step 4 — Start the Cascade daemon

The File Provider bridge is an explicit opt-in via `--file-provider`. Without the flag,
`cascade start` uses FSKit on macOS (the default native presenter), which does not open
the File Provider socket. The two are alternatives, not a fallback chain — both are
macOS-native and self-mounting — so pick one:

```bash
GROUP=~/Library/Group\ Containers/<your-team-id>.group.io.cascade.shared
mkdir -p "$GROUP"
./target/release/cascade start --file-provider --app-group "$GROUP"
```

Unlike the FSKit and WebDAV strategies, this does not mount anything itself. It stands up
the File Provider RPC server and runs the sync runner so the state database and the
engine's change feed stay populated; the Swift extension owns the Finder mount once its
domain is registered (step 5). The daemon prints the RPC socket path on startup; state,
cache and staging live in the same container.

Confirm the socket exists before continuing:

```bash
ls -l "$GROUP/fp.sock"
```

If the socket is missing, the daemon did not start the File Provider server — check that
you passed `--file-provider` and that no other `cascade` daemon is already running
(`./target/release/cascade status`).

## Step 5 — Register the File Provider domain

Open the host app built in step 2:

```bash
open ~/Library/Developer/Xcode/DerivedData/CascadeFileProvider-*/Build/Products/Debug/CascadeFileProviderHost.app
```

In the window that appears, click **Register File Provider**. The status label should
change to "Cascade domain registered. Look for it under Locations in Finder."

If the button reports an error, check the system log (see step 9 below) for the
`com.cascade.fileprovider` subsystem.

To verify registration from the command line:

```bash
pluginkit -m -A -p com.apple.fileprovider-ui | grep -i cascade
```

You should see `io.cascade.CascadeFileProviderHost.FileProvider` in the output.

## Step 6 — Finder operations

Open a Finder window (`⌘N`). Under **Locations** in the sidebar you should see a
**Cascade** entry. Click it to open the domain root. The engine enumerates the contents
of `~/CascadeSmokeTest` through the bridge.

### Create a file

```bash
echo "smoke test content" > ~/CascadeSmokeTest/hello.txt
```

Switch to the Finder window and press `⌘R` to refresh. `hello.txt` should appear.

### Rename

Select `hello.txt` in Finder, press Return to enter rename mode, and type
`hello-renamed.txt`. Confirm. The engine receives a `moveItem` RPC with the new name.

Check the log stream for the engine signal:

```bash
log stream \
    --predicate 'subsystem == "com.cascade.fileprovider"' \
    --style compact \
    2>&1 | grep -E "moveItem|modifyItem"
```

### Move

Create a subdirectory and drag the file into it:

```bash
mkdir ~/CascadeSmokeTest/subdir
```

In Finder, drag `hello-renamed.txt` into the `subdir` folder. The engine receives a
`moveItem` RPC with `new_parent_id` set to the subdir's identifier.

### Delete

Select `hello-renamed.txt` inside `subdir` in Finder and press `⌘Delete`. The engine
receives a `deleteItem` RPC. Confirm the file no longer exists in the backend:

```bash
ls ~/CascadeSmokeTest/subdir/
```

### Create a directory

In Finder, use `⇧⌘N` (New Folder) while inside the Cascade location to create a
directory. The engine receives a `createDirectory` RPC.

### Nested directory move (cross-backend)

If a second backend is configured (e.g. a second local backend at a different
`root_path`), you can test a cross-backend move by dragging a directory from one
backend's mount path to the other. The engine decomposes the move into a recursive
copy followed by a delete of the source tree. The File Provider extension sees a
standard `moveItem` — the cross-backend decomposition happens inside the VFS layer.

To add a second local backend for this test:

```bash
mkdir -p ~/CascadeSmokeTest2

cat > ~/.config/cascade/local-smoke2.toml <<'EOF'
type = "local"
root_path = "/Users/$USER/CascadeSmokeTest2"
mount_path = "Second"
EOF
```

Append to `config.toml`:

```toml
[backends.local-smoke2]
type = "local"
```

Restart the daemon (`./target/release/cascade stop && ./target/release/cascade start`) and
then drag a directory from the Cascade root into the `Second/` folder in Finder.

### Large-directory enumeration (pagination)

Create a directory with enough entries to cross the engine's page boundary (currently
the engine returns 100 items per page):

```bash
mkdir ~/CascadeSmokeTest/large
for i in $(seq 1 250); do touch ~/CascadeSmokeTest/large/file-$i.txt; done
```

Navigate into `large/` in Finder. As you scroll, the File Provider framework calls
`enumerateItems(for:startingAt:)` repeatedly with the page cursor the engine returned
in the previous call. You can observe the page-cursor round-trips in the log:

```bash
log stream \
    --predicate 'subsystem == "com.cascade.fileprovider"' \
    --style compact \
    2>&1 | grep -i "enumerate"
```

You should see multiple calls, each carrying a different `page` parameter.

### Offline-then-reconnect — enumerateChanges delta

This exercises the `enumerateChanges` path and the sync-anchor cursor.

1. Stop the daemon while Finder still has the domain open:

   ```bash
   ./target/release/cascade stop
   ```

2. While offline, add a file directly to the backend root:

   ```bash
   echo "added offline" > ~/CascadeSmokeTest/new-offline.txt
   ```

3. Restart the daemon:

   ```bash
   ./target/release/cascade start
   ```

4. In Finder, navigate away from the Cascade location and then back to it. The system
   detects that its cached sync anchor no longer matches the engine's current cursor
   and calls `enumerateChanges(from:)`. The extension fetches the delta — `new-offline.txt`
   appears as added — and reports the new anchor.

   Confirm the engine signal in the log:

   ```bash
   log stream \
       --predicate 'subsystem == "com.cascade.fileprovider"' \
       --style compact \
       2>&1 | grep -E "enumerateChanges|syncAnchor"
   ```

   You should see a `enumerateChanges RPC` entry and, shortly after, `new-offline.txt`
   appearing in the Finder window without a full re-enumeration.

## Step 7 — Remove the domain

When finished testing, remove the domain so macOS does not retain a stale registration:

In the host app, click **Remove**. Or from the command line:

```bash
pluginkit -r \
    -p com.apple.fileprovider-ui \
    ~/Library/Developer/Xcode/DerivedData/CascadeFileProvider-*/Build/Products/Debug/CascadeFileProviderHost.app
```

Then stop the daemon:

```bash
./target/release/cascade stop
```

## Step 8 — Clean up test data

```bash
rm -rf ~/CascadeSmokeTest ~/CascadeSmokeTest2
rm ~/.config/cascade/local-smoke.toml ~/.config/cascade/local-smoke2.toml
```

## Step 9 — Reading the log stream

All extension log entries use `subsystem = "com.cascade.fileprovider"`. Two
categories are useful:

- `ActionHandler` — logs every RPC sent to the engine, with method name, item
  identifier, and any error code the engine returns.
- `FileProviderEnumerator` — logs sync-anchor decode failures and `enumerateChanges`
  RPC errors.

Live stream, compact format:

```bash
log stream \
    --predicate 'subsystem == "com.cascade.fileprovider"' \
    --style compact
```

Filter to a specific method:

```bash
log stream \
    --predicate 'subsystem == "com.cascade.fileprovider" AND eventMessage CONTAINS "moveItem"' \
    --style compact
```

Show the last minute of stored entries (useful after the fact):

```bash
log show \
    --last 1m \
    --predicate 'subsystem == "com.cascade.fileprovider"' \
    --style compact
```

Errors the system surfaces in Finder (the "Unable to…" dialogues) correspond to
`NSFileProviderError` codes. The mapping from engine error codes to File Provider
errors is in `swift/CascadeFileProvider/Sources/ActionHandler.swift` under
`makeError(from:method:itemID:)`.

## Expected outcomes summary

| Operation | Engine RPC | Log signal |
|-----------|-----------|-----------|
| Directory listing | `enumerateItems` | `FileProviderEnumerator: enumerateItems` |
| File open / download | `fetchContents` | `ActionHandler: fetchContents` |
| Create file | `importDocument` | `ActionHandler: importDocument` |
| New folder | `createDirectory` | `ActionHandler: createDirectory` |
| Rename | `moveItem` (same parent) | `ActionHandler: moveItem` |
| Move | `moveItem` (new parent) | `ActionHandler: moveItem` |
| Delete | `deleteItem` | `ActionHandler: deleteItem` |
| Pagination | `enumerateItems` (repeated) | multiple `enumerateItems` entries |
| Reconnect delta | `enumerateChanges` | `enumerateChanges RPC` |

## Troubleshooting

**Domain does not appear in Finder sidebar.** The extension process may not have
started. Check Activity Monitor for `CascadeFileProvider` (it should appear as a
child of `fileproviderd`). If absent, check the system log for
`subsystem == "com.apple.fileprovider"` for load errors.

**"Register File Provider" returns an error.** The most common cause is a
code-signing problem — the `.appex` must be team-signed for `NSFileProviderManager.add`
to accept it. Rebuild with your team id (step 2) and check the signature:

```bash
codesign -dv \
    ~/Library/Developer/Xcode/DerivedData/CascadeFileProvider-*/Build/Products/Debug/\
CascadeFileProviderHost.app/Contents/PlugIns/CascadeFileProvider.appex
```

**Finder shows "Unable to connect"** or the extension crashes immediately after
registration. The daemon is not running, the socket does not exist, or the extension
was built without the App Group. Run `./target/release/cascade start --file-provider
--app-group "$GROUP"` and confirm `"$GROUP/fp.sock"` exists before clicking Register.
The extension logs `no App Group container is entitled` when its signature carries no
App Group.

**enumerateChanges never fires.** The system only calls `enumerateChanges` when it
believes the sync anchor may be stale. Navigate away from the Cascade location and
back, or trigger a full sync via Finder → Go → Connect to Server (this reloads the
domain). If the engine restarted between calls the anchor it held in memory is gone
and the extension falls back to a full re-enumeration — this is correct behaviour,
not a bug.

## Walking-skeleton check (AC-1)

This check requires the `smb` backend type in the CLI (SEC-008); run it once that has
landed. Before then the daemon refuses `type = "smb"` and does not start.

Lists a folder of the Docker Samba share through a team-signed build and confirms the
entries are dataless placeholders whose listing read no file content.

1. Start the share (needs Docker):

   ```bash
   docker compose -f test/smb/compose.yml up -d --wait
   ```

2. Configure the share as the only backend:

   ```bash
   mkdir -p ~/.config/cascade
   cat > ~/.config/cascade/config.toml <<'EOF'
   [backends.nas]
   type = "smb"
   EOF
   cat > ~/.config/cascade/nas.toml <<'EOF'
   type = "smb"
   id = "nas"
   host = "127.0.0.1"
   port = 1445
   share = "data"
   username = "cascade"
   password = "cascade"
   EOF
   ```

3. Build and sign the host app with your team id (step 2), then start the daemon in the
   App Group container with debug logging, keeping the log:

   ```bash
   GROUP=~/Library/Group\ Containers/<your-team-id>.group.io.cascade.shared
   mkdir -p "$GROUP"
   ./target/release/cascade start --file-provider --app-group "$GROUP" -v \
       2>&1 | tee /tmp/cascade-ac1.log
   ```

4. Open the host app and click Register File Provider (step 5).

5. In Finder, open Cascade under Locations, then `nas`, then `sub`. The root shows
   `nas`; `sub` shows `hello.txt`.

6. Check that the listed file is a dataless placeholder:

   ```bash
   stat -f %Sf ~/Library/CloudStorage/*Cascade*/nas/sub/hello.txt
   ```

   The output contains `dataless`. Run this before opening the file; opening it
   downloads the content.

7. Check that no file content was read while listing. The extension logs every RPC it
   sends, and the daemon logs `fetchContents` with the item id before every backend
   content download:

   ```bash
   log show --last 10m --info \
       --predicate 'subsystem == "com.cascade.fileprovider"' \
       --style compact | grep fetchContents
   grep fetchContents /tmp/cascade-ac1.log
   ```

   Both commands print nothing.

8. Record the result:

   | Field | Value |
   |-------|-------|
   | Date | |
   | macOS version | |
   | Xcode version | |
   | Team type (Personal Team or paid Apple Developer Program) | |
   | Team id | |
   | Root listed `nas` (yes/no) | |
   | `stat -f %Sf` output for `hello.txt` | |
   | Content reads in the extension and daemon logs (none, or the lines) | |
   | Result (pass/fail) | |
   | Notes | |
