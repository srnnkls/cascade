# Shared setup, waits and teardown of the hybrid tree tests, sourced by the
# scrut documents beside it. One run owns:
#   - a temporary root under scrut's TMPDIR with the config directory, the
#     App Group container, the mount point and Mutagen's local directory;
#   - /storage/$RUN on the share, of which $IGN is ignored by Cascade and
#     replicated by Mutagen session $SESSION.
#
# The mount point's `.cascade` holds an anchored rule for the replicated
# folder (`/nas/$RUN/mirror/`) and a name rule (`scratch`). The share holds
# for each run:
#   mirror/seed.txt          ignored, anchored rule; replicated by Mutagen
#   scratch                  ignored, name rule at depth one
#   deep/scratch/inner.txt   ignored, name rule on a folder at depth two
#   deep/kept.txt            served
#   keep/mirror/kept.txt     served: the anchored rule does not reach it
#   plain.txt move-a.txt move-b.txt   served

HYBRID_REPO=$(cd "$TESTDIR/../.." && pwd -P)
HYBRID_BUILT=$HYBRID_REPO/target/debug/cascade
HYBRID_CONTAINER=cascade-smb
HYBRID_WAIT=${HYBRID_WAIT:-30}

hybrid_require() {
  local tool
  for tool in docker mutagen sqlite3 scrut timeout; do
    command -v "$tool" >/dev/null 2>&1 || {
      echo "missing prerequisite: $tool" >&2
      return 1
    }
  done
  [[ "$(docker inspect -f '{{.State.Running}}' "$HYBRID_CONTAINER" 2>/dev/null)" == true ]] || {
    echo "missing prerequisite: running docker container $HYBRID_CONTAINER (docker compose -f test/smb/compose.yml up -d --wait)" >&2
    return 1
  }
  [[ -x "$HYBRID_BUILT" ]] || {
    echo "missing prerequisite: $HYBRID_BUILT (cargo build -p cascade)" >&2
    return 1
  }
}

# Runs `$@` until it succeeds, for at most `$1` seconds.
poll() {
  local seconds=$1
  shift
  local deadline=$((SECONDS + seconds))
  until "$@" >/dev/null 2>&1; do
    if ((SECONDS >= deadline)); then
      echo "timed out after ${seconds}s waiting for: $*" >&2
      return 1
    fi
    sleep 0.2
  done
}

# A shell command inside the share's container, as the share's owner.
nas() {
  timeout 30 docker exec -u 1000:1000 "$HYBRID_CONTAINER" sh -c "$1"
}

# Every path below the run's directory on the share, relative and sorted.
nas_tree() {
  nas "cd /storage/$RUN && find . -mindepth 1 | sed 's#^\./##' | LC_ALL=C sort"
}

nas_has() {
  nas "test -e '/storage/$RUN/$1'"
}

# Whether the file `$1` below the run's directory holds the line `$2`.
nas_is() {
  [[ "$(nas "cat '/storage/$RUN/$1'")" == "$2" ]]
}

# A bounded command on the volume, which a stuck NFS server must not hang.
vol() {
  timeout 20 "$@"
}

db() {
  sqlite3 -cmd '.timeout 5000' "$GRP/state.db" "$1"
}

# Paths of the replica_items rows below the run's directory, relative and
# sorted.
replica_paths() {
  db "WITH RECURSIVE t(uid, path) AS (
        SELECT uid, '' FROM replica_items WHERE name = '$RUN'
        UNION ALL
        SELECT r.uid, CASE WHEN t.path = '' THEN r.name ELSE t.path || '/' || r.name END
          FROM replica_items r JOIN t ON r.parent_uid = t.uid)
      SELECT path FROM t WHERE path <> '' ORDER BY path;"
}

# The replica rows of ignored paths: the replicated folder and every
# `scratch`, with everything beneath them.
ignored_rows() {
  replica_paths | grep -E "^$IGN(/|\$)|(^|/)$SCRATCH(/|\$)" || true
}

writeback_idle() {
  [[ "$(db "SELECT count(*) FROM content_objects WHERE dirty")" == 0 ]]
}

# Runs a command on the volume; prints `ok`, or `refused` and the error
# text after its last colon.
attempt() {
  local out
  if out=$(timeout 20 "$@" 2>&1); then
    echo ok
  else
    echo "refused:${out##*:}"
  fi
}

# Bytes of large.bin present in the content cache once no transfer adds
# more for a second, at most 30 seconds.
large_cached_bytes() {
  local query="SELECT coalesce(sum(cr.\"end\" - cr.start), 0)
    FROM content_ranges cr JOIN content_objects o USING (object_id)
    JOIN replica_items r ON r.uid = o.uid WHERE r.name = 'large.bin';"
  local before after deadline=$((SECONDS + 30))
  before=$(db "$query")
  while sleep 1; after=$(db "$query"); [[ "$after" != "$before" ]]; do
    if ((SECONDS >= deadline)); then
      break
    fi
    before=$after
  done
  echo "$after"
}

# The content cache rescans listed folders once a minute and signals no
# finished scan, so this waits one interval with margin.
wait_scan_interval() {
  sleep 65
}

# Every path below a local directory, relative and sorted.
local_tree() {
  (cd "$1" && find . -mindepth 1 | sed 's#^\./##' | LC_ALL=C sort)
}

# Creates the run's directories, config, `.cascade` rules and share layout.
hybrid_setup() {
  export RUN="hybrid-$$"
  export IGN=mirror
  export SCRATCH=scratch
  export ROOT
  ROOT=$(mkdir -p "$TMPDIR/run" && cd "$TMPDIR/run" && pwd -P)
  export CFG="$ROOT/cfg" GRP="$ROOT/grp" MNT="$ROOT/mnt" LOCAL="$ROOT/local"
  export VOL="$MNT/nas/$RUN"
  export SESSION="cascade-$RUN"
  mkdir -p "$CFG" "$GRP" "$MNT" "$LOCAL"
  # The build cache behind target/ may evict the binary while it runs.
  export HYBRID_BIN="$ROOT/cascade"
  cp -c "$HYBRID_BUILT" "$HYBRID_BIN"
  cat >"$CFG/config.toml" <<EOF
[backends.nas]
type = "smb"
mount = "nas"

[mount]
point = "$MNT"
EOF
  cat >"$CFG/nas.toml" <<EOF
type = "smb"
id = "nas"
host = "127.0.0.1"
port = 1445
share = "data"
username = "cascade"
password = "cascade"
EOF
  printf '/nas/%s/%s/\n%s\n' "$RUN" "$IGN" "$SCRATCH" >"$MNT/.cascade"
  if [[ -n "${HYBRID_REGISTRY:-}" ]]; then
    printf '%s\n' "$ROOT" >"$HYBRID_REGISTRY/$RUN"
  fi
  declare -p RUN IGN SCRATCH ROOT CFG GRP MNT LOCAL VOL SESSION HYBRID_BIN >"$ROOT/env"
  nas "mkdir -p /storage/$RUN/$IGN /storage/$RUN/deep/$SCRATCH /storage/$RUN/keep/$IGN \
    && printf 'seed\n' > /storage/$RUN/$IGN/seed.txt \
    && printf 'nas scratch\n' > /storage/$RUN/$SCRATCH \
    && printf 'inner\n' > /storage/$RUN/deep/$SCRATCH/inner.txt \
    && printf 'kept\n' > /storage/$RUN/deep/kept.txt \
    && printf 'kept\n' > /storage/$RUN/keep/$IGN/kept.txt \
    && printf 'plain\n' > /storage/$RUN/plain.txt \
    && printf 'move a\n' > /storage/$RUN/move-a.txt \
    && printf 'move b\n' > /storage/$RUN/move-b.txt"
}

seed_large() {
  nas "dd if=/dev/urandom of=/storage/$RUN/large.bin bs=1M count=64 2>/dev/null"
}

hybrid_mounted() {
  mount | grep -qF " on $MNT ("
}

daemon_pid() {
  cat "$ROOT/daemon.pid" 2>/dev/null
}

daemon_alive() {
  local pid
  pid=$(daemon_pid) && kill -0 "$pid" 2>/dev/null
}

# Starts the daemon and waits until it mounted the volume.
hybrid_start() {
  nohup "$HYBRID_BIN" --config "$CFG" --app-group "$GRP" start \
    </dev/null >>"$ROOT/daemon.log" 2>&1 &
  echo $! >"$ROOT/daemon.pid"
  local deadline=$((SECONDS + HYBRID_WAIT))
  until hybrid_mounted && [[ -S "$GRP/fp.sock" ]]; do
    if ! daemon_alive || ((SECONDS >= deadline)); then
      echo "the daemon did not mount $MNT; daemon.log:" >&2
      tail -n 20 "$ROOT/daemon.log" >&2
      return 1
    fi
    sleep 0.2
  done
}

# Stops the daemon with SIGTERM and waits for its exit and the unmount.
hybrid_stop() {
  local pid
  pid=$(daemon_pid) || return 0
  pkill -TERM -P "$pid" 2>/dev/null || true
  kill -TERM "$pid" 2>/dev/null || true
  poll "$HYBRID_WAIT" bash -c "! kill -0 $pid" || {
    kill -KILL "$pid" 2>/dev/null || true
    poll 5 bash -c "! kill -0 $pid" || true
  }
  if hybrid_mounted; then
    /usr/sbin/diskutil unmount force "$MNT" >/dev/null 2>&1 || umount -f "$MNT" 2>/dev/null || true
  fi
  rm -f "$ROOT/daemon.pid"
}

sync_create() {
  mutagen sync create --name="$SESSION" --label="cascade-hybrid=$RUN" \
    --sync-mode=two-way-safe \
    --default-owner-beta=id:1000 --default-group-beta=id:1000 \
    "$LOCAL" "docker://$HYBRID_CONTAINER/storage/$RUN/$IGN" >/dev/null
}

sync_flush() {
  timeout 60 mutagen sync flush "$SESSION" >/dev/null
}

# Conflict and problem counts of the session.
sync_health() {
  mutagen sync list "$SESSION" --template '{{range .}}conflicts={{len .Conflicts}} alpha-scan={{len .Alpha.ScanProblems}} alpha-transition={{len .Alpha.TransitionProblems}} beta-scan={{len .Beta.ScanProblems}} beta-transition={{len .Beta.TransitionProblems}} error={{.LastError}}
{{end}}' | sed '/^$/d'
}

# Undoes everything the run created. Takes the run's root when called
# outside the document, as run.sh does after an aborted document.
hybrid_teardown() {
  if [[ -n "${1:-}" ]]; then
    [[ -f "$1/env" ]] || return 0
    # shellcheck source=/dev/null
    source "$1/env"
  fi
  [[ -n "${RUN:-}" && -n "${ROOT:-}" ]] || return 0
  hybrid_stop
  mutagen sync terminate "$SESSION" >/dev/null 2>&1 || true
  timeout 30 docker exec "$HYBRID_CONTAINER" rm -rf "/storage/$RUN" || true
  if ! hybrid_mounted; then
    rm -rf "$ROOT"
  fi
}

# Reports whatever of the run outlived the teardown.
hybrid_leftovers() {
  mount | grep -F "$MNT" || true
  pgrep -fl -- "--config $CFG" || true
  mutagen sync list --label-selector="cascade-hybrid=$RUN" --template '{{range .}}session {{.Name}}
{{end}}' | grep . || true
  timeout 30 docker exec "$HYBRID_CONTAINER" sh -c "test ! -e /storage/$RUN || echo 'share directory /storage/$RUN'"
  [[ ! -e "$ROOT" ]] || echo "directory $ROOT"
}
