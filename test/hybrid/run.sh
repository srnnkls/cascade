#!/usr/bin/env bash
# Hybrid tree tests: one SMB share, part served by Cascade, part replicated
# by Mutagen, kept apart by `.cascade` ignore rules.
#
# Run from the repository root: test/hybrid/run.sh [test/hybrid/<file>.md ...]
#
# Builds target/debug/cascade, runs every h*.md document (or those given)
# under `timeout -k 5 600`, then tears down any run a killed document left.

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd -P)"
HYBRID_REGISTRY="$(mktemp -d /tmp/hybrid-registry.XXXXXX)"
export HYBRID_REGISTRY

cleanup() {
  local entry
  for entry in "$HYBRID_REGISTRY"/*; do
    [[ -f "$entry" ]] || continue
    (
      TESTDIR="$HERE"
      # shellcheck source=lib.sh
      source "$HERE/lib.sh"
      hybrid_teardown "$(cat "$entry")"
      [[ ! -d "$(cat "$entry")" ]] || trash "$(cat "$entry")"
    )
  done
  trash "$HYBRID_REGISTRY"
}
trap cleanup EXIT

documents=("$@")
if ((${#documents[@]} == 0)); then
  documents=("$HERE"/h*.md)
fi

timeout -k 5 600 bash -c 'cargo build -p cascade --quiet && scrut test "$@"' _ "${documents[@]}"
