#!/bin/bash
# Xcode build phase of CascadeFileProviderHost: compiles the cascade daemon
# with cargo, places it in the app's Contents/MacOS and, when the build signs,
# signs it with the app's identity before Xcode seals the bundle.
set -euo pipefail

if [[ -f "$HOME/.cargo/env" ]]; then
    # shellcheck source=/dev/null
    source "$HOME/.cargo/env"
fi

repo_root="$(cd "$SRCROOT/.." && pwd)"
export CARGO_TARGET_DIR="$repo_root/target"

case "$CONFIGURATION" in
    Release)
        cargo build --manifest-path "$repo_root/Cargo.toml" --package cascade --bin cascade --release
        built="$CARGO_TARGET_DIR/release/cascade"
        ;;
    *)
        cargo build --manifest-path "$repo_root/Cargo.toml" --package cascade --bin cascade
        built="$CARGO_TARGET_DIR/debug/cascade"
        ;;
esac

destination="$TARGET_BUILD_DIR/$EXECUTABLE_FOLDER_PATH/cascade"
mkdir -p "${destination%/*}"
install -m 0755 "$built" "$destination"

if [[ "${CODE_SIGNING_ALLOWED:-NO}" == YES && -n "${EXPANDED_CODE_SIGN_IDENTITY:-}" ]]; then
    codesign --force --sign "$EXPANDED_CODE_SIGN_IDENTITY" --options runtime --timestamp=none "$destination"
fi
