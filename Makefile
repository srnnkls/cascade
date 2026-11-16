.PHONY: start stop build dev debug release fileprovider-build fileprovider-app-path fileprovider-smoke wasm

BINARY := ./target/release/cascade

# Always defer to cargo for build decisions — cargo's incremental
# compilation makes a no-op build cheap, and a file-existence check
# would silently run a stale binary after source edits.
release:
	cargo build --release

start: release
	exec $(BINARY) start

stop:
	$(BINARY) stop

build:
	cargo build

dev:
	exec env RUST_LOG="$${RUST_LOG:-debug}" cargo watch -x "run -- start"

debug: release
	exec env RUST_LOG="$${RUST_LOG:-debug}" $(BINARY) start

CONFIGURATION ?= Debug
XCODEBUILD_HOST = xcodebuild \
	-project swift/CascadeFileProvider.xcodeproj \
	-scheme CascadeFileProviderHost \
	-configuration $(CONFIGURATION) \
	-destination "platform=macOS" \
	DEVELOPMENT_TEAM=$(TEAM)
HOST_APP_PATH = $$($(XCODEBUILD_HOST) -showBuildSettings 2>/dev/null | awk '$$1 == "BUILT_PRODUCTS_DIR" {print $$3; exit}')/CascadeFileProviderHost.app

# Signed build of the macOS File Provider host app with the embedded extension
# and cascade daemon. Requires macOS 26+, Xcode with an Apple account signed in
# and TEAM=<development team id>; CONFIGURATION defaults to Debug.
fileprovider-build:
	@test -n "$(TEAM)" || { echo "fileprovider-build: set TEAM=<development team id>" >&2; exit 1; }
	$(XCODEBUILD_HOST) -allowProvisioningUpdates build

# Prints the path of the built CascadeFileProviderHost.app and nothing else,
# for APP="$$(make -s fileprovider-app-path TEAM=<team id>)".
fileprovider-app-path:
	@test -n "$(TEAM)" || { echo "fileprovider-app-path: set TEAM=<development team id>" >&2; exit 1; }
	@echo "$(HOST_APP_PATH)"

# Builds and opens the host app, a menu-bar agent that registers the Cascade
# File Provider domain and starts the embedded daemon itself. The manual
# checklist is docs/fileprovider-smoke-test.md.
fileprovider-smoke: fileprovider-build
	open "$(HOST_APP_PATH)"

# Build the cascade-wasm crate to wasm32-unknown-unknown and run wasm-bindgen
# to generate the JS glue in crates/cascade-wasm/pkg/.
wasm:
	bash scripts/build-wasm.sh
