.PHONY: start stop build dev debug release fileprovider-smoke wasm

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

# Build the macOS File Provider host app and extension, then open the host
# app so the developer can click "Register File Provider" to register the
# Cascade domain with macOS. No automated registration — that step requires
# an interactive UI click in the host app window.
#
# Prerequisites: macOS 26+, Xcode, and an Apple development team id passed as
# TEAM=<team id> (signing is automatic). See docs/fileprovider-smoke-test.md
# for the full step-by-step guide including backend setup and Finder operations.
fileprovider-smoke:
	@test -n "$(TEAM)" || { echo "fileprovider-smoke: set TEAM=<development team id>"; exit 1; }
	xcodebuild \
		-project swift/CascadeFileProvider.xcodeproj \
		-scheme CascadeFileProviderHost \
		-configuration Debug \
		-destination "platform=macOS" \
		-allowProvisioningUpdates \
		DEVELOPMENT_TEAM=$(TEAM) \
		build
	open "$$(xcodebuild \
		-project swift/CascadeFileProvider.xcodeproj \
		-scheme CascadeFileProviderHost \
		-configuration Debug \
		-destination "platform=macOS" \
		-showBuildSettings \
		DEVELOPMENT_TEAM=$(TEAM) \
		2>/dev/null \
		| awk '/BUILT_PRODUCTS_DIR/{print $$3}')/CascadeFileProviderHost.app"

# Build the cascade-wasm crate to wasm32-unknown-unknown and run wasm-bindgen
# to generate the JS glue in crates/cascade-wasm/pkg/.
wasm:
	bash scripts/build-wasm.sh
