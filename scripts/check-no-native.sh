#!/usr/bin/env bash
# Verifies the SHIPPED dependency tree (normal + build deps of the node binary)
# contains no native code. Dev-dependencies (test oracle tooling) are exempt
# per the spec's safety policy.
# The conformance crate is intentionally NOT checked: it is test tooling.
set -euo pipefail
cd "$(dirname "$0")/.."
# The `-sys` suffix conventionally marks a C-library wrapper, but a few crates
# use it while being pure Rust. linux-raw-sys is pure-Rust raw Linux syscall
# bindings (the rustix project's libc-free backend) — no C, no build-time
# compilation — so it is allowlisted. It only appears on Linux targets (CI),
# which is why it is invisible on a macOS dev box.
allow='^linux-raw-sys$'
bad=$(cargo tree -p crabgresql -e normal,build --prefix none \
    | awk '{print $1}' | sort -u \
    | grep -E '(^cc$|-sys$)' \
    | grep -vE "$allow" || true)
if [ -n "$bad" ]; then
    echo "FAIL: native-code crates in shipped dependency tree:" >&2
    echo "$bad" >&2
    exit 1
fi
echo "OK: shipped dependency tree is pure Rust"
