#!/usr/bin/env bash
# End-to-end check with a real psql client. sslmode=prefer exercises the
# SSLRequest -> 'N' -> plaintext fallback path.
set -euo pipefail
cd "$(dirname "$0")/.."

if ! command -v psql >/dev/null; then
    echo "SKIP: psql not installed"
    exit 0
fi

PORT="${1:-54333}"
cargo build -p crabgresql
./target/debug/crabgresql --listen "127.0.0.1:${PORT}" &
SERVER_PID=$!
trap 'kill "$SERVER_PID" 2>/dev/null || true' EXIT
sleep 1

out=$(psql "host=127.0.0.1 port=${PORT} user=crab dbname=crab sslmode=prefer" -tAc 'SELECT 1')
if [ "$out" = "1" ]; then
    echo "PASS: psql SELECT 1 -> ${out}"
else
    echo "FAIL: expected '1', got '${out}'" >&2
    exit 1
fi
