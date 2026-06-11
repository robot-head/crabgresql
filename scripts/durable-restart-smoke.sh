#!/usr/bin/env bash
# Durability across a real binary restart: insert, kill, restart on the same
# data dir, select the data back. Mirrors psql-smoke.sh's structure.
set -euo pipefail
cd "$(dirname "$0")/.."

if ! command -v psql >/dev/null; then
    echo "SKIP: psql not installed"
    exit 0
fi

PORT="${1:-54337}"
DATA_DIR="$(mktemp -d)"
trap 'rm -rf "$DATA_DIR"' EXIT
cargo build -p crabgresql

start() {
    ./target/debug/crabgresql --listen "127.0.0.1:${PORT}" --data-dir "$DATA_DIR" \
        >"${DATA_DIR}/server.log" 2>&1 &
    echo $!
}
ready() {
    for _ in $(seq 20); do
        psql "host=127.0.0.1 port=${PORT} user=crab dbname=crab sslmode=prefer" -tAc 'SELECT 1' >/dev/null 2>&1 && return 0
        sleep 0.3
    done
    return 1
}

# First boot: create + insert.
PID=$(start); ready || { echo "FAIL: first boot not ready" >&2; kill "$PID"; exit 1; }
psql "host=127.0.0.1 port=${PORT} user=crab dbname=crab sslmode=prefer" -c \
    "CREATE TABLE t (id int4, name text); INSERT INTO t VALUES (1,'durable');"
kill "$PID"; wait "$PID" 2>/dev/null || true

# Second boot on the same data dir: the row must still be there.
PID=$(start); ready || { echo "FAIL: second boot not ready" >&2; kill "$PID"; exit 1; }
trap 'kill "$PID" 2>/dev/null || true; rm -rf "$DATA_DIR"' EXIT
out=$(psql "host=127.0.0.1 port=${PORT} user=crab dbname=crab sslmode=prefer" -tAc \
    "SELECT name FROM t WHERE id = 1")
if [ "$out" = "durable" ]; then
    echo "PASS: data survived restart -> ${out}"
else
    echo "FAIL: expected 'durable', got '${out}'" >&2
    exit 1
fi
