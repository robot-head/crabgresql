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
trap 'kill "$SERVER_PID" "${TLS_PID:-}" 2>/dev/null || true' EXIT

# Non-standard default port avoids clashing with a local postgres.
# Readiness loop: cold CI runners can take >1s to start the binary.
for _ in $(seq 10); do
    if psql "host=127.0.0.1 port=${PORT} user=crab dbname=crab sslmode=prefer" -tAc 'SELECT 1' >/dev/null 2>&1; then
        break
    fi
    sleep 0.3
done

out=$(psql "host=127.0.0.1 port=${PORT} user=crab dbname=crab sslmode=prefer" -tAc 'SELECT 1')
if [ "$out" = "1" ]; then
    echo "PASS: psql SELECT 1 -> ${out}"
else
    echo "FAIL: expected '1', got '${out}'" >&2
    exit 1
fi

CERT_DIR="crates/pgwire/tests/fixtures"
if [ -f "${CERT_DIR}/test-server.pem" ]; then
    TLS_PORT=$((PORT + 1))
    ./target/debug/crabgresql --listen "127.0.0.1:${TLS_PORT}" \
        --tls-cert "${CERT_DIR}/test-server.pem" \
        --tls-key "${CERT_DIR}/test-server-key.pem" &
    TLS_PID=$!
    for _ in $(seq 10); do
        if psql "host=127.0.0.1 port=${TLS_PORT} user=crab dbname=crab sslmode=require sslrootcert=${CERT_DIR}/test-ca.pem" -tAc 'SELECT 1' >/dev/null 2>&1; then
            break
        fi
        sleep 0.3
    done
    out=$(psql "host=127.0.0.1 port=${TLS_PORT} user=crab dbname=crab sslmode=require sslrootcert=${CERT_DIR}/test-ca.pem" -tAc 'SELECT 1')
    if [ "$out" = "1" ]; then
        echo "PASS: psql over TLS SELECT 1 -> ${out}"
    else
        echo "FAIL (TLS): expected '1', got '${out}'" >&2
        exit 1
    fi
fi
