#!/usr/bin/env bash
# Regenerates the committed test CA + server cert (ECDSA P-256, 10 years).
set -euo pipefail
cd "$(dirname "$0")/../crates/pgwire/tests/fixtures"

openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -keyout test-ca-key.pem -out test-ca.pem -days 3650 \
    -subj "/CN=crabgresql test CA"
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -keyout test-server-key.pem -out server.csr -subj "/CN=localhost"
openssl x509 -req -in server.csr -CA test-ca.pem -CAkey test-ca-key.pem \
    -CAcreateserial -out test-server.pem -days 3650 \
    -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1")
rm server.csr test-ca.srl
echo "wrote test-ca.pem, test-server.pem, test-server-key.pem (+ CA key)"
