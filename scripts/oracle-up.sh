#!/usr/bin/env bash
# Starts the PostgreSQL 18 test oracle (trust auth, port 54320).
set -euo pipefail
docker rm -f crabgresql-oracle 2>/dev/null || true
docker run -d --name crabgresql-oracle \
    -e POSTGRES_HOST_AUTH_METHOD=trust \
    -p 54320:5432 postgres:18
echo "oracle on 127.0.0.1:54320 (user=postgres dbname=postgres)"
