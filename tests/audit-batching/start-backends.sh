#!/usr/bin/env bash
set -euo pipefail

docker network inspect audit-perf >/dev/null 2>&1 || docker network create audit-perf
docker run -d --name audit-s3 --network audit-perf --memory 1g \
  -p 127.0.0.1:19000:5000 \
  -e S3_IGNORE_SUBDOMAIN_BUCKETNAME=true \
  motoserver/moto:5.0.22
docker run -d --name audit-clickhouse --network audit-perf --memory 3g \
  -p 127.0.0.1:9000:9000 -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  -v "$PWD/tests/audit-batching/clickhouse.xml:/etc/clickhouse-server/config.d/audit-test.xml:ro" \
  clickhouse/clickhouse-server:24.8.14.39
for attempt in $(seq 1 90); do
  if curl --fail --silent http://127.0.0.1:19000/moto-api/ >/dev/null && \
    docker exec audit-clickhouse clickhouse-client --query 'SELECT 1' >/dev/null 2>&1; then
    exit 0
  fi
  sleep 1
done
docker logs audit-clickhouse
exit 1
