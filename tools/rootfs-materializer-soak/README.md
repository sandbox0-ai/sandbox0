# RootFS materializer soak

This opt-in acceptance tool drives 10,000 composite RootFS generations through
the production PostgreSQL materializer over an actual 24-hour wall clock. It
uses the production 32 MiB minimum pack, five-minute forced flush, one forced
lane per pass, one-hour uploading-stale threshold, and 24-hour terminal
retention defaults.

The input database and bucket must be dedicated and empty. The tool refuses to
reuse either when durable rows or objects are present. Evidence is written as
fsynced JSONL to an exclusive path outside the repository; do not commit result
files.

During the run, an HTTP proxy forwards to real RustFS. After the next data PUT
at one-third of the run, it returns 503 for all requests. The tool reconstructs
the primary-fenced PostgreSQL pool, S3 client, store, and worker while the
outage remains active, verifies the exact uploading batch survives, then
restores RustFS and requires publication without an extra object key. This is a
real object-store outage and a manager connection/process-state restart. The
separate PostgreSQL HA integration test remains the source of truth for actual
primary promotion.

Example full gate:

```sh
HTTP_PROXY= HTTPS_PROXY= ALL_PROXY= NO_PROXY=172.16.100.2,127.0.0.1 \
SANDBOX0_SOAK_DATABASE_URL='postgres://postgres@127.0.0.1:55432/sandbox0_soak?sslmode=disable' \
SANDBOX0_RUSTFS_ENDPOINT='http://172.16.100.2:19000' \
SANDBOX0_RUSTFS_BUCKET='sandbox0-materializer-soak' \
SANDBOX0_RUSTFS_ACCESS_KEY='soak-access' \
SANDBOX0_RUSTFS_SECRET_KEY='soak-secret' \
SANDBOX0_RUSTFS_DATA_DIR='/var/lib/rustfs-soak' \
go run ./tools/rootfs-materializer-soak \
    --output /tmp/rootfs-materializer-soak.jsonl
```

The final event fails the process unless all 10,000 generations are byte-safe
and materialized, no upload or deletion work remains, object count is bounded
by shared-pack batches, PostgreSQL growth stays within 512 MiB, and measured
RustFS growth stays within 4,096 files and 512 MiB. The default invocation is
the acceptance gate; shorter duration, lower generation count, and reduced
maximum delay are only smoke-test controls and must not be reported as 24-hour
evidence.
