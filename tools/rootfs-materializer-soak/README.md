# RootFS materializer soak

This opt-in acceptance tool drives 10,000 composite RootFS generations through
the production PostgreSQL materializer over at least 24 hours of active
process time. It
uses the production 32 MiB minimum pack, five-minute forced flush, one forced
lane per pass, one-hour uploading-stale threshold, and 24-hour terminal
retention defaults.

The input database and bucket must be dedicated and empty when a run is first
created. The tool writes a run identity to both PostgreSQL and RustFS, then
refuses a resume if either identity, the complete configuration digest, or the
fixed executable digest changes. Evidence and the latest application
checkpoint are written together as an fsynced, hash-chained JSONL log. A
partial final record from power loss is truncated, but a corrupt complete
record fails closed.

Use `--mode auto` under a boot-persistent supervisor. A restarted process
appends a `resumed` record with the previous and current boot IDs. Only
monotonic time observed while the gate process is running contributes to
`active_elapsed_ns`; host downtime never satisfies the 24-hour threshold. The
evidence path, PostgreSQL data, and RustFS data must therefore live on durable
storage, not `/tmp`.

During the run, an HTTP proxy forwards to real RustFS. After the next data PUT
at one-third of the run, it returns 503 for all requests. The tool reconstructs
the primary-fenced PostgreSQL pool, S3 client, store, and worker while the
outage remains active, verifies the exact uploading batch survives, then
restores RustFS and requires publication without an extra object key. This is a
real object-store outage and a manager connection/process-state restart. The
separate PostgreSQL HA integration test remains the source of truth for actual
primary promotion.

Build one fixed binary and reuse it for every process incarnation of one run:

```sh
go build -buildvcs=false -trimpath \
    -o /usr/local/libexec/rootfs-materializer-soak \
    ./tools/rootfs-materializer-soak
sha256sum /usr/local/libexec/rootfs-materializer-soak

HTTP_PROXY= HTTPS_PROXY= ALL_PROXY= NO_PROXY=172.16.100.2,127.0.0.1 \
SANDBOX0_SOAK_DATABASE_URL='postgres://postgres@172.16.100.2:55432/sandbox0_soak?sslmode=disable' \
SANDBOX0_RUSTFS_ENDPOINT='http://172.16.100.2:19000' \
SANDBOX0_RUSTFS_BUCKET='sandbox0-materializer-soak' \
SANDBOX0_RUSTFS_ACCESS_KEY='soak-access' \
SANDBOX0_RUSTFS_SECRET_KEY='soak-secret' \
SANDBOX0_RUSTFS_DATA_DIR='/var/lib/rustfs-soak' \
/usr/local/libexec/rootfs-materializer-soak \
    --mode auto \
    --output /var/lib/sandbox0-soak/materializer/evidence.jsonl
```

The final event fails the process unless all 10,000 generations are byte-safe
and materialized, no upload or deletion work remains, object count is bounded
by shared-pack batches, PostgreSQL growth stays within 512 MiB, and measured
RustFS growth stays within 4,096 files and 512 MiB. The default invocation is
the acceptance gate; shorter duration, lower generation count, and reduced
maximum delay are only smoke-test controls and must not be reported as 24-hour
evidence.

Run the production runtime-slot Bolt journal companion for the same active-time
window. Compile one fixed test executable so every reboot uses the same digest:

```sh
go test -c -buildvcs=false -trimpath \
    -o /usr/local/libexec/nomadruntime-soak.test \
    ./pkg/nomadruntime
sha256sum /usr/local/libexec/nomadruntime-soak.test

SANDBOX0_RUNTIME_SLOT_SOAK_DURATION=24h \
SANDBOX0_RUNTIME_SLOT_SOAK_PROOFS=10000 \
SANDBOX0_RUNTIME_SLOT_SOAK_OUTPUT=/var/lib/sandbox0-soak/bolt/evidence.jsonl \
SANDBOX0_RUNTIME_SLOT_SOAK_STATE_DIR=/var/lib/sandbox0-soak/bolt/state \
SANDBOX0_RUNTIME_SLOT_SOAK_MODE=auto \
/usr/local/libexec/nomadruntime-soak.test \
    -test.run '^TestRuntimeSlotJournalTwentyFourHourSoak$' \
    -test.timeout 30h -test.v
```

It writes and prunes the exact terminal cleanup record/proof format, binds the
Bolt file to the evidence run identity, reopens the journal at one-third of the
run, and requires the final Bolt file to remain within one host page of its
warm size. Uncheckpointed records left by a hard stop are pruned and then
replayed idempotently. It uses the same hash-chained active-time evidence
format as the materializer gate.

Both binaries should run as `systemd` services with durable environment files,
`Restart=on-failure`, and dependencies on the dedicated PostgreSQL and RustFS
services. The materializer service must start after both dependencies are
ready. Its normal shutdown writes an `interrupted` checkpoint; a hard stop can
discard at most the fixed five-second checkpoint interval in either gate. After reboot,
`--mode auto` resumes from the last fsynced event. Only a hash-valid `final`
event with `passed=true` from each run satisfies the combined 10,000/24-hour
gate; an older log that merely stops at a sample is incomplete evidence.

After both writers have exited with a `final` event, audit the immutable logs
with the independent verifier. Build the verifier once and record its hash,
then pass the previously recorded hash of each gate executable (not the hash of
the verifier):

```sh
go build -buildvcs=false -trimpath \
    -o /usr/local/libexec/soak-evidence-verify \
    ./tools/soak-evidence-verify
sha256sum /usr/local/libexec/soak-evidence-verify

/usr/local/libexec/soak-evidence-verify \
    --kind materializer \
    --path /var/lib/sandbox0-soak/materializer/evidence.jsonl \
    --expected-executable-sha256 "$MATERIALIZER_GATE_SHA256" \
    --output /var/lib/sandbox0-soak/materializer/verification.json

/usr/local/libexec/soak-evidence-verify \
    --kind bolt \
    --path /var/lib/sandbox0-soak/bolt/evidence.jsonl \
    --expected-executable-sha256 "$BOLT_GATE_SHA256" \
    --output /var/lib/sandbox0-soak/bolt/verification.json
```

When a configuration digest was fixed in the supervisor manifest, also pass it
with `--expected-config-sha256`. The verifier refuses an active writer, a
partial tail, a broken identity or hash chain, a non-final log, less than 24
hours of active time, or final state outside the exact production contract.
The generated report is an audit artifact; it does not replace either gate's
hash-bound `final` event.
