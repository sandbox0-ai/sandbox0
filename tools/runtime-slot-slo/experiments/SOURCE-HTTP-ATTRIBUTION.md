# D-SOURCE-HTTP: separate connection, first-byte and body costs

Date: 2026-09-12. Component diagnosis only; no new startup acceptance.

## Result and decision

The previous owned Coding read fetched5490 encoded bytes but spent116ms in
source delivery. This fixed-range remote probe confirms that a small response
can still spend tens of milliseconds **before its body arrives**. It does not
establish that every historical slow request has the same cause.

All78 encrypted reads and independent original-content checks succeeded.
All98 observed SDK HTTP attempts returned206 using HTTP/1.1, without retry,
admission rejection or body failure. Requested ciphertext totaled5,625,932bytes.
The candidate's encryption, original decoded checksums, credential provider,
retryer, connection pool and timeout settings were not replaced or enlarged.

Six sequential repetitions for each range/state; values below are median/max
milliseconds from encrypted Get through consuming and closing its returned body.
Original-content verification and provider preparation are recorded separately.

| Original range | Fresh provider/header | Reused provider, fresh header | Reused provider/header |
| --- | ---: | ---: | ---: |
| Coding root,5177bytes | 43.939 /69.639 | 10.163 /11.863 | 4.526 /5.259 |
| Coding mapping group,175438bytes | 42.248 /51.490 | 12.149 /12.528 | 6.035 /7.139 |
| Coding data,5490bytes at5850541 | 49.187 /88.286 | 12.247 /19.668 | 6.308 /12.940 |

These are observed groups, not causal subtraction or savings promised for claim.
Fresh-provider/header still allows its own parallel header/data calls to reuse
one another's connection: one of12 sequential data attempts did so. Reused-
provider groups had all their requests reuse connections. No tenant RootFS was
prepared for a later claim; this helper performed no claims at all.

## Exact slow request

Attempt63 reads the SAME original ciphertext range as the prior owned slow data
example: offset5856255,length17401. Its corresponding operation is
`data-5-fresh`; the complete encrypted read takes88.286ms.

| Observed HTTP interval | Milliseconds |
| --- | ---: |
| Do entry to response-body acquisition | 86.654 |
| Connection acquisition, including DNS/TCP/TLS | 16.699 |
| DNS envelope, included above | 2.543 |
| TCP connection envelope, included above | 1.060 |
| TLS envelope, included above | 12.994 |
| Request written to first response byte | 69.873 |
| Response acquired to last body read/EOF | 0.128 |

Header attempt62 overlaps this data request; neither retries. Do not sum these
overlapping calls. The major observed interval is waiting after sending the
request, not consuming its17KB response. It includes network, server processing
and client scheduling; this probe does not distinguish those three causes.
TLS/DNS/connect envelopes can overlap with speculative dials and are not an
additive breakdown, especially when an existing connection wins a race.

The fixed order and repeated objects also mean connection reuse is correlated
with request order/provider-side state. Fresh provider is not proof of cold OS
DNS or a cold object-store backend. The higher cold values recur after earlier
reads, but that alone is not a controlled server-cache experiment.

## Concurrency and source lifetime

Three eight-reader batches use independent cold encryption wrappers with one
provider per batch and existing transport limits. Balanced totals are eight
reads of each range. Median/max read time: root37.788/61.956ms,
mapping58.770/69.724ms,data51.884/58.143ms. These are not eight running sandboxes,
occupied memory, production-width claims or the historical16CPU trace load.

Source audit of the frozen candidate establishes:

- `pkg/nomadruntime/service.go` constructs one runtime for the daemon lifetime.
- `runtime.go` creates one provider/encryption wrapper and supplies it to one
  `rootfssession.Manager`; per-session Reader construction reuses `m.source`.
- `initializeRuntimeObjectStore` prepares the SDK credential provider before
  serving runtime capacity, without reading a tenant RootFS. Per-provider
  preparation in this helper was112.671–125.456ms, median116.078ms; this is
  NOT charged to every claim in the candidate and is not the historical116ms
  source wait merely because their numbers resemble one another.
- Pinned SDK corev1.41.1/S3v1.72.3 copies per-operation options while retaining
  the underlying HTTP client. The diagnostic decorator changes only that
  operation's HTTP client wrapper, not the client it calls. Local tests check
  identity stability and actual SDK retry-attempt recording.

Therefore, adding another provider/credential cache is not a new fix. Neither
pool exhaustion nor a need for larger pool/cache limits has been demonstrated.

## Next discriminating check

Keep the ordinary layout and frozen product as the control. Before new format
or transport tuning, observe the actual complete claim-plus-command workload
with bounded HTTP-attempt metadata and contemporaneous CPU/queue pressure.
Compare observer-on/off overhead with identical configured hardware and load;
do not compare this2CPU helper directly with the earlier16CPU traced startup.
Answer whether slow critical-path attempts use new or reused connections and
whether their post-write/pre-first-byte wait grows under that real workload.
Do not reintroduce a tuning sweep or reopen the stopped joint-packet branch on
the strength of this component result.

The1s target/2s fallback, no tenant prewarming, empty-node and cached-new
cohorts, immediate real `node -v`, populated-large roots, actual occupied density,
authentication, encryption/checksums, fencing, durability and cleanup all remain.

## Reproducibility and lifecycle

Evidence root: `/tmp/sandbox0-source-http.0a3MTm`; isolated remote helper root:
`/data/sandbox0-source-http-0a3MTm`. Original source ranges came from individually
verified files in the sealed D-JOINT-BYTES artifact index. Only three exact
original keys were admitted; max256HTTP attempts,64MiB aggregate requested
bytes,1MiB per HTTP range and10s per source operation.

Temporary build overlay only; four Go race tests passed before remote execution.
No local e2e, product changes, service rollout, object writes, imports, claims,
guest commands, production actions, PR merge or tag. Existing193 worktree
entries and1353 product files are preserved;164 prior trace files reverified.

Remote stayed2CPU/8GiB. Helper exited successfully, inactive/dead/MainPID0;
original service processes/files, PG row counts, Nomad job68814/two groups,
physical empty state and64detached NBD devices were verified before/after.
All ten exported receipts were individually checked; raw report SHA256:
`d93948c159c461a016395c820d5392ec2a55f979ac337dab0217685e3ba11010`.
SSH explicitly closed with terminal confirmation; /data preserved and compute
stopped. Cloud stop receipt is retained; final independently queried cloud state
and artifact hashes are recorded in the experiment ledger.
