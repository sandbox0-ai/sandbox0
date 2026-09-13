# D-COMBINED-TRANSPORT: actual encrypted net cost, mixed result

2026-09-12. Evidence: `/tmp/sandbox0-combined-transport.szlb74`.
Verdict: **no consistent both-image gain; do not admit this candidate to runtime**.
This is a component experiment, not claim latency, real `node -v`, occupied
production width or evidence that the 1s target/2s fallback has been achieved.

## Fixed comparison and scope

The corrected D-COMBINED-LIFETIME selector/packet helper is unchanged. A temporary
Go overlay runs the actual Reader and actual encrypted S3-compatible store on
the Singapore test ECS. Four predeclared arms execute ordinary/candidate/
candidate/ordinary, each with all 14,373 historical mixed-Node/Coding request
shapes and 16 constructors. Cold starts with fresh Reader AND encryption-header
caches; cached-new retains those caches but creates new Reader identities.
All 57,492 ReadAt outputs, 64 constructors, exact source schedules and full cache
snapshots verify against the sealed predecessor. No local fixture fallback is
available to the measured source.

Keep 128MiB total/16MiB protected mapping cache, 8MiB/1024-entry encryption-header
cache, 256KiB prefix/parallel bounds, eight source slots and the existing 10s
request timeout. Schedules are serialized helpers, not eight occupied sandboxes.
One provider/credential lifetime spans all four arms; encryption wrappers and
their caches are fresh per arm. Initial provider/credential preparation is
0.146/112.045ms, separately recorded, not silently charged to just one variant.
Wrapper construction is 0.413–0.452ms per arm. Connections are not forced cold.

Only 33 accessed objects from the already generated generic format fixtures are
conditionally created under `diagnostics/combined-transport-szlb74/`, totaling
1,984,949 encoded plaintext bytes. Actual encryption and plaintext verification
complete in a separate uploader process before measurement. The exact objects
remain for audit; no existing RootFS object/head/key/configuration is replaced.
OSS server cache is uncontrolled: new candidate objects were just uploaded and
verified; original objects have earlier history. ABBA is not randomization or
a sufficient population for a statistical SLO or cold-storage guarantee.

## Measured component costs

Times below are milliseconds across a complete mixed-image serialized cohort.
Reader includes constructors but excludes output-hash verification. HTTP and
crypto intervals are intersected with Reader intervals and unioned; exclusive
crypto removes overlap with HTTP. Do not add overlapping phase unions.

| Arm | Cohort | Reader + constructor | HTTP union | Exclusive header crypto | Process CPU | HTTP attempts |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 0 ordinary | Cold | 6418.111 | 5626.293 | 232.223 | 1974.537 | 389 |
| 1 candidate | Cold | 4002.467 | 3157.786 | 312.885 | 2032.229 | 355 |
| 2 candidate | Cold | 3591.043 | 2731.597 | 327.300 | 2069.471 | 355 |
| 3 ordinary | Cold | 3856.041 | 3103.411 | 251.039 | 1999.518 | 389 |
| 0 ordinary | Cached-new | 1687.268 | 1480.305 | 0 | 959.070 | 192 |
| 1 candidate | Cached-new | 1355.686 | 1083.091 | 41.880 | 1038.897 | 167 |
| 2 candidate | Cached-new | 1808.185 | 1547.207 | 41.383 | 1053.057 | 167 |
| 3 ordinary | Cached-new | 1317.314 | 1117.262 | 0 | 994.657 | 192 |

Process CPU includes the diagnostic observer, harness and output checks, not
only production decode. Header crypto includes validation, key unwrap and AEAD
construction, not isolated RSA CPU or all payload decryption.

| Candidate minus paired ordinary | Cold Node Reader | Cold Coding Reader | Cached-new Node Reader | Cached-new Coding Reader |
| --- | ---: | ---: | ---: | ---: |
| Candidate 1 minus ordinary 0 | -899.439 | -1516.205 | -131.058 | -200.525 |
| Candidate 2 minus ordinary 3 | -202.330 | -62.668 | +153.934 | +336.936 |

Cold improves in both observed orders, but cached-new reverses for BOTH images.
The preregistered both-image/both-cohort/both-order screen therefore does not
pass. This is not proof that the candidate is inherently slower in every
environment, nor permission to retry/tune until the negative control disappears.

Source windows remain 355→330 cold and 192→167 cached-new, with +741,750/+925,820
encoded plaintext bytes. HTTP attempts instead change 389→355 and 192→167;
actual returned ciphertext changes by only +22,445/+137,660 bytes. These are
different boundaries: do not substitute plaintext-byte deltas for wire bytes or
source-call reductions for serial HTTP round trips. All 2,206 HTTP attempts are
successful 206/closed-body responses with globally unique validated actual S3
request IDs; 2,088 source calls and 266 header-crypto operations are bound to
their exact physical objects and enclosing source lifetimes.

## What this says about machine and S3

The same ordinary HTTP shapes and bytes change from 6418.111 to 3856.041ms Reader
time without a code change. HTTP union changes by -2522.882ms, versus Reader
-2562.070ms. Post-write-to-first-byte union changes from 5186.431 to 2767.142ms;
its per-attempt median/P95 changes from 10.594/35.550 to 6.782/14.219ms. Connection
acquisition union changes only 39.439→23.704ms. Thus the large initial-pair gain
cannot be attributed entirely to layout or a new connection cost.

The candidate's own cached-new repeats also change by +452.499ms Reader and
+464.117ms HTTP with identical request shapes. Post-write union rises
958.457→1398.061ms. These observations locate major variation in the client HTTP
wait boundary. They do NOT separate provider service time, network path and
local callback scheduling, or establish an OSS defect. This campaign fetches
no per-request service logs; prior D-OSS-SERVICE/D-OSS-ID evidence has its own
explicit population and correlation limits.

The actual host is two CPUs/8GiB, not production parity. CPU idle is 73.34–80.98%;
no observed steal, cgroup throttling, memory PSI or OOM. CPU pressure is nonzero;
host I/O pressure is small and probe-cgroup I/O pressure is zero. This does not
exonerate hardware at occupied width or justify an instance-size sweep. Probe
RSS peaks at 325,516/353,620/368,120/370,736KiB across the four arms, but one
process retains earlier reports/heap. Those peaks are not comparable isolated
candidate-per-sandbox memory costs or density acceptance.

## A real structural cost, not a header-cache eviction

The candidate has 68 versus 57 first-header operations cold, and 8 versus 0
cached-new. All eight later operations belong to Coding frame-group objects,
with ZERO prior source touches in that arm. Both candidate arms touch the exact
same eight new objects. They are not repeated unwraps of evicted headers.
Increasing header-cache capacity cannot eliminate these first accesses.

Those eight independent objects add 41.383–41.880ms exclusive header processing.
Cold exclusive header cost also increases 76.261–80.662ms; paired total process
CPU increases 57.692–79.827ms across the cohorts. Fewer source windows alone
omitted this cost. Conditional mapping/data delivery can encounter a new
physical encrypted object even when the node already has some RootFS data.
The exact member/cache decision behind each such switch remains a separate
audit; do not infer it merely from the object type.

Next use the existing member locators and cache snapshots to evaluate whether
bounded authenticated pack placement can avoid independent envelopes WITHOUT
adding serial lookup/read amplification, losing recipient binding or creating
new cache authority. This is a feasibility question, not an approved format
implementation. Removing roughly 41ms alone is not a structural solution to
the earlier 48–68% payload-wait coverage requirement. Reject any proposed next
mechanism whose fixed-other-cost bound cannot cover the remaining gap for both
images. Do not repeat this unchanged replay or use a timeout/cache/hardware
sweep to turn an inconclusive result into acceptance.

## Validation, retained failures and cleanup

The first local test build fails on a misspelled existing ChaCha constant; its
source and failure receipt remain. Corrected Go race suite passes 12 top-level
tests plus nine subtests, including both encryption algorithms, upload scope,
conditional non-overwrite, Reader outputs, HTTP admission and request-ID guards.
Seven Ruby analysis tests/29 assertions pass. Review identified an overly broad
default tolerance in an earlier synthetic nanosecond interval test; the original
remains and a new 1e-12ms-tolerance guard independently checks all six quantities.

Remote boot `6650f8a6-4fba-4b69-8efc-ef43d93327ff` has ready1, so no claims are
attempted and no warm-ready repair is performed. Both diagnostic units finish
inactive/success/0 with MainPID0. Before/after original service PIDs, binaries,
configuration, job modify70668/two groups, data rows2072/2072/263, empty physical
runtimes and all64 detached NBD devices are unchanged. All22 remote reports
are transferred and hash-verified before SSH closes.

The remote-test skill supplies only workspace Makefile start/stop lifecycle;
its legacy Kind/deployment flow is not used. Preserve `/data`. No logging
policy/IAM/production/merge/tag operation. Stop finishes 05:53:44UTC; the separate
05:57:19.347072188UTC receipt confirms Stopped/StopCharging with no public IP.
The earlier Stopping receipt remains rather than being overwritten. User's
explicit experiment-log request takes precedence over the skill's generic
no-summary-docs rule. All1353 frozen product files remain unchanged.

Generic tenant-neutral carriers, claim-time RootFS binding, stock gVisor,
stateless nodes, encryption/checksums, readiness/fencing, durable cleanup and
the ingress-to-procd plus immediate real-command acceptance boundary remain
unchanged. Populated-large roots, occupied production width, durable format
import/COW/retry/inventory/GC and the universal 2s requirement remain open.
