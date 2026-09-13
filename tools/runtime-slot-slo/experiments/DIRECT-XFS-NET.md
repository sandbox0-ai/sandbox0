# D-DIRECT-XFS-NET: direct projection does not qualify

2026-09-13. Remote encrypted-OSS/stock-runsc experiment; no product adoption.

## Decision

Close this projection-only candidate. Neither direct sample improves cold
combined time AND read cost against the reverse ordinary control for either
image. All eight direct rows, including cached NEW identities, take longer than
their matched reverse controls. This is a failed admission screen, not proof
that direct XFS must always be slower under every workload.

Do not implement a production layout discriminator/migration on this evidence,
or repeat the same projection sweep. The native lifecycle subset in
[DIRECT-XFS-CONTRACT.md](DIRECT-XFS-CONTRACT.md) was not a performance result.
The general block-payload dependency opportunity in
[CRITICAL-PATH-BUDGET.md](CRITICAL-PATH-BUDGET.md) remains the relevant selection
criterion; removing one filesystem layer did not materially change read demand.

## Fixed inputs and boundaries

- Frozen product inventory: 1,362 files, unchanged before/after; sandbox0
  origin/main and worktree base `0f09220460581bfc1fdc331f34ebc85bf38381e7`.
  The dirty diagnostic candidate is not relabeled as unmodified main.
- Same immutable format2 descriptors and encrypted OSS objects in both arms.
  No reimport, object publication, repack, local-image fallback or tenant prewarm.
  Node artifact `sha256:2b5208c97335f4e25312150a1d980ed6039efe1884139129efd79f00a7f1b155`;
  Coding artifact `sha256:21a2f3bac82e85bf353d9d625165c79c9d915f5c107fe2f36c349cc84e806ceb`.
  Node is logically 16GiB; Coding is sparse logical 1TiB, NOT populated 1TiB.
- Ordinary exposes the usual OverlayFS. Direct exposes the exact private
  writable XFS branch's pristine lower tree through a nosuid/nodev/noatime
  bind. Nonempty upper/work and symlinked layout directories are rejected.
  This isolated counterfactual does NOT reinterpret authenticated production
  descriptors or support bypassing old populated upper directories.
- Stock runsc20260817, systrap, DirectFS, shared file access and overlay2=none.
  Frozen guest procd SHA256
  `29dedac3bce92b6a9a3d507889110927ede8727f87186376524e577617c69f2d`
  predates the copied-session-owner fix; not current guest-production parity.
- Each image/pair starts a fresh provider, empty 128MiB Reader cache and empty
  encrypted-header cache. Cold is followed by a NEW identity retaining those
  caches, with new WAL, mounts, guest and procd proof. No image cross-cache test.
  Eight source slots, 1MiB coalescing, 128KiB NBD request cap and 4MiB readahead
  stay fixed. Source and command HTTP budgets remain 10s; no command POST replay.
- Provider credentials and supplied descriptor preparation precede measurement;
  zero object GETs/attempts occur during provider preparation. Measured local
  operation includes generic network/auth setup, Reader opening, mount, runsc,
  authenticated readiness and one literal `node -v`, expected `v22.23.2`, exit0.
  The 45s diagnostic Reader lifetime is NOT a public claim/command timeout.
- Single boot `e5c4b787-2be6-4a7d-bdf1-ad01f23a616f`, 2CPU/8GiB, one guest at a
  time. This is empty-Reader/header cold, not provider-cold, fresh-boot-per-arm,
  regional/PostgreSQL/Nomad claim, occupied width or production acceptance.
  Per-read diagnostic recording is enabled equally, not calibrated away.

## All startup observations

Units are milliseconds. Ready and combined start at the same node-local
operation boundary. Command is the first real `node -v` alone. None of these
columns is a regional claim measurement. The intended order is O1/D1/D2/O2;
the cleanup interruptions below prevent calling it a seamless campaign.

| Image | Pair | Cache | Ready | First command | Combined |
| --- | --- | --- | ---: | ---: | ---: |
| Node | O1 | cold | 904.312 | 1355.157 | 2259.471 |
| Node | O1 | cached NEW | 169.788 | 96.881 | 266.671 |
| Coding | O1 | cold | 1820.904 | 1913.333 | 3734.239 |
| Coding | O1 | cached NEW | 240.152 | 95.686 | 335.841 |
| Node | D1 | cold | 824.159 | 1236.964 | 2061.125 |
| Node | D1 | cached NEW | 165.628 | 91.496 | 257.126 |
| Coding | D1 | cold | 1987.162 | 1654.490 | 3641.654 |
| Coding | D1 | cached NEW | 195.229 | 98.804 | 294.034 |
| Node | D2 | cold | 599.881 | 732.495 | 1332.377 |
| Node | D2 | cached NEW | 162.269 | 95.567 | 257.838 |
| Coding | D2 | cold | 1182.391 | 1045.237 | 2227.630 |
| Coding | D2 | cached NEW | 225.448 | 104.137 | 329.586 |
| Node | O2 | cold | 539.818 | 761.561 | 1301.380 |
| Node | O2 | cached NEW | 149.098 | 93.372 | 242.472 |
| Coding | O2 | cold | 1127.678 | 1030.270 | 2157.949 |
| Coding | O2 | cached NEW | 171.764 | 95.181 | 266.947 |

All16 have distinct sandbox identities and authenticated procd instance proofs.
All commands succeed, but cold ready misses1s in4/8; cold command-only misses1s
in6/8; cold combined misses1s in8/8 and2s in6/8. No cold ready/command-only row
exceeds2s. Cached NEW rows have no1s/2s misses at this narrower boundary.

## Read cost and environmental variance

Cold direct versus O2:

- Node D1/D2 combined is +759.744/+30.997ms; ciphertext is +16,404/+32,808B.
  NBD demand falls only16,384B from about95.4million bytes. Encoded bytes vary
  -3,271/+22,548B; there is no material read-amplification reduction.
- Coding D1/D2 combined is +1483.704/+69.680ms; ciphertext is
  +268,527/+202,911B, NBD demand +54,272/+50,176B, and HTTP attempts +13/+11.
  D2 mount is63.085ms faster but combined remains69.680ms slower. A smaller
  mount duration does not establish a startup benefit.
- Cached NEW direct Coding unexpectedly needs10/11 source HTTP attempts and
  311,676/328,080 ciphertext bytes versus zero in both ordinary controls.
  Preserve this regression; its precise dependency cause is not proven.
  Cached NEW direct Node remains source-cache-hit but combined is14.654/15.366ms
  slower than O2. No claimed density advantage follows from these samples.

The controls themselves change materially. Node O1/O2 first-command ciphertext
is exactly37,052,776B in186 calls, while median acquisition-through-body-close
falls18.123 to8.210ms. Coding has38,768,844/38,719,632B in211/210 calls, with
medians20.311/8.870ms. Thus the faster later samples cannot be credited to the
projection. Fresh Reader caches do not establish an unchanged machine/network/
object-storage path or exclude provider-side cache effects. These client timings
include scheduling, transport and consumption; they do not isolate S3 service
time or prove that S3 alone is the cause.

Three separate host-sampler segments report zero steal ticks, zero cgroup CPU
throttling and zero OOM/high/max events. Minimum available memory remains about
5.94million KiB. CPU busy averages24.1-44.0percent and iowait8.2-22.4percent over
their respective windows, including cleanup. These coarse observations do not
exonerate CPU/scheduler or local/remote I/O, and are not occupied-width evidence.
Do not subtract cgroup counters across the restarted runner segments.

Object rows count calls initiated in pre-cleanup phases; some bodies close later.
Encoded/ciphertext rows overlap. Their duration sums are not latency; interval
unions are exposure, not demonstrated guest critical-path savings. NBD totals
include readahead and must not all be called necessary startup file bytes.

## Failures, explicit resumes and validation

1. Initial local compilation found an unused import; retained `unit.json` fails.
   Corrected local race count3 passes21 top-level test events, no skips, with
   vet/static build passing. No guest trial occurred before qualification.
2. First launcher treated PostgreSQL descriptor bytea as JSON; it failed before
   launch intent/unit/trials. Independent observation proved no unit. Corrected
   launcher uses `convert_from(descriptor,'UTF8')` and verifies exact bindings.
3. Original oneshot runner completed Node O1 cold/cached commands, then failed
   the physical-absence guard. Its RuntimeMaxSec property was ineffective for
   oneshot. Preserve that failed runner, rather than treating Go exit0 as enough.
4. Explicit Type=exec resume skipped Node O1, completed Coding O1, then failed
   cleanup. Seventy-five observations over more than5s show persistent exact
   per-runtime `runsc/null-netns` nsfs mounts, not RootFS/NBD mount residues.
   Waiting longer is not a fix. No public HTTP timeout changed.
5. Runner v3 validates guest/NBD/cgroup cleanup and the exact nsfs mount type and
   path before normal `umount -- exact-path`. Twelve such unmounts are recorded;
   no lazy/unscoped cleanup. It resumes ONLY the remaining six pairs using the
   unchanged binary. Original four commands are never replayed. The resumed
   matrix completes, while both original runner failures remain failures.
6. Offline verifier v1 incorrectly expected JSON Go-test output from a verbose
   text log. Its failed receipt is retained; v2 validates the actual PASS lines,
   race/count flags and absence of skips/failures. This changes no runtime data.

Independent verification rechecks all101 exported reports against bytes, SHA256
and original Base64 export; separately reconstructs16 raw command/timing rows,
identities, mount types, cache continuity, HTTP status and cleanup receipts.
Nine arithmetic checks distinguish overlapping sums from unions. All1,362
product-file hashes and the complete path set remain unchanged.

## Restoration and remaining goal

Original binaries, service PIDs/start identities, configs, carrier job77379,
source fixtures and DBs remain unchanged; retained history DB stays2174rows.
Independent final guard proves all64NBD detached and owned mount/network/cgroup
absence. Final original ready-carrier count is1, not a two-carrier health claim.
All `/data` fixtures, WALs and caches are retained. Per remote-test skill lifecycle
rules, the ECS is restored to2CPU/8GiB Stopped/StopCharging, freshly confirmed at
2026-09-13T12:20:38.507602969Z. No production mutation, merge or tag.

Still required: full regional-ingress-to-procd readiness and first real command,
cold and cached NEW identities, large populated/history roots, occupied actual
machine width, current guest/runtime parity, and the original1s preferred/2s
fallback gates. This experiment does not complete or narrow that goal.

Sealed187 artifacts: `/tmp/sandbox0-direct-xfs-net.wMe4BC/evidence.json`, SHA256
`84a2dc9a04ba045655e7b44d8c16f8e12156279a5929d0f5c9449aae962705a7`.
Full rows, phases, per-phase reads, cache snapshots and separate host windows are
in `analysis.json`; frozen plan, all failed attempts and raw exports are retained.
