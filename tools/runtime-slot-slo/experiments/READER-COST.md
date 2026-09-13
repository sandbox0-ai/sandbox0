# D-READER-COST: full original Reader with actual encrypted transport

Date: 2026-09-12. Component attribution, **not sandbox startup or density**.
The previous host/S3 probe was progress; this run closes its missing full-Reader
ownership and overlap measurements without changing product behavior.

## Scope and integrity

Replay all 14,373 retained original requests using the SAME two recorded-start
serialized mixed-root schedules from D-COMBINED-CACHE: eight Reader identities
constructed before each batch, four Node/four Coding, with NEW identities in
the second batch and the SAME 128 MiB block/mapping cache. Retain its existing
16 MiB mapping protection, eight source slots, bulk geometry and data checksums.
One actual OSS provider and its existing 8 MiB / 1,024-entry header cache serve
both batches. Credentials are prepared once, separately from Reader measurements.

This is serialized ordering of complete requests, not real overlap of eight
sandboxes, not a production-width run, and not a replay of their original elapsed
startup times. Both original RootFS images, encryption key and stored algorithm
remain unchanged. No RootFS prewarm, PUT, import, runtime or configuration edit.

A temporary Go overlay times the existing objectAEAD call (header validation,
RSA unwrap and symmetric AEAD construction) with the exact context/source owner.
An HTTP observer retains connection, post-write, body and close intervals without
credentials or signed URLs. No crypto payloads or plaintext outputs are exported.
Removing the one named encrypted-store instrumentation call recovers the exact
frozen source. The Reader is unchanged except a read-only cache accessor.

All **14,373 ReadAt output checks + 16 constructors** pass. Actual **547 source
calls** match the original per-ReadAt key/offset/length multiset, and both final
cache states match exactly. **581 HTTP requests succeed**, 577 reuse a connection.
There are 57 actual crypto/header misses in the cold batch, 0 in cached-new.
The 34 additional cold HTTP requests match the header+data range mechanism;
do not equate source windows with HTTP requests or serial round trips.

## Where the serialized Reader time goes

The table is aggregate work across all eight identities per batch, not a single
claim. Timing excludes test-output SHA verification where stated. Intervals are
unions clipped to actual constructor/ReadAt spans, not summed overlapping waits.

| Component, ms | Cold batch | Cached-new batch |
| --- | ---: | ---: |
| Constructor + ReadAt time | 7,674.903 | 2,027.743 |
| HTTP interval union | 6,794.207 | 1,796.192 |
| Of HTTP: post-write to first byte union | 6,204.082 | 1,660.651 |
| Crypto interval union | 307.297 | 0 |
| Crypto overlapping HTTP | 54.302 | 0 |
| Crypto outside HTTP | 252.995 | 0 |
| Other source time outside HTTP/crypto | 69.743 | 27.580 |
| Reader time outside source reads | 557.959 | 203.972 |
| Test-output SHA verification, separately | 460.581 | 458.861 |

Total batch wall time is 8,199.890/2,570.130 ms including test verification and
loop/allocation overhead. Never compare those values to the 1s/2s startup gate.
Cold constructors themselves total 84.432 ms; cached-new constructors 0.020 ms.
Credential setup before either batch is 124.715 ms, not a per-object charge.

HTTP intervals occupy **88.525%/88.581%** of measured Reader time; unoverlapped
crypto occupies **3.296%/0%**. This is interval attribution, not a causal prediction
that optimizing a component would remove exactly that much time. It demonstrates
that RSA-only tuning is not the principal remedy for this measured workload.

Cold crypto calls are **50 Coding + 7 Node**, not once per source call or once
per lane. Median objectAEAD span is 5.265 ms, range 5.091–6.929 ms. Coding has
216.942 ms of crypto outside HTTP; Node has 36.053 ms. Existing header reuse and
singleflight are functioning; do not rediscover their benefit as a new fix.

Actual source counts cold/cached-new are 355/192. Plaintext encoded source bytes
are 53,571,464/20,248,020; ciphertext returned 59,104,366/23,408,734. Total
requested ciphertext is 82,700,816 bytes. These retain the old ordinary shared
cache behavior, including rereads due to eviction, not an ideal cache model.

## The long waits are not explained by aggregate CPU saturation

Cold post-write median/max: **12.728/175.016 ms**; cached-new: **6.793/56.268 ms**.
The 175.016 ms wait reuses its connection, with 0.004 ms connection acquisition
and 2.442 ms body reading. A separate **1,024-byte header request waits 142.642 ms**
before its first byte, then reads its body in 0.085 ms. Bulk throughput alone
does not explain that small-header wait.

During the complete 10.897 s observation, the 2 CPU / 8 GiB host is 81.729% idle,
with zero steal or cgroup-throttling increments and no memory PSI/events.
Host CPU PSI some increases 512.358 ms and probe CPU PSI some 200.743 ms: contention
is not literally zero. Host I/O PSI some/full increases 7.764/6.806 ms; the probe's
own I/O PSI stays zero. Process CPU totals 3,338.182 ms including the observer and
test verification; RSS peaks at 324,368 KiB, not a production memory bound.

546 host samples cost median 0.626 ms / max 3.944 ms, with maximum sample gap
23.062 ms. During the longest 175 ms request wait, eight samples complete; their
maximum internal gap is 20.717 ms. The enclosing 180.574 ms window has only
0.134 ms host CPU PSI some, 0.309 ms probe CPU PSI some, and no I/O/memory pressure
or TCP retransmission/timeout increments. The next two longest waits similarly
have regular samples and little pressure. A whole-runtime pause lasting the
entire long wait is inconsistent with these samples; per-goroutine/kernel/network
delay remains possible.

No observer-off calibration was run here. HTTP body hashing, timers, source
records, sampling and test verification have overhead; these descriptive
component observations are not an uninstrumented latency forecast.

No TCP retransmission or timeout increment occurs over the run. Host-wide
TCPAbortOnData/Close increase 3/1, with zero such changes in the three longest
enclosing wait windows. Counters are host-wide, not exact connection attribution.
No proxy environment is present on the original services or systemd manager.
This does not exclude transparent networking or remote service latency.

**Still unknown:** response arrival at the kernel versus Go callback wakeup,
and the OSS service's processing time for these requests. HTTP traces do not
carry request IDs or server-side durations yet. Do not label post-write time as
proven S3 processing time, or extrapolate this lightly occupied two-core run to
the historical 16-core runtime fixture.

## Environment and mistakes

- Refs fetched: sandbox0 0f092204, infra af04ea9. Frozen product/configuration,
  process PIDs, source rows, physical-empty state and NBD64 absence match before
  and after; warm job modify index remains 70668.
- This boot has zero ready carriers. Authenticated read-only listing finds
  277 failed / 74 completed historical allocations, all terminal (previous boot
  had 275/74). Two previous-boot carriers now show Driver Failure / Not Restarting;
  no new carrier was created or repaired here. Native CLI listing failed and
  the existing authenticated helper succeeded. Full startup tests require a
  healthy current fixture; do not turn this into a cause of old cold misses.
- Initial input preparation mistook batch stream stubs for full request lists
  and failed before writing outputs; corrected lookup and coverage tests pass.
- Eight Go race tests pass, including crypto context/cache-hit semantics and
  all request/checksum coverage. Three interval-analysis tests / 606 assertions
  pass an independent per-unit oracle; overlap is not counted twice.
- Eight existing encrypted-store regression tests also pass with the overlay
  under race detection: header reuse, independent cancellation, all-cancel retry,
  corruption rejection, object/chunk AAD, parallel header/data and stale-header
  safety. This is not a full product or privileged/runtime acceptance suite.
- The single remote probe finishes inactive/success/0. No restart/replay on a
  timeout. Skill uses Makefile lifecycle only, no Kind/bootstrap/deploy/local e2e,
  production changes, key changes, merges or tags. Preserve /data and stop compute.
  Final cloud read at 2026-09-12T03:39:10.526463750Z confirms
  Stopped/StopCharging, 2 CPU / 8 GiB, no public IP.

Evidence root: `/tmp/sandbox0-reader-cost.I5N7UF`. Reports, source manifests,
all timing intervals and sanitized environment observations are retained.

## Next action and unchanged acceptance

Follow-up correction (D-ARRIVAL-PATH): sealed September 9 kernel-arrival evidence
already answers the broad post-arrival-versus-pre-arrival question for an older
real claim + node-v cohort. Reuse it; do not repeat broad kernel tracing unless
current-version evidence changes that mechanism. It does not give arrival times
for this run's175ms outlier. The missing boundary is request-ID-bound OSS service
time versus network, not another three-object or uninstrumented Reader replay.
The next boot automatically replaces the two failed old allocations with two
new ready carriers, with no job/configuration change. Exact earlier errors are
registration409unique constraint conflicts, not storage timeouts. See
[ARRIVAL-PATH.md](ARRIVAL-PATH.md); no preceding result is erased by that recovery.

Do not prioritize RSA-only tuning, weaken crypto, resize blindly, add timeouts,
prewarm user RootFS or claim the serialized component numbers meet the SLO.
Size independence, true empty-node and cached-new runtime cohorts, authenticated
ingress-to-procd plus real command, populated large roots and actually occupied
production width remain required. Combined-layout net cost and correctness guards
also remain open; this experiment does not admit that prototype. Goal active.
