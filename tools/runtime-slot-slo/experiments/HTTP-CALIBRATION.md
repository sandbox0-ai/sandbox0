# D-HTTP-CALIBRATION: complete-path HTTP attribution and observer controls

Date: 2026-09-12. Diagnostic result, not production acceptance or a new SLO.
The fundamental requirements in [STRUCTURED-OPTIMIZATION.md](STRUCTURED-OPTIMIZATION.md)
remain unchanged. No product or RootFS format changes in this campaign.

## Outcome and fixed experiment

Four fresh boots, observer OFF/ON/ON/OFF. Each has eight concurrent claims
(four Node22, four Coding), followed by a second eight-wide cohort of cached-node
NEW identities. Each lane immediately executes real `node -v` after its claim;
there is no batch-wide claim barrier, POST retry or replacement sample.
All64claims and64commands succeeded; all cleanup and cold-proof checks passed.
Functional success is not a latency pass.

The previous failed `D-RUNTIME-HTTP` campaign remains failed with zero claims.
This new campaign first validated actual cgroup ancestry and a bounded trace
storage budget on the original2CPU machine, before installation or resizing.
Eight observer/budget tests (16assertions) and complete objectstore/rootfsblock
Go race suites passed. Missing leaf `cpu.max` is explicit null, not a fabricated
quota; ancestor limits and required stat/pressure files remain checked.

Use the same frozen1353-file candidate, ordinary artifacts, diagnostic binary,
100ms external sampler, provider, encryption/checksums, request timeout10s,
admission and carrier/resource semantics in both arms. No tenant RootFS prewarm.
Each cold proof has a new boot, eight ready carriers, no owned physical runtime,
zero target-store outgoing packets and zero NBD I/O before claims.

Historical16CPU/64GiB/eight-carrier fixture is for comparable diagnosis, NOT a
hardware optimization or production parity. It still uses private regional TLS
and runsc release-20260810.0. Coding is1TiB logical but only5,471,080,448allocated
bytes; Node is16GiB logical and333,643,776allocated bytes. This does not establish
populated-large size independence. Eight exact leases total14CPU/14GiB, but their
observed memory.current sum is only921,292,800–924,766,208bytes: not occupied-density
acceptance. Current main refs checked at preparation:0f092204/ceb895c.

## All cohort maxima

Milliseconds; each row contains four samples. Claim is authenticated regional
ingress through procd readiness; combined is the measured claim-request start
through command completion, not the sum of marginal maxima. Client claim response
time, per-sample timestamps, minima and medians remain in the raw/derived JSON.

| Cycle / observer | Cache | Image | Claim max | Command max | Combined max |
| --- | --- | --- | ---: | ---: | ---: |
| 0 / OFF | Empty node | Node | 809.826 | 1154.560 | 1962.491 |
| 0 / OFF | Empty node | Coding | 1612.911 | 1061.593 | 2672.228 |
| 1 / ON | Empty node | Node | 894.195 | 1114.114 | 2004.862 |
| 1 / ON | Empty node | Coding | 1629.101 | 1116.760 | 2753.768 |
| 2 / ON | Empty node | Node | 738.923 | 1116.454 | 1851.948 |
| 2 / ON | Empty node | Coding | 1535.784 | 1034.985 | 2568.261 |
| 3 / OFF | Empty node | Node | 853.052 | 1206.664 | 2066.036 |
| 3 / OFF | Empty node | Coding | 1562.647 | 1072.996 | 2633.991 |
| 0 / OFF | Cached-new | Node | 626.078 | 449.812 | 1060.955 |
| 0 / OFF | Cached-new | Coding | 789.223 | 796.237 | 1579.736 |
| 1 / ON | Cached-new | Node | 665.194 | 484.410 | 1144.129 |
| 1 / ON | Cached-new | Coding | 836.469 | 763.584 | 1584.510 |
| 2 / ON | Cached-new | Node | 531.037 | 504.501 | 1024.153 |
| 2 / ON | Cached-new | Coding | 748.616 | 686.258 | 1421.558 |
| 3 / OFF | Cached-new | Node | 609.820 | 378.296 | 988.000 |
| 3 / OFF | Cached-new | Coding | 1029.755 | 636.604 | 1633.294 |

| Cohort | Samples | Claim >1s / >2s | Command >1s / >2s | Combined >1s / >2s |
| --- | ---: | ---: | ---: | ---: |
| Empty node | 32 | 16 / 0 | 32 / 0 | 32 / 24 |
| Cached-node new identity | 32 | 1 / 0 | 0 / 0 | 28 / 0 |

Cold Coding misses combined2s in all16samples, including both OFF controls.
Cold Node misses combined2s in8of16. Cached results must not stand in for empty
workers, and the final OFF Coding claim1s miss remains recorded.

## Observer calibration is not a blanket pass

Predeclared caution rule: if BOTH ON cohort maxima are >50ms OR >5% above BOTH
OFF maxima, do not treat traced absolute latency as representative. Compare
cohorts, not correlated lanes as independent repetitions. Aggregate mixed-image
maxima do not trigger the rule; this is not proof of zero or bounded overhead.

The per-image extension DOES reject cached-new Node command timing: OFF maxima
449.812/378.296ms versus ON484.410/504.501ms. Keep this exception; the mixed-image
aggregate hides it. No other per-image metric triggers that rule. Use these
traces to establish ownership/wait locations, not as an unbiased absolute
baseline for that rejected metric. Two repetitions cannot disentangle observer
cost from boot/order/service variability or justify subtracting a fixed overhead.

## HTTP ownership and actual waiting

Eight complete valid trace files total51,531,521bytes; every file remains below
its64MiB hard cap. No overflow/automatic stop, incomplete task/region, ambiguous
singleflight edge or trace-control error. Go1.25.5 parsing recovers28,761NBD reads;
the standby has no NBD/HTTP work in these captures. Clock-offset drift magnitude
is at most22,845ns. These facts do not prove whole-lifecycle HA correctness.

All1,298HTTP attempts return206/HTTP1.1 with one GotConn and one WroteRequest,
no observed response/body/close errors. Only6connections are new;1,292are reused.
Actual ciphertext bodies total170,429,098bytes. Fifty-two bounded ranges end
short at object EOF, never exceeding the requested range; not read failures.
Of these attempts,1,282have exact NBD ownership through an active region or a
goroutine born inside that read. All overlap their owner's claim/command window.
The remaining16attempts retain unknown NBD ownership. Subsequent exact-window
analysis in [HTTP-DEPENDENCY.md](HTTP-DEPENDENCY.md) corrects their earlier
constructor annotation:4are startup mapping constructors and12occur after all
first commands.1298is the whole-capture total;1286belong to startup windows.

| Primary trace | HTTP attempts (new / reused) | Reused post-write-to-first-byte median / max, ms |
| --- | ---: | ---: |
| Cycle1 cold | 430 (3 / 427) | 15.284 / 50.822 |
| Cycle1 cached-new | 219 (0 / 219) | 13.581 / 66.860 |
| Cycle2 cold | 431 (3 / 428) | 12.883 / 64.148 |
| Cycle2 cached-new | 218 (0 / 218) | 10.194 / 54.998 |

For example, cycle1 cached Node command attempt563 waits66.860ms after write:
its first-byte callback goroutine spends66.802ms Waiting,0.000256ms Runnable,
0.016384ms Running and0.041472ms Syscall, with zero traced stop-the-world overlap.
Cycle2 cold Coding command attempt316 waits64.148ms; callback Waiting64.124ms,
stop-the-world overlap0.158ms. This argues against connection churn, long GC
pauses or prolonged Go runnable delay as the explanation for THESE waits.
It does not distinguish kernel/network/remote-service delay or establish an
end-to-end savings budget. Callback goroutine waiting is not the entire transport.
Body drain is usually short but not uniformly so: reused body tails reach43.551ms.

The two cold captures assign respectively266/264HTTP attempts to Node and156/159
to Coding. Node command alone owns186in each; Coding command73/75. More GETs
do not imply a longer serial critical path: Coding still finishes later. Do not
sum HTTP durations or count shared-flight followers as additional remote work.
Headers/data already overlap; provider reuse already exists in the candidate.

## Concurrent pressure and remaining causal boundary

The100ms sampler brackets the complete workload, retaining before/after slack
(at most93.449/82.392ms) and actual sample gaps (max101.603ms). Across cold cohorts,
primary ctld cpu.stat usage increases2.359–2.604CPU-seconds over2.613–2.813s enclosing
windows; its CPU PSI `some` increases12.696–15.873ms. Leaf quota is absent, both
non-root ancestors are `max100000`; no finite ancestor quota was observed.
No memory PSI increment is observed in either ctld or host over these windows.
This is not proof of no CPU bottleneck, exact CPU attribution or density fitness.

Host I/O PSI `some` increases986.422–1135.661ms in cold windows; this is not enough
to identify a physical disk, remote store or NBD dependency as its cause. Retain
that unresolved boundary instead of labeling all waiting as transport latency.
Both-ctld sampled RSS max ranges361,136,128–461,139,968bytes across all cohorts;
host minimum sampled available memory stays above63,261,335,552bytes. Sampled
RSS is not the complete RootFS subsystem peak.

The subsequent [HTTP-DEPENDENCY.md](HTTP-DEPENDENCY.md) links HTTP subphases to
explicit source/shared-flight dependencies, with an independent interval oracle.
An admissible
structural change must remove enough measured dependent work from the combined
path, preserve independent authentication/COW/cleanup, bound node-wide memory,
and not depend on pre-claim tenant data. Existing leaf-only packet and whole-root
constructor screens remain rejected as standalone fixes. Do not restart a format,
cache, timeout, pool or concurrency sweep merely because an aggregate looks better.
No additional broad remote tracing run is justified by these already-owned data.

## Retention and recovery

Local evidence: `/tmp/sandbox0-http-calibration.myJt7P`.
Remote retained evidence: `/data/sandbox0-http-calibration-myJt7P`.
All124allowlisted exported artifacts individually SHA-256 verified; no private
configuration, database dump, credentials or signed URLs exported. The isolated
test DB `s0_http_calibration_myjt7p` retains2136rows; source2072/263rows and exact
artifact inventories are unchanged. Original binaries/configs/running processes
and two-carrier job70668 verified restored at2026-09-12T00:36:17UTC, with no owned
leases/mounts/writers/branches/import scratch and all64NBD devices idle.

SSH is closed and terminal. At2026-09-12T00:40:25.305309375UTC the authoritative
cloud query confirms original ecs.g9i.large2CPU/8192MiB, Stopped/StopCharging.
All owned test, collection, analysis and cloud handles are terminal; /data is
preserved. The final audit reverifies1353product files,164prior runtime-trace
artifacts,138previous failed-campaign artifacts and54fixed-range HTTP artifacts.
Preparation failures, including the initially unsuccessful same-boot cold check,
remain in attempts.md. No product edits, production rollout, merge or tag. The
fundamental size-independence/full-command/occupied-density gates remain open.

All323local artifacts sealed. Evidence SHA-256:
`044dab092235d0462c4bd6a33f2903b22591c09a186a966915f0224a013b0b85`.
Artifact-index SHA-256:
`c2d495c681cba6726d8d4b7ca6d6ce33eb07154055157c2f858515d5f86d6976`.
