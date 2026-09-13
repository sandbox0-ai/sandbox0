# D-FIRST-TOUCH: screen cache retention against the cold-path deficit

2026-09-12. Evidence: `/tmp/sandbox0-first-touch.6eS2Bm`.
Local reanalysis of retained runtime evidence, not a new latency measurement.

## Outcome

The recorded cold cohort does not support payload-retention-only work as a
sufficient fix for its approximately666ms Coding combined deficit. There is no
eviction-induced source reload during claim. Only three relevant repeated source
attempts occur before command completion; their source interval union is24.145ms.
Even a deliberately generous screen crediting entire related NBD requests leaves
the recorded Coding combined time around2.560s, holding all other costs fixed.

This narrows candidate selection; it does not establish a universal lower bound,
prove that caches cannot help, or provide a measured speedup. The latest actual
current-pin cold combined maxima remain Node2.221s/Coding2.668s, with all eight
cold samples over2s. See [REGIONAL-RUN.md](REGIONAL-RUN.md).

## Inputs and independent checks

Reuse the sealed D-MAPPING-TRACE full regional cold cohort: eight new identities,
four per artifact, an initially empty target-node cache, real runsc/procd and
immediate node-v commands. Its historical stock pin is20260810; these source
spans are not timing attribution for the newer20260817 cohort. The complete
164-file trace index and frozen1353-product-file inventory are verified.

Replay cache entry/add/page/eviction events independently, keyed by cache identity,
content checksum and decoded length. Events must precede the exact source start;
an eviction without a retained entry fails closed. All2082 per-member prior-cache
flags match the original attribution. The cold trace has396 RangeSource attempts
and7187 NBD requests:388 source attempts are NBD-owned claim/command work, two
are initial constructors, and six are post-command work outside measured windows.

The table aggregates shared-source attempts across all four guests per image;
it is neither per-guest demand nor a count of mandatory application reads.

| Image / phase | Source attempts | No member previously available | Any prior member | All prior members | Any previously evicted member |
| --- | ---: | ---: | ---: | ---: | ---: |
| Coding claim | 66 | 65 | 1 | 0 | 0 |
| Node claim | 75 | 70 | 5 | 0 | 0 |
| Node first command | 185 | 180 | 5 | 0 | 0 |
| Coding first command | 62 | 54 | 8 | 3 | 3 |

"Prior" includes currently retained or earlier inserted content, not arbitrary
provider/OS caches. A coalesced read containing one known member can still require
first-touch content; eliminating that entire read is not a cache-only operation.
RangeSource attempts are not SDK HTTP request counts. Stored lengths are encoded
range coverage, not transferred ciphertext bytes.

## Fixed-other-cost screening

Select source attempts using three rules: any previously evicted member, all
members already known, or any member already known. Follow leader-to-waiter NBD
singleflight edges transitively. For the generous screen, credit whole affected
NBD requests from BOTH images, including their unrelated work and followers from
any time in the same trace. Clip unions to each sample's claim-start-to-command-
completion interval. Do not sum parallel durations or multiply shared work by
four. Six post-command selected calls remain in the evidence but receive no credit.

The first two rules select the same three measured Coding command calls
(IDs334/383/385), totaling481125 encoded range bytes. Their durations are
8.809/5.924/9.412ms. There is no selected cold-claim source interval. All four
Coding combined clocks originally lie between2665.827 and2665.969ms.

| Credit rule | Source interval union | Whole affected NBD union | Coding combined after source credit | After whole-NBD credit |
| --- | ---: | ---: | ---: | ---: |
| Evicted content / all content known | 24.145ms | 105.621ms | 2641.683-2641.825ms | 2560.206-2560.348ms |
| Any content known, including mixed first-touch reads | 298.211ms | 494.355ms | 2367.617-2367.759ms | 2171.473-2171.615ms |

The last row even credits eliminating19 pre-completion source attempts containing
some previously known content, including reads that require new data. This
overcredits a retention-only change and still does not close2s in this screen.
Changed scheduling, admission, CPU/decode cost and request geometry are not
modeled. These subtractions are not counterfactual runtime measurements or bounds
on every possible indirect effect. They do not justify promising the credited
milliseconds as an achievable optimization.

## Machine, S3 and historical cache variation

The current full-path run shows no lease throttling/OOM or whole-host CPU/memory
exhaustion in its unoccupied16CPU fixture. It does not exonerate serial CPU work,
transient scheduling or densely occupied nodes. NBD-backed host I/O wait does not
identify physical-disk slowness, S3 processing time or network latency.

Prior exact request-ID/SLS correlation establishes a contribution from the remote
response path in that separate Reader trial. D-COMMAND-COST also records similar
CPU/bytes but markedly different response waits and local command durations.
Neither result is proof of an OSS fault or a current-pin regional causal split.
Keep provider-cache limitations explicit; no provider-side cold claim is made.

Correct the preceding next-action premise: cached Coding command variation is
not new with the current stock pin. D-MAPPING-TRACE already recorded562-578ms,
B-RECENCY-ABBA607-759ms and D-HTTP-CALIBRATION up to796ms. Existing exact cache
evidence proves payload eviction/reload in its own cohort, with zero cached
mapping/header provider reads. It does not explain each newer sample. Do not
repeat a complete cached trace merely to rediscover this known behavior.

## Decision and handoff

Do not pursue larger caches, recency-only changes or compressed retention as the
main cold-start fix on the basis of cached-new latency. No such candidate is
implemented or adopted here. Focus candidate admission on first-touch payload
dependencies and remote response cost, with enough coverage of the actual regional
deficit. [HTTP-DEPENDENCY.md](HTTP-DEPENDENCY.md) already contains request-level
coverage screens; do not repeat broad S3, machine, header or layout baselines.
Any proposed mechanism must identify which first-touch dependencies it changes,
quantify net CPU/memory/request costs, and pass an actual controlled full-path run.

Six local tests/210 assertions and48 independent endpoint-bin checks pass;
full analysis reproduces exactly. Zero new claims/commands, remote starts,
imports/object writes, product edits or production changes. A fresh read-only
cloud receipt confirms the restored2CPU/8GiB test ECS isStopped/StopCharging.
Shared tenant-neutral carriers, claim-time RootFS binding, stateless nodes,
encrypted durable authority and the unchanged10s request budget remain intact.
The1s/2s gates, populated-root/upper-history coverage and genuinely occupied
physical-width acceptance remain open. The goal is not complete.
