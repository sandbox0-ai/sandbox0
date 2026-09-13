# D-READ-HEDGE: response-tail duplication does not pass admission

2026-09-13 Asia/Beijing. Offline dependency/cost screen, not a product change or
a measured startup improvement. Evidence: `/tmp/sandbox0-read-hedge.cfUMAs`.

## Decision

Do not implement the screened delayed-duplicate GET policy or start remote
compute to tune it. Under the declared optimistic response assumption, its
bounded request budget cannot cover the recorded cold Coding2s deficit. Removing
the budget requires over60% additional cold requests and still misses the slower
cycle. This is rejection of this particular admission premise, not proof that
all hedging, lower-latency storage or other scheduling approaches are impossible.

This is distinct from increasing source concurrency or request timeouts: the
hypothesis duplicates only an already-issued immutable read after a response
delay. It performs no tenant prewarm. No such behavior is enabled in product code.

## Fixed policy and assumptions

- Preserve the original request; after10ms from request write, a still-unanswered
  range no larger than1MiB may launch one duplicate, with no recursive duplicate.
- Assume a duplicate returns its first byte in7ms. This is an optimistic screening
  assumption, NOT measured independent latency, a provider guarantee, or an
  assertion that a just-requested cold object becomes available that quickly.
- Original earlier response wins. Retain the original observed body duration,
  decoding/authentication/checksum work and all other costs. A real implementation
  would also need bounded cancellation/drain and verified body ownership.
- One token initially, cap one, refill one fifth per original startup HTTP GET;
  at most two modeled duplicate requests active. This caps duplicates at
  `1 + floor(original_attempts / 5)` per cohort, not an unbounded burst.
- Also screen all eligible requests with that same10+7ms assumption, an explicitly
  unrealistic instantaneous-duplicate case, and perfect response correlation.
  No delay, cache, machine or concurrency parameter sweep is performed.

Reuse the sealed D-HTTP-CALIBRATION's two complete cold/cached-new cycles and
D-HTTP-DEPENDENCY's exact HTTP/source-flight recipient maps. There are1286
startup HTTP attempts, not all1298capture attempts:12post-command attempts are
excluded. Four unowned startup constructors remain distinct from1282NBD-owned
attempts. There are32original sandbox identities and64original phase windows;
four modeled policies do not create128new runtime samples.

For each selected response, intersect only the modeled removed post-write tail
with its explicit recipients' NBD blocking intervals, then union within the exact
sandbox and claim/command boundaries. Also report single-outstanding NBD credit.
Do not multiply one physical duplicate by the number of shared-flight followers.

## Cold results

Each timing range covers four original Coding samples; these are hypothetical
fixed-other-cost remainders, NOT measured elapsed times or kernel critical-path
bounds. Both cycles precede the latest production runsc pin.

| Policy / cycle | Extra GETs / original | Extra requested cipher bytes | Original Coding combined | Owned tail credit | Hypothetical combined remainder |
| --- | ---: | ---: | ---: | ---: | ---: |
| Bounded /1 | 58/424 (13.68%) | 5,959,698 | 2753.642–2753.768ms | 226.790ms | 2526.853–2526.978ms |
| Bounded /2 | 60/425 (14.12%) | 8,903,182 | 2568.143–2568.261ms | 222.835–223.338ms | 2344.821–2345.427ms |
| Unbounded /1 | 268/424 (63.21%) | 43,053,318 | 2753.642–2753.768ms | 545.342–545.365ms | 2208.283–2208.413ms |
| Unbounded /2 | 256/425 (60.24%) | 44,518,576 | 2568.143–2568.261ms | 594.179–596.193ms | 1971.965–1974.082ms |

Even crediting every overlapping unowned constructor tail to each Coding sample,
the bounded remainders are2490.041–2490.167ms and2328.558–2329.164ms. It still
fails before charging extra traffic, cancellation or induced queueing. The
unbounded second-cycle number below2s does not validate the policy: cycle1 fails,
its own single-outstanding screen remains2092.784–2120.892ms in cycle2, and neither
screen establishes mandatory guest dependencies or changed network/load behavior.

Ordinary Node is retained: bounded owned credits are98.590–105.330ms and110.870ms,
with hypothetical combined remainders1899.430–1906.272ms and1740.981–1741.078ms.
These are not new Node passes. Bounded cached-new cohorts still launch30/219 and
29/218 duplicates; they add3,904,152 and2,034,096 requested bytes. All cached-Node
absolute timings retain the preceding observer-representativeness rejection.

Unbounded instantaneous duplicates after10ms yield a favorable Coding remainder,
but require a zero-time second response and over60% additional requests. This
unrealistic case only shows how much the conclusion depends on storage response
cost. With perfectly correlated responses the bounded policy still incurs58/60
cold duplicates and yields zero credit. Neither independence nor correlation is
measured here. Byte counts are requested-range cost; canceled bytes actually
transferred and provider billing/queue work are not observed.

## Limits, validation and preservation

The model keeps original request arrival times. Changed dependencies could move
later requests, alter cache/admission order or increase contention. NBD may include
kernel readahead. Unrecorded header-waiter edges remain unmodeled. Thus subtracting
owned intervals is an admission screen, not predicted speedup, a universal lower
bound, proof of an OSS defect or evidence of healthy occupied machines.

Nine policy tests/23assertions pass, covering delay, original wins, body lifetime,
range/budget/concurrency bounds, correlation, invalid inputs and deterministic
ties. Independent endpoint-bin integration validates512owned/sole phase measures
across8748covered bins. The initial analysis used the wrong image-name predicate
for cached-Node exclusion. Preserve its source/results; v2 fixes all32modeled
cached-Node flags, with every numeric/policy result independently proved unchanged.

Reverify93preceding recovery artifacts,30dependency artifacts,323calibration
artifacts and the current1355-file product inventory. Do not invoke the old
1353-file constructor audit against the new recovery candidate. Fresh main refs
remain sandbox0`0f092204` and infra`af04ea978`. Only requested experiment notes
change in the repository; product code and user changes remain intact.

No remote/cloud query, GET, claim, command, import, prewarm, deployment, timeout
increase, production operation, merge/tag, deletion or local e2e occurs. The last
remote observation is the prior recovery turn's16:43:29UTCStopped/StopCharging
receipt, not a new observation. The real current-pin cold combined maxima remain
Node2.221s/Coding2.668s. Generic shared carriers, claim-time RootFS binding,
stateless nodes, encrypted durable authority, populated-root/history coverage,
actually occupied width and all1s/2s regional/first-command gates remain open.

## Next distinct acceptance work

Do not replace the rejected policy with another delay/budget sweep. Qualify the
missing occupied-node condition using the existing regional ingress/claim/real
command harness and exact leased resources. A synchronized eight-way burst on an
otherwise empty16CPU/64GiB machine is not that condition. Existing workload steps
finish and each batch is deleted; they do not retain verified resident CPU/memory
load while another identity starts. Any resident-load extension needs ownership,
live occupancy checks through the target window, no POST replay and complete
terminal cleanup, plus an unloaded matched control. No such test is claimed here.

Do not label a node with already-running guests as globally cache-empty. Separate
the existing genuinely empty-node cohort from occupied cached-new or explicitly
measured target-content misses. Keep the shared cache and all lease/authority
boundaries; do not clear live caches or prewarm target roots to manufacture a
result. This closes a real requirement gap rather than repeating the prior
unoccupied hardware-size comparison.
