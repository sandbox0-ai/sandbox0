# D-RESIDENT-PREFLIGHT: bounded occupied-node fixture preparation

2026-09-13. Local preparation only; no remote/cloud/S3/claim/command operation,
latency improvement, production change, merge, tag, or local e2e.

## Why this lane and what it does not repeat

The previous bounded read-hedge model was rejected before implementation. The
next missing qualification is current-candidate occupied cached-new admission,
with resident identities/leases held constant across idle, loaded and released
controls. Earlier occupied trials do exist: historical `D-OCCUPIED` used six
residents/two targets and remains valid for its own artifacts and boundaries.
Do not overwrite that ID or pretend density was never measured. This turn's
initial PLAN.md reused the ID by mistake; AMENDMENT.md corrects it before remote
activity. The newer current-pin eight-way regional run used only about868MiB
guest memory, so that run is not occupied-memory evidence.

The intended topology is seven existing residents and one target startup slot,
not eight concurrent new claims. Targets keep real authenticated regional claim
and immediate `node -v`, unchanged10s request budgets, no POST retries and separate
claim/command/combined metrics. Their new identities run on an occupied cached
node; no cache-empty label or target RootFS prewarm is allowed. Empty-node and
populated-large-root/upper-history gates remain separate and unsatisfied.

## Preflight caught an invalid resource assumption

The tentative7GiB/1750m lease with6GiB per resident is rejected before use. A
local call through the actual `pkg/template.ResourcePolicy` with the historical
regional baseline's1GiB/CPU, max4GiB rejects7GiB. Even without that ceiling it
would derive7000m, not1750m. No platform ratio/ceiling was changed and this is not
a fresh deployed-policy inspection.

Keep1792MiB/1750m for each of eight leases (14GiB/14CPU reservation). Proposed
resident load is1536MiB plus two workers each:10.5GiB allocation across seven
residents on the prior16CPU/64GiB test shape. This is a CPU-busy occupancy
comparison, **not**64GiB memory pressure, reclaim, or production-width acceptance.
The same policy and enforcement must be rechecked on the actual test node.

Proposed observation gates are actual resident charge >=1536MiB per lease,
increase above measured idle maximum >=95%of allocation, zero swap/OOM/max
events, and average CPU >=80%of the lease across the bracketing target interval.
Idle/released controls require <=512MiB and <=200m average CPU per resident.
These are fixture validity screens, not platform SLOs or instantaneous CPU
guarantees. A failure remains invalid evidence; do not tune thresholds after
seeing a latency result.

## Implementation and verification

Added an explicitly controlled native guest helper at `resident-load/`. It
touches each page before acknowledging load, starts workers before reporting
loaded, joins workers before memory release, and has one load/release lifecycle.
It stops on invalid/duplicate input, EOF, signal, telemetry error, cancellation
and fixed expiry. Bounds: up to8GiB,32workers,1–50s lifetime including idle;
these ceilings are not default workload sizes. Stdout is ordered250ms telemetry,
not evidence of host residency. Expiry may have no terminal event and exit0
does not satisfy an occupied interval. A bounded async-context TTL and independent
owned-sandbox cleanup remain necessary.

The local pure evidence verifier rejects missing/aliased residents, changed
boot/lease/cgroup inode, insufficient resident memory or CPU, swap/OOM/max events,
CPU counter resets, gaps/stalled sampling, stopped/paused/expired contexts,
missing/rewritten/stale events and early release. It requires before/after
live exact-context evidence and complete host bracketing. It does not yet collect
these observations or replace authenticated HTTP/PG/physical-cleanup proofs.

- Normal tests for both `tools/runtime-slot-slo` and the helper pass.
- Helper race tests repeated three times pass, including real local small
  subprocess SIGINT/SIGTERM tests. Only tiny16KiB allocations are used locally.
- Helper vet passes. Go1.25.5, CGO0, linux/amd64, trimpath, stripped build succeeds.
  Artifact SHA-256:
  `292554c32eb536c6e1438bfe6768d63c4be1fcfffa7aef24185d0a01354140ce`.
- Pure observer fixtures:33tests/38assertions, no failures/errors/skips. These
  are synthetic negative/positive tests, not real cgroup/runtime acceptance.
- Prior114 sealed artifacts and the1355-file source baseline were verified
  before edits. Only the diagnostic README and three new helper Go files enter
  the revised product inventory; experiment notes remain separately recorded.
  No manager/ctld/driver/runtime or RootFS format change occurred.

## Next concrete step

Wire and test the one-shot authenticated async-context control, same-host
cgroup/PG sampler and target orchestration. Preserve the same seven guest
contexts through idle/load/release; collect actual cgroup memory fall before
reverse control. Resolve unknown POST outcomes without replay. Only after those
checks pass, start the exact test ECS, qualify original state/policy, run the
bounded comparison and independently restore/stop compute.

No new remote sample is added by this preparation. Last actual current-pin cold
combined maxima remain2.221s Node and2.668s Coding; cached-new maxima remain
1.029/1.673s. All original generic-carrier, claim-time RootFS, stateless durable
authority,1s/2s, full regional boundary and real-command requirements stay open.
The last remote stopped receipt is historical, not a fresh state query.

Private evidence: `/tmp/sandbox0-occupied.VpIIc2`.
