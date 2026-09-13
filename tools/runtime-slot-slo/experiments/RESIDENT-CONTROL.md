# D-RESIDENT-CONTROL: one-shot control and sampled target windows

2026-09-13. Local component integration; no remote/cloud/S3/claim/guest-command
operation, runtime change, latency improvement, production action, merge/tag or
local e2e. Full occupied-node acceptance remains pending.

## Concrete progress

- Preserved the1358-file current source candidate and verified the preceding
  22sealed artifacts. Reused exact current claim/workload/case implementations
  in the private fixture; only the copied CLI main name changes for composition.
- Implemented authenticated resident upload/readback, literal async CMD creation,
  single-use input control and read-only live-context polling. File path absence
  is required before upload; hash readback and a literal preparation command
  precede helper execution. Only already-claimed owned residents are eligible.
- Current OpenAPI and procd require `ContextInputRequest.data`, not the earlier
  narrative `input` assumption. Use the generated type and an actual newline;
  the input route forwards bytes without adding one. No invalid remote request
  was sent. Request budget remains10s, async TTL60s, guest lifetime45s.
- One-shot intent is required before each application mutation. Failed intent,
  duplicate/concurrent input, transport loss, timeout,503, redirect and malformed
  or oversized response cannot trigger application replay. Context IDs cannot
  alias another resident; owner/sandbox/context must all match. Live responses
  require explicit running/unpaused status, no terminal exit/state or stderr.
- Stdout parsing checks exact owner/PID, sequence, fixed lifetime and monotonic
  clock. An unfinished final line is counted but never invented as an event.
  Complete invalid lines fail. Guest environment/raw response contents are not
  exported in observations or errors.
- Added a real file-based host sampler, tested against temporary regular-file
  fixtures. It pins seven distinct lease/cgroup directory inodes and reads fixed
  files relative to their open descriptors without following symlinks. Missing,
  renamed/replaced cgroups, changed enforcement, invalid/duplicate counters and
  changed node boot fail. Raw OOM/swap/pressure and host memory/CPU/disk/vmstat
  counters remain visible; allocation is not substituted for resident memory.
- Composed a single target window: exact PG bindings and live guest checks before,
  continuous100ms host sampling across the target callback, then the same
  identity checks after. All clocks use the same host's CLOCK_MONOTONIC; the
  target's canonical readiness/HTTP/command timing remains the original harness.
  Failed targets keep known sandbox identity for independent cleanup. Collection
  success is explicitly not occupancy/SLO acceptance; strict evidence checks
  still have to admit the window.
- Added the isolated read-only PG query adapter. SQL binds lease/slot/claim,
  allocation/sandbox runtime generation, node incarnation, exact resources and
  RootFS artifact, and requires live active records plus writer authority.
  Its guarded execution requires the separately prepared config hash and exact
  test host/database; credentials and server errors are withheld. It has not
  connected to PostgreSQL in this turn.

## Checks and retained failures

Normal corrected TLS tests pass. Final21top-level component/window tests pass
three race repetitions; vet and native/amd64 test-executable builds pass.
Three pure SQL/guard tests pass26assertions. Negative cases cover application
POST replay, wrong owner, aliased identities, missing liveness, partial/malformed
telemetry, cgroup replacement, counter/enforcement drift, stopped samplers,
changed generation before target and failed-target identity retention. Existing
bounded sandbox cleanup is reused and mock-checked after run-context cancellation,
including an unknown command context ID. This is not physical remote absence.

Retain two fixture failures: an initial1ms cleanup poll violated the common
minimum and was rejected; a fake server timeout handler then held test cleanup
until the unchanged30s test bound fired. Correct the fixture to100ms polling
and an explicit handler release/body consumption. Do not count either as a
runtime defect or extend a production/public request timeout to hide it.

These checks use local TLS servers, small temporary counter files and a target
callback, not a real regional claim, guest process, kernel cgroup or high-density
workload. The cross-built file is a test executable, **not** a complete remote
acceptance runner. No new performance conclusion follows from it.

## Remaining immediate work

Wire the complete bounded matrix and its durable owned-identity cleanup set:
seven resident claim/upload preparations, parallel helper creation, same-identity
idle -> loaded -> released phases, real Node/Coding target callbacks, measured
phase-entry occupancy, strict final evidence verification and automatic carrier
refill. Then qualify the current isolated Nomad setup and execute remotely with
independent physical absence, original runtime/job restoration and StopCharging.

Keep the prior corrected1792MiB/1750m leases and1536MiB/two-worker resident plan.
Seven residents plus one target is one-wide CPU-busy occupied cached-new admission,
not global cache-empty,64GiB reclaim or eight simultaneous new starts. Historical
occupied results remain intact. All generic-carrier, claim-time RootFS, stateless
PostgreSQL/encrypted-S3 authority, populated-root/history,1s/2s regional startup
and immediate real-command requirements stay open. Actual cold combined maxima
remain2.221s Node and2.668s Coding; there are no new remote timings this turn.

Private source/tests/receipts: `/tmp/sandbox0-resident-control.aIJpy6`.

Read-only upstream-head checks at the end of preparation confirm core main
`0f09220460581bfc1fdc331f34ebc85bf38381e7` and infra main
`af04ea978f62b6eceaa78da98840227505ca764b`. No branch or remote ref was changed.
