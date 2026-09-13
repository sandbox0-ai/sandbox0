# D-PREFETCH-TRIAL: command-triggered ELF reads do not earn activation

2026-09-13. Local evidence: `/tmp/sandbox0-prefetch-trial.SeM8xy`; retained
remote diagnostics: `/data/sandbox0-prefetch-trial-SeM8xy`. Reuse the exact
[PREFETCH-IMPORT.md](PREFETCH-IMPORT.md) artifacts without repeating imports,
full image hashes, ELF geometry, stock-hint tests or legacy carrier diagnosis.

## Result

Do not activate this candidate. The complete same-binary comparison shows a
cold Node improvement in this pair, but a cold Coding regression and cached-new
regressions for both images. Coding still misses the accepted2s combined gate
in every cold sample, on and off. This is not a claim-readiness solution or a
universal startup guarantee. Four samples/image/cohort do not establish p99,
worst-case bounds or statistically stable treatment effects across boots.

All values below are seconds, maximum of four actual samples for each metric
independently; do not add separate maxima. The combined clock starts at the
regional ingress claim request and ends after immediate literal `node -v`.
Readiness is the separately retained authenticated command-ready clock. Optional
pre-exec work remains inside command and combined time, not excluded as setup.

| Node cache / image | Treatment | Claim | Readiness | Command | Combined | Combined >2s |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Cold / Node | off,03 | 0.914032022 | 0.898109000 | 1.157541922 | 2.055244450 | 4/4 |
| Cold / Node | on,02 | 0.704142024 | 0.689269000 | 0.954562644 | 1.637823509 | 0/4 |
| Cold / Coding | off,03 | 1.638870255 | 1.623491000 | 0.881405142 | 2.514395710 | 4/4 |
| Cold / Coding | on,02 | 1.419684050 | 1.403124000 | 1.468634062 | 2.872879377 | 4/4 |
| Cached-new / Node | off,03 | 0.512253327 | 0.509065000 | 0.402295653 | 0.911113563 | 0/4 |
| Cached-new / Node | on,02 | 0.495112327 | 0.492289000 | 0.873528885 | 1.356222159 | 0/4 |
| Cached-new / Coding | off,03 | 0.580071682 | 0.576931000 | 0.055946643 | 0.636043608 | 0/4 |
| Cached-new / Coding | on,02 | 0.737357278 | 0.733969000 | 0.850638149 | 1.573244887 | 0/4 |

The treatment starts only after each sandbox's user command arrives. Claim
differences across separate boots are not evidence of a direct claim speedup;
other concurrent commands can also overlap a slower member's claim. No claim
prewarm, sandbox-wide opt-in or Node-specific working-set profile is used.

## Trigger, budget and exact inputs

The executable is known after the real CMD request resolves its exec.Cmd with
the existing PATH/CWD/environment, not before tenant selection. The private
hook runs after cancelable runner creation and before exec. It reads initialized
PT_LOAD file ranges of the main ELF in parallel. It does not know which pages
the program will actually touch, predict the next command, speculate library
paths or fetch the whole RootFS. This distinction explains why extra I/O can
outweigh avoided serialized demand reads.

Both treatments use procd SHA256
`8eec5c8a9cbdc53fc77ae2dc82596a76f0df3aaccc40ced3ead934a55023cda5`,
protocol `sandbox0.procd.v3`, and the SAME artifact for each image:

- Node: `sha256:b31477ddf25372952ce0f1546c298654c359368397f71a02fe534b9b886c49a1`.
- Coding: `sha256:058e029538b7c20c3bd14dba439930181b5f6be3012dc691244561f089d6e089`.

The unchanged bounded prototype admits one job/procd, four1MiB readers and
128MiB reserved ReadAt bytes including metadata. Optional waiting is capped
at500ms; actual public HTTP budgets remain10s. Kernel work can outlive the
optional wait while retaining its admission/resources. This procd does not
export internal completion/pending counters: neither a cap hit nor zero pending
I/O may be inferred from these latency samples. Terminal physical cleanup is
proved separately. Safety for arbitrary user-created non-noatime mounts remains
unqualified; no product default or production runtime source is changed here.

Only the diagnostic workload implementation/test/README change. Per-step
`env_vars` is forwarded through the already-supported top-level CMD request:
`S0_EXEC_PREFETCH=elf-v1` versus `off`. Literal argv stays `["node","-v"]`, with
expected stdout `v22.23.2\n`; there is no shell/env wrapper or claim-body change.
The context response must confirm all requested keys, including explicit empty
values. Reports retain only requested entries, never inherited environment
secrets. Empty maps preserve existing request/workload hashes. The64KiB workload
limit includes env; invalid keys/NUL values fail validation.

Full tool-package race count3 passes243 test events, with no failures; vet and
linux/amd64 CGO0 build pass. Tests cover literal env, incorrect/missing response
confirmation, no unrequested-secret leakage, cleanup/no retry, validation,
hashes and JSON roundtrip. No external OpenAPI contract change or local e2e.

## Cache, width and machine/S3 evidence

Each arm starts on a different boot, with empty owned RootFS branch state,
zero initial NBD counters and zero ctld TCP443 counters. Eight generic ready
carriers have no guest RootFS before claim. Each completed arm issues eight
barrier-synchronized cold claims, four/image, holds all eight for exact
post-command resource/mount observation, cleans up, refills, then repeats with
eight NEW sandbox identities on the cached node. S3-side caching is not under
this test's control; the cold label describes the disposable compute node.

Reuse the previous diagnostic16CPU64GiB shape, not a hardware improvement:
physical advertised capacity14,000millicores/56GiB, cpuset0-15; eight exact
leases each1750millicores/1792MiB. All32 complete-comparison claims have matching
lease, allocation, artifact and noatime guest-root proofs. This is eight-wide on
that test shape, NOT occupied-memory production acceptance. Node is16GiB
logical/333,692,928B allocated; Coding is1TiB logical/5,471,105,024B allocated,
not a populated/history-bearing1TiB image. These are different image contents,
not a RootFS-size-only control.

| Complete arm,16 claims each | Off,03 | On,02 |
| --- | ---: | ---: |
| Completed NBD read bytes, including cleanup | 1,534,084,608 | 2,193,003,520 |
| Completed NBD read I/Os | 14,391 | 19,346 |
| Matched incoming ctld TCP443 wire bytes | 75,505,889 | 185,390,756 |
| Outgoing ctld TCP443 wire bytes | 1,797,775 | 3,745,085 |
| CPU busy, cold claim-through-command window | 13.22% | 15.17% |
| CPU busy, cached-new window | 20.15% | 21.26% |
| Minimum host available bytes, whole arm | 63,099,105,280 | 63,087,226,880 |
| Maximum combined ctld RSS bytes, whole arm | 495,583,232 | 503,463,936 |

NBD completed reads rise42.95% across equal16-claim arms. They are not S3 payload,
required application bytes or a phase-only measure. TCP443 counters include IP
overhead/retransmissions, not GET counts; INPUT association can undercount.
Their increase is observational wire evidence, not an authenticated S3-byte
amplification ratio. No new OSS server-latency correlation is claimed here.

CPU averages bracket actual claim-through-last-command windows with95-138ms
total sampling slack, not the diluted whole-arm average. Observed CPU steal,
summed lease throttled_usec and OOM/OOM-kill events are all zero in both phases
and treatments. These samples do not show CPU/RAM saturation; they do not rule
out storage waiting, short stalls, per-core effects or contention under genuine
occupied high density. Preserve the prior independently correlated S3 evidence.

## Retained pilots and observer corrections

There are49 new claims total,48 commands:32 claims in the two complete arms,
16 claims in two incomplete pilots, and one untimed mount-only diagnostic claim.
No failed cohort is silently retried or relabeled complete.

| Arm / boot | Outcome | Node cold combined max | Coding cold combined max |
| --- | --- | ---: | ---: |
| 00-off / `700a937a-af89-4b27-8eba-461a710bc6e5` | 8 successful commands; observer failure; no cached batch | 1.694120790 | 2.214078406 |
| 01-on / `61f704e1-366b-4c7f-946e-cec090f1387f` | 8 successful commands; observer failure; no cached batch | 1.761419241 | 2.822413773 |
| 02-on / `1f33c74d-1931-4075-9f05-be55082c367b` | Complete16, unit success, physical cleanup | 1.637823509 | 2.872879377 |
| 03-off / `183b6a2f-5145-419e-934a-621d7cbc37d2` | Complete16, unit success, physical cleanup | 2.055244450 | 2.514395710 |

First observer error: `/proc/self/mountinfo` refers to the client's private
hosts-file mount namespace, which does not see later ctld mounts. Read host
`/proc/1/mountinfo` and bind exact ctld PID/start ticks/mount namespace instead.
The five synthetic tests/seven assertions for this change pass but miss the
next wrong-path assumption; they are not adequate actual-mount qualification.

Second error: `branch_root` is WAL state, not the configured RootFS mount root.
One explicitly recorded Node claim, with zero guest CMDs, confirms actual
`nomad_runtime.mount_root=/run/sandbox0/rootfs`. XFS root/lower, merged OverlayFS
and the exact Nomad allocation's guest bind all carry noatime. The probe is
fully cleaned. Final observer validates eight exact XFS roots, eight merged
overlays and eight allocation guest binds against active leases/artifacts,
saving raw mount evidence BEFORE assertions. Six corrected fixture tests/
twelve assertions pass. Only post-command observation changes; runtime binaries,
density binary, timeouts and treatment remain identical. Old observer versions
are retained, not overwritten without evidence.

Pilot NBD reads766,962,176B off and1,096,505,856B on cover eight claims each;
do not compare these raw totals to a complete16-claim arm. The01-on wire snapshot
also includes the separate mount probe, explicitly flagged; its NBD sampler
ended before that probe. Initial install-time Nomad ECONNREFUSED and a failed
SSH connection during actual reboot/Stopping are retained. Neither triggers a
second install or reboot request.

## Disposition and remaining gate

This fixed broad-ELF read plan is rejected for default activation. Do not repeat
the same on/off, import, identity, layout or old-pool loop to seek a better sample.
Any materially different proposal needs an explicit bound on extra demand and
cross-sandbox contention, a generic command-time trigger and an end-to-end cost
case. Claim readiness must be addressed separately. The1s target/accepted2s
fallback, stateless nodes, claim-time tenant RootFS, no prewarm, populated/history
roots and occupied-width acceptance all remain unchanged and unmet.

Final verification restores original manager/ctld/driver/runsc/catalog hashes,
original configs and the two-carrier job definition. Its legitimate stop/resubmit
changes job index72102 to73058; do not claim an unchanged index. Original ready2
is observed, not a new acceptance test. No owned guest, RootFS mount or cgroup
remains; all64 NBDs are detached. Original263 and source2072 sandbox rows stay
unchanged. The reused owned DB grows2072 to2121, exactly49 trial/probe identities;
imported descriptors/attestations remain unchanged. Retain all DB/S3 artifacts,
private configs, original observer versions and `/data` diagnostics.

Close the owned SSH master and consume its terminal handle. Remote-skill stop
completes, then restore the instance from the diagnostic16CPU64GiB shape to
original2CPU8GiB while stopped. Independent sanitized Aliyun CLI at
2026-09-12T23:46:08.869134688Z confirms `Stopped` / `StopCharging`,
`ecs.g9i.large`, CPU2, memory8192MiB and no public IP. No production rollout,
merge, tag or default promotion. Preserve the prior61-file import seal and
verify1359 candidate files, with exactly the three named diagnostic harness
files changed; record the new source manifest and experiment-note hashes.
