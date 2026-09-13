# D-CLAIM-SUBSTAGES: recover the existing slow mount budget

2026-09-13. Evidence: `/tmp/sandbox0-claim-substages.0cnLzb`. Historical log
recovery for [PREFETCH-TRIAL.md](PREFETCH-TRIAL.md), not another workload run.
Zero new claims, commands, imports, tenant RootFS reads or runtime changes.

## Result and decision

Recover all32 manager and32 driver claim records for the two completed on/off
arms. Four additional slow-Ensure records match the four cold Coding claims in
03-off. This fills the missing setup-stage evidence with exact prior identities.
It does not change any latency sample, establish a new optimization, or prove
S3 causality. The rejected broad main-ELF prefetch stays rejected.

For those four slow cold Coding Ensure calls:

| Stage | Observed range, ms |
| --- | ---: |
| ctld Ensure total | 659.464–683.522 |
| Immutable Reader open | 57.923–76.611 |
| NBD attach | 13.478–21.344 |
| XFS mount | 452.498–460.359 |
| Overlay setup/mount | 116.714–118.291 |
| XFS plus Overlay, summed within each sample | 570.682–578.591 |
| Explicit session-state persist stages, per-sample sum | 5.692–6.621 |
| Session reservation, outside session Ensure | 0.729–1.766 |
| Writer authority RPC | 1.050–6.655 |
| Session lock wait | 0–0.001 |

Rows are nested or disjoint as indicated, not an additive total of maxima.
Explicit session persistence includes device-reserved, device-ready,
XFS-mounted and ready journal updates; the reserved-state update inside Ensure
is zero here because reservation already exists. It does not label every other
path's fsync or control-plane cost as zero.

This does not support journal/fsync, session-lock, NBD attachment or writer-RPC
tuning as the main fix for these observed slow claims. Mount-time work dominates
this part of the path. `MountXFS` calls mkdir/chmod/trusted-directory validation
and the XFS mount syscall; `MountOverlay` validates lower/upper/work, establishes
the read-only lower bind and mounts OverlayFS. These durations include possible
backing-block waits and kernel work. They do NOT distinguish physical local disk,
S3 waits, kernel CPU or required versus speculative reads.

Current code already streams authenticated encryption frames, but the block
Reader waits for the complete bounded source range and terminal transport status
before its decoded range/group result. Earlier delivery was considered during
this source audit, not implemented or benchmarked. A proposal would need an
end-to-end opportunity budget and preserved terminal-error/verification/lifetime
semantics; this audit is not permission to silently return before those checks.

## Same-sample complete-path budget

The following is ONE exact slowest-combined03-off cold Coding sample:
`rs-mfrwgzlqorqw4y3ffvrwy5ltorsxeljr-63-7s3ufdyn2oua3dpb`, allocation
`db6d9f5d-39b7-30e6-a1db-13d8d504d8de`, artifact
`sha256:058e029538b7c20c3bd14dba439930181b5f6be3012dc691244561f089d6e089`.
Do not assemble its budget from another sample's maxima.

| Scope | ms |
| --- | ---: |
| Client full claim | 1633.414920 |
| Authenticated readiness clock | 1616.657000 |
| Node claim RPC within readiness | 1116.873 |
| Driver claim within node RPC | 1115.406 |
| Driver RootFS Ensure within driver claim | 662.697 |
| XFS mount within Ensure | 453.587 |
| Overlay setup/mount within Ensure | 118.291 |
| runsc create within driver claim | 258.984 |
| runsc start within driver claim | 180.162 |
| procd readiness probe, after node claim | 419.253 |
| Actual first node-v command | 880.966133 |
| Original client claim-to-command completion | 2514.395710 |

The difference between client claim plus command and combined retains the
client's actual inter-call overhead. Manager phase totals and the separately
rounded readiness clock differ by13–55microseconds across all32 samples;
they are not a new latency discrepancy. Stage start/end timestamps were not
recorded, so no precise absolute sub-stage or guest-fault dependency DAG is
invented from the journal emission time.

For EACH of the four03-off cold Coding samples, optimistically zero only XFS
mount duration while keeping every other observed cost fixed: combined remains
2053.968–2061.861ms. Zeroing only explicit session persistence instead leaves
2507.723–2508.635ms. These are scope-sufficiency screens, not achievable savings,
architectural lower bounds, or permission to skip mount/durability work. Even
the largest single observed mount stage does not by itself close the2s gap.

## All completed cohorts and censoring

Four samples/cell. These are min–max milliseconds from the SAME prior regional
trial. The command opt-in starts after claim, so cross-boot claim differences
are not direct prefetch speedups; concurrent commands may overlap slower claims.

| Arm / cache / image | Driver Ensure | runsc create | runsc start | procd probe |
| --- | ---: | ---: | ---: | ---: |
| 03-off / cold / Node | 377.464–406.695 | 55.822–61.879 | 84.426–92.500 | 266.332–285.307 |
| 03-off / cold / Coding | 660.164–684.178 | 256.400–259.926 | 180.162–182.194 | 419.253–427.297 |
| 02-on / cold / Node | 269.184–290.459 | 47.367–51.624 | 70.435–76.909 | 169.813–194.685 |
| 02-on / cold / Coding | 447.423–489.102 | 224.576–228.118 | 226.013–228.044 | 371.280–390.476 |
| 03-off / cached-new / Node | 109.211–129.592 | 46.722–49.932 | 60.128–60.593 | 225.776–234.070 |
| 03-off / cached-new / Coding | 179.200–196.205 | 122.167–124.965 | 85.218–87.283 | 128.460–135.983 |
| 02-on / cached-new / Node | 126.271–140.164 | 35.983–45.405 | 85.239–95.123 | 166.986–180.507 |
| 02-on / cached-new / Coding | 251.706–264.068 | 115.028–119.973 | 90.871–94.167 | 194.734–211.840 |

The other28 samples have no detailed Ensure record. Current code logs successful
Ensure only at500ms or above, and every one of these28 enclosing driver Ensure
calls is below500ms. This is expected threshold censoring, NOT zero mount time.
Do not infer the absent per-stage on/off comparison from their totals.

## Collection and verification

- Verify the prior135-file seal and all1359 current candidate source files.
  Fresh sandbox0 origin/main remains0f092204; older local main HEAD is not used
  as architecture authority. Product/runtime code stays unchanged this turn.
- Select only the32 complete samples from immutable exports, joining their
  exact active-lease allocation/artifact bindings and prior boot/PID records.
- Read only the two historical boots' manager, ctld and Nomad journals. Scan
  5940/9705 records (8,623,046/13,392,249 bytes); export68 matching timing records.
  No unmatched target timing or invalid/sensitive-message suppression. Retain
  journal cursor, boot, PID, unit and real/monotonic emission timestamps.
- Join all32 manager/driver pairs; all nine reported driver substage values
  match exactly, task/slot/allocation agree, and readiness clocks agree within
  their microsecond rounding. ctld records bind the exact prior ctld PID/boot.
  Independent raw-manager joins check all32 results; negative real-data tests
  reject wrong PID/boot/slot, missing/duplicate record, changed duration,
  failed phase and changed readiness clock.
- Preserve the initial local parser failure: Nomad renders durations above one
  million as quoted scientific notation, e.g. `"1.135108e+06"`, not plain digits.
  Use exact rational parsing, rejecting fractional/negative/non-finite/overflow
  values rather than rounding. Original seven tests/15assertions missed this;
  corrected eight tests/20assertions pass, including unmatched-quote rejection.
  The final parser reproduces all saved analysis rows. No query/sample is rerun.

## Disposition and next action

The next storage candidate must address mandatory filesystem-metadata reads
across mount and guest launch/first command, not just bootstrap root-page size,
a faster journal or large speculative executable reads. Reuse the existing
metadata/read/HTTP evidence first; do not reopen rejected inline, compact-wrapper,
cache/admission or ELF variants unchanged. Mount substage totals do not supply
the still-missing exact mandatory-versus-speculative read edges. Any targeted
observation must fill that missing boundary, not repeat broad forensic sweeps.

Original warm carriers, claim-time RootFS binding, authenticated regional
readiness, literal first command,10s timeout and no tenant prewarm remain fixed.
Prior16CPU64GiB eight-wide samples are still unoccupied, and logical1TiB Coding
is still sparse rather than a populated/history-bearing1TiB root. Full1s/2s,
occupied production-width and populated-root acceptance remain unproven.

The remote skill is used only for original2CPU8GiB lifecycle and read-only log
retrieval, not old Kind/bootstrap/deploy steps. Original binary/config hashes,
service processes during retrieval, job73058 and263/2072/2121 sandbox counts are
unchanged; all64 NBDs remain detached, no active guest/RootFS resources. No files
are written remotely. Preserve all `/data` and S3/DB history. Owned SSH terminates;
independent CLI at2026-09-12T23:59:18.632492188Z confirms original2CPU8GiB,
Stopped/StopCharging and no public IP. No production change, merge or tag.
