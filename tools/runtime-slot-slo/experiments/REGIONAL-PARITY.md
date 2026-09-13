# D-REGIONAL-PARITY-PREP: prepare a current-pin full-path comparison

2026-09-12. Evidence: `/tmp/sandbox0-regional-parity.HpFSNd`.
Preparation only: zero new claims, commands or startup measurements.

## What changed

The frozen optimized candidate's worktree HEAD equals current `sandbox0/main`
`0f09220460581bfc1fdc331f34ebc85bf38381e7`. Its 193 dirty entries are retained;
the optimized candidate is not called clean main or merged. All 1353 product
files still match the frozen inventory. No gateway source differences from
HEAD are present. The previous scratch candidate is not adopted.

Current `sandbox0-infra/main` (`af04ea978f62b6eceaa78da98840227505ca764b`)
pins stock runsc release-20260817.0, while the previous full regional trace and
live Singapore fixture use release-20260810.0. This is a missing comparison
prerequisite, **not evidence that the runsc version causes the latency**.

The exact official object generation was downloaded and verified with both
infra-pinned SHA256 and SHA512. The 105,378,363-byte amd64 binary was staged
outside the active runtime and executed only with `--version`, reporting
release-20260817.0 / spec1.2.1. The host's original runsc stayed unchanged.
The current frozen public regional `runtime-slot-slo` harness was also built
without diagnostic overlays, and its hash retained for the next actual run.

## Compatibility and live configuration

Read-only inspection confirms:

- manager selects `/etc/sandbox0/manager.yaml`, ctld selects
  `/etc/sandbox0/ctld.yaml`;
- manager's catalog is
  `/etc/sandbox0/node-authority/claim/runtime-classes.json`;
- its sole standard class is resource-neutral, using systrap, overlay2=none,
  shared file access, DirectFS, static `/procd`, port49983;
- the selected runsc version string is exactly
  `runsc version release-20260810.0`;
- ctld owns nbd0..15, while all64 devices are detached in this idle fixture.

An isolated candidate catalog changes only that version string to20260817.
The real manager catalog loader and shared `RuntimeCompatibility.Digest` compute:

| Version | Compatibility digest |
| --- | --- |
| 20260810 | `sha256:b9f1f0fa2b56f9d9e1805349d9a06f18c2a7d320f5192f1c77638aed3407929f` |
| 20260817 | `sha256:d7fdba49664702dbee206ce7fbcb32dd217c50d5fe93d920d92a0978454a1dd7` |

No CPU, memory, PID, tenant or RootFS selector enters the compatibility class.
Replacing only runsc while retaining the old manager class is not a valid
matched test. The candidate catalog is local and has not been installed.
The existing pure manager/ctld/driver binaries staged by D-MAPPING-OCI still
match their exact manifest hashes; reuse them and the existing Ready artifacts
instead of repeating imports.

## Verification and retained failures

Complete race suites pass for `tools/runtime-slot-slo` (2.171s),
`pkg/runtimeslot` (1.131s) and `manager/pkg/nomadclaim` (1.198s).
Catalog comparison checks actual loader/digest behavior and rejects any
non-version difference. Original file/process/job/PG/artifact/absence state is
unchanged in the final remote observation.

Two read-only observer mistakes are preserved, not hidden as runtime failures:
the first assumed service configs still selected a historical formal directory;
reading only live CONFIG_PATH selectors found the exact current paths. The
second compared entire observations, including their different `at` timestamps.
A diagnostic export proves only `at` differed, and the final guard excludes
only that sampling timestamp while retaining every authority comparison.
Neither failure restarted a service, changed configuration or launched a guest.

No production mutation, merge/tag, source change, local e2e, Kind bootstrap,
tenant-data read/prewarm, OCI import, database write or object publication.
The test ECS was used only for read-only configuration checks and independent
binary version execution; original compute shape remains2CPU/8GiB. All compute
stop and fresh cloud-state receipts are retained with the evidence.
Stop completes at13:55:11UTC; a fresh13:57:12UTC check confirms
Stopped/StopCharging, original2CPU/8GiB and no public IP. The staged binary and
all prior data are retained for the next full-path run.

## Next actual measurement and remaining gates

Use the existing full regional runner, not the standalone storage-to-command
harness. Adapt its original-state guards and restore plan to the inspected live
configuration; pair the new stock runsc with the matching catalog. Use the same
truthful16CPU/64GiB, eight-carrier width as the prior full-path comparison, then
restore the original shape. This is version comparison, not a hardware remedy.
Detailed exact inputs and restoration requirements are in the evidence's
`NEXT.md`; no new measurement is implied by preparing them.

Measure empty-node then cached-node new identities with real immediate node-v,
unchanged10s requests and every1s/2s miss retained. The old sparse1TiB Coding
artifact (~5GiB allocated) does not prove populated-large size independence.
Eight leases without actual memory/CPU occupancy do not prove density. Public
production ingress, current-pin guest execution, populated roots/upper history,
full authority/semantics, occupied physical width and the complete latency gate
remain open. Generic warm carriers and claim-time RootFS selection remain fixed.
