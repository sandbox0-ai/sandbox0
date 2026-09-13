# D-NODE-BINDING: current executable identity and retired-trace exclusion

2026-09-13. Evidence: `/tmp/sandbox0-node-binding.KImNPR`; retained remote root:
`/data/sandbox0-node-binding-KImNPR`. This is a read-only identity investigation,
not a new startup/command sample or a runtime optimization. The full objective
and all gates in [STRUCTURED-OPTIMIZATION.md](STRUCTURED-OPTIMIZATION.md) remain.

## Decision-changing findings

The actual two current regional artifacts contain byte-identical Node binaries:
124,836,408bytes, SHA256
`3517c2df0b2f8cd7f422b4b8450ef81c6889f08eb03e281d6de9079b15e6a327`.
Their PT_LOAD metadata, interpreter and DT_NEEDED names also match the earlier
D-NODE-IDENTITY scan. The paths differ: Node image`/usr/local/bin/node`, Coding
image`/usr/bin/node`. Thus a larger/different Node executable does not explain
the timing difference between these exact current image paths.

All seven common standard-directory dependency candidates have different file
hashes between images, including the loader, libc and libstdc++. They are NOT a
controlled RootFS-size-only comparison. This does not establish that those
library differences cause the latency gap; no guest loader trace or command was
run. Do not replace a tenant's libraries to obtain an easier passing workload.

More importantly, the historical65,314,816-byte read union considered in
[EXEC-ADVICE.md](EXEC-ADVICE.md) is not merely a different artifact: its live
metadata identifies a RETIRED diagnostic storage format. Do not use that trace
to predict current read amplification, current extra bytes or saved latency.

| Exact target | Metadata database | Artifact prefix | Artifact format / mapping version | This turn |
| --- | --- | --- | --- | --- |
| Historical Coding read trace | `rootfs_span_tzfkaz2` | `37de2f09...` | 10005 / 5, descriptor version1 | Metadata and133-key inventory only; no object read or old decoder |
| Current regional Node | `s0_mapping_oci_uqo1av` | `2b5208c9...` | 2 / 2 | Full Node and bounded dependency identity scan |
| Current regional Coding | `s0_mapping_oci_uqo1av` | `21a2f3ba...` | 2 / 2 | Full Node and bounded dependency identity scan |

The initial plan included an exact historical Node hash bridge. That bridge is
NOT completed. Discovery of the retired descriptor changed the next action:
exclude it from current-cost admission rather than reintroduce an obsolete
decoder into the product or relabel it format2. Current files are now exactly
bound to current artifacts, but that does not turn the historical offsets into
a current working set. The prior approximately42MiB conditional difference
remains an offline byte-set calculation, not a measured additional current I/O.

## Current-source and read-only boundaries

Fresh main fetches remain sandbox0`0f09220460581bfc1fdc331f34ebc85bf38381e7`
and infra`af04ea978f62b6eceaa78da98840227505ca764b`. Main README/deployment
contracts remain Nomad-only/stock-gVisor. Reverify1359candidate source files and
the preceding23sealed artifacts before work; preserve the dirty worktree.

Build the private linux-amd64 inspector locally against the unchanged current
worktree. Its SHA256 is
`456e0a7a70e323f4ddaab27e92d65e4f840a1b7e90f88d420af109c0b9ab474c`.
The existing scanner is reused unchanged. No product runtime/service binary,
job, compatibility class, timeout, RootFS format, key or cache setting is changed.

The inspector requires the exact remote host/opt-in and an unowned NBD31. Each
supported image has a private mount namespace; XFS is mounted read-only,
norecovery, nosuid, nodev and noexec. A trusted child chroots before resolving
image paths, preventing absolute image symlinks from resolving to host files.
It hashes regular files and parses ELF metadata; it never executes image code.

The plaintext Reader verifies normal format2 checksums. The underlying store
allows only object keys in the exact artifact's PostgreSQL inventory (8Node,
97Coding), rejects writes, conditional writes, delete, HEAD and LIST, and caps
each ciphertext range at16MiB and each image's reserved reads at512MiB. Each
image has a180s diagnostic lifetime; NBD's10s request budget is unchanged.
These limits bound an inspection, not an arbitrary process-start guarantee.

## Actual collection, not startup demand

Both supported scans complete, with zero rejected mutation attempts and exact
artifact/descriptor readback. Neither scan uses ctld's shared cache.

| Current image | Underlying ciphertext range calls | Reserved bytes | Returned ciphertext bytes |
| --- | ---: | ---: | ---: |
| Node | 296 | 58,457,611 | 58,416,457 |
| Coding | 326 | 62,048,726 | 61,950,047 |

Total returned ciphertext is120,366,504bytes over622underlying range calls.
These counters are not independently observed HTTP attempts and do not count
possible provider-internal retries. They include full-file hashing and static
dependency lookup, not just bytes required by `node -v`; no startup saving can
be inferred from them. No claim, guest command, latency sample or populated-root/
occupied-width test was performed. Future cold-node trials require a fresh
target-node cache boundary; the inspector's data reads are not a cold SLO result.

## Rejected side hypothesis and local tests

Ordinary Reader shared flights retain their initiator's transport lifetime,
but actual mounted sessions pass the shared NODE lifetime at create, reopen
and rebase. A short claim cancellation therefore does not establish that one
sandbox cancels another's shared transport. Existing actual-session/fake-runtime
tests pass three race repetitions: a read outlives claim cancellation, while
manager/node close cancels active source I/O. No new Reader cancellation API or
runtime source change is justified by this hypothesis.

The scanner's three tests plus four exact-object/budget/oversize/mutation guard
tests pass three race repetitions; vet and the linux-amd64 build pass. These
tests do not constitute gVisor/NBD e2e or a concurrency speedup. No local e2e ran.

## Preserved preflight failures and fixture health

The first two preflights find0ready original carriers after boot. Original
service processes, binaries/configs, two-group job72102, database row counts
and all64detached NBD devices verify. The zero-ready condition remains at final
observation; it is NOT silently repaired, labeled healthy, or used for a runtime
claim result. Its cause is not diagnosed by these file scans, and it is not
evidence that earlier qualified eight-carrier startup runs used an empty pool.

An isolated read-only mount needs physical-device absence and preservation of
the original fixture, not a ready carrier. A separate inspection-only preflight
records that limited admission explicitly; it does not relax any claim gate.
Before any future timed claim, healthy current-pin carrier readiness remains
mandatory. This turn has no startup admission.

The next preflight incorrectly assumes all targets are in the old formal
database and fails before object I/O. An attempted source-database lookup is
then rejected by an old helper's database allowlist, also before object I/O.
Retain both failures. An explicit read-only adapter for the two exact historical/
current databases locates all targets and exposes format10005. No database or
artifact is imported, migrated, edited or deleted to make the preflight pass.

## Cleanup and remaining work

The single diagnostic unit is inactive/success, exit0 and MainPID0. Both mounts
are removed; all64NBD devices are independently verified detached. Original
service PIDs, files/configs, job index72102, formal2072rows and original263rows
are unchanged. The remote27-file export is downloaded and independently verified
at member and archive hashes. Test files and `/data` are retained.

The owned SSH master is explicitly closed; its terminal255follows that close,
not a failed file scan. Compute stop uses the workspace lifecycle entrypoint;
the Stopping observation is retained. The independent cloud read at
2026-09-12T20:34:54.336322500Z verifies Stopped/StopCharging, ecs.g9i.large,
original2CPU/8GiB and no public IP. No production action, merge, tag or material
deletion. Remote data and all failure receipts remain available.

Use current-format, exact-source evidence for any real-read cost case. Keep
unsupported guest hints closed, preserve the actual cold combined2.221/2.668s
and initial Coding2.375s misses, and do not exchange cold-node/first-command,
populated-history or real-density requirements for identity checks. The active
goal is not complete; no runtime improvement was demonstrated in this turn.
