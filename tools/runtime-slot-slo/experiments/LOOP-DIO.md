# D-LOOP-DIO: remove the intermediary's second read expansion

2026-09-12. Evidence: `/tmp/sandbox0-loop-dio.as6ioN`.
Four new native guests, no regional claim or encrypted/S3 workload.

## Result and decision

The same retained Node wrapper runs buffered/direct/direct/buffered, each with
a new COW journal, mount, network namespace and authenticated procd instance.
Both direct treatments remove the previously identified 57MiB of extra outer
image-file reads. Both buffered controls reproduce it, including the reverse
control. The useful inner per-file readahead remains enabled.

| Trial | Actual loop DIO | NBD unique bytes | Outer-file extra bytes | XFS buffered / direct entries |
| --- | ---: | ---: | ---: | ---: |
| B1 | 0 | 150,721,024 | 59,768,832 | 22,447 / 0 |
| D1 | 1 | 90,952,192 | 0 | 0 / 174 |
| D2 | 1 | 90,952,192 | 0 | 0 / 178 |
| B2 | 0 | 150,721,024 | 59,768,832 | 22,447 / 0 |

NBD unique reads fall 39.66% against this wrapper's buffered controls. The outer
image file has 149,987,328 unique bytes read in buffered mode and 90,218,496 in
direct mode. Its known inner request union is exactly 91,402,240 bytes in every
trial; 1,183,744 bytes are outer sparse holes, not missing downloaded data.
The two buffered trials have 226 outer-inode iomap readahead callbacks each;
the direct trials have none. Inner-inode callback counts/page histograms remain
equal, as do the backed executable file-offset sets:

- procd: 18,874,368 bytes, equal to the sealed ordinary control.
- Node: 65,314,816 bytes, equal to the sealed ordinary control.

All known backed executable bytes are present in the NBD projection. Shared
4KiB base reads have identical hashes at 22,234 common offsets across all four
trials. Immutable full logical wrapper identity and original source metadata
match before/after. This is a causal intermediary-I/O result for an **unchosen
candidate**, not a claim that production main has this nested layout.

The ordinary control from D-WRAPPER-DEMAND reads 93,221,376 NBD bytes. The direct
wrapper is only 2,269,184 bytes / 2.43% lower than that ordinary baseline; this
cross-campaign volume comparison is not an encrypted/S3 speedup. Removing the
wrapper's regression therefore reopens its demand screen, not format adoption.
The next useful gate is net encrypted block-COW/S3 cost against ordinary layout
with empty-node and cached-new identities, preserving actual NBD demand and
reader/branch adapter boundaries. Do not rerun this native ABBA as a new baseline,
rebuild the same images, or assume that a smaller metadata representation wins.

## Fixed treatment and actual geometry

Only the loop intermediary requests direct I/O. The base image source remains
the same readonly generic file-backed Branch/NBD diagnostic. No change to NBD
or loop readahead (both 4096KiB), timeouts, cache size, hardware, guest contract,
stock runsc settings, checksums, encryption authority or production code.

Both modes explicitly use the observed backing-device sector size. Actual NBD
and loop logical sectors are 512 bytes in all trials, matching the previous
diagnostic: no sector-size change was needed. XFS statx reports both memory and
offset DIO alignment of 512 bytes (DIOALIGN present). All observed XFS request
offsets/lengths satisfy that requirement. Loop DIO, backing path, size, offset,
limit and geometry are identical before and after each real command. The runner
does not silently fall back or retry with a new geometry on failure.

Upstream [loop source](https://github.com/torvalds/linux/blob/v6.8/drivers/block/loop.c)
requires backing-device alignment and a filesystem direct-I/O capability; its
buffered and direct paths differ. The observed
[XFS direct-read path](https://github.com/torvalds/linux/blob/v6.8/fs/xfs/xfs_file.c)
uses iomap DIO. The actual kernel mode and trace entries, not the command-line
flag alone, establish that this test took that path. Kernel is 6.8.0-124-generic.

Source staging is readonly/noload, chosen by exact cloud disk serial and ext4
UUID. This boot it is nvme1n1; device names are never reused as disk identity.
All temporary work is outside source staging. No OCI export/import, image
construction, full-tree scan, encrypted object publication or S3 request occurs.
Source verification can populate the source page cache: no empty-node latency
claim follows from this file-backed experiment.

## Observation limitations and preserved failed analysis

All 58,736 recorded events survive: per-CPU counts equal trace headers with zero
overrun, commit-overrun or dropped events. Global trace configuration is unchanged
and all four private instances are removed. NBD full-prefix reads have exact
later completions, no errors, and the same range union as the Go NBD observer:
1,388 / 907 / 908 / 1,393 issues and completions in trial order.

The initial offline analyzer fails its full-prefix loop completion assumption.
Keep its source, error and the separate completion inspection. Registering the
loop BIO, issue and completion filters takes separate writes after losetup.
Udev requests can arrive while only some event filters include the loop. This
produces missing setup completions as well as the previously known possibility
of missing setup issues. A pending setup issue must not consume a later
same-range guest completion and create a fictitious guest I/O failure.

| Trial | Full loop issues / completions | Post-mount issues / completions |
| --- | ---: | ---: |
| B1 | 129 / 129 | 128 / 128 |
| D1 | 174 / 170 | 128 / 128 |
| D2 | 172 / 138 | 132 / 132 |
| B2 | 168 / 140 | 129 / 129 |

The planned complete-prefix loop issue/completion proof is **not achieved**.
No zero trace-loss counter repairs an event that was filtered out. NBD retains
full-prefix validation; loop validation uses the proven post-mount boundary,
after all filters are installed, and every observed issue there has a matching
later completion. All recorded kernel completion errors remain checked. Demand
accounting still includes every observed setup BIO/issue/completion and XFS
read entry, not just the complete post-mount stream. No guest was rerun to
replace this observation gap. Tests cover setup gaps, repeated same-range I/O,
future-issue errors, direct-read sizes, markers and explicit loss counters.

XFS entries contain requested ranges, not individual read-return values. The
real command, NBD returned bytes, base identities and cleanup are independently
checked. No per-BIO causal parent or universal future-uselessness claim is made.

## Local timings, lifecycle and remaining gates

| Trial | Attach to authenticated ready, ms | node-v, ms | Combined, ms |
| --- | ---: | ---: | ---: |
| B1 | 204.410 | 267.805 | 472.265 |
| D1 | 178.837 | 159.890 | 338.787 |
| D2 | 185.883 | 159.821 | 345.774 |
| B2 | 199.530 | 263.574 | 463.164 |

These include observers whose work drops substantially with fewer reads/events.
They exclude regional ingress, manager/PG, encrypted Reader/S3 and production
occupancy. Do not credit their entire timing difference to a production speedup,
or count any row as a new <=2s pass. Combined includes between-stage intervals,
so it is not exactly the displayed phase sum.

Nine diagnostic Go race tests, vet and build pass. Eight offline tests / 22
assertions pass. All four real node-v results are v22.23.2. The single supervised
campaign ends inactive/success/0 with all guests, loops, NBD, mounts and private
traces absent. Final checks inspect 236 process mount tables. Original runtime
files/PIDs, PG rows and Nomad job remain unchanged within this boot. All 36
remote reports transfer with matching hashes; SSH closes and its master exits.
Stop completes and a fresh 12:01:22UTC cloud read confirms Stopped/StopCharging
without a public IP. Preserve all /data assets and prior failed experiments.

All 1,353 frozen product files and 193 dirty worktree entries remain; only the
requested experiment notes change. Main refs are freshly fetched and unchanged
(core 0f092204, infra af04ea978). No production rollout, merge, tag or local e2e.
The test's stock runsc remains older than the production main pin.

Generic carriers, claim-time RootFS and disposable nodes remain non-negotiable.
Net encrypted/S3 gain, full sparse/hardlink/rebase semantics, authenticated
image/worker/writer/cleanup authority, populated large roots and upper histories,
actual occupied production width, and regional ingress through procd plus real
node-v at preferred 1s / accepted 2s still require verification. OSS response
tails and occupied-machine effects remain separate, relevant attribution tracks.

Follow-up: [LOOP-NET.md](LOOP-NET.md) now measures actual encrypted OSS through
new guests. Request count falls, but the reverse ordinary control catches up
in elapsed time. Keep the causal 57MiB result above without treating it as a
proven net S3/regional improvement or format adoption.
