# D-JOINT-DELIVERY: joint mapping/data delivery needs early bounded selection

2026-09-12. Offline complete-geometry and design screen. No new source objects,
wire codec, Reader, remote test, sandbox claim, command or latency measurement.

## Decision

Do not build a runtime variant that simply wraps the old mapping pages with all
their metadata, or automatically includes every related mapping page. Both pay
substantial unnecessary decoded-page costs under the unchanged packet budget.

Retain one narrower candidate for a format/selector feasibility test: a complete
unchanged trigger leaf plus complete related original data frames. Foreign mapping
pages are NOT required to prefill independently verified content-cache entries.
They are still required, through the current authoritative path, to authorize
future reads at those logical locations. This distinction must never disappear.

This candidate passes only the packet-size screen. Its selector, serialization,
I/O, CPU, cache pressure, COW and full startup benefit remain unqualified. Do not
start another remote trial or advertise 2s from these counts.

## Why selection is part of the mechanism

Current `Reader.resolve` obtains the authenticated parent entry, reads the child,
checks version/level/logical bounds, and recursively resolves the requested block.
Only then does it know the original data range. A plan stored inside that target
leaf cannot remove the leaf-then-payload dependency on a cold leaf.

The proposed dependency change is:

```text
Current:  authenticated parent -> original leaf -> data frame
Proposed: authenticated parent + bounded selector -> [original leaf + full frames]
```

The second line is not implemented. It requires selecting the packet before the
leaf, while retaining an exact ordinary fallback. A separate selector fetch adds
another dependent round trip; putting every selector in a flat root frontloads
work proportional to image contents. Neither cost may be treated as zero.

Current pages reject trailing bytes, unsupported versions and reserved bits.
Current parent validation also requires child version/level/range agreement.
Therefore this is a versioned format/protocol design, not a safe implicit appendix
to an existing format-2 page. No current compatibility or public contract changed.

## Frozen complete-image screen

Use the exact Node/Coding profiles and full XFS graph from D-FRAME-GROUPS. The
original 4,341/80,781 full mapping entries and 64KiB frames remain unchanged.
The inventory names inode-containing blocks and directory data-fork blocks; it
does not claim to enumerate every possible filesystem metadata class.

Each packet is limited to 1MiB decoded and 1MiB stored, INCLUDING modeled records:
64-byte envelope and 80 bytes per complete page/frame. This is the predeclared
combined packet limit, not a claim about an existing joint-packet runtime limit.
The existing mapping-only reader guard is separately 1MiB stored/4MiB decoded/
32 pages; current mapping publication uses up to 4 pages/240KiB stored/1MiB decoded.
These policies are not enlarged or conflated to qualify a packet.

| Variant | Node admitted/candidates | Coding admitted/candidates |
| --- | ---: | ---: |
| One original leaf + all inventoried frames in that leaf | 0/3 | 15/60 |
| Directory frame union + all referenced original leaves | 33/61 | 743/1,794 |
| Same frame union + ONLY the trigger's original leaf | 59/61 | 1,751/1,794 |

The graph variants start with all 61/1,794 groups that passed D-FRAME-GROUPS,
not a trace-selected subset. The earlier 3/38 full-frame budget rejections remain
excluded and visible; total whole-frame triggers were 64/1,832. Every packet keeps
its complete data-frame union; oversize packets fall back rather than dropping
members. The minimal variant was added after correctness review of the initial
geometry, with unchanged budgets and group membership. Initial results and the
exact initial model hash are retained and regression-checked.

Largest leaf-local decoded charge is 2,853,248 bytes Node and 9,179,168 Coding.
The previous ~196KiB decoded leaf granularity contains many unrelated frames, so
wrapping a full leaf indiscriminately is not a small operation.

## What the minimal variant still costs

| Complete-image geometry | Node | Coding |
| --- | ---: | ---: |
| Largest admitted packet decoded charge | 983,152 | 1,022,144 |
| Largest admitted packet stored charge | 147,777 | 235,258 |
| Encoded original page+frame content summed per admitted trigger | 5,110,803 | 159,816,009 |
| Decoded page+frame content summed per admitted trigger | 37,657,440 | 1,065,928,416 |
| Packet member/envelope modeled bytes summed per trigger | 40,576 | 1,137,184 |
| Parent selector modeled unencoded bytes | 16,048 | 476,272 |
| Original root page decoded bytes | 982 | 15,200 |

Selector convention: 256-byte bounded locator/identity record plus 16-byte logical
trigger interval. It assumes a packet object key of at most 160 bytes; longer
locators need explicit ordinary fallback or a separately audited representation,
not truncation. These record counts are NOT serialized/compressed/authenticated
wire sizes. Original encoded content lengths are exact profile lengths; the new
capsule/encryption/object overhead and actual transfer sizes remain unmeasured.

The sums include per-trigger duplication, including repeated unchanged leaves;
they are immutable publication costs, not node-resident cache or request working
set measurements. An import-time durable layout does not authorize prewarming
tenant RootFS on workers. Larger caches are not part of this candidate.

Adding 476,272 bytes of raw selector records to a current 15,200-byte Coding root
is a material cold-path cost, even though it is below the 8MiB correctness ceiling.
All Coding plans appear in 49 trigger leaves (1..133 admitted plans per leaf).
A hierarchical selector must bound bytes as well as child count, including worst
case concentration inside one leaf. Existing fanout bounds alone do not bound a
new auxiliary record list. Real encoded size and traversal depth are the next
test, not another payload-window-only replay.

## Correctness and ownership conditions before implementation

- Hash DAG: publish original immutable frames/leaves first, then a capsule with
  packet-relative ranges and their identities, then a parent referencing both the
  ordinary leaf and the capsule. Never annotate a leaf with a locator to a capsule
  containing that newly annotated leaf; never put a future root digest into a
  capsule whose locator participates in computing that same root digest.
- RootFS truth: capsule membership only supplies verified checksum/length bytes.
  The demanded original leaf must match the current authenticated parent, then
  its original entry chooses the frame. Foreign cached frames do not authorize
  their own logical offsets. Branch/composite tail overrides still take priority.
- COW: unchanged leaf identity may retain its matching hint. A changed trigger
  leaf must drop or rebuild its hint during the same path-copy publication.
  Stale foreign frames cannot satisfy changed authoritative hashes; their wasted
  speculation and storage must still be accounted for. No wholesale rewrite of
  unrelated pages or second mutable RootFS index is accepted.
- Publication/GC: current `BuildResult.References` inventories every published
  reachable immutable object, with exact whole-object checksum/size; existing
  object kinds and reachability walkers do not know capsules. A versioned design
  must integrate retry inventory, reference ownership, deletion and recovery in
  the existing PG/S3 lifecycle, not leave untracked hint objects or another truth.
- Runtime: shared original verified caches, source admission and total scratch/
  residency budgets must cover page+payload buffers, including decode and parsed
  page charges. Never recursively expand hints or trigger them on complete cache
  hits; retain ordinary 128KiB bulk coalescing. The packet decoded limit alone
  does not prove a node memory bound. Transport/corruption/cancellation fallback
  and overlapping page/frame flights need explicit tests, not recursive deadlock.

These are design obligations, not completed implementation or security tests.

## Verification and next stopping gate

Six model tests/5,602 assertions pass: both byte ceilings include records; full
content deduplication; empty/invalid inputs; unchanged initial results; exact
complete group membership and original entries for both images; fixed screen
counts and selector/publication arithmetic. They do not test cryptographic byte
validation, an actual Reader, I/O or startup. Reverified 1,353 product files and
inventory, 164 trace artifacts, 13 frame-group artifacts, 77 full-graph artifacts
and 393 prior input artifacts. No product source changed.

Next gate: one bounded selector/packet format sketch and exact encoded-size/read
dependency model for the minimal candidate, with explicit ordinary fallback and
COW/reference rules. If it merely moves the old dependent read into a new selector
read, or requires full-image/root preloading, reject it before remote work. Only
a candidate with credible combined-path savings and bounded real costs advances
to byte-correct codec/Reader testing, then paired remote acceptance.

The complete 1s/2s target, cold and cached-new claims, immediate `node -v`, populated
large RootFS and occupied actual node width all remain open and unchanged. No
remote status query/start, production mutation, merge or tag occurred this turn.

Evidence root: `/tmp/sandbox0-joint-delivery.ecG6HA`. `geometry.json` is the initial
two-variant screen; `geometry-minimal.json` includes the correctness refinement.
