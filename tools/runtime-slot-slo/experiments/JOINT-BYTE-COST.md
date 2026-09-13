# Complete joint-packet bytes and a corrected net-cost screen

Recorded: 2026-09-12. Diagnostic evidence, not startup acceptance or a durable
format proposal. Fundamental requirements in
[STRUCTURED-OPTIMIZATION.md](STRUCTURED-OPTIMIZATION.md) remain unchanged.

## Decision

Complete packets are byte-correct and bounded for these two exact source images.
This does not establish a startup win. Reject unconditional small-read packet use
as the next implementation candidate: it reduces source events while substantially
amplifying bytes/decoding, especially for Node. Keep only the mapping-leaf-miss
strategy for further causal screening. Do not implement a production format or
run another full startup trial on the strength of these helper counters.

The first net model was wrong: it omitted the current ordinary Reader's grouped
mapping delivery. Preserve that rejected model. Its corrected successor matches
the actual unchanged Reader's source counts/bytes and exact mapping range schedule
for all four baseline workloads. Candidate-side costs are still modeled.

## Scope and preservation

- Fetched and read current main authority: sandbox0
  `0f09220460581bfc1fdc331f34ebc85bf38381e7`; infrastructure
  `ceb895c6225b9b01f8535b721d1156fa58fe4cd9`.
- Singapore test ECS only, original 2CPU/8GiB. No resize, deployment, service or
  Nomad mutation, database write, claim, guest command, NBD attachment or source
  object write. No production operation, merge or tag.
- Source descriptors/artifacts, original binaries/configuration/PIDs, Nomad job
  index 68814, database row counts and 64 detached NBD devices verified before
  and after. Preserve /data and all inherited evidence. Remote skill used only
  for environment preservation and Makefile start/stop lifecycle; its legacy
  Kind deployment procedure was not used.
- Source helper exited successfully; unit inactive/dead, MainPID 0, status 0.
  Exported archive is 140,359,680 bytes, 5,776 individually verified files.
  SSH master was explicitly closed and its session terminated. Fresh cloud
  observation at 2026-09-11T22:05:45.722992032Z: Stopped/StopCharging, 2CPU/8GiB.
- Product changes: none. Temporary Go overlay exposes the existing bounded
  decoder to the diagnostic binary without editing product files. Credentials
  and RootFS encryption keys remain remote. Local transport tests use a fresh
  in-memory RSA key and in-memory object store, not cloud credentials.

## Real source bytes, not length-matched filler

Authenticated tree traversal validates every original mapping page and its exact
current parent. All selected source frames are complete unchanged encoded ranges,
checked against the frozen full mapping profile, then decoded and checksum-verified
by the current product implementation. Selection covers every admitted layout
group plus ordinary helper/all retained request-shape source ranges. Historical
requests select export fixtures, never layout membership.

| Source image | Mapping pages | Full data frames exported | Encoded bytes including maps |
| --- | ---: | ---: | ---: |
| Node | 6 | 1,732 | 45,277,052 |
| Coding | 80 | 3,936 | 66,117,943 |

The complete trees contain 4,341/80,781 original data entries. Export is not a
claim and does not establish empty-node behavior; later remote startup trials
must recreate/reset the relevant worker caches. Coding remains a sparse 1TiB
logical fixture with about 5GiB allocated, not a populated-large acceptance root.

## Packet and parent identity

Construct all 59/1,751 admitted trigger-leaf packets using the untouched original
compressed leaf and complete original data frames. Packet contents are the
64-byte manifest header, 80-byte member records and actual original encoded bytes.
The selector now contains the SHA256 of the ENTIRE encoded packet. It does not
address a manifest-only hash. The diagnostic selector has a distinct magic from
the previous manifest-addressed experiment.

The diagnostic 128-byte parent envelope contains the actual original compressed
root and encoded selector. Validate full parent hash, both section checksums,
original root authority, selector scope and every exact original child hash.
No original mapping entry is split or rewritten. Corruption, truncation, stale
leaf/parent binding, missing trigger and manifest-only addressing are rejected.
Packet checksum validation does not replace each original member checksum.

| Real bytes | Node | Coding |
| --- | ---: | ---: |
| All packets, stored bytes | 5,151,379 | 160,953,193 |
| All packets, decoded charge | 37,698,016 | 1,067,065,600 |
| Largest packet, stored / decoded | 147,777 / 983,152 | 235,258 / 1,022,144 |
| Selector, decoded / stored | 7,673 / 4,684 | 224,251 / 130,641 |
| Complete parent, decoded / stored | 8,783 / 5,297 | 239,579 / 135,946 |
| Original root, stored | 485 | 5,177 |

The same unique source page/frame ranges used by all groups occupy 617,994 /
18,122,779 encoded bytes; capsule duplication is real immutable storage cost.
These are extra packets, not a replacement for the entire original RootFS.
No new object was published to S3 or referenced from PostgreSQL. Import/COW,
publication collision/retry, inventory reachability and GC remain unimplemented
for this diagnostic format; a full-byte address alone does not solve them.

Unchanged product encryption wrapper, both AEADs, 16KiB chunks and existing
256KiB prefix/parallel-read limits: original roots, real parents and largest real
packets each require one underlying read, with both cold/header-hit passes.
All 24 fixture reads round-trip actual bytes. Coding AES cold parent requests
148,660 and returns 136,600 ciphertext bytes; largest packet requests 247,084 and
returns 236,032. Original root requests 17,428 and returns 5,671. This is transport
geometry in memory, not remote RTT or performance.

## Important failed model and its correction

`net-shape.json` assumed one mapping fetch per demanded leaf. Actual current
Reader replay contradicted it:

| Metadata workload | Incorrect modeled mapping reads / stored bytes | Actual source mapping reads / stored bytes |
| --- | ---: | ---: |
| Node | 3 / 131,056 | 1 / 174,410 |
| Coding | 20 / 867,599 | 15 / 2,567,720 |

The current `reader_mapping_group.go` derives contiguous same-object sibling
groups from the authenticated parent, with 32-page/4MiB-decoded/1MiB-stored bounds.
It trims cached edges, decodes/verifies missing siblings and reuses those bytes.
Here, four-page groups are common. Grouped bytes and cached sibling effects must
be represented in BOTH layouts. Fewer demanded leaves are not equivalent to
fewer source reads or less transferred data.

`net-shape-v2.json` includes this behavior. It matches actual baseline root,
mapping and data source counts/bytes plus exact mapping key/offset/length sequence
for Node/Coding metadata and 128KiB workloads. The actual Reader also produces
retained SHA256s for every returned request payload. No missing export range or
speculative-read failure was observed.

## Corrected candidate screen, not measured startup

These are offline source-event counts and encoded byte/decode charges with empty
private unlimited caches, serialized immutable-base requests and no encryption,
network, dirty-tail, shared singleflight or occupied-node scheduling simulation.
Only baseline source counts/bytes/schedules are checked against actual Reader
execution; candidate execution and its CPU/latency are NOT measured.

| Metadata screen | Source events | Encoded bytes | Decoded charge |
| --- | ---: | ---: | ---: |
| Node ordinary | 25 | 334,232 | 2,258,006 |
| Node unconditional packet | 15 | 696,515 | 4,919,279 |
| Node leaf-miss-only packet | 17 | 339,908 | 2,266,671 |
| Coding ordinary | 53 | 2,871,857 | 14,080,013 |
| Coding unconditional packet | 40 | 3,172,638 | 16,793,032 |
| Coding leaf-miss-only packet | 42 | 2,714,065 | 13,506,872 |

Unconditional packets more than double Node's byte/decode costs. Leaf-miss-only
use selects only 1/3 packets in these metadata sequences: Node has 8 fewer events
with +1.70% bytes; Coding has 11 fewer with -5.49% bytes and -4.07% decoded charge.
Do not multiply those event counts by an assumed RTT or call them critical-path
savings. Payload retention is not total mapping/selector/temporary RSS.

For pure 128KiB requests, both policies preserve every non-root ordinary source
event. The extra parent still adds 4,812 Node / 130,769 Coding encoded bytes.
No ordinary bulk fragmentation is introduced, but this is not zero-cost fallback.
The first rejected net model remains in the artifact set and is not used to
support the decision.

## Next gate

Use the corrected grouped-mapping model on the retained complete claim plus
first-command read streams, with their explicit immutable-replay limitations.
Map eligible leaf-miss deliveries back to causal wait ownership and quantify the
remaining combined 2s gap BEFORE admitting another remote startup variant.
If coverage cannot plausibly close it, reject this as a standalone solution.
If it can, validate actual candidate Reader I/O/CPU/memory/cancellation and
unchanged bulk bytes under the existing shared cache/admission limits before
paired remote testing. No timeout/cache/width sweep.

All 1s/2s, regional ingress to authenticated procd, immediate real `node -v`,
empty-node versus cached-new identities, populated-large RootFS, occupied actual
width, isolation, durability and cleanup acceptance remain open.

Evidence root: `/tmp/sandbox0-joint-bytes.Ntq5cM`. Source export, packet construction,
tests, original Reader replay, rejected/corrected models and lifecycle receipts
are retained there. Nine top-level Go tests ran successfully under race across
the four stages; this is not local e2e or remote startup acceptance.
