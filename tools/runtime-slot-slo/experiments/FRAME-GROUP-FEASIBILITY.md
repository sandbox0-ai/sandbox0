# D-FRAME-GROUPS: preserve whole frames, test delivery coverage

2026-09-12. Offline feasibility screen; no new Reader, wire format, publication,
remote operation, startup latency or production acceptance.

## Decision

Do not implement or remote-test this whole-frame-group-only design as a standalone
2s solution. It preserves the original data layout and reduces modeled delivery
windows, but the demonstrated request coverage is too limited to justify the added
representation/transfer costs as the whole solution. This is a budget-based
prioritization decision, not proof that every frame-group design is impossible.

Keep the useful constraints: no split original views, no4KiB override assembly,
no second index traversal, no separate content-cache truth, no cache-hit-triggered
or recursively triggered speculation, and original bulk dispatch/thresholds.

Next examine joint delivery of mapping information and its referenced complete
data frames, so both sides of mapping-then-payload dependency can change. Any
selector must stay hierarchically bounded; loading a flat full-RootFS index at
claim would violate the size-independence objective. This next mechanism is NOT
implemented or qualified by this report.

## Candidate and complete input

- Reconstruct ALL1010Node/15893Coding directory records from the sealed XFS
  inventory, including inode-containing and directory-data-fork blocks. These
  two named classes are not all possible XFS metadata.
- Fold every inode-trigger relation inside a complete original compressed frame
  into one union. Do not choose a directory owner using the command/path/trace.
- Verify the reconstructed capped4KiB plans equal all454/9588 previous admitted
  plans; additionally preserve the three original Coding overflows before folding.
  The initial exploratory query omitted these overflows; only audited results
  below are authoritative for this candidate.
- Original4341Node/80781Coding mapping entries remain unchanged, with zero split
  entries. Whole frame cache identity remains original checksum plus decoded size.
- Admit only complete groups of at least two distinct source frames, at most1MiB
  decoded and1MiB stored. Oversized groups fall back, never truncate members.
- A small (<128KiB) cache miss may fetch one group and populate verified ORIGINAL
  complete frame entries. Bulk reads retain ordinary coalescing. In a future
  implementation, hint data cannot override RootFS bytes or bypass parent proof.

## Representation cost

Local-entry-index-only plans have insufficient topology coverage: only3Node and
37Coding groups survive both locality and size constraints. Almost every useful
directory relation crosses an original mapping leaf. A same-page foreign-reference
dictionary is therefore an explicit candidate cost, not a free cross-page lookup.

| Full-image model | Node | Coding |
| --- | ---: | ---: |
| Whole-frame triggers | 64 | 1832 |
| Cross-leaf candidate unions | 61 | 1795 |
| Admitted bounded groups | 61 | 1794 |
| Whole-union budget fallbacks | 3 | 38 |
| Additional modeled page bytes | 37,098 | 1,258,462 |
| Largest added page bytes | 13,381 | 86,530 |
| Grouped encoded frame payload, summed per trigger | 2,703,730 | 87,494,560 |

Page-byte conventions:256 bytes per group header (including explicit bounded
locator/range/checksum fields), two bytes per member index, and80 bytes plus the
original key length per foreign reference. These are modeled uncompressed record
costs, NOT a serialized/compressed/authenticated wire measurement. Group payload
uses exact original stored-frame lengths; per-trigger duplication is charged.
Do not add these unlike quantities or claim physical object/ciphertext sizes.

The largest group is851,968/1,048,576 decoded bytes and102,413/189,361 stored bytes.
All modeled pages remain below the existing8MiB decoded page ceiling, but that
ceiling is a correctness limit, not proof of acceptable mapping I/O or density.

## Request-model results

All families start empty with ideal instantaneous, unlimited frame retention.
The model counts plain source transfer windows, not provider GETs or latency.
Mapping access, encryption/header effects, actual decoding, eviction, concurrent
completion and original writable-tail state are excluded. Historical cached-new
labels name the source request shapes, NOT simulated cached-node claims.

| Immutable helper shapes | Ordinary windows | Group windows, including fallback | Stored frame bytes, ordinary -> candidate |
| --- | ---: | ---: | ---: |
| Node metadata,36 requests | 23 | 13 | 159,337 -> 294,195 |
| Coding metadata,51 requests | 37 | 21 | 298,960 -> 867,013 |

Candidate group fetches number6Node/10Coding; remaining windows use original
fallback. Pure128KiB mixed workloads retain EXACTLY identical baseline/candidate
modeled source-event sequences:8 windows/1,623,267 bytes Node and26 windows/
3,925,537 bytes Coding. This does not establish equal total mapping/CPU cost;
interleaved small reads also change cache contents before later bulk requests.

Replay all14,373 retained NBD shapes across16 sandbox traces, with a private empty
ideal cache for each family/sample. The complete claim-plus-command shape model:

- Coding windows266 ->250; stored frame bytes44,760,300 ->45,456,449;
  ideal retained decoded payload110,759,936 ->115,806,208 bytes.
- Node windows236 ->225; stored frame bytes43,870,919 ->44,041,113;
  ideal retained decoded payload106,373,120 ->107,356,160 bytes.

These private ideal working sets are not a node memory budget. The mixed-root
aggregate exceeds the real128MiB shared cache before mapping charges; real cache
eviction/pressure must not be inferred away. No high-density claim is made.

## Relate coverage to the end-to-end deficit

For each recorded sandbox, identify requests whose modeled payload window count
decreases, then union their OLD block-payload wait intervals. Mapping waits are
excluded because this candidate does not remove the authoritative mapping lookup.
All16Coding/11Node affected requests reduce windows; no increased-window request
appears in these particular ideal streams.

- Cold Coding affected-request payload-wait exposure:313.216-314.652ms.
- Cold Node:138.387-149.292ms.
- If ALL those old waits vanished and every other cost stayed fixed, Coding's
  combined path would remain2351.176-2352.754ms. Added bytes, dictionary fetch/
  decode work and real cache competition have not been included.

This is not a causal speedup prediction or an architectural lower bound. A future
mechanism could alter other dependencies, which must be separately demonstrated.
The result does not justify advertising a666ms gap closure from a43% metadata
window reduction or spending a remote trial on this design as the entire fix.

## Verification and remaining scope

Seven model tests/31 assertions pass: complete frame-union membership, cross-leaf
dictionary accounting, no view split, full overflow fallback including original
4KiB overflows, small sector requests, no recursive/cache-hit activation, unchanged
bulk schedules, holes and malformed input. An initial String-versus-Symbol test
expectation failed and was corrected without changing model behavior.

Verified all1353 product files/inventory,164 trace artifacts,77 directory-graph
artifacts,393 prior inline-input artifacts and12 critical-budget artifacts.
No runtime source, RootFS object, cloud resource or production state changed.
Remote status was not queried; last known stopped state is from the prior remote
turn. All requirements and full cold/cached-new claim+real command, populated-large
RootFS, occupied actual width and1s/2s acceptance remain open.

Evidence root: `/tmp/sandbox0-frame-groups.ZNyWRh`; complete source-window models
in `analysis.json`, request/wait correlation in `correlation.json`.
