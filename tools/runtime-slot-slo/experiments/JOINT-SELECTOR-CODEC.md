# D-JOINT-SELECTOR: bounded early selection, measured encoding and limits

2026-09-12. Local diagnostic codec and transport-geometry experiment. No product
source change, live provider request, new RootFS object, runtime claim or command.

## Decision

Early selection is representable within explicit byte bounds for both retained
images without an extra current-tree level. A size-matched encrypted-store test
also shows no extra cold transport round trip at the proposed parent sizes.
This removes two uncertainty gates; it does not establish net startup improvement.

Do NOT publish the diagnostic manifest-addressed namespace. A new counterexample
shows that manifest identity does not identify the full encoded object bytes.
The next complete-packet prototype must use actual full-object byte checksums and
the existing immutable publication/retry/inventory semantics. Do not weaken those
contracts to make a geometry-only identifier convenient.

Next obtain the actual original encoded mapping/frame bytes and build complete
packets with their real full-content identities. Then compare total mapping+data
transport, decoding, shared-cache charges and ordinary bulk behavior, including
the larger initial parent. Do not run a new claim benchmark of this incomplete
codec or treat its nanosecond lookup time as a startup result.

## What was encoded

Use all 59 Node/1,751 Coding minimal candidates admitted by D-JOINT-DELIVERY.
No trace/path selection, membership truncation, new split views or larger packet
budget. Original source profile and prior artifact seals were reverified.

- Capsule manifest: 64-byte canonical header and 80 bytes per original complete
  page/frame. Include decoded checksum, encoding, stored/decoded lengths, relative
  offset, original leaf bounds and trigger member. All members stay present.
- Selector: 64-byte header, shared bounded prefix and sorted 128-byte records.
  Records carry the logical interval, current leaf hash, diagnostic manifest hash,
  trigger frame hash/length and all packet/manifest lengths. Full 32-byte hashes
  remain; long object-name repetition is removed rather than hashes shortened.
- Zstd uses the worktree's settings: one encoder thread, 64KiB window, default
  level and raw fallback if compression expands. Decode bounds and SHA256 checks
  remain explicit. This is version 65533 in standalone scratch code, not a newly
  supported durable RootFS version.

The diagnostic manifest hashes are hashes of actual encoded manifests here, not
zero/random future object-hash stand-ins. They nevertheless are NOT the final
packet content hashes; substituting the real packet hashes requires re-encoding
and remeasuring the selector. Fixed raw record size remains the same, but exact
compressed bytes may change. See the identity counterexample below.

## Complete-image encoding results

| Bytes | Node | Coding |
| --- | ---: | ---: |
| Actual manifest bytes, all candidates | 40,576 | 1,137,184 |
| Actual selector before compression | 7,675 | 224,253 |
| Actual selector stored bytes | 4,686 | 130,695 |
| Original root core decoded bytes | 982 | 15,200 |
| Original root core stored bytes | 485 | 5,177 |
| Calculated wrapped parent decoded bytes | 8,785 | 239,581 |
| Calculated wrapped parent stored bytes | 5,299 | 136,000 |

Wrapped-parent arithmetic is an explicit 128-byte envelope plus the independently
encoded original mapping core and selector. The original core bytes are absent
locally; their sealed lengths are used. This is NOT a serialized complete parent
object, new descriptor, new Reader or proof of parent binding.

The original mapping core's checksum and child entries would remain the authority
for selecting the original leaf. The selector can only nominate prefetch content;
it cannot authorize logical bytes. Actual parent/envelope version handling,
current-leaf checks, branch-tail priority and packet/frame cache integration still
need implementation and tests.

## Hierarchical byte bounds are not free constant height

Diagnostic parent bounds are 1MiB decoded, 240KiB stored and at most 1,024 ordinary
children, using existing publication budget values as an explicit design choice.
They are not a claim about an implemented joint-parent runtime contract.

Packing keeps every existing leaf's selector records together. Charge actual
compressed selector bytes and a conservative 1,104 bytes per ordinary child
(80 fixed + maximum legal 1,024-byte key), plus envelope/core header. A single
oversized leaf falls back as a whole; malformed oversized input is rejected, not
silently turned into a fallback. Upper routing levels carry no trigger table.

Both real images fit one parent with no missing plans under this conservative
model: 5/79 children, 13,355/311,629 decoded charge, 10,369/218,085 stored charge.
Their selector scope ends at the last original leaf, unlike the full-logical-size
root scope above; those different headers explain the few-byte compression
differences. Neither calculation measures a new complete parent object.

Synthetic stress cases, not populated-large RootFS or startup tests:

| Shape | Leaf-parent groups | Whole-leaf fallbacks | Conservative parent levels vs ordinary 1,024-child geometry |
| --- | ---: | ---: | ---: |
| 64 leaves, each 1,024 plans | 22 | 0 | 2 vs 1 |
| One leaf with 8,200 plans | 1 | 1 | 1 vs 1 |
| 2,500 leaves without plans | 12 | 0 | 2 vs 2 |

The dense case uses at most three children per group, 396,775 decoded and 211,174
stored charged bytes. An extra dependent mapping level is real model cost, not
something to omit from a later savings calculation. Upper-level fanout is222
under the maximum-key byte bound. These calculations neither materialize all
upper routing objects nor prove a constant-height tree for arbitrary RootFS size.
Whole-leaf fallback is a safety bound, not evidence that a large case meets2s.

## Actual unchanged encrypted-store behavior, size-matched fixtures

Use the product `objectstore.EncryptingImmutable`, ephemeral in-memory RSA2048
key, both supported AEAD algorithms, 16KiB encryption chunks, 1,024 header entries,
8MiB header cache and 256KiB prefix/parallel-read limits. No real key is exported.
The root core/envelope are length-matched filler; the selector bytes are real.
This test isolates transport geometry and authenticated byte round-trip behavior,
not full RootFS content validity or live OSS latency.

All eight image/algorithm/baseline-candidate cases need ONE underlying read for
the cold offset-zero demand and ONE for the subsequent header-cache-hit demand.
The latter still fetches ciphertext; the header cache does not retain it.

- Node candidate 5,299 plaintext bytes: cold request17,428 bytes, returned
  5,792-5,793 bytes depending on algorithm's serialized header length.
- Coding baseline 5,177 plaintext bytes: cold request17,428, returned5,670-5,671.
- Coding candidate 136,000 plaintext bytes: cold request148,660, returned
  136,653-136,654. Header-hit demand requests147,636 and returns136,180.

Thus this size does not itself add a second cold transport read in the tested
geometry. It does add about131KB of initial ciphertext. Do not infer live transfer
time, same-flight sharing at eight-wide concurrency, other legacy encryption
geometry or production acceptance from an in-memory provider fixture.

## Local CPU/allocation observations

Three 200ms microbenchmark repetitions on this local Linux ARM64 host:

| Operation | Node | Coding |
| --- | ---: | ---: |
| Fresh decode + checksum checks + parse | 46.601-48.221us | 0.871-0.990ms |
| Allocated bytes/op | about50,292 | about951,518 |
| Allocations/op | 22 | 28 |
| Parsed lookup | 27.50-32.68ns | 48.90-56.85ns |

Fresh decode includes a new zstd decoder and a second checksum in selector parse.
These figures are not remote x86 startup CPU or a zero-cost cache assumption.
Allocated bytes are allocation churn, NOT retained memory, peak scratch or
node-wide residency. Real cache/source-flight ownership and concurrent accounting
must be measured before high-density admission. No cache enlargement is proposed.

## Full-object identity counterexample

Two valid 60-byte zstd representations are constructed using different equal-size
skippable frames. Both decode to the same aligned original payload and therefore
have the same decoded checksum, encoding and stored/decoded lengths. Their
diagnostic manifests are identical, but their encoded byte hashes differ.

`TestManifestIdentityDoesNotBindEncodingBytes` verifies this using the actual
bounded decoder. It does not claim current import emits skippable frames; it
demonstrates the distinction the publication contract must retain.

Consequently the diagnostic manifest-derived object name cannot replace the
existing whole-object content identity. A real packet must preserve its exact
full-byte SHA256, conditional-create collision checks, retry inventory and PG/S3
reference ownership. Original page/frame decoded checksums remain independently
mandatory too. No manifest-named object has been published by this experiment.

## Verification, failures and remaining gate

- Ten top-level local tests plus fuzz seeds pass with `-race`; a separate identity
  scope test also passes with `-race`. They cover strict parsing, rehashed malformed
  input, truncation/overlap, lookup against a mismatched current leaf, packet and
  hierarchy budgets, all real candidates and size-matched encrypted transport.
- Initial prefix validation incorrectly accepted `../root/`; the real defect
  was fixed and the negative test preserved. The failed attempt is retained.
- Initial10s mutation runs passed but lacked coverage instrumentation; they are
  retained as unguided only. Separately recompiled coverage-guided10s targets pass:
  249,948 selector executions and238,067 manifest executions. This finite fuzz
  work is not an exhaustive correctness/security proof.
- All experiments remain outside the product tree. Reverified1,353 product files
  and inventory,164 trace artifacts and12 D-JOINT-DELIVERY artifacts. No runtime
  implementation, live source request, cloud lifecycle change, claim or command.

The next gate is complete real packets and their actual content-addressed index,
then net joint mapping+payload I/O/CPU/cache tests, preserving ordinary bulk reads
and including initial-parent costs. Only a supported full variant can advance to
paired remote claim+`node -v`. COW/path-copy/GC/retry/cancellation/flight behavior,
empty-node and cached-new identities, populated-large roots, occupied actual width
and complete1s/2s acceptance all remain open. No production rollout, merge or tag.

Evidence root: `/tmp/sandbox0-joint-selector.x1Xc4S`; `analysis.json`, `race.json`,
`benchmark.json`, `guided-*.json` and `identity-scope.json` retain detailed results.
