# D-COMBINED-PARENT-BINDING: retain authority on selector cache hits

2026-09-12. Evidence root: `/tmp/sandbox0-combined-bind.8xh6cW`.
A diagnostic-prototype correctness fix, not product/runtime admission or a
measured startup improvement. The predecessor remains sealed and unchanged.

## Reproduced gap

The combined-delivery prototype memoizes a parsed selector by its wrapped-parent
content identity in the existing bounded ReadCache. On a cold miss,
`decodeCombinedParent` validates the caller's expected original parent. On a hit,
`DiagnosticMemo` returns the prior parsed selector without invoking that decoder.
The old `delivery.selector` did not revalidate the original-parent binding.

The negative test uses two DIFFERENT VALID original Node and Coding roots:

1. A cold Coding constructor with Node's wrapped parent correctly rejects it.
2. Prime a shared128MiB cache with Node's selector and both valid ordinary roots.
3. Repeat the mismatched constructor with Coding's descriptor and Node's wrapped
   parent. The old prototype accepts it without source reads.

`before.json` retains the failing `cached selector bypassed original-parent
binding` assertion. The same-parent no-extra-read control passes. This is evidence
of a prototype cache-authority gap, not a claim of a deployed production exploit
or proof that arbitrary cross-root payload bytes were returned.

## Correction and validation

The new temporary `delivery.go` wraps each memoized selector with the original
parent's checksum, decoded length, stored length and encoding. Every return from
the memo, including shared/cache-hit results, must match this decode contract.
Retained binding strings are cloned and their bytes/struct size charged to the
SAME bounded cache; there is no second metadata cache or larger budget.

Physical object key/offset are deliberately not added to content authority:
the decoder and existing cache permit the same immutable content at another
physical location. A separate test verifies that such reuse still needs no read.

The original two tests pass after the fix with unchanged test SHA. The expanded
regression passes under the race detector:5top-level tests and8subtests cover
two-valid-root mismatch, same-parent hits, each binding dimension, relocated
same-content reuse, joint/cached dispatch, corrupt-packet no-partial-publish,
ordinary fallback and selector eviction. No mismatch triggers speculative I/O.
These tests are not concurrent caller/lifetime qualification merely because
`-race` is enabled.

Original delivery SHA9e0020d7abe6d40a… is preserved by the predecessor seal; fixed
SHA3658fc3de3df0ff6… and exact before/after/test/overlay hashes are in the receipts.
The original two-test source is retained as `parent_binding_v1_test.go`. Tests use
verified original fixture bytes and unchanged Reader/cache overlay; the only
prototype implementation change is the return-value binding and its accounting.

## Effect and remaining gates

This fixes a correctness gate with zero extra reads on the tested same-content
cache hits. It does NOT establish a latency improvement or carry forward the
preceding full matrix as proof for the changed cache charges. After the remaining
guard changes stabilize, revalidate the exact complete-stream cache matrix before
net-cost admission; do not tune budgets to preserve its earlier counters.

Cancellation, concurrent selector/packet use and independent caller ownership
remain open. The current prototype still has diagnostic-only background lifetimes
and serial fixture counters. Net actual encrypted-source/decode/CPU cost, RSS
under occupied load, arbitrary populated large roots, durable import/COW/GC and
current regional ingress→procd→real `node -v` acceptance are also unproven.

No remote operation is performed by this guard campaign. D-OSS-ID separately
completes its one-request smoke test and stops compute. No product file, public
wire contract, production resource, PR merge or tag changes. Generic warm
carriers, claim-time RootFS, crypto/proofs,10s timeout and1s/2s goals remain.
