# D-COMBINED-FIRST-TOUCH: cache-driven representation switching

2026-09-12. Evidence: `/tmp/sandbox0-combined-first-touch.cXbbDJ`.
Local diagnostic progress after the mixed D-COMBINED-TRANSPORT result. No
product change, new remote operation, claim, command or startup acceptance.

## Finding

All eight new cached-new Coding headers correspond to a real decoded-data
eviction while the exact original mapping leaf remains resident and protected.
The next request therefore chooses a data-only object, not the earlier mapping+
data object. This is a cache-dependent representation switch, not encryption-
header eviction. Three preceding data evictions are caused by Node-image reads
in the same mixed cache. This connects the original density concern to an actual
cache mechanism, but the serialized helper is NOT occupied-runtime evidence.

The chain is:

    Verified data admitted -> data evicted, leaf retained -> later data miss
        -> data-only representation selected -> first header for a new object

## Complete, passive observation

The unchanged corrected candidate replays all 14,373 shared mixed-image request
shapes plus 16 constructors using the actual Reader and the same 128MiB total/
16MiB protected mapping cache. Both complete source sequences, output hashes and
cache snapshots match D-COMBINED-TRANSPORT exactly: 330 cold/167 cached-new source
calls. No source/caching policy, LRU touch order, retained charge or admission
limit changes. The new journal only records add/page/evict operations under the
existing cache mutex for eight trigger identities and eight exact leaves; it has
a 1,024-event cap and fails on overflow. It records 41 events without overflow.
It contains no cache data and cannot satisfy a read.

At every target dispatch the trigger is absent, its leaf is present, and exactly
one data-group source read makes the trigger present again. Each trigger has an
earlier verified admission followed by an explicit LRU eviction. None of the
eight leaves is evicted; each is charged 517,543 bytes after parsing/protection.
The source of the last eviction before each new-header dispatch is:

| Trigger index | First admitted at read | First source | Preceding eviction read | Evicting image/cohort | Later data-group read |
| --- | ---: | --- | ---: | --- | ---: |
| 17454 | 1102 | Its joint packet | 5178 | Coding cold | 8294 |
| 33958 | 1710 | Its joint packet | 5249 | Coding cold | 8471 |
| 38650 | 1751 | Its joint packet | 5252 | Coding cold | 8478 |
| 40413 | 1799 | Its joint packet | 5252 | Coding cold | 8703 |
| 26089 | 1975 | Ordinary mapping/data | 5308 | Coding cold | 8922 |
| 75259 | 3085 | Its joint packet | 8624 | Node cached-new | 9963 |
| 10843 | 3756 | Ordinary mapping/data | 8742 | Node cached-new | 10138 |
| 76898 | 4016 | Another trigger's joint packet | 8827 | Node cached-new | 10482 |

Read IDs identify the retained complete schedule, not timing or a production
sandbox ID. Ordering uses actual schedule position, not numeric ID assumptions.
Seven eviction-causing requests are 128KiB, one is 16KiB; all perform ordinary
data reads. This does not prove those evictions are avoidable without other costs.

## Why simple old-packet reuse is not enough

Only five of the eight targets previously read their OWN joint packet. Two have
no joint packet: adding their original mapping leaf would require decoded groups
of 1,063,184 and 1,116,432 bytes, above the unchanged 1MiB bound. The eighth was
incidentally populated by another trigger's packet; its own joint was untouched.
No group may be truncated or prewarmed to make those cases disappear.

Actual packet/data decoders verify all eight data objects and all six available
joint objects, including full-object identity, exact original checksums/lengths/
encodings, encoded bytes and decoded bytes. Available pairs have identical
original data-member sets. However, five of the six pairs have a different data
publication order: joint packets keep the earlier ordering; data groups put the
trigger first. Four of the five reusable pairs differ. Switching implementations
would therefore change LRU effects as well as fetching an extra mapping leaf.

For the five reusable objects, the previous actual encrypted trace records five
HTTP attempts, not a separate header GET for every object: the existing bounded
offset-zero prefix path already co-reads each header and demanded ciphertext.
Making their header processing free removes no source window. Under the explicit
fixed-other-cost assumption, the exclusive crypto credit is 26.255ms in arm1 and
25.811ms in arm2, not all 41ms from the eight new headers.

The second cached-new pair's aggregate candidate-minus-ordinary Reader delta
would still be +465.059ms; Node stays +153.934ms and Coding +311.125ms. These are
counterfactual component calculations, not achieved savings or a causal/statistical
rejection of all packing designs. HTTP variability from the prior campaign is
unchanged and cannot be assigned to code by this subtraction.

Always fetching the five old full joint packets instead also adds 220,164 encoded
plaintext bytes and 988,720 decoded bytes before further cache/order effects.
Those are not encrypted wire-byte predictions. Do not implement this shortcut
as a purported structural cold-start fix or enlarge the header cache.

## Next structural gate

Evaluate complete-topology authenticated pack-range placement, not just these
eight hot objects and not another envelope-only timing sweep. Co-locating the
UNCHANGED independently verified representations could share headers without
changing logical member/publication semantics, but its real costs must be checked:

- Exact physical key/offset/length, full member identities and current-parent/
  recipient binding; immutable aliases cannot become a second RootFS authority.
- Actual whole-topology pack and selector geometry, including ordinary fallbacks,
  large-root hierarchy and materialization/COW/import/inventory/GC obligations.
- Cipher-chunk alignment and returned bytes. Nonzero-offset first reads can lose
  offset-zero prefix co-reading and add a header HTTP request even when safe
  header/data overlap is available. Count both requests and overlap, not just
  fewer encrypted object identities.
- Complete mixed-root source/cache behavior, decoding, retained/temporary memory
  and both-image net costs. A smaller header count alone does not establish the
  previously required payload-wait coverage or ingress+real-command 2s gate.

Reject a placement that only moves costs or lacks sufficient critical-path
coverage. Do not reopen previously rejected always-encoded caches, platform
seeding, pinning, cache-budget/timeout/hardware/concurrency sweeps. The current
candidate remains outside runtime admission; no production format is approved.

## Tests, correction and state

The initial member test wrongly assumes positional equality and fails. Its full
source and failed receipt remain. The corrected test compares exact immutable
identities AND both encoded/decoded bytes, while recording the order difference;
it does not silently treat different publication order as equivalent.

Go race checks pass for the passive observer (three subtests), actual member
binding, complete shared-cache replay, and an independent guard tying all eight
target records/leaves to the decoded selector and exact descriptor/parent from
the measured encrypted transport input. The replay takes 22.00s locally,
which is NOT a startup measurement. Ruby analysis has three passing tests/eight
assertions, including invalid eviction history, nonretained mapping and schedule
ordering. The passive overlay is reversible to the exact prior cache code after
format normalization. All 1,353 frozen product files remain unchanged.

No remote/cloud command this turn and no live remote work was launched. The last
remote observation is D-COMBINED-TRANSPORT's 05:57:19UTC Stopped/StopCharging
receipt, not a newly observed state. Preserve its data/objects, failed controls
and full evidence. Generic warm carriers, claim-time tenant RootFS, stateless
nodes, cryptography/checksums, authenticated readiness, fencing and durable
cleanup remain unchanged. Empty-node/cached-new, populated-large, occupied-width,
claim/real-command/combined and the original 1s/2s acceptance gates remain open.
