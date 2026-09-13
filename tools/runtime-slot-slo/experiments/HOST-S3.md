# D-HOST-S3: host, transport and encrypted-header attribution

Date: 2026-09-12. Diagnostic only; no achieved startup/SLO or density gate.

## Why this experiment

The user's machine/S3 hypothesis takes priority over further combined-layout
qualification. Long post-write waits do not identify S3 service as their sole
cause: host scheduling, kernel/netpoll and network remain possible contributors.
No product, algorithm, key, timeout, cache budget, machine-size or concurrency
change was made. Combined prototype correctness and net-cost gates remain open.

Use the existing Singapore ECS at its current **2 CPU / 8 GiB**, not the earlier
16 CPU / 64 GiB runtime fixture. All main/infra refs were fetched: sandbox0 stays
0f092204; infra advances ceb895c -> af04ea9 (builtin template digests only).
The frozen runtime/source/configuration remains unchanged.

## Fixed original-object probe

Use three original Coding ranges from D-SOURCE-HTTP: root mapping (5,177 encoded
plaintext bytes), mapping group (175,438) and data frame (5,490). One reused
provider, credentials prepared separately, eight sequential paired rounds per
range with alternating raw/encrypted order. The raw control requests the exact
physical ciphertext windows observed by each initial encrypted read; digest and
returned length must match. New-header and cached-header stores both validate
original encoded and decoded checksums. No full Reader, NBD, claim or guest runs.

All **83 logical reads / 92 HTTP attempts** succeed, zero retries or admission
rejects. Requested 5,400,473 and returned 4,993,508 ciphertext bytes; EOF-short
original objects explain the difference. 90/92 connections reused. Credential
preparation is separately 123.343 ms; do not assign it to every object read.

| Median per read, ms (8 samples each) | Raw original ciphertext | Fresh encryption header | Cached encryption header |
| --- | ---: | ---: | ---: |
| Root mapping | 5.648 | 10.773 | 4.630 |
| Mapping-page group | 5.711 | 13.196 | 7.085 |

Raw/fresh pairs have identical physical ciphertext ranges; cached-header reads
omit the envelope prefix and are not identical byte geometries. The data range
needs two cold HTTP windows, which can overlap in the encrypted store; its raw
control issues the two windows sequentially. Do not compare its per-HTTP median
4.588 ms to the full encrypted read as if they were identical operations.

Keep first reads: root 55.289 ms, mapping group 35.073 ms, data 43.937 ms. The
root's 33.549 ms post-write wait and 15.833 ms connection acquisition are separate.
Repeated raw reads still have a 14.542 ms post-write outlier. No application
plaintext data cache serves these GETs, but repeated objects and unknown service
caches mean the later samples are **not** a truly cold S3-service guarantee.

## Concurrent host observations

47 samples span 909.632 ms, including provider preparation. Sampling every 20 ms
costs median 0.575 ms / max 1.723 ms; maximum observed sample gap 20.994 ms. This
observer has nonzero overhead and cannot resolve every sub-millisecond stall.

- GOMAXPROCS=2; host aggregate CPU idle 71.823%, steal 0 ticks, iowait 1 tick.
- All observed cgroup ancestors have zero CPU throttling increments; service
  and system.slice cpu.max are unlimited. Root cgroup limit files are absent
  and reported missing, not invented as observations.
- CPU PSI some increases 43.462 ms host-wide and 22.619 ms in the probe service;
  scheduling contention is not literally zero despite aggregate idle capacity.
- Memory PSI and memory.events increments are zero. Host I/O PSI some/full
  3.180/2.505 ms; the probe's own I/O PSI does not increase.
- TCP retransmission/timeouts do not increase. One host-wide TCPAbortOnClose
  does increase; the aggregate counter does not identify its connection.
- Probe process RSS peaks at 17,548 KiB; process CPU over the observation is
  318.995 ms. Neither is occupied-runtime/density evidence.

This run does not show sustained host saturation, CPU quota throttling, swapping
pressure or retransmission explaining its latency. It does not exonerate machines
under production occupancy, other nodes or the earlier 16-core fixture.

## A separate local cost was found and tested

With a fresh root header, the interval from the final HTTP body close until
encrypted-read completion has median **5.595 ms**; cached-header median is
0.002 ms. Mapping-group tails are 5.839/0.005 ms. Independent plaintext decode
and checksum validation occur later and are separately timed.

Current objectAEAD calls KeyEncryptor.Decrypt on a header miss, implemented by
RSA-OAEP. To test this specific residual, fetch the SAME original root ciphertext
once more, verify its prior digest, retain its envelope only in process memory,
and call the unchanged production NewKeyEncryptor on that SAME wrapped data key
16 times. No key, wrapped-key payload, nonce or body is exported; results are
checked for identical 32-byte plaintext and cleared from scratch memory.

All 16 decryptions succeed: wall **5.199–5.438 ms**, median **5.271 ms**; median
process CPU **5.284 ms**. Process CPU includes the concurrent host sampler, not
precise RSA-thread cycles. This isolates a real CPU cost with the existing
aes256gcm-rsa envelope and matches the observed fresh-header residual. Exact
per-object RSA spans inside full Reader/runtime are still not measured here.

The extra raw HTTP read itself takes about 70.455 ms: connection 17.844 ms,
post-write 52.420 ms, body about 0.083 ms. This delay exists without layout or
decryption, but it still does not isolate S3 service from transport/host waits.
Total campaign HTTP attempts: **93, all successful**. Two finite probes only.

The sealed ordinary Reader evidence has 50 distinct source object keys for each
Coding stream versus 7 for Node; the union across eight streams is also 50/7.
This is not a live header-miss count and must not be multiplied by lane count or
5 ms to claim startup savings. Header reuse/singleflight and parallel dependencies
matter. Still, the number of distinct cold envelopes belongs in the structural
cost model alongside HTTP windows, bytes and mapping decode.

## Integrity, mistakes and environment lifecycle

- Original source/configuration/binary hashes, service PIDs, boot, source row
  counts, warm-job modify index 70668, physical-empty state and detached NBD64
  match before/after. Ready carriers are 2 on this boot, without any repair.
- The inherited zero-ready preflight fails twice before measurement; inspect
  actual state, then admit the known 0/2 carrier counts only for object-only
  diagnostics. Do not label that predicate as runtime acceptance.
- First native build omits two new Go files and fails; retained build.json.
  Corrected build-v2 uses the tested manifest, static linux/amd64. No failed
  binary runs. First probe: 5 Go race tests; key follow-up: 6 Go race tests.
- Analyzer: 3 tests / 796 assertions, including exact ciphertext replay hashes.
- Skill controls Makefile lifecycle only: no historical Kind/bootstrap/deploy.
  Preserve /data; no production, object writes, import, merge, tag or local e2e.
  Probe units finish inactive/success/0. Final cloud read at
  2026-09-12T03:19:18.723349375Z confirms Stopped/StopCharging, 2 CPU / 8 GiB,
  no public IP. /data is preserved.

Evidence: `/tmp/sandbox0-combined-net.3HqrAF`. Full receipts, failed preflights,
failed build, exact source manifests and both remote reports are retained.

## Next decision

Reconnect these costs to the original complete Reader and real claim + node-v
critical path, with authenticated regional ingress timing and actual host-load
correlation. Measure actual header misses/unwrap CPU and overlapping HTTP waits;
do not repeat this three-object baseline, blindly resize, weaken cryptography,
increase timeouts or credit cache hits as empty-node acceptance. S3-side timing
or connection-level evidence is still needed to separate service from network.
All 1s/2s, populated-large-root, occupied-width, durability and lifecycle gates
remain unchanged and open. The goal is not complete.
