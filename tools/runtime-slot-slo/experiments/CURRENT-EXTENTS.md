# D-CURRENT-EXTENTS: exact file coordinates and the cost of eager ELF reads

2026-09-13 Asia/Beijing. Evidence root `/tmp/sandbox0-current-extents.0exACR`;
remote `/data/sandbox0-current-extents-0exACR`. Read-only metadata collection and
offline geometry, not new claim/command timings or a runtime optimization.

## What changed in the evidence

D-CURRENT-READ-SCOPE supplies exact CURRENT format2 volume requests. Earlier
regular-file layouts belong to B-UTC8/retired artifacts, and the retained current
directory graph does not supply regular-file data extents. This turn queries
only FIEMAP for the16 files already identified in D-NODE-BINDING: Node and seven
dependency candidates in each exact current immutable image. No full-file hash
scan, guest execution, new trace, artifact import or obsolete decoder is repeated.

The trusted scanner chroots before path resolution, requires canonical regular
files on the mounted filesystem, checks prior sizes and complete bounded FIEMAP,
accepts sparse gaps, and rejects unknown/delayed/unwritten/inline/encoded flags.
Queries use no SYNC flag. File SHA256/ELF metadata comes from the earlier verified
scan of the SAME authenticated immutable artifact, not new file hashing here.
Object-key allowlisting, encryption/checksum validation,128MiB private cache,
512MiB reserved-ciphertext/180s per-image diagnostic bounds remain in force.

Both images mount read-only/no-recovery/no-exec in a private namespace onNBD31.
The current source builds the inspector; no ctld cache or product binary changes.
Seven top-level Go tests, including15 adversarial extent cases, pass three race
repetitions; vet and linux/amd64 build pass. Binary SHA256:
`40fcf7adc99d96ec2f3524c696c603e991783dfc9c7afce9d27244f06bd3ca87`.

## Current file-read geometry

Project the existing16-sandbox NBD traces through each file's actual extents,
clip file EOF, keep sparse holes separate, and compare initialized ELF PT_LOAD
file pages. All256 file/phase groups independently match a512-byte cell oracle;
four projection tests/seven assertions pass. All16 identities have zero observed
reads of these selected files during claim. The values below are identical in
both images and their cold/cached-new cohorts.

| Node executable scope | Bytes |
| --- | ---: |
| File-relative union reached by first-command NBD reads | 65,314,816 |
| Allocated file bytes covered by all initialized ELF pages | 107,102,208 |
| Intersection of those two sets | 63,401,984 |
| ELF plan outside observed file-read union | 43,700,224 |
| Observed file-read union outside initialized ELF pages | 1,912,832 |

The largest read-only segment has57,749,504 allocated planned bytes but only
24,420,352 overlap. The large executable segment has38,457,344 planned bytes and
28,086,272 overlap. Flags alone do not identify the observed working set.

The seven dependency candidates' complete file bytes appear in the first-command
NBD union in each image. Their initialized ELF plan adds no previously unobserved
file bytes in these samples. Their different sizes account for much of the
between-image volume-byte difference, but do not prove library-latency causality.
This is not an actual dynamic-loader choice trace or minimum mandatory demand.

The Node union happens to equal the old retired trace's65,314,816 value. It is
now computed independently from current artifacts and their current file extents;
it does NOT rehabilitate the retired trace as current-runtime evidence. The
43,700,224 extra-byte set excludes20,480 sparse bytes from the unqualified old
ELF comparison. Keep the earlier exclusion and its provenance correction intact.

## Decision and next implementation boundary

An unconditional all-segment real-read plan could touch about43.7MB more Node
file data than this observed NBD union, approximately67% of its size. This is
NOT a prediction of43.7MB additional S3 transfer: compression, already-decoded
coalesced interiors, kernel caching and shared flights must still be charged
using their actual semantics. Nor do extra bytes alone prove a latency regression;
parallel early delivery could trade bytes for fewer sequential waits.

The missing file-coordinate gate is complete. Do not repeat identity/layout
scans, volume-trace extraction, unsupported guest advice, or legacy-carrier
recovery. The next useful step is a bounded opt-in GENERIC execution-time real-
read prototype and its safety/cost test, not enabling eager reads by default.
It must use actual reads (stock gVisor hints remain unsupported/no-op), start
only after claim binds RootFS and the real command is known, and retain literal
`node -v` as the measured command. No Node-specific profile or tenant prewarm.

Inspecting current command construction identifies constraints for that prototype:

- Reuse the already-resolved `exec.Cmd.Path` plus its working directory and
  existing error state. Do not invent another PATH resolver after merged Env.
- Do not substitute the seven static library candidates for actual loader rules
  involving LD variables, RPATH/RUNPATH, interpreter or cache behavior. A main-
  executable-only first prototype avoids this unproved loader equivalence.
- Preserve arguments, stdin/no-stdin, PTY, process-group signals, reaping, output,
  command failures and cancellation. Bound concurrency, retained/read scratch
  memory and abandoned work; admission must not rely on every read canceling
  instantly. Execute-only/unreadable or unsupported files must keep normal exec
  semantics, not become new command failures.
- Measure all preparation/overlap/teardown from the original external command
  start. DirectRunner records process StartTime only after `cmd.Start`; that
  narrower timer cannot hide pre-exec work or serve as the end-to-end result.
- Test one explicit policy with matching observer-free on/off artifacts and
  bounded cost before full cold/cached-new, occupied-width, populated-root and
  regional1s/2s admission. Do not infer success from this geometry or use a
  current-format historical-runsc trace as current-pin latency acceptance.

No prototype implementation, production default or speedup is claimed here.

## Remote result and cleanup

Node metadata inspection:23 underlying ciphertext range calls,712,486 reserved /
690,353 returned bytes. Coding:45 calls,2,208,140 reserved /2,152,025 returned.
Total68 calls /2,842,378 returned bytes; these are not separately counted HTTP
attempts or required startup bytes. Both queries explicitly read zero file
payloads; mounting and metadata access can still cause underlying block reads.

The unit `sandbox0-current-extents-0exACR.service` completes successfully, MainPID0,
inactive/exit0. Both mounts removed, all64NBD devices detached. Exact original
processes/configs/binaries/job72102 and263/formal2072 rows remain unchanged.
The deliberately old baseline has1ready carrier; it was not repaired or admitted
as runtime acceptance. Zero claims, guest commands, object/DB writes or production
actions. No local e2e, merge, tag, timeout increase or historical data deletion.

SSH is explicitly closed; fresh21:07:07.456UTC CLI observation confirms
Stopped/StopCharging at original2CPU/8GiB, no public IP. Only temporary diagnostics
and experiment records change; all1359 product source files remain unchanged.
The regional cold2.221/2.668s misses and every original goal gate remain open.
