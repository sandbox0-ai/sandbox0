# Public runtime acceptance

The cold-start investigation has converged; see the
[closeout decision](experiments/CONVERGENCE.md). Further performance experiments
are not part of this handoff. The [experiment log](EXPERIMENTS.md) preserves
frozen baselines, observed changes, negative results and missing coverage; it is
not a production acceptance report. Accepting the observed approximately
two-second results does not rewrite historical failures or change this tool's
explicit, configurable acceptance thresholds.

`runtime-slot-slo` measures sandbox startup from regional ingress to authenticated
procd readiness and the complete public claim response. Its default startup
profile requires both to finish within a one-second hard limit; that profile is
not a claim that current deployments meet it. User executable startup is a separate diagnostic:
small and large programs need not take the same time. The tool sends
`POST /api/v1/sandboxes`, then optionally ordered
`POST /api/v1/sandboxes/{id}/contexts` requests with `type: cmd`, literal
`cmd.command` argv, `wait_until_done: true`, and a bounded `ttl_sec`.

The default workload starts `/bin/sh` and checks a printed marker. A successful
procd readiness proof does not execute that shell, load Node, or start Codex.
Use `--workload none` for the sandbox-startup gate without user program reads.
This does not skip runtime launch, RootFS mounting, isolation, resource/network
binding, authenticated readiness, or generation publication.

## Run

Build one artifact from the repository root and retain its hash for all reports:

```sh
go build -buildvcs=false -trimpath -o /tmp/runtime-slot-slo ./tools/runtime-slot-slo
sha256sum /tmp/runtime-slot-slo
/tmp/runtime-slot-slo \
    --url https://region.example.com/api/v1/sandboxes \
    --token-file /secure/api-token \
    --template default \
    --batches 1000 --concurrency 1 \
    --workload none \
    --request-timeout 10s --context-ttl 15s \
    --p50-target 500ms --hard-limit 1s \
    --output serial-1000.json
```

Use the same binary for synchronized runs. See the production-width prerequisites
and commands in [the driver SLO guide](../../nomad-driver-sandbox0/README.md#regional-ingress-to-procd-slo).
Local HTTP tests do not establish runtime or production-width acceptance.
These examples explicitly preserve the existing canary's 10-second request
budget. The tool's original 15-second default is unchanged; always pass
`--request-timeout 10s` for this canary, including coding-agent workloads.

## Workloads

`--workload shell` is the default. Its shell script is fixed; the expected marker
is passed as a positional argument. `--workload coding-agent` runs Node with a
marker assertion first, then `codex --version` with a `codex` stdout substring
assertion. The selected template must contain those executables on its command
PATH. This scenario proves executable startup, not a model-backed coding task.
`--workload none` dispatches no CMD requests and reports no executable evidence.

For exact executable paths, different arguments, stricter version assertions, or
Codex as the first executable, use `--workload-file workload.json`. It overrides
the built-in selection. For example:

```json
{
    "name": "coding-agent-pinned-paths",
    "steps": [
        {
            "name": "node",
            "argv": ["/usr/local/bin/node", "-e", "process.stdout.write(process.argv[1])", "node-ready\n"],
            "expect_stdout": "node-ready\n"
        },
        {
            "name": "codex-version",
            "argv": ["/usr/local/bin/codex", "--version"],
            "stdout_contains": "codex-cli"
        }
    ]
}
```

Every step requires exit code **0** and exactly one non-empty `expect_stdout`
(exact match, including newlines) or `stdout_contains` expectation. Missing exit
status, missing stdout, running/nonterminal responses, HTTP/API errors, duplicate
context identities, or mismatches fail acceptance. `output_raw` is not substituted
for stdout. The first failed step stops the workload; skipped steps cannot pass.

Workloads contain 1–16 uniquely named steps, at most 128 argv entries per step,
and at most 64 KiB of configuration. Unknown fields and NUL arguments are rejected.
Argv is JSON-encoded directly, without interpolation, concatenation into shell
text, variable expansion, or execution on the runner. A custom `sh -c` or
`node -e` program is explicitly supplied code; pass variable data as separate
arguments. Configuration and captured output are retained in the report.

An optional per-step `env_vars` string map is sent as the command request's
top-level `env_vars`, not added to the claim or a shell wrapper. Empty maps are
omitted, preserving existing workload hashes and request bodies. Keys must be
non-empty and contain neither `=` nor NUL; values must not contain NUL. The
existing64KiB workload limit includes environment entries. Values remain literal.
The context response must confirm every explicitly requested entry, including
empty values; missing or changed entries fail the workload. Reports retain only
these requested entries, never the whole inherited sandbox environment, and
final accounting rechecks them. Requested values are part of the workload/report
and its hash: do not put credentials in diagnostic workload files. Omit the field
when no command-specific environment is needed. This does not change any timeout,
claim body, process argv, retry or measurement boundary.

Only step zero is the first user executable in a fresh sandbox. Later steps may
benefit from earlier reads. Put the executable of interest first, and run separate
fresh claims when comparing independent cold workloads. This tool does not yet
measure first commands after pause/resume.

## Metric boundaries and gates

For heterogeneous RootFS cohorts, use `--cases-file cases.json` instead of
`--template`, `--body-file`, `--workload`, or `--workload-file`. The file contains
`{"version":1,"cases":[...]}`; each case has a unique `name`, a complete
`claim_body` JSON object, and a `workload` using the schema above. There must be
2–64 cases, each with executable steps, and concurrency must cover every case.
The file is bounded to 4 MiB, each claim body to 1 MiB. Cases are assigned by
global sample index modulo case count, across synchronized batches. No additional
claim/command retry, timeout, prewarm, or per-image pool is introduced.

Mixed runs emit report **v8** with a `cases` inventory, expected per-case sample
counts, body/workload hashes, and per-sample `case_name` and matching hashes.
Top-level single-body/workload hashes are empty because they cannot describe a
mixed cohort. Claim JSON whitespace is compacted before hashing and sending;
workload hashes retain the Go JSON encoding rule below. Every command is checked
against its own case, including final accounting and failed/unattempted samples.
Consumers must verify this mapping, counts, and exact expected outputs rather
than substituting a single global workload. Homogeneous runs retain report v7.
Mixed aggregate distributions are not a substitute for per-case analysis.

All JSON durations are integer nanoseconds. All client wall clocks use monotonic
elapsed time. Cleanup and batch settle are outside command measurements.

Samples also carry UTC `claim_started_at` / `claim_completed_at` and
`steps[].started_at` / `steps[].completed_at` for correlation with node diagnostics.
These wall-clock timestamps never determine SLO durations or gates. Account for
clock skew when comparing hosts. Completion marks the measured operation's end,
including errors, not proof of success; claim timestamps are zero if request
construction fails before dispatch. Claim completion excludes subsequent commands.

| Report field | Boundary | Gate |
| --- | --- | --- |
| `command_ready` / `command_ready_duration_ns` | Canonical `sandbox0-command-ready` Server-Timing from ingress to authenticated procd API readiness | Hard limit for every sample, p99/max, and canonical SLO header; p50 target is diagnostic |
| `wall` / `wall_duration_ns` | Client claim request start through the complete claim response body | Existing hard limit for every sample and p99/max |
| `first_command` / `first_command_duration_ns` | Same claim start through the first public CMD response body and validated exit/stdout | Optional workload diagnostic; `--first-command-hard-limit` defaults to 0 (disabled), never part of sandbox startup |
| `workload_wall` / `workload_duration_ns` | Same claim start through completion of all configured command steps | All steps must succeed within the shared command request budget |
| `steps[].duration_ns` | One public command invocation through its response body and validation | Exit 0, stopped, not running, matching stdout |
| `steps[].claim_to_completion_ns` | Original claim start through that step's response/validation | Evidence for the specific ordered workload |
| `cleanup` / `cleanup_duration_ns` | Sandbox DELETE through canonical public absence | All known claimed sandboxes must converge |

For failed attempts, sample/step wall fields record elapsed time to failure, not
successful completion. Successful distributions exclude failed or missing
evidence and expose their counts. Canonical claim distributions still include a
validated claim whose later executable fails. A successful first step followed by
a failed later step remains in `first_command`, but not `workload_wall`; the
overall report fails. Read counts, errors, misses, and step evidence together.

`startup_passed` requires every requested claim to succeed, both startup clocks
to meet the hard limit, and canonical SLO evidence. Missing/unattempted claims
and claim timeouts fail it; successful samples cannot hide failed attempts.
`p50_target_met` reports the engineering target without adding a second startup
contract. `workload_measured` identifies whether commands were requested;
`workload_passed` is false when none were requested, not invented command success.
For measured workloads it checks all command evidence and any explicitly enabled
workload-only latency limit. `passed` combines startup, cleanup, and any selected
workload's functional result. A failed user program can make `passed` false while
`startup_passed` remains true. Use the startup-only workload for the platform gate.

## Timeouts and cleanup

The tool's original request-timeout default remains 15 seconds; its existing
maximum remains one minute. This canary uses an explicit `--request-timeout 10s`.
The claim request is bounded by 10 seconds, and all subsequent command steps
together share one 10-second budget after claim. Node and Codex do not each get
a fresh 10 seconds. End-to-end metrics still start before claim. There is no
default user-executable latency gate; an explicitly configured first-command
limit affects workload acceptance only, not `startup_passed`.

If Node succeeds but Codex exhausts the remaining shared budget, the sample and
overall report fail: `errors` increases and `workload_wall` excludes the failed
workload. Node's verified completion remains in `first_command`; it cannot make
the entire workload pass. Neither request nor HTTP-client timeouts are increased.
Context TTL defaults to 15 seconds and must be a whole number of seconds from 1
to 60; it is a separate cleanup bound and does not extend the 10-second request
or shared command budget.

Claim and command POSTs are never retried, including ambiguous connection loss,
timeouts, and 5xx responses. The production client rejects redirects and ambient
proxies. A command timeout can leave a context running without a known context
ID. The harness deletes the entire owned sandbox, including all its contexts,
and polls for a canonical `not_found` envelope; it does not rely on context-ID
recovery or TTL alone. This cleanup uses a separate bounded context even when
the command/run context was canceled. Every batch waits for cleanup before the
next batch. Cleanup failure independently fails acceptance.

If the claim response itself is lost before a valid sandbox ID is known, the
harness cannot target cleanup. It reports failure and does not replay the claim.
It makes no claim of absence for such an unknown identity.

## Report v7 and consumers

Version **7** separates the sandbox-startup contract from user-executable
diagnostics. v6 required a one-second first-command gate by default; v7 does not.
Do not rewrite old reports or interpret a v6 workload miss as a v7 startup miss.
The canonical timing field names and hard startup boundary are retained.
`wall` now includes reading the full claim body; v5 stopped at response headers.
New fields include the workload and its SHA-256, context TTL, first-command limit,
first-command errors/misses, command distributions, and per-step evidence.

Consumers/verifiers must require v7, match the fixed executable and claim-body
hashes, match workload configuration/hash and timing inputs, require every expected
startup sample, and enforce canonical readiness, complete claim-response, and
cleanup bounds. Recheck exit/stdout evidence for selected diagnostic workloads,
but keep their result separate from `startup_passed`. Do not reinterpret a
startup-only pass or an absent command field as executable success.
`workload_sha256` hashes the compact
Go JSON encoding of the validated workload in the report, not the original file
whitespace. Existing `executable_sha256` and `claim_body_sha256` meanings are
unchanged. The report records trusted ingress headers; it is not itself signed.

At the inspected `sandbox0-infra/main` commit
`239156e9b9e4bd65bbfc92be33c51b06f4207d98`, infra builds and installs this tool
but does not enforce public-command acceptance. Workflow gate alignment remains
pending; no infra gate implementation is included in this change.

## RootFS-size matrix

The one-second startup target applies across supported, already-published RootFS
artifacts, with generic carrier and compute capacity available. OCI import and
new worker provisioning are separate preparation/capacity paths, not silently
excluded failures in a claim run. Retain and report unavailable-capacity attempts.

Vary logical capacity, actual stored data volume, file count, and mapping depth;
a very large empty sparse disk alone does not prove independence from RootFS size.
Record artifact/source identity, storage/encryption mode, cache state, and true
machine/concurrency width for each case. Bind each RootFS only at claim, perform
no tenant-data prewarm, and use startup-only runs before separate executable
diagnostics. Preserve original reports: a small number of fast claims for one
RootFS cannot establish the all-size production gate.

Package verification:

```sh
go test ./tools/runtime-slot-slo -count=1
go test -race ./tools/runtime-slot-slo -count=1
go vet ./tools/runtime-slot-slo
```

## Finite resident-load helper

`resident-load` is an optional guest-side diagnostic, not part of the startup
path and not an occupied-node acceptance runner. Build it separately for the
guest architecture. It must be uploaded only to an already-claimed, owned
resident sandbox; it does not prepare a target sandbox's RootFS or change the
generic carrier pool.

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -buildvcs=false -trimpath -ldflags='-s -w' \
    -o /tmp/resident-load ./tools/runtime-slot-slo/resident-load
```

The program requires explicit `--id`, `--memory-bytes` and `--workers` arguments.
It starts idle. Default `--control=stdin` accepts one JSON control per line with the same
owner ID. The only load sequence is `idle -> loaded -> released`; `stop` exits
from any state. Loading or releasing twice fails and stops the workload.

```json
{"id":"resident-01","command":"load"}
{"id":"resident-01","command":"release"}
{"id":"resident-01","command":"stop"}
```

Non-PTY HTTP CMD creation does **not** open a stdin pipe; the `/input` route's
existence is not proof of one. For this creation path, use `--control=signals`:
after observing `idle`, send `USR1` to the exact context's `/signal` endpoint,
await `loaded`, then send `USR2` and await `released`. `TERM` stops the helper.
Signal handlers are registered before the initial idle event. This opt-in mode
does not read stdin and does not stop on stdin EOF. Default stdin mode keeps its
EOF cleanup. Neither mode changes product CMD or PTY semantics.

Signals carry no owner payload and can coalesce. Bind the sandbox/context to the
expected owner and PID, persist each one-shot send intent, and check guest
acknowledgements; a `signaled: true` HTTP response is not a phase acknowledgement.
Never retry an ambiguous signal send. Duplicate or out-of-order delivered
controls fail closed through the same single-use state machine.

Each allocated 4096-byte page is written before `loaded` is acknowledged. CPU
workers must perform their first work before that acknowledgement. Release joins
all workers before dropping the allocation and requesting memory return. Stdin-mode EOF,
invalid input, cancellation, SIGINT/SIGTERM, or telemetry failure stops the load.
The lifetime defaults to45s and is restricted to1–50s, including initial idle and
allocation time; input never refreshes it. Use a separate bounded async context
TTL and owned-sandbox cleanup. This cooperative process timer is not a hard
scheduler guarantee, and a stopped/expired process is not occupancy success.

Stdout emits version1 NDJSON with owner, PID, sequence, monotonic elapsed time,
fixed lifetime, state, allocated bytes, worker count and per-worker iterations.
Heartbeat period is250ms. A blocked output consumer cannot keep workers alive
after cancellation, but terminal telemetry may be absent. Reject missing or stale
evidence; do not interpret exit0 at lifetime expiry as a passed load window.

Allocated bytes are **not** host residency proof. The runner must independently
bind PostgreSQL lease/claim/sandbox/node-boot identity to a unique cgroup and
sample actual memory, swap, OOM/max events, CPU usage/enforcement and pressure
throughout the target interval. Check the same live guest context and process
before and after each target. Memory must actually fall for a released-memory
control. Resource limits derive from the platform memory/CPU policy, not from
these helper flags; leave room for procd, gVisor and file/cache charges.

An occupied cached-new run is not globally cache-empty, and seven residents plus
one new target is one-wide admission at eight active carriers, not eight
simultaneous cold starts. CPU-busy leases are not proof of memory reclaim.
Local helper tests and evidence-validator fixtures do not establish any regional
claim/command latency or remote cleanup result. See
[the preparation record](experiments/RESIDENT-PREFLIGHT.md).

```sh
go test ./tools/runtime-slot-slo/... -count=1
go test -race ./tools/runtime-slot-slo/resident-load -count=3
go vet ./tools/runtime-slot-slo/resident-load
```
