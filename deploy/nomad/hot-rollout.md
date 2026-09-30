# Runtime releases without interrupting existing sandboxes

Runtime upgrades use coexisting release generations. A committed release selects
which workers may accept **new** assignments. Existing assignments, resource
leases, RootFS writers, guest processes and network flows stay on their original
workers. Release activation never pauses, migrates, stops, or restarts a guest.

## Authorities and sequence

1. Publish an immutable worker bundle and every referenced procd digest. Control
   services, enrollment's worker bundle and the cold-start procd selector are
   independent release pointers. A control-only release preserves the latter two.
2. Stage the worker bundle for enrollment. Pin the complete artifact to each
   enrolled node UID; retries keep that artifact even if enrollment's default
   changes. Record existing fixed workers from verified on-host release receipts.
3. Start replacement capacity with the candidate bundle. Verify authenticated
   capacity, exact runtime compatibility, ready carriers and a real guest command.
   Keep the previous admission policy until candidate readiness is proven.
4. Compare-and-swap the PostgreSQL release policy to the candidate's exact source
   commit **and bundle SHA-256**. Claims hold a shared transaction-level release
   lock; activation holds the exclusive lock. Recheck eligibility after locking,
   so a claim selected before cutover cannot acquire an old worker afterwards.
5. New assignments go to the selected release. Unknown or stale enrollment and
   late heartbeats cannot reopen a predecessor for new claims. Existing claims
   and lifecycle recovery retain their immutable original identities.
6. Retire predecessor elastic workers only after their durable resource leases
   reach zero. Use the existing physical-cleanup and cloud lifecycle protocol;
   exclude these release retirements from execution-state migration. Update an
   idle fixed worker through fenced maintenance with an empty pause allowlist.
   Wait for both Nomad's terminal allocations and the regional terminal slot
   proofs before replacing an empty fixed worker, including warm carriers.
   Rebind its installed artifact only while the exact owned fence remains and
   no active lease or pending lifecycle work exists. Bootstrap retries cannot
   change this binding; fixed replacement uses a separate audited operation.
   Busy predecessors may remain indefinitely; there is no forced release deadline.

The release tables have their own migration ledger within the manager schema,
independent of sandbox-storage migrations. They describe admission and artifact
identity, not sandbox or RootFS authority.

The first implementation retains the qualified runsc/driver compatibility
catalog and the cold-start procd selector. A change to those compatibility
contracts needs separate qualification. Retain existing gateway and HAProxy
processes for a hot worker release. The infrastructure installer refuses a
hot release that would require a gateway executable change; that component
needs its own connection-preserving deployment.

## Failure and rollback

Failed download, missing historical procd, incompatible driver/runsc, failed
command readiness or inadequate candidate capacity prevent activation. Failed
activation preserves the old policy. A lost response is reconciled by operation
identity and revision; an operation ID cannot be reused for another bundle.
Rollback is another compare-and-swap selecting a retained, verified release. It
changes future assignments only and must not stop candidate workloads already
running. Foreign maintenance fences and active migration/checkpoint work remain
under their original owners.

An exact completed create retry acknowledges the original command-ready slot.
It does not reinitialize the writable filesystem or replay the initial startup
probe after its claim deadline. Changed requests and cleanup-owned operations
remain conflicts. A partial retry after writer acquisition keeps the original
slot and proceeds through the existing claim recovery protocol.

## Acceptance requirements

Use an isolated regional control plane, PostgreSQL and encrypted object storage,
plus at least two real Nomad/gVisor workers. Across activation, repeated release
and rollback, verify original guest PID/process token, full memory contents,
open-file offset, mmap/tmpfs, continuously written RootFS and established network
flows. Verify fresh claims select the intended bundle. Exercise concurrent
claim/cutover, stale bootstrap, node reconnect, duplicate operation, mismatched
bundle, missing artifact, unavailable candidate, foreign fences, manager restart,
rollback with occupied candidate workers, and idle retirement. Preserve receipts
and remove all temporary cloud resources after verification.
