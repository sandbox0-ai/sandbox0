// Package runtimeslotclaim plans and executes the region-authoritative warm
// Nomad slot claim path without assuming that manager can dial node-local Unix
// sockets directly.
package runtimeslotclaim

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

const (
	defaultClaimTTL            = 15 * time.Second
	defaultSLO                 = time.Second
	maxTrustedIngressClockSkew = 5 * time.Second
	commandProbeRetryInitial   = 5 * time.Millisecond
	commandProbeRetryMaximum   = 25 * time.Millisecond
)

// Store is the PostgreSQL authority needed by one claim. Get by deterministic
// grant ID closes the issue-response-loss window after writer_epoch advances.
type Store interface {
	AcquireRuntimeSlot(context.Context, *sandboxstore.AcquireRuntimeSlotRequest) (*sandboxstore.RuntimeSlot, error)
	GetRootFSFilesystem(context.Context, string) (*sandboxstore.RootFSFilesystem, error)
	GetRootFSGeneration(context.Context, string) (*sandboxstore.RootFSGeneration, error)
	GetRootFSWriterGrant(context.Context, string) (*sandboxstore.RootFSWriterGrant, error)
	IssueRootFSWriterGrant(context.Context, *sandboxstore.IssueRootFSWriterGrantRequest) (*sandboxstore.IssuedRootFSWriterGrant, error)
	BindRuntimeSlotWriterGrant(context.Context, *sandboxstore.BindRuntimeSlotWriterGrantRequest) (*sandboxstore.RuntimeSlot, error)
}

// NodeTarget contains only durable routing identity. NodeExecutor must deliver
// through an authenticated node channel; manager must not dial ControlEndpoint
// as though its unix:// path were region-reachable.
type NodeTarget struct {
	SlotID          string
	ClusterID       string
	AllocationID    string
	NodeID          string
	NodeUID         string
	NodeBootID      string
	ControlEndpoint string
}

// NodeExecutor is the secure manager-to-node delivery boundary.
type NodeExecutor interface {
	Claim(context.Context, NodeTarget, protocol.NodeClaimControlRequest) (protocol.NodeControlResponse, error)
	CommandReady(context.Context, NodeTarget, protocol.CommandReadyControlRequest) (protocol.NodeControlResponse, error)
}

// NetworkPrepareRequest asks the trusted node/network controller to apply an
// exact policy incarnation before the writer grant is issued.
type NetworkPrepareRequest struct {
	OperationID   string
	ClaimID       string
	SlotID        string
	ClusterID     string
	AllocationID  string
	NodeID        string
	NodeUID       string
	NodeBootID    string
	NetNSIdentity string
	NetworkPolicy string
	PolicyDigest  string
}

// NetworkPreparer must be exactly idempotent by OperationID and return the
// applied incarnation rather than a caller-invented token.
type NetworkPreparer interface {
	Prepare(context.Context, NetworkPrepareRequest) (rootfshandoff.NetworkPolicyToken, error)
}

// CommandProber executes the authenticated runtime-gated procd command.
type CommandProber interface {
	ProbeCommandReady(context.Context, string, string) (*procdapi.CommandReadyProbeResult, error)
}

// TokenGenerator creates a manager-to-procd internal token.
type TokenGenerator interface {
	GenerateToken(teamID, userID, sandboxID string) (string, error)
}

// Observation is one end-to-end regional ingress to procd timer sample. It is
// deliberately not assembled from unrelated per-phase histograms.
type Observation struct {
	OperationID string
	SandboxID   string
	SlotID      string
	StartedAt   time.Time
	CompletedAt time.Time
	Duration    time.Duration
	Succeeded   bool
	WithinSLO   bool
	Phases      []PhaseObservation
}

// PhaseObservation is one bounded internal segment of the same end-to-end
// claim sample. Phase names are constants so metrics never acquire operation,
// sandbox, slot, or dependency-specific cardinality.
type PhaseObservation struct {
	Phase     string
	Duration  time.Duration
	Succeeded bool
}

const (
	PhaseRequestValidation  = "request_validation"
	PhaseIngressToPlanner   = "ingress_to_planner"
	PhaseRootFSMetadata     = "rootfs_metadata"
	PhaseSlotAcquire        = "slot_acquire"
	PhaseNetworkPrepare     = "network_prepare"
	PhaseWriterIssueBind    = "writer_issue_bind"
	PhaseNodeClaim          = "node_claim"
	PhaseProcdProbe         = "procd_probe"
	PhaseCommandReadyCommit = "command_ready_commit"
)

// Observer receives one terminal sample for every Planner.Claim call.
type Observer interface {
	ObserveRuntimeSlotClaim(Observation)
}

// Config wires regional authorities to authenticated network, node, and procd
// execution boundaries.
type Config struct {
	Store          Store
	Network        NetworkPreparer
	Node           NodeExecutor
	Prober         CommandProber
	TokenGenerator TokenGenerator
	Observer       Observer
	// WriterTokenKey must remain stable for the lifetime of every retryable
	// operation, including during rolling upgrades.
	WriterTokenKey []byte
	ClaimTTL       time.Duration
	SLO            time.Duration
	Now            func() time.Time
}

// Request contains manager-owned logical inputs. Claim, grant, parent, and
// launch IDs are derived from OperationID and cannot drift on retries.
type Request struct {
	OperationID         string
	SandboxID           string
	TeamID              string
	UserID              string
	CompatibilityDigest string
	ClusterID           string
	NetworkPolicy       string
	Runtime             runtimecontrol.Assignment
	// StartedAt may only be populated from the trusted regional ingress clock.
	// A zero value starts the sample inside Planner.Claim.
	StartedAt time.Time
}

// Result is returned only after the regional slot has accepted command-ready
// proof for the exact procd process.
type Result struct {
	Slot            *sandboxstore.RuntimeSlot
	Grant           *sandboxstore.RootFSWriterGrant
	Stage           rootfshandoff.StageRequest
	ProcdAddress    string
	ProcdInstanceID string
	CommandProof    protocol.CommandReadyProof
	Duration        time.Duration
	WithinSLO       bool
	Phases          []PhaseObservation
}

// Planner executes one region-authoritative Nomad warm-slot claim.
type Planner struct {
	store          Store
	network        NetworkPreparer
	node           NodeExecutor
	prober         CommandProber
	tokenGenerator TokenGenerator
	observer       Observer
	writerTokenKey []byte
	claimTTL       time.Duration
	slo            time.Duration
	now            func() time.Time
}

// New validates immutable claim policy and constructs a Planner.
func New(config Config) (*Planner, error) {
	if config.Store == nil || config.Network == nil || config.Node == nil ||
		config.Prober == nil || config.TokenGenerator == nil {
		return nil, errors.New("runtime slot claim dependencies are required")
	}
	if len(config.WriterTokenKey) < sha256.Size {
		return nil, fmt.Errorf("writer token derivation key must contain at least %d bytes", sha256.Size)
	}
	claimTTL := config.ClaimTTL
	if claimTTL == 0 {
		claimTTL = defaultClaimTTL
	}
	if claimTTL < time.Second || claimTTL > time.Minute {
		return nil, errors.New("claim TTL must be between one second and one minute")
	}
	claimTTL = time.Duration(claimTTL.Milliseconds()) * time.Millisecond
	slo := config.SLO
	if slo == 0 {
		slo = defaultSLO
	}
	if slo <= 0 {
		return nil, errors.New("runtime slot claim SLO must be positive")
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	return &Planner{
		store: config.Store, network: config.Network, node: config.Node,
		prober: config.Prober, tokenGenerator: config.TokenGenerator,
		observer: config.Observer, writerTokenKey: append([]byte(nil), config.WriterTokenKey...),
		claimTTL: claimTTL, slo: slo, now: now,
	}, nil
}

// Claim executes acquire -> network incarnation -> writer issue/bind -> node
// claim -> authenticated procd command -> node command-ready. Every durable
// mutation and node call is exactly retryable from the same OperationID.
func (p *Planner) Claim(ctx context.Context, request Request) (result *Result, resultErr error) {
	wallCallStarted := time.Now()
	callStarted := p.now().UTC()
	startedAt := request.StartedAt.UTC()
	if request.StartedAt.IsZero() {
		startedAt = callStarted
	}
	ingressClockSkewed := startedAt.After(callStarted)
	observedSlotID := ""
	phases := make([]PhaseObservation, 0, 9)
	recordPhase := func(phase string, started time.Time, succeeded bool) {
		duration := time.Since(started)
		if duration < 0 {
			duration = 0
		}
		phases = append(phases, PhaseObservation{Phase: phase, Duration: duration, Succeeded: succeeded})
	}
	defer func() {
		completedAt := p.now().UTC()
		duration := completedAt.Sub(startedAt)
		if ingressClockSkewed {
			// A bounded regional clock lead must not fail an otherwise valid
			// claim, but it also must not make the SLO appear met. Use the
			// manager-local elapsed time as a conservative fallback for the
			// reported duration; the public acceptance gate additionally
			// requires its monotonic request round trip to stay within budget.
			duration = max(duration, completedAt.Sub(callStarted))
		}
		if duration < 0 {
			duration = 0
		}
		withinSLO := resultErr == nil && !ingressClockSkewed && duration <= p.slo
		if result != nil {
			result.Duration = duration
			result.WithinSLO = withinSLO
			result.Phases = append([]PhaseObservation(nil), phases...)
		}
		if p.observer != nil {
			p.observer.ObserveRuntimeSlotClaim(Observation{
				OperationID: request.OperationID, SandboxID: request.SandboxID, SlotID: observedSlotID,
				StartedAt: startedAt, CompletedAt: completedAt, Duration: duration,
				Succeeded: resultErr == nil, WithinSLO: withinSLO,
				Phases: append([]PhaseObservation(nil), phases...),
			})
		}
	}()

	phaseStarted := time.Now()
	normalized, err := p.validateRequest(request, callStarted)
	recordPhase(PhaseRequestValidation, phaseStarted, err == nil)
	if err != nil {
		return nil, err
	}
	ingressToPlanner := callStarted.Sub(startedAt)
	if ingressToPlanner < 0 {
		ingressToPlanner = 0
	}
	phases = append(phases, PhaseObservation{
		Phase: PhaseIngressToPlanner, Duration: ingressToPlanner, Succeeded: !ingressClockSkewed,
	})
	ids := p.identities(normalized.OperationID)
	runtimeRevision := normalized.RuntimeAssignmentRevision
	policyDigest := normalized.NetworkPolicyDigest

	phaseStarted = time.Now()
	filesystem, err := p.store.GetRootFSFilesystem(ctx, normalized.SandboxID)
	if err != nil {
		recordPhase(PhaseRootFSMetadata, phaseStarted, false)
		return nil, fmt.Errorf("load RootFS filesystem: %w", err)
	}
	if filesystem == nil {
		recordPhase(PhaseRootFSMetadata, phaseStarted, false)
		return nil, errors.New("sandbox has no RootFS filesystem")
	}
	if filesystem.TeamID != normalized.TeamID || filesystem.StorageFormat != sandboxstore.RootFSStorageFormatBlockCOWV1 ||
		filesystem.HeadGenerationID == "" {
		recordPhase(PhaseRootFSMetadata, phaseStarted, false)
		return nil, errors.New("sandbox RootFS is not a team-owned block-COW generation")
	}
	generation, err := p.store.GetRootFSGeneration(ctx, filesystem.HeadGenerationID)
	if err != nil {
		recordPhase(PhaseRootFSMetadata, phaseStarted, false)
		return nil, fmt.Errorf("load RootFS generation: %w", err)
	}
	descriptor, err := generationDescriptor(filesystem, generation)
	if err != nil {
		recordPhase(PhaseRootFSMetadata, phaseStarted, false)
		return nil, err
	}

	existingGrant, err := p.store.GetRootFSWriterGrant(ctx, ids.grantID)
	if err != nil && !errors.Is(err, sandboxstore.ErrRootFSWriterGrantNotFound) {
		recordPhase(PhaseRootFSMetadata, phaseStarted, false)
		return nil, fmt.Errorf("load deterministic writer grant: %w", err)
	}
	if errors.Is(err, sandboxstore.ErrRootFSWriterGrantNotFound) {
		existingGrant = nil
	}
	expectedWriterEpoch := filesystem.WriterEpoch
	writerEpoch := filesystem.WriterEpoch + 1
	if existingGrant != nil {
		expectedWriterEpoch = existingGrant.WriterEpoch - 1
		writerEpoch = existingGrant.WriterEpoch
		if err := validateExistingGrantBeforeAcquire(existingGrant, normalized, ids, filesystem, generation); err != nil {
			recordPhase(PhaseRootFSMetadata, phaseStarted, false)
			return nil, err
		}
	} else if generation.WriterEpoch != filesystem.WriterEpoch {
		recordPhase(PhaseRootFSMetadata, phaseStarted, false)
		return nil, errors.New("RootFS generation and writer epoch are inconsistent")
	}
	recordPhase(PhaseRootFSMetadata, phaseStarted, true)

	phaseStarted = time.Now()
	slot, err := p.store.AcquireRuntimeSlot(ctx, &sandboxstore.AcquireRuntimeSlotRequest{
		OperationID: normalized.OperationID, ClaimID: ids.claimID, SandboxID: normalized.SandboxID,
		FilesystemID: filesystem.ID, SourceGenerationID: generation.ID,
		CompatibilityDigest: normalized.CompatibilityDigest, ClusterID: normalized.ClusterID,
		RuntimeAssignmentRevision: runtimeRevision, NetworkPolicyDigest: policyDigest,
		ClaimTTL: p.claimTTL,
	})
	if err != nil {
		recordPhase(PhaseSlotAcquire, phaseStarted, false)
		return nil, fmt.Errorf("acquire runtime slot: %w", err)
	}
	observedSlotID = slot.ID
	if err := validateClaimedSlot(slot, normalized, ids, filesystem, generation, p.claimTTL); err != nil {
		recordPhase(PhaseSlotAcquire, phaseStarted, false)
		return nil, err
	}
	if existingGrant != nil && existingGrant.SlotID != slot.ID {
		recordPhase(PhaseSlotAcquire, phaseStarted, false)
		return nil, errors.New("deterministic writer grant is bound to another runtime slot")
	}
	if existingGrant == nil && slot.WriterGrantID != "" {
		recordPhase(PhaseSlotAcquire, phaseStarted, false)
		return nil, errors.New("runtime slot claim is bound to an unknown writer grant")
	}
	recordPhase(PhaseSlotAcquire, phaseStarted, true)

	target := nodeTarget(slot)
	phaseStarted = time.Now()
	policyToken, err := p.network.Prepare(ctx, NetworkPrepareRequest{
		OperationID: normalized.OperationID, ClaimID: ids.claimID, SlotID: slot.ID,
		ClusterID: slot.ClusterID, AllocationID: slot.AllocationID,
		NodeID: slot.NodeID, NodeUID: slot.NodeUID,
		NodeBootID: slot.NodeBootID, NetNSIdentity: slot.NetNSIdentity,
		NetworkPolicy: normalized.NetworkPolicy, PolicyDigest: policyDigest,
	})
	if err != nil {
		recordPhase(PhaseNetworkPrepare, phaseStarted, false)
		return nil, fmt.Errorf("prepare runtime slot network incarnation: %w", err)
	}
	if err := validatePolicyToken(policyToken, slot, ids.claimID, policyDigest); err != nil {
		recordPhase(PhaseNetworkPrepare, phaseStarted, false)
		return nil, err
	}
	procdAddress, err := protocol.NomadProcdAddress(policyToken.PodIP)
	if err != nil {
		recordPhase(PhaseNetworkPrepare, phaseStarted, false)
		return nil, fmt.Errorf("derive procd address from applied network token: %w", err)
	}
	recordPhase(PhaseNetworkPrepare, phaseStarted, true)

	phaseStarted = time.Now()
	stage := rootfshandoff.StageRequest{
		BindingVersion: rootfshandoff.WriterBindingVersion,
		Parent:         ids.parent, InitialGeneration: generation.ID, Generation: descriptor,
		ExpectedPolicyToken: policyToken,
		Labels:              map[string]string{protocol.RuntimeAssignmentRevisionLabel: runtimeRevision},
		Identity: rootfshandoff.Identity{
			NodeUID: slot.NodeUID, BootID: slot.NodeBootID,
			RuntimeGeneration: strconv.FormatInt(normalized.Runtime.RuntimeGeneration, 10),
			PodUID:            slot.AllocationID, PodSandboxID: policyToken.PodSandboxID,
			ContainerName: protocol.NomadTaskName, Image: generation.SourceOCIDigest,
			Snapshotter: "nomad-driver", RuntimeName: "sandbox0-gvisor",
			SlotNonce: slot.ID, ClaimID: ids.claimID, LaunchAttempt: ids.launchAttempt,
			RootFSID: filesystem.ID, WriterEpoch: writerEpoch, WriterGrantID: ids.grantID,
			WriterGrantTokenDigest: rootfshandoff.WriterGrantTokenDigest(ids.rawToken),
			WriterGrantToken:       ids.rawToken,
		},
	}
	if err := stage.Validate(); err != nil {
		recordPhase(PhaseWriterIssueBind, phaseStarted, false)
		return nil, fmt.Errorf("build RootFS stage: %w", err)
	}
	bindingDigest, err := stage.BindingDigest()
	if err != nil {
		recordPhase(PhaseWriterIssueBind, phaseStarted, false)
		return nil, fmt.Errorf("derive RootFS stage binding: %w", err)
	}
	if existingGrant != nil && !grantMatchesStage(existingGrant, stage, slot, ids, normalized, bindingDigest[:]) {
		recordPhase(PhaseWriterIssueBind, phaseStarted, false)
		return nil, errors.New("deterministic writer grant does not match rebuilt RootFS stage")
	}
	if existingGrant == nil && !slot.ClaimLeaseExpiresAt.After(p.now()) {
		recordPhase(PhaseWriterIssueBind, phaseStarted, false)
		return nil, errors.New("runtime slot claim lease expired before writer issue")
	}

	issued, err := p.store.IssueRootFSWriterGrant(ctx, &sandboxstore.IssueRootFSWriterGrantRequest{
		GrantID: ids.grantID, SandboxID: normalized.SandboxID,
		ExpectedFilesystemID: filesystem.ID, ClaimID: ids.claimID, SlotID: slot.ID,
		OperationID: ids.issueOperationID, RawToken: ids.rawToken,
		BindingVersion: rootfshandoff.WriterBindingVersion, BindingDigest: append([]byte(nil), bindingDigest[:]...),
		NodeUID: slot.NodeUID, NodeBootID: slot.NodeBootID,
		PodNamespace: slot.AllocationNamespace, PodName: protocol.NomadTaskName,
		PodUID: slot.AllocationID, NodeName: slot.NodeID, GateParent: ids.parent,
		RuntimeGeneration:   strconv.FormatInt(normalized.Runtime.RuntimeGeneration, 10),
		InitialGenerationID: generation.ID, ExpectedWriterEpoch: expectedWriterEpoch,
		ConsumeExpiresAt: slot.ClaimLeaseExpiresAt,
	})
	if err != nil {
		recordPhase(PhaseWriterIssueBind, phaseStarted, false)
		return nil, fmt.Errorf("issue RootFS writer grant: %w", err)
	}
	if issued == nil || issued.Grant == nil || issued.RawToken != ids.rawToken ||
		!grantMatchesStage(issued.Grant, stage, slot, ids, normalized, bindingDigest[:]) {
		recordPhase(PhaseWriterIssueBind, phaseStarted, false)
		return nil, errors.New("writer authority returned another grant binding")
	}
	bound, err := p.store.BindRuntimeSlotWriterGrant(ctx, &sandboxstore.BindRuntimeSlotWriterGrantRequest{
		SlotID: slot.ID, OperationID: normalized.OperationID, ClaimID: ids.claimID, GrantID: ids.grantID,
	})
	if err != nil {
		recordPhase(PhaseWriterIssueBind, phaseStarted, false)
		return nil, fmt.Errorf("bind runtime slot writer grant: %w", err)
	}
	if err := validateClaimedSlot(bound, normalized, ids, filesystem, generation, p.claimTTL); err != nil {
		recordPhase(PhaseWriterIssueBind, phaseStarted, false)
		return nil, err
	}
	if bound.ID != slot.ID || bound.WriterGrantID != ids.grantID {
		recordPhase(PhaseWriterIssueBind, phaseStarted, false)
		return nil, errors.New("runtime slot authority returned another grant binding")
	}
	recordPhase(PhaseWriterIssueBind, phaseStarted, true)

	phaseStarted = time.Now()
	runtimeAssignment := cloneAssignment(normalized.Runtime)
	nodeClaim := protocol.NodeClaimControlRequest{
		OperationID: normalized.OperationID, ClaimID: ids.claimID,
		PolicyToken: ids.rawToken, WriterEpoch: strconv.FormatInt(writerEpoch, 10),
		Stage: &stage, NetworkPolicy: normalized.NetworkPolicy, Runtime: &runtimeAssignment,
	}
	if err := nodeClaim.ValidateRegional(); err != nil {
		recordPhase(PhaseNodeClaim, phaseStarted, false)
		return nil, fmt.Errorf("validate node claim: %w", err)
	}
	nodeClaimResponse, err := p.node.Claim(ctx, target, nodeClaim)
	if err != nil {
		recordPhase(PhaseNodeClaim, phaseStarted, false)
		return nil, fmt.Errorf("deliver runtime slot claim: %w", err)
	}
	if err := nodeClaimResponse.Validate(); err != nil {
		recordPhase(PhaseNodeClaim, phaseStarted, false)
		return nil, fmt.Errorf("validate runtime slot claim response: %w", err)
	}
	recordPhase(PhaseNodeClaim, phaseStarted, true)

	phaseStarted = time.Now()
	internalToken, err := p.tokenGenerator.GenerateToken(normalized.TeamID, normalized.UserID, normalized.SandboxID)
	if err != nil {
		recordPhase(PhaseProcdProbe, phaseStarted, false)
		return nil, fmt.Errorf("generate procd internal token: %w", err)
	}
	if internalToken == "" {
		recordPhase(PhaseProcdProbe, phaseStarted, false)
		return nil, errors.New("procd internal token generator returned an empty token")
	}
	probeBudget := p.slo - callStarted.Sub(startedAt)
	if ingressClockSkewed {
		probeBudget = p.slo
	}
	if probeBudget < 0 {
		probeBudget = 0
	}
	probeDeadline := wallCallStarted.Add(probeBudget)
	if slot.ClaimLeaseExpiresAt.Before(probeDeadline) {
		probeDeadline = slot.ClaimLeaseExpiresAt
	}
	probe, err := p.probeCommandReady(ctx, procdAddress, internalToken, probeDeadline)
	if err != nil {
		recordPhase(PhaseProcdProbe, phaseStarted, false)
		return nil, fmt.Errorf("probe procd command readiness: %w", err)
	}
	if probe == nil || probe.Status != "ready" || probe.InstanceID == "" {
		recordPhase(PhaseProcdProbe, phaseStarted, false)
		return nil, errors.New("procd command probe returned an invalid result")
	}
	proof := protocol.CommandReadyProof{
		Version: protocol.CommandReadyProofVersion, SlotID: slot.ID,
		OperationID: normalized.OperationID, ClaimID: ids.claimID,
		LaunchAttempt: ids.launchAttempt, RunscContainerID: protocol.NomadRunscContainerID(slot.ID),
		ProcdInstanceID: probe.InstanceID, ProcdAddress: procdAddress,
		RequestMethod: "PUT", RequestPath: protocol.ProcdCommandReadyProbePath,
		ResponseStatus: 200, ResponseBodyDigest: probe.ResponseBodyDigest,
	}
	if err := proof.Validate(); err != nil {
		recordPhase(PhaseProcdProbe, phaseStarted, false)
		return nil, fmt.Errorf("validate procd command proof: %w", err)
	}
	recordPhase(PhaseProcdProbe, phaseStarted, true)
	phaseStarted = time.Now()
	nodeCommandResponse, err := p.node.CommandReady(ctx, target, protocol.CommandReadyControlRequest{Proof: proof})
	if err != nil {
		recordPhase(PhaseCommandReadyCommit, phaseStarted, false)
		return nil, fmt.Errorf("commit runtime slot command readiness: %w", err)
	}
	if err := nodeCommandResponse.Validate(); err != nil {
		recordPhase(PhaseCommandReadyCommit, phaseStarted, false)
		return nil, fmt.Errorf("validate runtime slot command readiness response: %w", err)
	}
	recordPhase(PhaseCommandReadyCommit, phaseStarted, true)
	return &Result{
		Slot: bound, Grant: issued.Grant, Stage: stage, ProcdAddress: procdAddress,
		ProcdInstanceID: probe.InstanceID, CommandProof: proof,
	}, nil
}

// probeCommandReady closes the runsc-create/procd-listen race without hiding
// an end-to-end SLO miss. The first attempt is immediate; transient failures
// are retried only inside the trusted ingress budget and the durable slot
// claim lease.
func (p *Planner) probeCommandReady(
	ctx context.Context,
	procdAddress, internalToken string,
	deadline time.Time,
) (*procdapi.CommandReadyProbeResult, error) {
	retryDelay := commandProbeRetryInitial
	var lastErr error
	for {
		attemptCtx := ctx
		cancel := func() {}
		if !deadline.IsZero() {
			attemptCtx, cancel = context.WithDeadline(ctx, deadline)
		}
		probe, err := p.prober.ProbeCommandReady(attemptCtx, procdAddress, internalToken)
		cancel()
		if err == nil {
			return probe, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return nil, fmt.Errorf("command-ready probe canceled: %w", ctx.Err())
		}
		remaining := time.Until(deadline)
		if deadline.IsZero() || remaining <= 0 {
			return nil, fmt.Errorf("command-ready probe deadline exceeded: %w", lastErr)
		}
		wait := min(retryDelay, remaining)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("command-ready probe canceled: %w", ctx.Err())
		case <-timer.C:
		}
		retryDelay = min(retryDelay*2, commandProbeRetryMaximum)
	}
}

type normalizedRequest struct {
	Request
	RuntimeAssignmentRevision string
	NetworkPolicyDigest       string
}

func (p *Planner) validateRequest(request Request, now time.Time) (normalizedRequest, error) {
	normalized := normalizedRequest{Request: request}
	for name, value := range map[string]string{
		"operation_id": request.OperationID, "sandbox_id": request.SandboxID,
		"team_id": request.TeamID, "compatibility_digest": request.CompatibilityDigest,
	} {
		if value == "" || strings.TrimSpace(value) != value || len(value) > 512 {
			return normalizedRequest{}, fmt.Errorf("%s is required, canonical, and at most 512 bytes", name)
		}
	}
	if request.ClusterID != strings.TrimSpace(request.ClusterID) || len(request.ClusterID) > 512 {
		return normalizedRequest{}, errors.New("cluster_id must be canonical and at most 512 bytes")
	}
	if len(request.NetworkPolicy) > protocol.MaxNetworkPolicyBytes {
		return normalizedRequest{}, errors.New("network policy exceeds 64 KiB")
	}
	policySpec, err := v1alpha1.ParseNetworkPolicyFromAnnotationStrict(request.NetworkPolicy)
	if err != nil {
		return normalizedRequest{}, fmt.Errorf("network policy: %w", err)
	}
	if policySpec == nil || policySpec.Version != "v1" ||
		policySpec.SandboxID != request.SandboxID || policySpec.TeamID != request.TeamID ||
		(policySpec.Mode != v1alpha1.NetworkModeAllowAll && policySpec.Mode != v1alpha1.NetworkModeBlockAll) {
		return normalizedRequest{}, errors.New("network policy must be v1 and match the requested sandbox, team, and supported mode")
	}
	compatibility, err := digest.Parse(request.CompatibilityDigest)
	if err != nil || compatibility.Algorithm() != digest.SHA256 || compatibility.String() != request.CompatibilityDigest {
		return normalizedRequest{}, errors.New("compatibility_digest must be a canonical sha256 digest")
	}
	if err := request.Runtime.Validate(); err != nil {
		return normalizedRequest{}, fmt.Errorf("runtime assignment: %w", err)
	}
	if request.Runtime.SandboxID != request.SandboxID || request.Runtime.TeamID != request.TeamID ||
		request.Runtime.EnvVars[runtimecontrol.EnvSandboxID] != request.SandboxID {
		return normalizedRequest{}, errors.New("runtime assignment does not match sandbox and team")
	}
	if !request.StartedAt.IsZero() && request.StartedAt.After(now.Add(maxTrustedIngressClockSkew)) {
		return normalizedRequest{}, errors.New("regional ingress start time exceeds the trusted clock-skew bound")
	}
	normalized.Runtime = cloneAssignment(request.Runtime)
	runtimePayload, err := json.Marshal(normalized.Runtime)
	if err != nil {
		return normalizedRequest{}, fmt.Errorf("marshal runtime assignment: %w", err)
	}
	if len(runtimePayload) > protocol.MaxRuntimeAssignmentBytes {
		return normalizedRequest{}, errors.New("runtime assignment exceeds 64 KiB")
	}
	runtimeRevision, err := normalized.Runtime.Revision()
	if err != nil {
		return normalizedRequest{}, fmt.Errorf("derive runtime assignment revision: %w", err)
	}
	normalized.RuntimeAssignmentRevision = runtimeRevision
	normalized.NetworkPolicyDigest = protocol.NetworkPolicyDigest(normalized.NetworkPolicy)
	return normalized, nil
}

type derivedIdentities struct {
	claimID          string
	grantID          string
	issueOperationID string
	launchAttempt    string
	parent           string
	rawToken         string
}

func (p *Planner) identities(operationID string) derivedIdentities {
	derive := func(label string) []byte {
		sum := sha256.Sum256([]byte("sandbox0-runtime-slot-claim-v1\x00" + label + "\x00" + operationID))
		return sum[:]
	}
	mac := hmac.New(sha256.New, p.writerTokenKey)
	_, _ = mac.Write([]byte("sandbox0-runtime-slot-writer-token-v1\x00" + operationID))
	return derivedIdentities{
		claimID:          "claim-" + hex.EncodeToString(derive("claim")),
		grantID:          "grant-" + hex.EncodeToString(derive("grant")),
		issueOperationID: "issue-" + hex.EncodeToString(derive("issue")),
		launchAttempt:    "launch-" + hex.EncodeToString(derive("launch")),
		parent:           digest.FromBytes(derive("parent")).String(),
		rawToken:         "s0wt_" + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)),
	}
}

func generationDescriptor(filesystem *sandboxstore.RootFSFilesystem, generation *sandboxstore.RootFSGeneration) (*rootfshandoff.GenerationDescriptor, error) {
	if filesystem == nil || generation == nil || generation.ID != filesystem.HeadGenerationID ||
		generation.FilesystemID != filesystem.ID || generation.BaseArtifactDigest != filesystem.BaseArtifactDigest ||
		generation.FormatGeneration != filesystem.FormatGeneration {
		return nil, errors.New("RootFS generation does not match filesystem head")
	}
	descriptor := &rootfshandoff.GenerationDescriptor{
		Version:      rootfshandoff.GenerationDescriptorVersion,
		GenerationID: generation.ID, FilesystemID: generation.FilesystemID,
		SourceOCIDigest: generation.SourceOCIDigest, BaseArtifactDigest: generation.BaseArtifactDigest,
		BaseBlockRoot: generation.BaseBlockRoot, CurrentBlockHead: generation.CurrentBlockHead,
		WriterEpoch: generation.WriterEpoch, FormatGeneration: generation.FormatGeneration,
		DurabilityState: generation.DurabilityState, LocatorVersion: generation.LocatorVersion,
		Descriptor: append([]byte(nil), generation.Descriptor...),
	}
	if err := descriptor.Validate(); err != nil {
		return nil, fmt.Errorf("RootFS generation descriptor: %w", err)
	}
	return descriptor, nil
}

func validateExistingGrantBeforeAcquire(grant *sandboxstore.RootFSWriterGrant, request normalizedRequest, ids derivedIdentities, filesystem *sandboxstore.RootFSFilesystem, generation *sandboxstore.RootFSGeneration) error {
	if grant.ID != ids.grantID || grant.SandboxID != request.SandboxID || grant.ClaimID != ids.claimID ||
		grant.IssueOperationID != ids.issueOperationID || grant.FilesystemID != filesystem.ID ||
		grant.InitialGenerationID != generation.ID || grant.WriterEpoch <= generation.WriterEpoch ||
		filesystem.WriterEpoch != grant.WriterEpoch ||
		(grant.State != sandboxstore.RootFSWriterGrantStateIssued && grant.State != sandboxstore.RootFSWriterGrantStateConsumed) {
		return errors.New("deterministic writer grant is not a retryable binding")
	}
	return nil
}

func validateClaimedSlot(slot *sandboxstore.RuntimeSlot, request normalizedRequest, ids derivedIdentities, filesystem *sandboxstore.RootFSFilesystem, generation *sandboxstore.RootFSGeneration, claimTTL time.Duration) error {
	if slot == nil || slot.ID == "" || slot.ClaimOperationID != request.OperationID || slot.ClaimID != ids.claimID ||
		slot.SandboxID != request.SandboxID || slot.FilesystemID != filesystem.ID ||
		slot.SourceGenerationID != generation.ID || slot.CompatibilityDigest != request.CompatibilityDigest ||
		slot.ClaimClusterFilter != request.ClusterID || slot.ClaimTTL != claimTTL ||
		slot.ClaimRuntimeAssignmentRevision != request.RuntimeAssignmentRevision ||
		slot.ClaimNetworkPolicyDigest != request.NetworkPolicyDigest ||
		slot.ClusterID == "" || (request.ClusterID != "" && slot.ClusterID != request.ClusterID) ||
		slot.AllocationID == "" || slot.AllocationNamespace == "" || slot.NodeID == "" ||
		slot.NodeUID == "" || slot.NodeBootID == "" || slot.NetNSIdentity == "" || slot.ControlEndpoint == "" ||
		slot.ClaimLeaseExpiresAt.IsZero() || slot.ClaimedAt.IsZero() || slot.AuthorityObservedAt.IsZero() {
		return errors.New("runtime slot authority returned another claim binding")
	}
	if slot.State != sandboxstore.RuntimeSlotStateClaiming && slot.State != sandboxstore.RuntimeSlotStateStarting &&
		slot.State != sandboxstore.RuntimeSlotStateActive {
		return fmt.Errorf("runtime slot claim is not retryable from state %s", slot.State)
	}
	return nil
}

func validatePolicyToken(token rootfshandoff.NetworkPolicyToken, slot *sandboxstore.RuntimeSlot, claimID, policyDigest string) error {
	if err := token.Validate(); err != nil {
		return fmt.Errorf("applied network policy token: %w", err)
	}
	if token.PodUID != slot.AllocationID || token.ClaimID != claimID ||
		token.NetNSIdentity != slot.NetNSIdentity || token.PolicyDigest != policyDigest ||
		token.PodSandboxID != protocol.RuntimeSlotNetworkIncarnationID(protocol.NodeNetworkPrepareControlRequest{
			SlotID: slot.ID, ClusterID: slot.ClusterID, AllocationID: slot.AllocationID,
			NodeID: slot.NodeID, NodeUID: slot.NodeUID, NodeBootID: slot.NodeBootID,
			NetNSIdentity: slot.NetNSIdentity,
		}) {
		return errors.New("applied network policy token does not match runtime slot claim")
	}
	if _, err := protocol.NomadProcdAddress(token.PodIP); err != nil {
		return fmt.Errorf("applied network policy token: %w", err)
	}
	return nil
}

func grantMatchesStage(grant *sandboxstore.RootFSWriterGrant, stage rootfshandoff.StageRequest, slot *sandboxstore.RuntimeSlot, ids derivedIdentities, request normalizedRequest, binding []byte) bool {
	return grant != nil && grant.ID == ids.grantID && grant.FilesystemID == stage.Identity.RootFSID &&
		slot != nil && grant.SlotID == slot.ID && grant.SandboxID == request.SandboxID && grant.ClaimID == ids.claimID &&
		grant.IssueOperationID == ids.issueOperationID && grant.WriterEpoch == stage.Identity.WriterEpoch &&
		grant.InitialGenerationID == stage.InitialGeneration && grant.BindingVersion == stage.BindingVersion &&
		hmac.Equal(grant.BindingDigest, binding) && grant.NodeUID == stage.Identity.NodeUID &&
		grant.NodeBootID == stage.Identity.BootID && grant.PodNamespace == slot.AllocationNamespace &&
		grant.PodName == protocol.NomadTaskName && grant.PodUID == stage.Identity.PodUID &&
		grant.NodeName == slot.NodeID && grant.GateParent == stage.Parent &&
		grant.RuntimeGeneration == stage.Identity.RuntimeGeneration &&
		grant.ConsumeExpiresAt.Equal(slot.ClaimLeaseExpiresAt)
}

func nodeTarget(slot *sandboxstore.RuntimeSlot) NodeTarget {
	return NodeTarget{
		SlotID: slot.ID, ClusterID: slot.ClusterID, AllocationID: slot.AllocationID,
		NodeID: slot.NodeID, NodeUID: slot.NodeUID, NodeBootID: slot.NodeBootID,
		ControlEndpoint: slot.ControlEndpoint,
	}
}

func cloneAssignment(source runtimecontrol.Assignment) runtimecontrol.Assignment {
	clone := source
	if source.EnvVars != nil {
		clone.EnvVars = make(map[string]string, len(source.EnvVars))
		for key, value := range source.EnvVars {
			clone.EnvVars[key] = value
		}
	}
	if source.Webhook != nil {
		webhook := *source.Webhook
		clone.Webhook = &webhook
	}
	return clone
}
