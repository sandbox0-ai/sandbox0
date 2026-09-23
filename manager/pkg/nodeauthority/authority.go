// Package nodeauthority assembles the manager's dedicated mTLS node listener.
package nodeauthority

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauth"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/rootfswriterauthority"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotauthority"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotclaim"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotnode"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotnomad"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotreconciler"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotterminal"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsrebase"
	writerprotocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

const (
	defaultWriterLeaseTTL          = 30 * time.Second
	defaultRuntimeSlotHeartbeatTTL = 30 * time.Second
)

// Store is the single PostgreSQL authority shared by writer, runtime-slot
// claim, heartbeat, and terminal transitions.
type Store interface {
	rootfswriterauthority.GrantStore
	rootfswriterauthority.LifecycleStore
	runtimeslotauthority.Store
	runtimeslotclaim.Store
	runtimeslotnode.CapacityStore
	runtimeslotterminal.Store
	nomadmigration.Store
	nomadmigration.DestinationStore
	nomadmigration.HandoverStore
	nomadmigration.CPUPreflightStore
	nomadmigration.SourceRecoveryStore
	nomadmigration.StagingStore
	nomadmigration.PreparationCancellationStore
	nomadmigration.SourceExecutionStore
	nomadmigration.CheckpointPauseStore
	nomadmigration.EvacuationStore
	nomadmigration.FailureStore
	nomadmigration.FailureStopStore
	nomadmigration.FailureCleanupStore
	nomadmigration.FailureFinalizationStore
	nomadmigration.CaptureFailureStore
	GetRootFSCompositeBacklogUsage(context.Context) (sandboxstore.RootFSCompositeBacklogUsage, error)
	RecordRuntimeNodePoolDemand(context.Context, *sandboxstore.RuntimeNodePoolDemandRequest) error
	GetRuntimeNodeCertificateIdentity(context.Context, string) (*sandboxstore.RuntimeNodeCertificateIdentity, error)
	GetRuntimeNodeEndpointIdentity(context.Context, string, string) (*sandboxstore.RuntimeNodeEndpointIdentity, error)
}

// Config defines the dedicated verified-mTLS listener and regional lease
// policy. It is intentionally separate from manager's normal internal HTTP
// listener.
type Config struct {
	Store                   Store
	Address                 string
	CertFile                string
	KeyFile                 string
	ClientCAFile            string
	Identities              []nodeauth.CertificateIdentity
	WriterLeaseTTL          time.Duration
	WriterRenewalGrace      time.Duration
	RuntimeSlotHeartbeatTTL time.Duration
	Terminal                runtimeslotterminal.Config
	RegionID                string
}

// ClaimPlannerConfig provides the non-listener dependencies needed by the
// request path. Node and network delivery are always the component's own hub.
type ClaimPlannerConfig struct {
	CapacityWait       runtimeslotclaim.CapacityWaitConfig
	CapacityWake       func()
	Prober             runtimeslotclaim.CommandProber
	TokenGenerator     runtimeslotclaim.TokenGenerator
	Observer           runtimeslotclaim.Observer
	MigrationObserver  func(runtimeslotclaim.Observation)
	CheckpointObserver func(runtimeslotclaim.Observation)
	DemandPoolID       string
	DemandTTL          time.Duration
	WriterTokenKey     []byte
	ClaimTTL           time.Duration
	SLO                time.Duration
	Now                func() time.Time
}

// Component owns one listener-local node channel registry. Every replica may
// run the terminal loop so the instance holding a node stream can make
// progress; PostgreSQL fences and deterministic operations serialize effects.
type Component struct {
	migrationProgress    nomadmigration.Progress
	store                Store
	hub                  *runtimeslotnode.ChannelHub
	server               *rootfswriterauthority.Server
	terminal             *runtimeslotreconciler.Worker
	allocation           *runtimeslotnomad.Controller
	pressure             *writerPressureCoordinator
	sourceRecovery       *nomadmigration.Coordinator
	staging              *nomadmigration.Coordinator
	stagingRelease       *nomadmigration.Coordinator
	preflights           *nomadmigration.Coordinator
	transfers            *nomadmigration.Coordinator
	migrationMu          sync.RWMutex
	destinations         *nomadmigration.Coordinator
	handovers            *nomadmigration.Coordinator
	cancellations        *nomadmigration.Coordinator
	sourceExecution      *nomadmigration.Coordinator
	checkpointPauses     *nomadmigration.Coordinator
	evacuation           *nomadmigration.Coordinator
	failures             *nomadmigration.Coordinator
	failureStops         *nomadmigration.Coordinator
	failureCleanups      *nomadmigration.Coordinator
	failureFinalizations *nomadmigration.Coordinator
	captureFailures      *nomadmigration.Coordinator
}

type checkpointSourceObserver struct {
	allocation *runtimeslotnomad.Controller
}

func (o checkpointSourceObserver) ObserveCheckpointSource(ctx context.Context, source protocol.NodeChannelTarget, namespace string) (bool, []byte, error) {
	if o.allocation == nil {
		return false, nil, fmt.Errorf("nomad allocation observer is unavailable")
	}
	target := runtimeslotreconciler.AllocationTarget{
		ClusterID: source.ClusterID, AllocationID: source.AllocationID,
		AllocationNamespace: namespace, NodeID: source.NodeID,
	}
	observation, err := o.allocation.Observe(ctx, target)
	if err != nil {
		return false, nil, err
	}
	if observation.Target != target || len(observation.ProofDigest) != sha256.Size {
		return false, nil, fmt.Errorf("nomad allocation absence observation changed source")
	}
	return !observation.PhysicalPresent, observation.ProofDigest, nil
}

func (o checkpointSourceObserver) RetireTerminalCheckpointSource(ctx context.Context, operation string, source protocol.NodeChannelTarget, namespace string) error {
	if o.allocation == nil {
		return fmt.Errorf("nomad allocation observer is unavailable")
	}
	target := runtimeslotreconciler.AllocationTarget{
		ClusterID: source.ClusterID, AllocationID: source.AllocationID,
		AllocationNamespace: namespace, NodeID: source.NodeID,
	}
	return o.allocation.PurgeTerminal(ctx, runtimeslotreconciler.AllocationPurgeRequest{
		OperationID: operation + "-cancel-terminal", Target: target,
	})
}

var _ runtimeslotclaim.NetworkPreparer = (*Component)(nil)

type writerPressureCoordinator struct {
	mu     sync.RWMutex
	pauser rootfswriterauthority.PressurePauser
}

func (c *writerPressureCoordinator) RequestRootFSWriterPressurePause(
	ctx context.Context,
	request *sandboxstore.RootFSWriterPressurePauseRequest,
) (string, error) {
	c.mu.RLock()
	pauser := c.pauser
	c.mu.RUnlock()
	if pauser == nil {
		return "", fmt.Errorf("writer pressure pauser is not configured: %w", errdefs.ErrUnavailable)
	}
	return pauser.RequestRootFSWriterPressurePause(ctx, request)
}

func (c *writerPressureCoordinator) set(pauser rootfswriterauthority.PressurePauser) {
	c.mu.Lock()
	c.pauser = pauser
	c.mu.Unlock()
}

// New constructs all node-facing authorities over one verifier, store, and
// outbound channel hub.
func New(config Config) (*Component, error) {
	if config.Store == nil {
		return nil, fmt.Errorf("node authority store is required")
	}
	if config.WriterLeaseTTL == 0 {
		config.WriterLeaseTTL = defaultWriterLeaseTTL
	}
	if config.WriterRenewalGrace == 0 {
		config.WriterRenewalGrace = min(config.WriterLeaseTTL/2, 5*time.Second)
	}
	if config.RuntimeSlotHeartbeatTTL == 0 {
		config.RuntimeSlotHeartbeatTTL = defaultRuntimeSlotHeartbeatTTL
	}

	verifier, err := nodeauth.NewCertificateVerifierWithLookup(config.Identities, func(ctx context.Context, commonName string) (nodeauth.CertificateIdentity, error) {
		identity, err := config.Store.GetRuntimeNodeCertificateIdentity(ctx, commonName)
		if err != nil {
			return nodeauth.CertificateIdentity{}, err
		}
		return nodeauth.CertificateIdentity{
			CommonName: identity.CommonName, ClusterID: identity.ClusterID,
			NodeID: identity.NodeID, NodeUID: identity.NodeUID, AgentUID: identity.AgentUID,
		}, nil
	})
	if err != nil {
		return nil, fmt.Errorf("create node authority verifier: %w", err)
	}
	pressure := &writerPressureCoordinator{}
	writerHandler, err := rootfswriterauthority.NewHandler(rootfswriterauthority.HandlerConfig{
		Verifier:       verifier,
		Store:          config.Store,
		LeaseTTL:       config.WriterLeaseTTL,
		PressurePauser: pressure,
		RenewalPolicy: sandboxstore.RootFSWriterLeaseRenewalPolicy{
			LeaseTTL: config.WriterLeaseTTL, GracePeriod: config.WriterRenewalGrace,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create RootFS writer authority handler: %w", err)
	}
	lifecycleHandler, err := rootfswriterauthority.NewLifecycleHandler(verifier, config.Store, writerHandler)
	if err != nil {
		return nil, fmt.Errorf("create RootFS lifecycle authority handler: %w", err)
	}
	runtimeSlotHandler, err := runtimeslotauthority.NewHandler(runtimeslotauthority.HandlerConfig{
		Verifier: verifier, Store: config.Store, HeartbeatTTL: config.RuntimeSlotHeartbeatTTL,
	})
	if err != nil {
		return nil, fmt.Errorf("create runtime slot authority handler: %w", err)
	}
	hub, err := runtimeslotnode.NewChannelHub(verifier, config.Store)
	if err != nil {
		return nil, fmt.Errorf("create runtime slot node channel: %w", err)
	}
	config.Terminal.DynamicNodeStore = config.Store
	config.Terminal.DynamicRegionID = config.RegionID
	terminal, allocation, err := runtimeslotterminal.NewWithAllocation(config.Store, hub, config.Terminal)
	if err != nil {
		_ = hub.Close()
		return nil, err
	}

	failures, err := nomadmigration.NewFailure(config.Store)
	if err != nil {
		_ = hub.Close()
		return nil, err
	}
	failureStops, err := nomadmigration.NewFailureStop(config.Store, hub)
	if err != nil {
		_ = hub.Close()
		return nil, err
	}
	failureCleanups, err := nomadmigration.NewFailureCleanup(config.Store, hub)
	if err != nil {
		_ = hub.Close()
		return nil, err
	}
	failureFinalizations, err := nomadmigration.NewFailureFinalization(config.Store, hub)
	if err != nil {
		_ = hub.Close()
		return nil, err
	}
	captureFailures, err := nomadmigration.NewCaptureFailure(config.Store, hub)
	if err != nil {
		_ = hub.Close()
		return nil, err
	}
	evacuation, err := nomadmigration.NewEvacuation(config.Store)
	if err != nil {
		_ = hub.Close()
		return nil, err
	}
	transfers, err := nomadmigration.New(config.Store, hub)
	if err != nil {
		_ = hub.Close()
		return nil, err
	}

	preflights, err := nomadmigration.NewCPUPreflight(config.Store, hub)
	if err != nil {
		_ = hub.Close()
		return nil, err
	}

	sourceRecovery, err := nomadmigration.NewSourceRecovery(config.Store, hub)
	if err != nil {
		_ = hub.Close()
		return nil, err
	}
	staging, err := nomadmigration.NewStaging(config.Store, hub)
	if err != nil {
		_ = hub.Close()
		return nil, err
	}
	stagingRelease, err := nomadmigration.NewStagingRelease(config.Store, hub)
	if err != nil {
		_ = hub.Close()
		return nil, err
	}

	mux := newNodeAuthorityMux(lifecycleHandler, runtimeSlotHandler, hub, backlogHealthHandler(config.Store))
	authorized, err := nodeauth.NewVerifiedCertificateMiddleware(verifier, mux)
	if err != nil {
		_ = hub.Close()
		return nil, fmt.Errorf("create node authority certificate middleware: %w", err)
	}
	server, err := rootfswriterauthority.NewServer(rootfswriterauthority.ServerConfig{
		Address: config.Address, CertFile: config.CertFile, KeyFile: config.KeyFile,
		ClientCAFile: config.ClientCAFile, Handler: authorized,
	})
	if err != nil {
		_ = hub.Close()
		return nil, fmt.Errorf("create node authority server: %w", err)
	}
	component := &Component{
		store: config.Store, hub: hub, server: server,
		terminal: terminal, allocation: allocation, pressure: pressure, transfers: transfers, preflights: preflights, sourceRecovery: sourceRecovery,
		staging: staging, stagingRelease: stagingRelease, evacuation: evacuation, failures: failures, failureStops: failureStops, failureCleanups: failureCleanups, failureFinalizations: failureFinalizations, captureFailures: captureFailures,
	}
	for _, lane := range []*nomadmigration.Coordinator{sourceRecovery, staging, stagingRelease, preflights, transfers,
		evacuation, failures, failureStops, failureCleanups, failureFinalizations, captureFailures} {
		lane.SetProgress(&component.migrationProgress)
	}
	return component, nil
}

func newNodeAuthorityMux(writer, slots, channel, health http.Handler) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/internal/v1/rootfs-writer-grants", http.NotFoundHandler())
	mux.Handle("/internal/v1/rootfs-writer-grants/", writer)
	// The collection action is outside the per-grant slash subtree. Keep it
	// behind the same node certificate middleware and writer authentication.
	mux.Handle(writerprotocol.BatchRenewPath, writer)
	mux.Handle(strings.TrimSuffix(protocol.PathPrefix, "/"), http.NotFoundHandler())
	mux.Handle(protocol.PathPrefix, slots)
	mux.Handle(protocol.NodeChannelPath, channel)
	mux.Handle("/healthz", health)
	return mux
}

// SetWriterPressurePauser installs the fully assembled runtime backend before
// the node listener starts. A coordinator indirection keeps construction free
// of package cycles while requests remain race-safe.
func (c *Component) SetWriterPressurePauser(pauser rootfswriterauthority.PressurePauser) {
	if c == nil || c.pressure == nil {
		return
	}
	c.pressure.set(pauser)
}

// RunServer serves the dedicated mTLS endpoint until cancellation and closes
// every hijacked node channel before returning.
func (c *Component) RunServer(ctx context.Context) error {
	if c == nil || c.server == nil || c.hub == nil {
		return fmt.Errorf("node authority is not initialized")
	}
	defer c.hub.Close()
	return c.server.Start(ctx)
}

// Ready closes after the dedicated TCP listener binds successfully.
func (c *Component) Ready() <-chan struct{} {
	if c == nil || c.server == nil {
		return nil
	}
	return c.server.Ready()
}

// TerminalEnabled reports whether this component has a destructive terminal
// worker configured.
func (c *Component) TerminalEnabled() bool {
	return c != nil && c.terminal != nil
}

// NomadAllocationController returns the endpoint-catalog-pinned controller
// shared by planned lifecycle stop and terminal purge.
func (c *Component) NomadAllocationController() *runtimeslotnomad.Controller {
	if c == nil {
		return nil
	}
	return c.allocation
}

// RunningFork dispatches a live checkpoint over this replica's authenticated
// node channel. PostgreSQL operation authority makes caller retries portable
// across manager replicas.
func (c *Component) RunningFork(
	ctx context.Context,
	target protocol.NodeChannelTarget,
	request protocol.NodeRunningForkControlRequest,
) (rootfshandoff.RunningForkCheckpointResult, error) {
	if c == nil || c.hub == nil {
		return rootfshandoff.RunningForkCheckpointResult{}, fmt.Errorf("node authority is not initialized")
	}
	return c.hub.RunningFork(ctx, target, request)
}

// PlannedRetire durably marks the exact source-node RootFS session before any
// manager-owned quiesce or Nomad stop side effect.
func (c *Component) PlannedRetire(
	ctx context.Context,
	target protocol.NodeChannelTarget,
	request protocol.NodePlannedRetireControlRequest,
) (protocol.NodePlannedRetireControlProof, error) {
	if c == nil || c.hub == nil {
		return protocol.NodePlannedRetireControlProof{}, fmt.Errorf("node authority is not initialized")
	}
	return c.hub.PlannedRetire(ctx, target, request)
}

// Prepare applies one exact active or claiming runtime-slot network policy
// through this replica's authenticated node channel. Durable retry authority
// remains in PostgreSQL; the channel itself is deliberately transient.
func (c *Component) Prepare(
	ctx context.Context,
	request runtimeslotclaim.NetworkPrepareRequest,
) (rootfshandoff.NetworkPolicyToken, error) {
	if c == nil || c.hub == nil {
		return rootfshandoff.NetworkPolicyToken{}, fmt.Errorf("node authority is not initialized")
	}
	return c.hub.Prepare(ctx, request)
}

// SelectPausedRebaseNode chooses a live worker before PostgreSQL binds its
// durable NodeID and NodeUID.
func (c *Component) SelectPausedRebaseNode(
	_ context.Context,
	clusterID, operationID string,
) (protocol.NodeChannelTarget, error) {
	if c == nil || c.hub == nil {
		return protocol.NodeChannelTarget{}, fmt.Errorf("node authority is not initialized")
	}
	return c.hub.SelectPausedRebaseNode(clusterID, operationID)
}

// ResolvePausedRebaseNode resolves a PostgreSQL-bound worker to its current
// authenticated boot without failing over to another durable node.
func (c *Component) ResolvePausedRebaseNode(
	_ context.Context,
	clusterID, nodeID, nodeUID string,
) (protocol.NodeChannelTarget, error) {
	if c == nil || c.hub == nil {
		return protocol.NodeChannelTarget{}, fmt.Errorf("node authority is not initialized")
	}
	return c.hub.ResolvePausedRebaseNode(clusterID, nodeID, nodeUID)
}

// PausedRebase executes one exact offline worker command.
func (c *Component) PausedRebase(
	ctx context.Context,
	target protocol.NodeChannelTarget,
	request protocol.NodePausedRebaseControlRequest,
) (rootfsrebase.WorkerResult, error) {
	if c == nil || c.hub == nil {
		return rootfsrebase.WorkerResult{}, fmt.Errorf("node authority is not initialized")
	}
	return c.hub.PausedRebase(ctx, target, request)
}

// RejectPausedRebase fences an exact worker and returns its durable outcome.
func (c *Component) RejectPausedRebase(
	ctx context.Context,
	target protocol.NodeChannelTarget,
	request protocol.NodePausedRebaseControlRequest,
) (rootfsrebase.WorkerRejection, error) {
	if c == nil || c.hub == nil {
		return rootfsrebase.WorkerRejection{}, fmt.Errorf("node authority is not initialized")
	}
	return c.hub.RejectPausedRebase(ctx, target, request)
}

// AcknowledgePausedRebase releases one exact cached worker result.
func (c *Component) AcknowledgePausedRebase(
	ctx context.Context,
	target protocol.NodeChannelTarget,
	request protocol.NodePausedRebaseControlRequest,
) (rootfsrebase.WorkerAcknowledgement, error) {
	if c == nil || c.hub == nil {
		return rootfsrebase.WorkerAcknowledgement{}, fmt.Errorf("node authority is not initialized")
	}
	return c.hub.AcknowledgePausedRebase(ctx, target, request)
}

// RunTerminal waits for listener readiness, then runs the bounded terminal
// loop. It is safe to invoke on every manager replica.
func (c *Component) RunTerminal(
	ctx context.Context,
	report func(runtimeslotreconciler.WorkerReport),
) error {
	if c == nil || c.server == nil || c.terminal == nil {
		return fmt.Errorf("runtime slot terminal worker is not enabled")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.server.Ready():
	}
	return c.terminal.Run(ctx, report)
}

// NewClaimPlanner binds the request path to the exact same authenticated hub
// used for node registration, network preparation, and terminal cleanup. It
// also installs destination restoration and handover before the manager starts.
func (c *Component) NewClaimPlanner(config ClaimPlannerConfig) (*runtimeslotclaim.Planner, error) {
	if c == nil || c.store == nil || c.hub == nil {
		return nil, fmt.Errorf("node authority is not initialized")
	}
	planner, err := runtimeslotclaim.New(runtimeslotclaim.Config{
		CapacityWait: config.CapacityWait, CapacityWake: config.CapacityWake,
		Store: c.store, Network: c.hub, Node: c.hub,
		Prober: config.Prober, TokenGenerator: config.TokenGenerator,
		Observer: config.Observer, WriterTokenKey: config.WriterTokenKey,
		MigrationObserver: config.MigrationObserver, CheckpointObserver: config.CheckpointObserver,
		DemandRecorder: c.store, DemandPoolID: config.DemandPoolID, DemandTTL: config.DemandTTL,
		ClaimTTL: config.ClaimTTL, SLO: config.SLO, Now: config.Now,
	})
	if err != nil {
		return nil, err
	}
	destination, err := nomadmigration.NewDestination(c.store, planner)
	if err != nil {
		return nil, err
	}
	procd, ok := config.Prober.(nomadmigration.Procd)
	if !ok {
		return nil, fmt.Errorf("migration requires the procd handover client")
	}
	tokens, ok := config.TokenGenerator.(nomadmigration.HandoverTokens)
	if !ok {
		return nil, fmt.Errorf("migration requires scoped procd tokens")
	}
	handover, err := nomadmigration.NewHandover(c.store, procd, tokens, migrationReadyNode{c.hub})
	if err != nil {
		return nil, err
	}
	cancellation, err := nomadmigration.NewPreparationCancellation(c.store, procd, tokens)
	if err != nil {
		return nil, err
	}
	sourceExecution, err := nomadmigration.NewSourceExecution(c.store, procd, tokens, c.hub)
	if err != nil {
		return nil, err
	}
	checkpointProcd, ok := config.Prober.(nomadmigration.CheckpointProcd)
	if !ok {
		return nil, fmt.Errorf("memory pause requires the procd checkpoint client")
	}
	checkpointTokens, ok := config.TokenGenerator.(nomadmigration.CheckpointTokens)
	if !ok {
		return nil, fmt.Errorf("memory pause requires scoped checkpoint tokens")
	}
	checkpointPauses, err := nomadmigration.NewCheckpointPause(c.store, c.hub, checkpointSourceObserver{c.allocation}, checkpointProcd, checkpointTokens)
	if err != nil {
		return nil, err
	}
	c.migrationMu.Lock()
	c.checkpointPauses = checkpointPauses
	c.sourceExecution = sourceExecution
	for _, lane := range []*nomadmigration.Coordinator{checkpointPauses, sourceExecution, handover, cancellation, destination} {
		lane.SetProgress(&c.migrationProgress)
	}
	c.handovers = handover
	c.cancellations = cancellation
	c.destinations = destination
	c.migrationMu.Unlock()
	return planner, nil
}

func backlogHealthHandler(store Store) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		usage, err := store.GetRootFSCompositeBacklogUsage(request.Context())
		if err != nil {
			http.Error(writer, "composite backlog unavailable", http.StatusServiceUnavailable)
			return
		}
		writer.Header().Set("X-Sandbox0-RootFS-Composite-Bytes", strconv.FormatInt(usage.UsedDescriptorBytes, 10))
		writer.Header().Set("X-Sandbox0-RootFS-Composite-Limit", strconv.FormatInt(usage.MaxDescriptorBytes, 10))
		writer.Header().Set("X-Sandbox0-RootFS-Composite-Generations", strconv.FormatInt(usage.GenerationCount, 10))
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ok\n"))
	}
}

// RunMigrationCPUPreflights recovers eligibility checks for reserved operations.
// Probes use only this listener's authenticated channels and never gate procd.
func (c *Component) RunMigrationCPUPreflights(ctx context.Context, report func(nomadmigration.Report)) error {
	if c == nil || c.server == nil || c.preflights == nil {
		return fmt.Errorf("migration CPU preflight worker is unavailable")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.server.Ready():
	}
	return c.preflights.Run(ctx, report)
}

// RunMigrationFailures closes expired or terminating destination authority.
// It neither fabricates node cleanup nor releases either capacity reservation.
func (c *Component) RunMigrationFailures(ctx context.Context, report func(nomadmigration.Report)) error {
	if c == nil || c.server == nil || c.failures == nil {
		return fmt.Errorf("migration failure worker is unavailable")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.server.Ready():
	}
	return c.failures.Run(ctx, report)
}

func (c *Component) RunMigrationFailureStops(ctx context.Context, report func(nomadmigration.Report)) error {
	if c == nil || c.server == nil || c.failureStops == nil {
		return fmt.Errorf("migration failure stop worker is unavailable")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.server.Ready():
	}
	return c.failureStops.Run(ctx, report)
}

func (c *Component) RunMigrationFailureCleanups(ctx context.Context, report func(nomadmigration.Report)) error {
	if c == nil || c.server == nil || c.failureCleanups == nil {
		return fmt.Errorf("migration failure cleanup worker is unavailable")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.server.Ready():
	}
	return c.failureCleanups.Run(ctx, report)
}

func (c *Component) RunMigrationCaptureFailures(ctx context.Context, report func(nomadmigration.Report)) error {
	if c == nil || c.server == nil || c.captureFailures == nil {
		return fmt.Errorf("migration capture failure worker is unavailable")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.server.Ready():
	}
	return c.captureFailures.Run(ctx, report)
}

// RunMigrationSourceRecovery completes existing captured-source custody without
// access to the first-capture dispatch capability.
func (c *Component) RunMigrationSourceRecovery(ctx context.Context, report func(nomadmigration.Report)) error {
	if c == nil || c.server == nil || c.sourceRecovery == nil {
		return fmt.Errorf("migration source recovery worker is unavailable")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.server.Ready():
	}
	return c.sourceRecovery.Run(ctx, report)
}

func (c *Component) RunMigrationEvacuation(ctx context.Context, report func(nomadmigration.Report)) error {
	if c == nil || c.server == nil || c.evacuation == nil {
		return fmt.Errorf("migration evacuation worker is unavailable")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.server.Ready():
	}
	return c.evacuation.Run(ctx, report)
}

func (c *Component) RunMigrationStaging(ctx context.Context, report func(nomadmigration.Report)) error {
	if c == nil || c.server == nil || c.staging == nil {
		return fmt.Errorf("migration staging worker is unavailable")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.server.Ready():
	}
	return c.staging.Run(ctx, report)
}

func (c *Component) RunMigrationStagingRelease(ctx context.Context, report func(nomadmigration.Report)) error {
	if c == nil || c.server == nil || c.stagingRelease == nil {
		return fmt.Errorf("migration staging release worker is unavailable")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.server.Ready():
	}
	return c.stagingRelease.Run(ctx, report)
}

// RunMigrationTransfers resumes authorized image transfers on the listener's
// authenticated channels. It is independent of the terminal worker and never
// initiates a migration or exposes a user-selectable destination.
func (c *Component) RunMigrationTransfers(ctx context.Context, report func(nomadmigration.Report)) error {
	if c == nil || c.server == nil || c.transfers == nil {
		return fmt.Errorf("migration transfer worker is unavailable")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.server.Ready():
	}
	return c.transfers.Run(ctx, report)
}

// RunMigrationDestinations restores physically fenced operations using the
// same configured claim planner and writer-token key as normal regional claims.
func (c *Component) RunMigrationDestinations(ctx context.Context, report func(nomadmigration.Report)) error {
	if c == nil || c.server == nil {
		return fmt.Errorf("node authority is not initialized")
	}
	c.migrationMu.RLock()
	worker := c.destinations
	c.migrationMu.RUnlock()
	if worker == nil {
		return fmt.Errorf("migration destination planner is not configured")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.server.Ready():
	}
	return worker.Run(ctx, report)
}

// RunMigrationHandovers delivers the exact restored-process command before
// probing and publishing readiness through the same authenticated node hub.
func (c *Component) RunMigrationHandovers(ctx context.Context, report func(nomadmigration.Report)) error {
	if c == nil || c.server == nil {
		return fmt.Errorf("node authority is not initialized")
	}
	c.migrationMu.RLock()
	worker := c.handovers
	c.migrationMu.RUnlock()
	if worker == nil {
		return fmt.Errorf("migration handover is not configured")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.server.Ready():
	}
	return worker.Run(ctx, report)
}

func (c *Component) RunMigrationPreparationCancellations(ctx context.Context, report func(nomadmigration.Report)) error {
	if c == nil || c.server == nil {
		return fmt.Errorf("node authority is not initialized")
	}
	c.migrationMu.RLock()
	worker := c.cancellations
	c.migrationMu.RUnlock()
	if worker == nil {
		return fmt.Errorf("migration cancellation is not configured")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.server.Ready():
	}
	return worker.Run(ctx, report)
}

func (c *Component) RunMigrationSourceExecution(ctx context.Context, report func(nomadmigration.Report)) error {
	if c == nil || c.server == nil {
		return fmt.Errorf("node authority is not initialized")
	}
	c.migrationMu.RLock()
	worker := c.sourceExecution
	c.migrationMu.RUnlock()
	if worker == nil {
		return fmt.Errorf("migration source execution is not configured")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.server.Ready():
	}
	return worker.Run(ctx, report)
}

// RunCheckpointPauses advances source-only captures through the shared node channel.
func (c *Component) RunCheckpointPauses(ctx context.Context, report func(nomadmigration.Report)) error {
	if c == nil || c.server == nil {
		return fmt.Errorf("node authority is not initialized")
	}
	c.migrationMu.RLock()
	worker := c.checkpointPauses
	c.migrationMu.RUnlock()
	if worker == nil {
		return fmt.Errorf("memory pause is not configured")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.server.Ready():
	}
	return worker.Run(ctx, report)
}

// Adapt the existing regional planner target at the composition boundary.
type migrationReadyNode struct{ hub *runtimeslotnode.ChannelHub }

// CancelCheckpointImage fences a speculative restore on the authenticated
// destination incarnation before its carrier enters generic reclamation.
func (c *Component) CancelCheckpointImage(ctx context.Context, request protocol.CheckpointImageCancelRequest) (*protocol.CheckpointImageCancelProof, error) {
	return c.hub.CancelCheckpointImage(ctx, request)
}

func (n migrationReadyNode) CommandReady(ctx context.Context, t protocol.NodeChannelTarget, request protocol.CommandReadyControlRequest) (protocol.NodeControlResponse, error) {
	return n.hub.CommandReady(ctx, runtimeslotclaim.NodeTarget{SlotID: t.SlotID, ClusterID: t.ClusterID, AllocationID: t.AllocationID,
		NodeID: t.NodeID, NodeUID: t.NodeUID, NodeBootID: t.NodeBootID, ControlEndpoint: t.ControlEndpoint}, request)
}

func (c *Component) RunMigrationFailureFinalizations(ctx context.Context, report func(nomadmigration.Report)) error {
	if c == nil || c.server == nil || c.failureFinalizations == nil {
		return fmt.Errorf("migration failure finalization worker is unavailable")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.server.Ready():
	}
	return c.failureFinalizations.Run(ctx, report)
}
