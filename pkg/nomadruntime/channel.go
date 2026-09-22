// Copyright 2026 Sandbox0 Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nomadruntime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsrebase"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

const (
	defaultRuntimeSlotChannelOperationTimeout = 30 * time.Second
	runtimeSlotChannelOperationTimeoutMargin  = 5 * time.Second
)

type nodeRuntimeChannelExecutor struct {
	clusterID     string
	nodeID        string
	nodeUID       string
	control       *protocol.NodeClient
	cleaner       runtimeSlotCleaner
	plannedRetire runtimeSlotPlannedRetirer
	forker        runtimeSlotRunningForker
	rebaser       runtimeSlotPausedRebaser
	network       runtimeSlotNetworkControl
	networkSource runtimeSlotNetworkPrepareSource
	resources     runtimeResourceCgroup
}

var _ protocol.NodeChannelExecutor = (*nodeRuntimeChannelExecutor)(nil)
var _ protocol.NodeChannelPlannedRetireExecutor = (*nodeRuntimeChannelExecutor)(nil)
var _ protocol.NodeChannelRunningForkExecutor = (*nodeRuntimeChannelExecutor)(nil)
var _ protocol.NodeChannelPausedRebaseExecutor = (*nodeRuntimeChannelExecutor)(nil)
var _ protocol.NodeChannelNetworkExecutor = (*nodeRuntimeChannelExecutor)(nil)

type runtimeSlotNetworkPrepareSource interface {
	runtimeSlotNetworkPrepareRequest(protocol.NodeNetworkPrepareControlRequest) (protocol.RuntimeSlotNetworkPrepareRequest, error)
}

type runtimeSlotNetworkControl interface {
	Register(context.Context, protocol.RuntimeSlotNetworkRegistrationRequest) error
	Prepare(context.Context, protocol.RuntimeSlotNetworkPrepareRequest) (rootfshandoff.NetworkPolicyToken, error)
	Cleanup(context.Context, protocol.NodeCleanupControlRequest) error
	Ping(context.Context) error
}

type runtimeSlotRunningForker interface {
	CaptureRunningRootFSFork(
		context.Context,
		protocol.NodeChannelTarget,
		protocol.NodeRunningForkControlRequest,
	) (rootfshandoff.RunningForkCheckpointResult, error)
}

type runtimeSlotPlannedRetirer interface {
	PlanRuntimeSlotRetire(
		context.Context,
		protocol.NodeChannelTarget,
		protocol.NodePlannedRetireControlRequest,
	) (protocol.NodePlannedRetireControlProof, error)
}

type runtimeSlotPausedRebaser interface {
	ExecutePausedRootFSRebase(
		context.Context,
		protocol.NodeChannelTarget,
		protocol.NodePausedRebaseControlRequest,
	) (rootfsrebase.WorkerResult, error)
	RejectPausedRootFSRebase(
		context.Context,
		protocol.NodeChannelTarget,
		protocol.NodePausedRebaseControlRequest,
	) (rootfsrebase.WorkerRejection, error)
	AcknowledgePausedRootFSRebase(
		context.Context,
		protocol.NodeChannelTarget,
		protocol.NodePausedRebaseControlRequest,
	) error
}

func newNodeRuntimeChannelAgent(
	config Config,
	nomadConfig NomadAllocationConfig,
	cleaner runtimeSlotCleaner,
	network runtimeSlotNetworkControl,
	resources runtimeResourceCgroup,
) (*protocol.NodeChannelAgentSet, error) {
	if !nomadConfig.RuntimeSlotChannelEnabled {
		return nil, nil
	}
	if network == nil {
		return nil, fmt.Errorf("ctld runtime slot network control is required for the node channel: %w", errdefs.ErrFailedPrecondition)
	}
	if resources == nil {
		return nil, fmt.Errorf("ctld runtime resource cgroup is required for the node channel: %w", errdefs.ErrFailedPrecondition)
	}
	rawControlRoot := nomadConfig.RuntimeSlotControlRoot
	controlRoot := filepath.Clean(strings.TrimSpace(rawControlRoot))
	if rawControlRoot != controlRoot || !filepath.IsAbs(controlRoot) || controlRoot == string(filepath.Separator) {
		return nil, fmt.Errorf("runtime slot control root must be a non-root absolute path: %w", errdefs.ErrInvalidArgument)
	}
	if err := os.MkdirAll(controlRoot, 0o750); err != nil {
		return nil, fmt.Errorf("create runtime slot control root: %w", err)
	}
	control, err := protocol.NewNodeClient(protocol.NodeClientConfig{
		AllowedSocketRoot: controlRoot,
		Timeout:           runtimeSlotNodeControlTimeout(nomadConfig),
	})
	if err != nil {
		return nil, fmt.Errorf("create runtime slot local control client: %w", err)
	}
	executor := &nodeRuntimeChannelExecutor{
		clusterID: strings.TrimSpace(nomadConfig.ClusterID),
		nodeID:    strings.TrimSpace(nomadConfig.NodeID),
		nodeUID:   strings.TrimSpace(nomadConfig.RuntimeSlotNodeUID),
		control:   control,
		cleaner:   cleaner,
		network:   network,
		resources: resources,
	}
	if forker, ok := cleaner.(runtimeSlotRunningForker); ok {
		executor.forker = forker
	}
	if plannedRetire, ok := cleaner.(runtimeSlotPlannedRetirer); ok {
		executor.plannedRetire = plannedRetire
	}
	if rebaser, ok := cleaner.(runtimeSlotPausedRebaser); ok {
		executor.rebaser = rebaser
	}
	if network != nil {
		source, ok := cleaner.(runtimeSlotNetworkPrepareSource)
		if !ok {
			return nil, fmt.Errorf("runtime slot cleaner cannot resolve ctld network namespaces: %w", errdefs.ErrFailedPrecondition)
		}
		executor.networkSource = source
	}
	if executor.clusterID == "" || executor.nodeID == "" {
		return nil, fmt.Errorf("runtime slot cluster and Nomad node IDs are required: %w", errdefs.ErrInvalidArgument)
	}
	agentConfig := protocol.NodeChannelAgentConfig{
		MigrationCPUPreflightExecutor: executor,
		BaseURL:                       config.RootFSAuthorityURL, CAFile: config.RootFSAuthorityCAFile,
		ClientCertFile: config.RootFSAuthorityClientCertFile,
		ClientKeyFile:  config.RootFSAuthorityClientKeyFile,
		TokenFile:      config.RootFSAuthorityTokenFile,
		PeerURISAN:     nomadConfig.RuntimeSlotChannelPeerURISAN,
		ClusterID:      executor.clusterID,
		NodeID:         executor.nodeID,
		NodeUID:        executor.nodeUID, NodeBootIDFile: config.RuntimeSlotNodeBootIDFile,
		Executor:         executor,
		Capacity:         runtimeNodeCapacity(nomadConfig),
		OperationTimeout: runtimeSlotNodeChannelOperationTimeout(nomadConfig),
	}
	if _, ok := cleaner.(MigrationSourceGC); ok {
		agentConfig.MigrationSourceGCExecutor = executor
	}
	if _, ok := cleaner.(MigrationSourceFinalizer); ok {
		agentConfig.MigrationFinalizeExecutor = executor
	}
	if _, ok := cleaner.(MigrationCapturePeerPreparer); ok {
		agentConfig.MigrationCapturePeerExecutor = executor
	}
	if _, ok := cleaner.(MigrationImagePrefetcher); ok {
		agentConfig.MigrationImagePrefetchExecutor = executor
	}
	if _, ok := cleaner.(MigrationImagePreparer); ok {
		agentConfig.MigrationImagePrepareExecutor = executor
	}
	if _, ok := cleaner.(protocol.NodeChannelMigrationFailureExecutor); ok {
		agentConfig.MigrationFailureExecutor = executor
	}
	if _, ok := cleaner.(protocol.NodeChannelMigrationFailureCleanupExecutor); ok {
		agentConfig.MigrationFailureCleanupExecutor = executor
	}
	if _, ok := cleaner.(protocol.NodeChannelMigrationFailureFinalizeExecutor); ok {
		agentConfig.MigrationFailureFinalizeExecutor = executor
	}
	if _, ok := cleaner.(protocol.NodeChannelMigrationCaptureFailureExecutor); ok {
		agentConfig.MigrationCaptureFailureExecutor = executor
	}
	if _, ok := cleaner.(MigrationSourceFencer); ok {
		agentConfig.MigrationFenceExecutor = executor
	}
	if _, ok := cleaner.(MigrationPublicationPlanner); ok {
		agentConfig.MigrationPublicationPlanExecutor = executor
	}
	if _, ok := cleaner.(MigrationImagePublisher); ok {
		agentConfig.MigrationPublishExecutor = executor
	}
	if _, ok := cleaner.(MigrationCaptureRecovery); ok {
		agentConfig.MigrationRecoveryExecutor = executor
	}
	if _, ok := cleaner.(protocol.NodeChannelMigrationStagingExecutor); ok {
		agentConfig.MigrationStagingExecutor = executor
	}
	if _, ok := cleaner.(MigrationCustodian); ok {
		agentConfig.MigrationCaptureExecutor = executor
	}
	if executor.plannedRetire != nil {
		agentConfig.PlannedRetireExecutor = executor
	}
	if executor.forker != nil {
		agentConfig.RunningForkExecutor = executor
	}
	if executor.rebaser != nil {
		agentConfig.PausedRebaseExecutor = executor
	}
	if network != nil {
		agentConfig.NetworkExecutor = executor
	}
	agent, err := protocol.NewNodeChannelAgentSet(protocol.NodeChannelAgentSetConfig{Agent: agentConfig})
	if err != nil {
		return nil, fmt.Errorf("create runtime slot node channel agent set: %w", err)
	}
	return agent, nil
}

func runtimeSlotNodeControlTimeout(config NomadAllocationConfig) time.Duration {
	if config.RuntimeSlotNodeControlTimeout == 0 {
		return protocol.DefaultNodeControlTimeout
	}
	return config.RuntimeSlotNodeControlTimeout
}

func runtimeSlotNodeChannelOperationTimeout(config NomadAllocationConfig) time.Duration {
	timeout := runtimeSlotNodeControlTimeout(config) + runtimeSlotChannelOperationTimeoutMargin
	return max(timeout, defaultRuntimeSlotChannelOperationTimeout)
}

func (e *nodeRuntimeChannelExecutor) PrepareNetwork(
	ctx context.Context,
	target protocol.NodeChannelTarget,
	request protocol.NodeNetworkPrepareControlRequest,
) (rootfshandoff.NetworkPolicyToken, error) {
	if err := e.validateTarget(target); err != nil {
		return rootfshandoff.NetworkPolicyToken{}, err
	}
	if e.network == nil || e.networkSource == nil {
		return rootfshandoff.NetworkPolicyToken{}, fmt.Errorf("ctld runtime slot network control is unavailable: %w", errdefs.ErrFailedPrecondition)
	}
	local, err := e.networkSource.runtimeSlotNetworkPrepareRequest(request)
	if err != nil {
		return rootfshandoff.NetworkPolicyToken{}, err
	}
	return e.network.Prepare(ctx, local)
}

func (e *nodeRuntimeChannelExecutor) Claim(
	ctx context.Context,
	target protocol.NodeChannelTarget,
	request protocol.NodeClaimControlRequest,
) (protocol.NodeControlResponse, error) {
	if err := e.validateTarget(target); err != nil {
		return protocol.NodeControlResponse{}, err
	}
	if e.control == nil {
		return protocol.NodeControlResponse{}, fmt.Errorf("runtime slot local control client is unavailable: %w", errdefs.ErrUnavailable)
	}
	if e.resources == nil {
		return protocol.NodeControlResponse{}, fmt.Errorf("runtime resource cgroup is unavailable: %w", errdefs.ErrUnavailable)
	}
	if request.Resources.ClusterID != target.ClusterID || request.Resources.NodeID != target.NodeID ||
		request.Resources.NodeUID != target.NodeUID || request.Resources.NodeBootID != target.NodeBootID ||
		request.Resources.SlotID != target.SlotID {
		return protocol.NodeControlResponse{}, fmt.Errorf("runtime resource lease does not match node target: %w", errdefs.ErrPermissionDenied)
	}
	if err := e.resources.Prepare(ctx, request.Resources); err != nil {
		return protocol.NodeControlResponse{}, fmt.Errorf("prepare runtime resource cgroup: %w", err)
	}
	return e.control.Claim(ctx, target.ControlEndpoint, request)
}

func (e *nodeRuntimeChannelExecutor) CommandReady(
	ctx context.Context,
	target protocol.NodeChannelTarget,
	request protocol.CommandReadyControlRequest,
) (protocol.NodeControlResponse, error) {
	if err := e.validateTarget(target); err != nil {
		return protocol.NodeControlResponse{}, err
	}
	if e.control == nil {
		return protocol.NodeControlResponse{}, fmt.Errorf("runtime slot local control client is unavailable: %w", errdefs.ErrUnavailable)
	}
	return e.control.CommandReady(ctx, target.ControlEndpoint, request)
}

// CaptureMigration returns durable intent or a later outcome. The driver owns
// the bounded checkpoint worker across authenticated channel reconnections.
func (e *nodeRuntimeChannelExecutor) CaptureMigration(ctx context.Context, request protocol.MigrationCaptureRequest) (*protocol.MigrationCapture, error) {
	if err := e.validateTarget(request.Target); err != nil {
		return nil, err
	}
	digest, err := request.Digest()
	if err != nil {
		return nil, fmt.Errorf("migration request: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	custodian, ok := e.cleaner.(MigrationCustodian)
	if !ok {
		return nil, errdefs.ErrUnavailable
	}
	custody, err := custodian.GetMigrationCapture(ctx, request.Target.SlotID)
	if err != nil {
		return nil, err
	}
	if custody != nil {
		if custody.Capture.RequestDigest != digest {
			return nil, errdefs.ErrAlreadyExists
		}
		if custody.Capture.State != protocol.MigrationCaptureIntent {
			// A durable outcome remains observable even if Nomad's plugin died.
			capture := custody.Capture
			return &capture, nil
		}
	}
	if e.control == nil {
		return nil, errdefs.ErrUnavailable
	}
	return e.control.CaptureMigration(ctx, request)
}

func (e *nodeRuntimeChannelExecutor) RecoverMigrationCapture(ctx context.Context, request protocol.MigrationCaptureRequest) (*protocol.MigrationCapture, error) {
	if err := e.validateTarget(request.Target); err != nil {
		return nil, err
	}
	recovery, ok := e.cleaner.(MigrationCaptureRecovery)
	if !ok {
		return nil, errdefs.ErrUnavailable
	}
	return recovery.RecoverMigrationCapture(ctx, request)
}

func (e *nodeRuntimeChannelExecutor) ReserveMigrationStaging(ctx context.Context, request protocol.MigrationStagingRequest) (*protocol.MigrationStagingReserved, error) {
	if err := e.validateTarget(request.Target); err != nil {
		return nil, err
	}
	staging, ok := e.cleaner.(protocol.NodeChannelMigrationStagingExecutor)
	if !ok {
		return nil, errdefs.ErrUnavailable
	}
	return staging.ReserveMigrationStaging(ctx, request)
}

func (e *nodeRuntimeChannelExecutor) ReleaseMigrationStaging(ctx context.Context, request protocol.MigrationStagingRequest) error {
	if err := e.validateTarget(request.Target); err != nil {
		return err
	}
	staging, ok := e.cleaner.(protocol.NodeChannelMigrationStagingExecutor)
	if !ok {
		return errdefs.ErrUnavailable
	}
	return staging.ReleaseMigrationStaging(ctx, request)
}

func (e *nodeRuntimeChannelExecutor) RunningFork(
	ctx context.Context,
	target protocol.NodeChannelTarget,
	request protocol.NodeRunningForkControlRequest,
) (rootfshandoff.RunningForkCheckpointResult, error) {
	if err := e.validateTarget(target); err != nil {
		return rootfshandoff.RunningForkCheckpointResult{}, err
	}
	if e.forker == nil {
		return rootfshandoff.RunningForkCheckpointResult{},
			fmt.Errorf("runtime slot running-fork controller is unavailable: %w", errdefs.ErrUnavailable)
	}
	return e.forker.CaptureRunningRootFSFork(ctx, target, request)
}

func (e *nodeRuntimeChannelExecutor) PlannedRetire(
	ctx context.Context,
	target protocol.NodeChannelTarget,
	request protocol.NodePlannedRetireControlRequest,
) (protocol.NodePlannedRetireControlProof, error) {
	if err := e.validateTarget(target); err != nil {
		return protocol.NodePlannedRetireControlProof{}, err
	}
	if e.plannedRetire == nil {
		return protocol.NodePlannedRetireControlProof{},
			fmt.Errorf("runtime slot planned-retire controller is unavailable: %w", errdefs.ErrUnavailable)
	}
	return e.plannedRetire.PlanRuntimeSlotRetire(ctx, target, request)
}

func (e *nodeRuntimeChannelExecutor) PausedRebase(
	ctx context.Context,
	target protocol.NodeChannelTarget,
	request protocol.NodePausedRebaseControlRequest,
) (rootfsrebase.WorkerResult, error) {
	if err := e.validateTarget(target); err != nil {
		return rootfsrebase.WorkerResult{}, err
	}
	if e.rebaser == nil {
		return rootfsrebase.WorkerResult{},
			fmt.Errorf("runtime slot paused-rebase controller is unavailable: %w", errdefs.ErrUnavailable)
	}
	return e.rebaser.ExecutePausedRootFSRebase(ctx, target, request)
}

func (e *nodeRuntimeChannelExecutor) RejectPausedRebase(
	ctx context.Context,
	target protocol.NodeChannelTarget,
	request protocol.NodePausedRebaseControlRequest,
) (rootfsrebase.WorkerRejection, error) {
	if err := e.validateTarget(target); err != nil {
		return rootfsrebase.WorkerRejection{}, err
	}
	if e.rebaser == nil {
		return rootfsrebase.WorkerRejection{},
			fmt.Errorf("runtime slot paused-rebase controller is unavailable: %w", errdefs.ErrUnavailable)
	}
	return e.rebaser.RejectPausedRootFSRebase(ctx, target, request)
}

func (e *nodeRuntimeChannelExecutor) AcknowledgePausedRebase(
	ctx context.Context,
	target protocol.NodeChannelTarget,
	request protocol.NodePausedRebaseControlRequest,
) error {
	if err := e.validateTarget(target); err != nil {
		return err
	}
	if e.rebaser == nil {
		return fmt.Errorf("runtime slot paused-rebase controller is unavailable: %w", errdefs.ErrUnavailable)
	}
	return e.rebaser.AcknowledgePausedRootFSRebase(ctx, target, request)
}

func (e *nodeRuntimeChannelExecutor) Cleanup(
	ctx context.Context,
	target protocol.NodeChannelTarget,
	request protocol.NodeCleanupControlRequest,
) (protocol.NodeCleanupControlProof, error) {
	if err := e.validateTarget(target); err != nil {
		return protocol.NodeCleanupControlProof{}, err
	}
	if e.cleaner == nil {
		return protocol.NodeCleanupControlProof{}, fmt.Errorf("runtime slot local cleaner is unavailable: %w", errdefs.ErrUnavailable)
	}
	return e.cleaner.CleanupRuntimeSlot(ctx, request)
}

func (e *nodeRuntimeChannelExecutor) validateTarget(target protocol.NodeChannelTarget) error {
	if e == nil || target.ClusterID != e.clusterID || target.NodeID != e.nodeID || target.NodeUID != e.nodeUID {
		return fmt.Errorf("runtime slot node channel target does not match this node: %w", errdefs.ErrPermissionDenied)
	}
	return nil
}

func (e *nodeRuntimeChannelExecutor) PreflightMigrationCPU(ctx context.Context, request protocol.MigrationCPUPreflightRequest) (*protocol.MigrationCPUPreflight, error) {
	if err := e.validateTarget(request.Target); err != nil {
		return nil, err
	}
	if e.control == nil {
		return nil, errdefs.ErrUnavailable
	}
	return e.control.PreflightMigrationCPU(ctx, request)
}

func (e *nodeRuntimeChannelExecutor) PublishMigration(ctx context.Context, request protocol.MigrationPublicationRequest) (*protocol.MigrationPublication, error) {
	if err := e.validateTarget(request.Capture.Request.Target); err != nil {
		return nil, err
	}
	publisher, ok := e.cleaner.(MigrationImagePublisher)
	if !ok {
		return nil, errdefs.ErrUnavailable
	}
	return publisher.PublishMigration(ctx, request)
}

func (e *nodeRuntimeChannelExecutor) FenceMigrationSource(ctx context.Context, request protocol.MigrationSourceFenceRequest) (*protocol.MigrationSourceFenceProof, error) {
	if err := e.validateTarget(request.PublicationRequest.Capture.Request.Target); err != nil {
		return nil, err
	}
	fencer, ok := e.cleaner.(MigrationSourceFencer)
	if !ok {
		return nil, errdefs.ErrUnavailable
	}
	return fencer.FenceMigrationSource(ctx, request)
}

func (e *nodeRuntimeChannelExecutor) PrepareMigrationImage(ctx context.Context, request protocol.MigrationImagePrepareRequest) (*protocol.MigrationImagePrepared, error) {
	if e == nil {
		return nil, errdefs.ErrUnavailable
	}
	if err := e.validateTarget(request.Target); err != nil {
		return nil, err
	}
	preparer, ok := e.cleaner.(MigrationImagePreparer)
	if !ok {
		return nil, errdefs.ErrUnavailable
	}
	return preparer.PrepareMigrationImage(ctx, request)
}

func (e *nodeRuntimeChannelExecutor) FinalizeMigrationSource(ctx context.Context, request protocol.MigrationSourceFinalizeRequest) (*protocol.MigrationSourceFinalizeProof, error) {
	if e == nil {
		return nil, errdefs.ErrUnavailable
	}
	if err := e.validateTarget(request.Fence.PublicationRequest.Capture.Request.Target); err != nil {
		return nil, err
	}
	finalizer, ok := e.cleaner.(MigrationSourceFinalizer)
	if !ok {
		return nil, errdefs.ErrUnavailable
	}
	return finalizer.FinalizeMigrationSource(ctx, request)
}

func (e *nodeRuntimeChannelExecutor) AcknowledgeMigrationSourceGC(ctx context.Context, request protocol.MigrationSourceGCRequest) (*protocol.MigrationSourceGCAcknowledgement, error) {
	if e == nil {
		return nil, errdefs.ErrUnavailable
	}
	if err := e.validateTarget(request.Target); err != nil {
		return nil, err
	}
	gc, ok := e.cleaner.(MigrationSourceGC)
	if !ok {
		return nil, errdefs.ErrUnavailable
	}
	return gc.AcknowledgeMigrationSourceGC(ctx, request)
}

func (e *nodeRuntimeChannelExecutor) StopFailedMigrationDestination(ctx context.Context, request protocol.MigrationFailureRequest) (*protocol.MigrationFailureStopProof, error) {
	if err := e.validateTarget(request.Restore.Image.Target); err != nil {
		return nil, err
	}
	fencer, ok := e.cleaner.(protocol.NodeChannelMigrationFailureExecutor)
	if !ok {
		return nil, errdefs.ErrUnavailable
	}
	return fencer.StopFailedMigrationDestination(ctx, request)
}

func (e *nodeRuntimeChannelExecutor) CleanupFailedMigrationDestination(ctx context.Context, request protocol.MigrationFailureCleanupRequest) (*protocol.MigrationFailureCleanupProof, error) {
	if err := e.validateTarget(request.Failure.Request.Restore.Image.Target); err != nil {
		return nil, err
	}
	cleaner, ok := e.cleaner.(protocol.NodeChannelMigrationFailureCleanupExecutor)
	if !ok {
		return nil, errdefs.ErrUnavailable
	}
	return cleaner.CleanupFailedMigrationDestination(ctx, request)
}

func (e *nodeRuntimeChannelExecutor) FinalizeFailedMigrationDestination(ctx context.Context, request protocol.MigrationFailureFinalizeRequest) (*protocol.MigrationFailureFinalizeProof, error) {
	if err := e.validateTarget(request.Request.Failure.Request.Restore.Image.Target); err != nil {
		return nil, err
	}
	cleaner, ok := e.cleaner.(protocol.NodeChannelMigrationFailureFinalizeExecutor)
	if !ok {
		return nil, errdefs.ErrUnavailable
	}
	return cleaner.FinalizeFailedMigrationDestination(ctx, request)
}

func (e *nodeRuntimeChannelExecutor) CleanupFailedMigrationCapture(ctx context.Context, request protocol.MigrationCaptureFailureRequest) (*protocol.MigrationCaptureFailureProof, error) {
	if err := e.validateTarget(request.Capture.Request.Target); err != nil {
		return nil, err
	}
	cleaner, ok := e.cleaner.(protocol.NodeChannelMigrationCaptureFailureExecutor)
	if !ok {
		return nil, errdefs.ErrUnavailable
	}
	return cleaner.CleanupFailedMigrationCapture(ctx, request)
}

func (e *nodeRuntimeChannelExecutor) FinalizeFailedMigrationCapture(ctx context.Context, request protocol.MigrationCaptureFailureFinalizeRequest) (*protocol.MigrationCaptureFailureFinalizeProof, error) {
	if err := e.validateTarget(request.Request.Capture.Request.Target); err != nil {
		return nil, err
	}
	cleaner, ok := e.cleaner.(protocol.NodeChannelMigrationCaptureFailureExecutor)
	if !ok {
		return nil, errdefs.ErrUnavailable
	}
	return cleaner.FinalizeFailedMigrationCapture(ctx, request)
}

// PlanMigrationPublication has the same exact source authority as publication,
// but its response cannot be used as a durable image receipt.
func (e *nodeRuntimeChannelExecutor) PlanMigrationPublication(ctx context.Context, request protocol.MigrationPublicationRequest) (*protocol.MigrationPublicationPlan, error) {
	if e == nil {
		return nil, errdefs.ErrUnavailable
	}
	if err := e.validateTarget(request.Capture.Request.Target); err != nil {
		return nil, err
	}
	planner, ok := e.cleaner.(MigrationPublicationPlanner)
	if !ok {
		return nil, errdefs.ErrUnavailable
	}
	return planner.PlanMigrationPublication(ctx, request)
}

func (e *nodeRuntimeChannelExecutor) PrefetchMigrationImage(ctx context.Context, request protocol.MigrationImagePrefetchRequest) (*protocol.MigrationImagePrefetched, error) {
	if e == nil {
		return nil, errdefs.ErrUnavailable
	}
	if err := e.validateTarget(request.Staging.Target); err != nil {
		return nil, err
	}
	prefetcher, ok := e.cleaner.(MigrationImagePrefetcher)
	if !ok {
		return nil, errdefs.ErrUnavailable
	}
	return prefetcher.PrefetchMigrationImage(ctx, request)
}

func (e *nodeRuntimeChannelExecutor) PrepareMigrationCapturePeer(ctx context.Context, request protocol.MigrationCapturePeerRequest) (*protocol.MigrationCapturePeerPrepared, error) {
	if e == nil {
		return nil, errdefs.ErrUnavailable
	}
	if err := e.validateTarget(request.Staging.Target); err != nil {
		return nil, err
	}
	prefetcher, ok := e.cleaner.(MigrationCapturePeerPreparer)
	if !ok {
		return nil, errdefs.ErrUnavailable
	}
	return prefetcher.PrepareMigrationCapturePeer(ctx, request)
}
