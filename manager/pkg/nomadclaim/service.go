// Package nomadclaim implements the runtime-neutral manager claim API over
// region-authoritative Nomad warm slots.
package nomadclaim

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/containerd/errdefs"
	distref "github.com/distribution/reference"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/credentialbinding"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/egressauthstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/networkpolicy"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotclaim"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotreconciler"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/apierror"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/quantity"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsrebase"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	v1alpha1 "github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
	templatepkg "github.com/sandbox0-ai/sandbox0/pkg/template"
	templatestore "github.com/sandbox0-ai/sandbox0/pkg/template/store"
	"go.uber.org/zap"
)

var errNomadSandboxPausePending = errors.New("nomad sandbox planned pause is pending")

// Store is the durable sandbox and block-COW product boundary needed before a
// slot can receive writer authority.
type Store interface {
	GetSandbox(context.Context, string) (*sandboxstore.SandboxRecord, error)
	GetActiveLifecycleTxn(context.Context, string) (*sandboxstore.SandboxLifecycleTxn, error)
	GetLifecycleTxn(context.Context, string) (*sandboxstore.SandboxLifecycleTxn, error)
	GetPendingNomadPausedRebase(context.Context, string) (*sandboxstore.SandboxLifecycleTxn, error)
	RetrySandboxClaim(context.Context, *sandboxstore.RetrySandboxClaimRequest) (*sandboxstore.SandboxRecord, bool, error)
	ReserveSandboxClaim(context.Context, *sandboxstore.ReserveSandboxClaimRequest) (*sandboxstore.SandboxRecord, error)
	CompleteSandboxClaim(context.Context, *sandboxstore.CompleteSandboxClaimRequest) (*sandboxstore.SandboxRecord, error)
	GetNomadSandboxCredentialBindings(context.Context, string, string) (*sandboxstore.NomadSandboxCredentialBindings, error)
	RequestSandboxRuntimeClaimCleanup(context.Context, string, string) (*sandboxstore.SandboxClaimCleanupCandidate, error)
	RequestHardExpiredSandboxRuntimeClaimCleanup(context.Context, string, string) (*sandboxstore.SandboxClaimCleanupCandidate, error)
	RequestNomadSandboxPause(context.Context, string, string) (*sandboxstore.NomadSandboxPauseCandidate, error)
	RequestNomadSandboxTTLPause(context.Context, string) (*sandboxstore.NomadSandboxPauseCandidate, error)
	RetryNomadSandboxResume(context.Context, *sandboxstore.RetryNomadSandboxResumeRequest) (*sandboxstore.NomadSandboxResumeCandidate, bool, error)
	RequestNomadSandboxResume(context.Context, *sandboxstore.RequestNomadSandboxResumeRequest) (*sandboxstore.NomadSandboxResumeCandidate, error)
	AbortNomadSandboxResume(context.Context, string, string, string) (bool, error)
	CompleteNomadSandboxResume(context.Context, *sandboxstore.CompleteNomadSandboxResumeRequest) (*sandboxstore.SandboxRecord, error)
	RequestNomadSandboxRunningFork(context.Context, *sandboxstore.NomadSandboxForkRequest) (*sandboxstore.NomadSandboxRunningForkCandidate, error)
	AbortNomadSandboxRunningFork(context.Context, string, string, string, string) (bool, error)
	ForkNomadPausedSandbox(context.Context, *sandboxstore.NomadSandboxForkRequest) (*sandboxstore.SandboxRecord, error)
	RequestNomadPausedRebase(context.Context, *sandboxstore.NomadPausedRebaseRequest) (*sandboxstore.NomadPausedRebaseCandidate, error)
	PublishPausedRootFSRebase(context.Context, *sandboxstore.PublishPausedRootFSRebaseRequest) (*sandboxstore.RootFSFilesystem, error)
	RejectNomadPausedRebaseWorker(context.Context, *sandboxstore.NomadPausedRebaseRequest, []byte) error
	AcknowledgeNomadPausedRebaseWorker(context.Context, string, string, string, string, string, []byte) error
	BeginRuntimeSlotQuiesce(context.Context, *sandboxstore.BeginRuntimeSlotQuiesceRequest) (*sandboxstore.RuntimeSlot, error)
	GetRuntimeSlotBySandboxID(context.Context, string) (*sandboxstore.RuntimeSlot, error)
	GetReadyRootFSBaseArtifact(context.Context, string, sandboxstore.RootFSArtifactPlatform, sandboxstore.ReadyRootFSArtifactRequirements) (*sandboxstore.RootFSBaseArtifact, error)
	GetReadyRootFSBaseArtifactByDigest(context.Context, string, sandboxstore.RootFSArtifactPlatform, sandboxstore.ReadyRootFSArtifactRequirements) (*sandboxstore.RootFSBaseArtifact, error)
	EnsureInitialRootFSGeneration(context.Context, *sandboxstore.EnsureInitialRootFSGenerationRequest) (*sandboxstore.RootFSFilesystem, *sandboxstore.RootFSGeneration, error)
	GetRootFSGeneration(context.Context, string) (*sandboxstore.RootFSGeneration, error)
	GetRootFSSnapshot(context.Context, string, string) (*sandboxstore.RootFSSnapshot, error)
	RestoreRootFSFromSnapshot(context.Context, *sandboxstore.RestoreRootFSFromSnapshotRequest) (*sandboxstore.RootFSFilesystem, error)
}

// QuotaLimitStore resolves region-authoritative team capacity policy.
type QuotaLimitStore interface {
	GetLimit(context.Context, string, quota.Dimension) (*quota.Limit, error)
}

type planner interface {
	Claim(context.Context, runtimeslotclaim.Request) (*runtimeslotclaim.Result, error)
}

type allocationStopper interface {
	Stop(context.Context, runtimeslotreconciler.AllocationPurgeRequest) error
}

type writerPressurePauseStore interface {
	RequestNomadSandboxPressurePause(
		context.Context,
		*sandboxstore.RootFSWriterPressurePauseRequest,
	) (*sandboxstore.NomadSandboxPauseCandidate, error)
}

type runningForkController interface {
	RunningFork(
		context.Context,
		protocol.NodeChannelTarget,
		protocol.NodeRunningForkControlRequest,
	) (rootfshandoff.RunningForkCheckpointResult, error)
}

type pausedRebaseController interface {
	SelectPausedRebaseNode(context.Context, string, string) (protocol.NodeChannelTarget, error)
	ResolvePausedRebaseNode(context.Context, string, string, string) (protocol.NodeChannelTarget, error)
	PausedRebase(context.Context, protocol.NodeChannelTarget, protocol.NodePausedRebaseControlRequest) (rootfsrebase.WorkerResult, error)
	RejectPausedRebase(context.Context, protocol.NodeChannelTarget, protocol.NodePausedRebaseControlRequest) (rootfsrebase.WorkerRejection, error)
	AcknowledgePausedRebase(context.Context, protocol.NodeChannelTarget, protocol.NodePausedRebaseControlRequest) (rootfsrebase.WorkerAcknowledgement, error)
}

// Config defines logical claim policy independently from the node listener.
type Config struct {
	Store                  Store
	Templates              templatestore.TemplateStore
	RuntimeClasses         *RuntimeClassCatalog
	Planner                planner
	Allocation             allocationStopper
	RunningFork            runningForkController
	PausedRebase           pausedRebaseController
	QuotaLimits            QuotaLimitStore
	NetworkPolicies        *networkpolicy.NetworkPolicyService
	ResourcePolicy         templatepkg.ResourcePolicy
	ClaimTTL               time.Duration
	DefaultTTL             time.Duration
	Now                    func() time.Time
	Logger                 *zap.Logger
	RootFSFormatGeneration int
	RootFSProcdProtocol    string
	RootFSProcdDigest      string
}

// Service claims resource-neutral Nomad slots and binds exact resource leases.
type Service struct {
	store                  Store
	templates              templatestore.TemplateStore
	runtimeClasses         *RuntimeClassCatalog
	planner                planner
	allocation             allocationStopper
	runningFork            runningForkController
	pausedRebase           pausedRebaseController
	quotaLimits            QuotaLimitStore
	networkPolicies        *networkpolicy.NetworkPolicyService
	resourcePolicy         templatepkg.ResourcePolicy
	claimTTL               time.Duration
	defaultTTL             time.Duration
	now                    func() time.Time
	logger                 *zap.Logger
	rootFSFormatGeneration int
	rootFSProcdProtocol    string
	rootFSProcdDigest      string
	pauseEnqueuer          service.SandboxPauseEnqueuer
}

// New validates all claim authorities. There is no partially configured mode.
func New(config Config) (*Service, error) {
	if config.Store == nil || config.Templates == nil || config.RuntimeClasses == nil ||
		config.Planner == nil || config.Allocation == nil || config.RunningFork == nil ||
		config.PausedRebase == nil ||
		config.QuotaLimits == nil || config.NetworkPolicies == nil {
		return nil, fmt.Errorf("nomad claim store, templates, runtime classes, planner, allocation controller, RootFS controllers, quota limits, and network policy service are required")
	}
	if config.DefaultTTL < 0 || config.DefaultTTL/time.Second > math.MaxInt32 {
		return nil, fmt.Errorf("default TTL must fit a non-negative int32 second count")
	}
	if config.ClaimTTL < time.Second || config.ClaimTTL > time.Minute {
		return nil, fmt.Errorf("nomad claim TTL must be between 1s and 1m")
	}
	defaultLogicalSize, err := templatepkg.ResolveRootFSLogicalSize(v1alpha1.SandboxTemplateSpec{})
	if err != nil {
		return nil, err
	}
	if err := (sandboxstore.ReadyRootFSArtifactRequirements{
		FormatGeneration: config.RootFSFormatGeneration,
		LogicalSizeBytes: defaultLogicalSize,
		ProcdProtocol:    config.RootFSProcdProtocol,
		ProcdDigest:      config.RootFSProcdDigest,
	}).Validate(); err != nil {
		return nil, fmt.Errorf("nomad claim RootFS artifact policy: %w", err)
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.Logger == nil {
		config.Logger = zap.NewNop()
	}
	return &Service{
		store: config.Store, templates: config.Templates, runtimeClasses: config.RuntimeClasses,
		planner: config.Planner, allocation: config.Allocation, runningFork: config.RunningFork,
		pausedRebase: config.PausedRebase,
		quotaLimits:  config.QuotaLimits, networkPolicies: config.NetworkPolicies,
		resourcePolicy: config.ResourcePolicy, claimTTL: config.ClaimTTL, defaultTTL: config.DefaultTTL,
		rootFSFormatGeneration: config.RootFSFormatGeneration,
		rootFSProcdProtocol:    config.RootFSProcdProtocol, rootFSProcdDigest: config.RootFSProcdDigest,
		now: config.Now, logger: config.Logger,
	}, nil
}

// TerminateSandbox commits deletion intent before the runtime-slot terminal
// reconciler fences writer authority and purges the exact Nomad allocation.
func (s *Service) TerminateSandbox(ctx context.Context, sandboxID string) error {
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" || len(sandboxID) > 512 {
		return fmt.Errorf("sandbox ID is required and must not exceed 512 bytes")
	}
	if _, err := s.store.RequestSandboxRuntimeClaimCleanup(
		ctx, sandboxID, "sandbox deletion requested",
	); err != nil {
		return fmt.Errorf("request Nomad sandbox cleanup: %w", err)
	}
	s.logger.Info("Nomad sandbox termination requested", zap.String("sandboxID", sandboxID))
	return nil
}

// TerminateHardExpiredSandbox commits deletion only after the store rechecks
// the hard deadline under the sandbox row lock.
func (s *Service) TerminateHardExpiredSandbox(ctx context.Context, sandboxID string) error {
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" || len(sandboxID) > 512 {
		return fmt.Errorf("sandbox ID is required and must not exceed 512 bytes")
	}
	if _, err := s.store.RequestHardExpiredSandboxRuntimeClaimCleanup(
		ctx, sandboxID, "sandbox hard TTL expired",
	); errors.Is(err, sandboxstore.ErrNomadSandboxHardTTLNotExpired) {
		return nil
	} else if err != nil {
		return fmt.Errorf("request hard-expired Nomad sandbox cleanup: %w", err)
	}
	s.logger.Info("Hard-expired Nomad sandbox termination requested", zap.String("sandboxID", sandboxID))
	return nil
}

// PauseSandboxAndWait persists planned retirement before asking Nomad to stop
// the exact allocation. Completion remains asynchronous.
func (s *Service) PauseSandboxAndWait(ctx context.Context, sandboxID string) (*service.PauseSandboxResponse, error) {
	candidate, err := s.requestNomadSandboxPause(ctx, sandboxID, sandboxstore.SandboxLifecycleSourceManual)
	if err != nil {
		return nil, err
	}
	if candidate.AlreadyPaused {
		return &service.PauseSandboxResponse{
			SandboxID: sandboxID, Paused: true, Status: managerapi.SandboxStatusPaused,
		}, nil
	}
	s.enqueueNomadSandboxPause(sandboxID)
	if err := s.CompletePausingSandboxRuntime(ctx, sandboxID); err != nil && !errors.Is(err, errNomadSandboxPausePending) {
		return nil, err
	}
	return &service.PauseSandboxResponse{
		SandboxID: sandboxID, Paused: false, Status: managerapi.SandboxStatusStarting,
	}, nil
}

// ResumeSandboxAndWait claims a fresh compatible slot from the paused RootFS
// head and returns only after its command-ready binding is committed.
func (s *Service) ResumeSandboxAndWait(ctx context.Context, sandboxID string) (*managerapi.ResumeSandboxResponse, error) {
	record, _, err := s.resumeNomadSandbox(ctx, sandboxID)
	if err != nil {
		return nil, err
	}
	return &managerapi.ResumeSandboxResponse{SandboxID: record.ID, Resumed: true}, nil
}

// PauseSandboxByID is the automatic TTL pause boundary.
func (s *Service) PauseSandboxByID(ctx context.Context, sandboxID string) error {
	candidate, err := s.requestNomadSandboxTTLPause(ctx, sandboxID)
	if errors.Is(err, sandboxstore.ErrNomadSandboxTTLNotExpired) {
		return nil
	}
	if err != nil || candidate.AlreadyPaused {
		return err
	}
	s.enqueueNomadSandboxPause(sandboxID)
	if err := s.CompletePausingSandboxRuntime(ctx, sandboxID); errors.Is(err, errNomadSandboxPausePending) {
		return nil
	} else {
		return err
	}
}

// RequestRootFSWriterPressurePause persists and enqueues a planned pause for
// one exact writer without waiting for Nomad Stop. The reporting node must
// first receive the deterministic operation and persist it locally; its
// plugin-independent reconciler then fences the runtime while the regional
// pause controller catches up asynchronously.
func (s *Service) RequestRootFSWriterPressurePause(
	ctx context.Context,
	request *sandboxstore.RootFSWriterPressurePauseRequest,
) (string, error) {
	if request == nil {
		return "", fmt.Errorf("exact writer pressure pause request is required: %w", errdefs.ErrInvalidArgument)
	}
	store, ok := s.store.(writerPressurePauseStore)
	if !ok {
		return "", fmt.Errorf("exact writer pressure pause store is unavailable: %w", errdefs.ErrUnavailable)
	}
	candidate, err := store.RequestNomadSandboxPressurePause(ctx, request)
	if err != nil {
		switch {
		case errors.Is(err, sandboxstore.ErrNomadSandboxPauseConflict),
			errors.Is(err, sandboxstore.ErrNomadSandboxPauseNotReady),
			errors.Is(err, sandboxstore.ErrSandboxClaimReservationConflict),
			errors.Is(err, sandboxstore.ErrRuntimeSlotConflict),
			errors.Is(err, sandboxstore.ErrRuntimeSlotInvalid):
			return "", fmt.Errorf("request exact writer pressure pause: %v: %w", err, errdefs.ErrFailedPrecondition)
		default:
			return "", fmt.Errorf("request exact writer pressure pause: %v: %w", err, errdefs.ErrUnavailable)
		}
	}
	if candidate == nil || candidate.SandboxID != request.SandboxID ||
		candidate.WriterGrantID != request.GrantID || strings.TrimSpace(candidate.OperationID) == "" {
		return "", fmt.Errorf("writer pressure pause returned a mismatched candidate: %w", errdefs.ErrUnavailable)
	}
	s.enqueueNomadSandboxPause(candidate.SandboxID)
	return candidate.OperationID, nil
}

// CompletePausingSandboxRuntime asks Nomad to cooperatively stop the exact
// allocation, then makes the slot terminally reconcilable only after the node
// has atomically published a planned RootFS generation.
func (s *Service) CompletePausingSandboxRuntime(ctx context.Context, sandboxID string) error {
	candidate, err := s.requestNomadSandboxPause(ctx, sandboxID, sandboxstore.SandboxLifecycleSourceManual)
	if err != nil {
		return err
	}
	if candidate.AlreadyPaused {
		if candidate.SlotID == "" {
			return nil
		}
		_, err := s.store.BeginRuntimeSlotQuiesce(ctx, &sandboxstore.BeginRuntimeSlotQuiesceRequest{
			SlotID: candidate.SlotID, OperationID: candidate.ClaimOperationID, ClaimID: candidate.ClaimID,
		})
		if err != nil {
			return fmt.Errorf("quiesce planned-paused Nomad runtime slot: %w", err)
		}
		return nil
	}
	if err := s.allocation.Stop(ctx, runtimeslotreconciler.AllocationPurgeRequest{
		OperationID: candidate.OperationID,
		Target: runtimeslotreconciler.AllocationTarget{
			ClusterID: candidate.ClusterID, AllocationID: candidate.AllocationID,
			AllocationNamespace: candidate.AllocationNamespace, NodeID: candidate.NodeID,
		},
	}); err != nil {
		return fmt.Errorf("stop Nomad allocation for planned pause: %w", err)
	}
	refreshed, err := s.requestNomadSandboxPause(ctx, sandboxID, candidate.Source)
	if err != nil {
		return err
	}
	if refreshed.AlreadyPaused {
		return s.CompletePausingSandboxRuntime(ctx, sandboxID)
	}
	return errNomadSandboxPausePending
}

// ResumePausedSandboxRuntime is the controller boundary for the same durable
// resume transaction used by the public lifecycle API.
func (s *Service) ResumePausedSandboxRuntime(ctx context.Context, sandboxID string) (*managerapi.Sandbox, error) {
	record, result, err := s.resumeNomadSandbox(ctx, sandboxID)
	if err != nil {
		return nil, err
	}
	return s.projectResumedNomadSandbox(ctx, record, result)
}

type nomadResumePlan struct {
	runtimeClass RuntimeClass
	resources    protocol.RuntimeResourceRequest
	request      service.ClaimRequest
	policy       string
	assignment   runtimecontrol.Assignment
}

func (s *Service) resumeNomadSandbox(
	ctx context.Context,
	sandboxID string,
) (*sandboxstore.SandboxRecord, *runtimeslotclaim.Result, error) {
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" || len(sandboxID) > 512 {
		return nil, nil, fmt.Errorf("sandbox ID is required and must not exceed 512 bytes")
	}
	startedAt := s.now().UTC()
	record, err := s.store.GetSandbox(ctx, sandboxID)
	if err != nil {
		return nil, nil, fmt.Errorf("load Nomad sandbox for resume: %w", err)
	}
	if record == nil || record.DesiredState == sandboxstore.SandboxDesiredStateDeleted || !record.DeletedAt.IsZero() {
		return nil, nil, apierror.NewNotFound("sandbox", sandboxID)
	}
	if record.ID != sandboxID {
		return nil, nil, apierror.NewConflict("sandbox", sandboxID,
			fmt.Errorf("sandbox identity changed during resume"))
	}
	plan, err := s.prepareNomadResumePlan(ctx, record)
	if err != nil {
		return nil, nil, err
	}
	candidate, found, err := s.store.RetryNomadSandboxResume(ctx, &sandboxstore.RetryNomadSandboxResumeRequest{
		SandboxID: sandboxID, ExpectedTeamID: record.TeamID,
	})
	if err != nil {
		return nil, nil, mapNomadResumeError("retry Nomad sandbox resume", sandboxID, err)
	}
	if !found {
		limit, limitErr := s.activeSandboxLimit(ctx, record.TeamID)
		if limitErr != nil {
			return nil, nil, limitErr
		}
		candidate, err = s.store.RequestNomadSandboxResume(ctx, &sandboxstore.RequestNomadSandboxResumeRequest{
			SandboxID: sandboxID, ExpectedTeamID: record.TeamID, ActiveSandboxLimit: limit,
		})
		if err != nil {
			return nil, nil, mapNomadResumeError("request Nomad sandbox resume", sandboxID, err)
		}
	}
	if err := validateNomadResumeCandidate(candidate, record, plan); err != nil {
		return nil, nil, apierror.NewConflict("sandbox", sandboxID, err)
	}
	if candidate.AlreadyActive {
		return candidate.Record, nil, nil
	}

	plan.request.RuntimeGeneration = candidate.RuntimeGeneration
	plan.assignment, err = runtimeAssignment(candidate.Record.TemplateSpec, &plan.request)
	if err != nil {
		resumeErr := apierror.NewConflict("sandbox", sandboxID,
			fmt.Errorf("stored runtime assignment changed during resume: %w", err))
		return nil, nil, s.abortFailedNomadResume(ctx, candidate, resumeErr)
	}
	if err := plan.assignment.Validate(); err != nil {
		resumeErr := apierror.NewConflict("sandbox", sandboxID,
			fmt.Errorf("stored runtime assignment changed during resume: %w", err))
		return nil, nil, s.abortFailedNomadResume(ctx, candidate, resumeErr)
	}
	result, err := s.planner.Claim(ctx, runtimeslotclaim.Request{
		OperationID: candidate.OperationID, SandboxID: candidate.SandboxID,
		TeamID: candidate.Record.TeamID, UserID: candidate.Record.UserID,
		CompatibilityDigest: plan.runtimeClass.CompatibilityDigest, ClusterID: plan.runtimeClass.ClusterID,
		Resources:     plan.resources,
		NetworkPolicy: plan.policy, Runtime: plan.assignment, StartedAt: startedAt,
	})
	if err != nil {
		err = s.abortFailedNomadResume(ctx, candidate, err)
		if errors.Is(err, sandboxstore.ErrRuntimeSlotUnavailable) {
			return nil, nil, fmt.Errorf("%w: %v", service.ErrSandboxLifecycleUnavailable, err)
		}
		return nil, nil, mapNomadResumeError("claim Nomad resume runtime slot", sandboxID, err)
	}
	if result == nil || result.Slot == nil || result.Slot.ID == "" ||
		result.Slot.AllocationID == "" || result.Slot.AllocationNamespace == "" {
		resumeErr := fmt.Errorf("%w: Nomad resume planner returned no exact runtime binding",
			service.ErrSandboxLifecycleUnavailable)
		return nil, nil, s.abortFailedNomadResume(ctx, candidate, resumeErr)
	}
	completed, err := s.store.CompleteNomadSandboxResume(ctx, &sandboxstore.CompleteNomadSandboxResumeRequest{
		SandboxID: sandboxID, OperationID: candidate.OperationID, SlotID: result.Slot.ID,
		AllocationID: result.Slot.AllocationID, AllocationNamespace: result.Slot.AllocationNamespace,
		ResourceLeaseID:     result.Slot.ResourceLease.LeaseID,
		ResourceLeaseDigest: append([]byte(nil), result.Slot.ResourceLeaseDigest...),
	})
	if err != nil {
		return nil, nil, mapNomadResumeError("complete Nomad sandbox resume", sandboxID, err)
	}
	if completed == nil || completed.ID != sandboxID ||
		completed.DesiredState != sandboxstore.SandboxDesiredStateActive ||
		completed.RuntimeGeneration != candidate.RuntimeGeneration ||
		completed.RuntimeID != result.Slot.AllocationID ||
		completed.RuntimeNamespace != result.Slot.AllocationNamespace {
		return nil, nil, apierror.NewConflict("sandbox", sandboxID,
			fmt.Errorf("committed Nomad resume binding does not match the command-ready slot"))
	}
	s.logger.Info("Resumed Nomad sandbox",
		zap.String("sandboxID", sandboxID), zap.String("operationID", candidate.OperationID),
		zap.String("slotID", result.Slot.ID), zap.Int64("runtimeGeneration", candidate.RuntimeGeneration),
		zap.Duration("endToEndDuration", result.Duration),
	)
	return completed, result, nil
}

func (s *Service) abortFailedNomadResume(
	ctx context.Context,
	candidate *sandboxstore.NomadSandboxResumeCandidate,
	resumeErr error,
) error {
	if candidate == nil || candidate.AlreadyActive || candidate.SandboxID == "" || candidate.OperationID == "" {
		return resumeErr
	}
	const abortTimeout = 5 * time.Second
	abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), abortTimeout)
	defer cancel()
	reason := "resume runtime did not reach command-ready"
	if resumeErr != nil {
		reason = fmt.Sprintf("%s: %v", reason, resumeErr)
	}
	reason = strings.ToValidUTF8(reason, "\uFFFD")
	for len(reason) > 2_048 {
		_, size := utf8.DecodeLastRuneInString(reason)
		reason = reason[:len(reason)-size]
	}
	_, err := s.store.AbortNomadSandboxResume(
		abortCtx, candidate.SandboxID, candidate.OperationID, reason,
	)
	if err == nil {
		return resumeErr
	}
	abortErr := fmt.Errorf("abort failed Nomad resume %s: %w", candidate.OperationID, err)
	if resumeErr == nil {
		return abortErr
	}
	return errors.Join(resumeErr, abortErr)
}

func (s *Service) prepareNomadResumePlan(ctx context.Context, record *sandboxstore.SandboxRecord) (nomadResumePlan, error) {
	if record == nil {
		return nomadResumePlan{}, fmt.Errorf("%w: stored Nomad sandbox is missing", service.ErrSandboxLifecycleUnavailable)
	}
	if record.TeamID == "" || record.ID == "" || record.ClusterID == "" || record.RuntimeGeneration < 0 ||
		record.RuntimeGeneration == math.MaxInt64 {
		return nomadResumePlan{}, fmt.Errorf("%w: stored Nomad sandbox identity is invalid",
			service.ErrSandboxLifecycleUnavailable)
	}
	config := service.CloneSandboxConfig(&record.Config)
	if err := service.NormalizeSandboxConfigForPersistence(config); err != nil {
		return nomadResumePlan{}, fmt.Errorf("%w: stored Nomad config is invalid: %v",
			service.ErrSandboxLifecycleUnavailable, err)
	}
	resources, err := s.effectiveResources(record.TemplateSpec, config)
	if err != nil {
		return nomadResumePlan{}, fmt.Errorf("%w: stored Nomad resources are invalid: %v",
			service.ErrSandboxLifecycleUnavailable, err)
	}
	securityClass, ok := v1alpha1.EffectiveSandboxSecurityClass(record.TemplateSpec.MainContainer.SecurityClass)
	if !ok {
		return nomadResumePlan{}, fmt.Errorf("%w: stored Nomad security class is invalid",
			service.ErrSandboxLifecycleUnavailable)
	}
	runtimeClass, err := s.runtimeClasses.Resolve(record.ClusterID, string(securityClass))
	if err != nil {
		return nomadResumePlan{}, fmt.Errorf("%w: no compatible Nomad warm-slot class for the stored sandbox",
			service.ErrSandboxLifecycleUnavailable)
	}
	resourceRequest, err := runtimeResourceRequest(resources)
	if err != nil {
		return nomadResumePlan{}, fmt.Errorf("%w: stored Nomad resources are invalid: %v",
			service.ErrSandboxLifecycleUnavailable, err)
	}
	req := service.ClaimRequest{
		SandboxID: record.ID, TeamID: record.TeamID, UserID: record.UserID,
		Template: record.TemplateID, Config: config, RuntimeGeneration: record.RuntimeGeneration + 1,
	}
	persistedBindings, err := s.store.GetNomadSandboxCredentialBindings(ctx, record.TeamID, record.ID)
	if err != nil {
		return nomadResumePlan{}, fmt.Errorf("%w: load stored Nomad credential bindings: %v",
			service.ErrSandboxLifecycleUnavailable, err)
	}
	if persistedBindings == nil {
		return nomadResumePlan{}, fmt.Errorf("%w: stored Nomad credential binding authority is missing",
			service.ErrSandboxLifecycleUnavailable)
	}
	if len(persistedBindings.Bindings) > 0 {
		if req.Config == nil {
			req.Config = &sandboxstore.SandboxConfig{}
		}
		if req.Config.Network == nil {
			req.Config.Network = &v1alpha1.SandboxNetworkPolicy{}
		}
		req.Config.Network.CredentialBindings = credentialbinding.FromStore(persistedBindings.Bindings)
	}
	policy, credentials, err := s.networkPolicy(record.TemplateSpec, &req)
	if err != nil {
		return nomadResumePlan{}, fmt.Errorf("%w: rebuild stored Nomad network policy: %v",
			service.ErrSandboxLifecycleUnavailable, err)
	}
	if credentialbinding.DigestPublic(credentials) != persistedBindings.Digest {
		return nomadResumePlan{}, fmt.Errorf("%w: stored Nomad credential binding semantics changed",
			service.ErrSandboxLifecycleUnavailable)
	}
	assignment, err := runtimeAssignment(record.TemplateSpec, &req)
	if err != nil {
		return nomadResumePlan{}, fmt.Errorf("%w: stored runtime assignment is invalid: %v",
			service.ErrSandboxLifecycleUnavailable, err)
	}
	if err := assignment.Validate(); err != nil {
		return nomadResumePlan{}, fmt.Errorf("%w: stored runtime assignment is invalid: %v",
			service.ErrSandboxLifecycleUnavailable, err)
	}
	return nomadResumePlan{runtimeClass: runtimeClass, resources: resourceRequest, request: req, policy: policy, assignment: assignment}, nil
}

func validateNomadResumeCandidate(
	candidate *sandboxstore.NomadSandboxResumeCandidate,
	expected *sandboxstore.SandboxRecord,
	plan nomadResumePlan,
) error {
	if candidate == nil || candidate.Record == nil || expected == nil {
		return fmt.Errorf("nomad resume authority returned no sandbox candidate")
	}
	record := candidate.Record
	if candidate.SandboxID != expected.ID || record.ID != expected.ID || record.TeamID != expected.TeamID ||
		record.UserID != expected.UserID || record.ClusterID != plan.runtimeClass.ClusterID ||
		!reflect.DeepEqual(record.TemplateSpec, expected.TemplateSpec) ||
		!nomadRuntimeConfigEqual(record.Config, expected.Config) {
		return fmt.Errorf("nomad resume sandbox identity changed before lifecycle reservation")
	}
	if candidate.AlreadyActive {
		if record.DesiredState != sandboxstore.SandboxDesiredStateActive || record.RuntimeID == "" ||
			record.RuntimeNamespace == "" || candidate.RuntimeGeneration != record.RuntimeGeneration {
			return fmt.Errorf("already-active Nomad resume candidate has no canonical runtime")
		}
		return nil
	}
	if strings.TrimSpace(candidate.OperationID) != candidate.OperationID || candidate.OperationID == "" ||
		len(candidate.OperationID) > 512 || candidate.FilesystemID == "" || candidate.SourceGenerationID == "" ||
		!activeNomadResumePhase(candidate.LifecyclePhase) ||
		record.DesiredState != sandboxstore.SandboxDesiredStatePaused || record.RuntimeID != "" ||
		record.RuntimeNamespace != "" || candidate.RuntimeGeneration != record.RuntimeGeneration+1 ||
		candidate.RuntimeGeneration != plan.request.RuntimeGeneration {
		return fmt.Errorf("nomad resume candidate does not bind the exact paused generation")
	}
	return nil
}

func activeNomadResumePhase(phase string) bool {
	switch phase {
	case sandboxstore.SandboxLifecyclePhasePreparing,
		sandboxstore.SandboxLifecyclePhaseBarriered,
		sandboxstore.SandboxLifecyclePhasePublishing,
		sandboxstore.SandboxLifecyclePhaseCommitting:
		return true
	default:
		return false
	}
}

func nomadRuntimeConfigEqual(actual, expected sandboxstore.SandboxConfig) bool {
	actual.TTL, expected.TTL = nil, nil
	actual.HardTTL, expected.HardTTL = nil, nil
	actual.AutoResume, expected.AutoResume = nil, nil
	return reflect.DeepEqual(actual, expected)
}

func (s *Service) activeSandboxLimit(ctx context.Context, teamID string) (*int64, error) {
	limit, err := s.quotaLimits.GetLimit(ctx, teamID, quota.DimensionActiveSandboxes)
	if err != nil {
		return nil, fmt.Errorf("load active sandbox quota for resume: %w", err)
	}
	if limit == nil {
		return nil, nil
	}
	if limit.TeamID != teamID || limit.Dimension != quota.DimensionActiveSandboxes {
		return nil, fmt.Errorf("active sandbox quota identity does not match resume")
	}
	value := limit.LimitValue
	return &value, nil
}

func mapNomadResumeError(operation, sandboxID string, err error) error {
	switch {
	case errors.Is(err, sandboxstore.ErrSandboxRecordNotFound):
		return apierror.NewNotFound("sandbox", sandboxID)
	case errors.Is(err, sandboxstore.ErrActiveSandboxQuotaExceeded):
		return fmt.Errorf("%w: %v", service.ErrQuotaExceeded, err)
	case errors.Is(err, sandboxstore.ErrNomadSandboxResumeConflict),
		errors.Is(err, sandboxstore.ErrNomadSandboxResumeNotReady),
		errors.Is(err, sandboxstore.ErrSandboxClaimReservationConflict),
		errors.Is(err, sandboxstore.ErrRuntimeSlotConflict),
		errors.Is(err, sandboxstore.ErrRuntimeSlotInvalid):
		return apierror.NewConflict("sandbox", sandboxID, err)
	default:
		return fmt.Errorf("%s: %w", operation, err)
	}
}

func (s *Service) projectResumedNomadSandbox(
	ctx context.Context,
	record *sandboxstore.SandboxRecord,
	result *runtimeslotclaim.Result,
) (*managerapi.Sandbox, error) {
	if record == nil {
		return nil, fmt.Errorf("%w: resumed Nomad sandbox record is missing", service.ErrSandboxLifecycleUnavailable)
	}
	procdAddress := ""
	if result != nil && result.Slot != nil {
		if result.Slot.AllocationID != record.RuntimeID ||
			result.Slot.AllocationNamespace != record.RuntimeNamespace {
			return nil, apierror.NewConflict("sandbox", record.ID,
				fmt.Errorf("resumed runtime projection changed after commit"))
		}
		procdAddress = result.ProcdAddress
	} else {
		slot, err := s.store.GetRuntimeSlotBySandboxID(ctx, record.ID)
		if err != nil {
			if errors.Is(err, sandboxstore.ErrRuntimeSlotNotFound) {
				return nil, fmt.Errorf("%w: active Nomad runtime slot disappeared", service.ErrSandboxLifecycleUnavailable)
			}
			return nil, fmt.Errorf("project active Nomad runtime slot: %w", err)
		}
		if slot == nil || slot.SandboxID != record.ID || slot.AllocationID != record.RuntimeID ||
			slot.AllocationNamespace != record.RuntimeNamespace || slot.State != sandboxstore.RuntimeSlotStateActive ||
			slot.ProcdInstanceID == "" || len(slot.CommandReadyDigest) != sha256.Size ||
			slot.CommandReadyAt.IsZero() || !slot.HeartbeatExpiresAt.After(slot.AuthorityObservedAt) {
			return nil, fmt.Errorf("%w: active Nomad runtime slot is not command-ready",
				service.ErrSandboxLifecycleUnavailable)
		}
		procdAddress = slot.ProcdAddress
	}
	if err := protocol.ValidateNomadProcdAddress(procdAddress); err != nil {
		return nil, fmt.Errorf("%w: active Nomad procd address: %v", service.ErrSandboxLifecycleUnavailable, err)
	}
	autoResume := true
	if record.Config.AutoResume != nil {
		autoResume = *record.Config.AutoResume
	}
	var resources *managerapi.SandboxResourceConfig
	if record.Config.Resources != nil {
		copy := *record.Config.Resources
		resources = &copy
	}
	services := append([]managerapi.SandboxAppService(nil), record.Config.Services...)
	return &managerapi.Sandbox{
		ID: record.ID, TemplateID: record.TemplateID, TeamID: record.TeamID, UserID: record.UserID,
		InternalAddr: procdAddress, Status: managerapi.SandboxStatusRunning, Paused: false,
		AutoResume: autoResume, Resources: resources, Services: services,
		RuntimeID: record.RuntimeID, RuntimeGeneration: record.RuntimeGeneration,
		ExpiresAt: optionalNomadTime(record.ExpiresAt), HardExpiresAt: optionalNomadTime(record.HardExpiresAt),
		ClaimedAt: record.ClaimedAt, CreatedAt: record.CreatedAt, UpdatedAt: record.UpdatedAt,
	}, nil
}

func optionalNomadTime(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	copy := value
	return &copy
}

// SetPauseEnqueuer installs the runtime-neutral durable pause worker.
func (s *Service) SetPauseEnqueuer(enqueuer service.SandboxPauseEnqueuer) {
	s.pauseEnqueuer = enqueuer
}

func (s *Service) enqueueNomadSandboxPause(sandboxID string) {
	if s != nil && s.pauseEnqueuer != nil {
		s.pauseEnqueuer.EnqueueSandboxPause(sandboxID)
	}
}

func (s *Service) requestNomadSandboxPause(
	ctx context.Context,
	sandboxID string,
	source string,
) (*sandboxstore.NomadSandboxPauseCandidate, error) {
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" || len(sandboxID) > 512 {
		return nil, fmt.Errorf("sandbox ID is required and must not exceed 512 bytes")
	}
	candidate, err := s.store.RequestNomadSandboxPause(ctx, sandboxID, source)
	if err == nil {
		return candidate, nil
	}
	return nil, mapNomadSandboxPauseError(sandboxID, err)
}

func (s *Service) requestNomadSandboxTTLPause(
	ctx context.Context,
	sandboxID string,
) (*sandboxstore.NomadSandboxPauseCandidate, error) {
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" || len(sandboxID) > 512 {
		return nil, fmt.Errorf("sandbox ID is required and must not exceed 512 bytes")
	}
	candidate, err := s.store.RequestNomadSandboxTTLPause(ctx, sandboxID)
	if err == nil {
		return candidate, nil
	}
	return nil, mapNomadSandboxPauseError(sandboxID, err)
}

func mapNomadSandboxPauseError(sandboxID string, err error) error {
	switch {
	case errors.Is(err, sandboxstore.ErrSandboxRecordNotFound):
		return apierror.NewNotFound("sandbox", sandboxID)
	case errors.Is(err, sandboxstore.ErrNomadSandboxPauseConflict),
		errors.Is(err, sandboxstore.ErrNomadSandboxPauseNotReady),
		errors.Is(err, sandboxstore.ErrSandboxClaimReservationConflict),
		errors.Is(err, sandboxstore.ErrRuntimeSlotConflict),
		errors.Is(err, sandboxstore.ErrRuntimeSlotInvalid):
		return apierror.NewConflict("sandbox", sandboxID, err)
	default:
		return fmt.Errorf("request Nomad sandbox pause: %w", err)
	}
}

// ClaimSandbox prepares a durable block-COW filesystem and returns only after
// authenticated procd command readiness has been committed regionally.
func (s *Service) ClaimSandbox(ctx context.Context, request *service.ClaimRequest) (*service.ClaimResponse, error) {
	if request == nil {
		return nil, fmt.Errorf("%w: claim request is required", service.ErrInvalidClaimRequest)
	}
	req := *request
	req.TeamID = strings.TrimSpace(request.TeamID)
	req.UserID = strings.TrimSpace(request.UserID)
	req.OperationID = strings.TrimSpace(request.OperationID)
	canonicalTemplate, err := naming.CanonicalTemplateID(request.Template)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", service.ErrInvalidClaimRequest, err)
	}
	req.Template = canonicalTemplate
	if req.TeamID == "" || req.OperationID == "" {
		return nil, fmt.Errorf("%w: signed team and operation identities are required", service.ErrInvalidClaimRequest)
	}
	req.Config = service.CloneSandboxConfig(request.Config)
	if request.Config != nil && request.Config.Network != nil {
		if req.Config == nil {
			req.Config = &sandboxstore.SandboxConfig{}
		}
		req.Config.Network = request.Config.Network.DeepCopy()
	}
	if err := service.NormalizeSandboxConfigForPersistence(req.Config); err != nil {
		return nil, err
	}
	tpl, err := s.templates.GetTemplateForTeam(ctx, req.TeamID, req.Template)
	if err != nil {
		return nil, fmt.Errorf("load template: %w", err)
	}
	if tpl == nil {
		return nil, fmt.Errorf("%w: %s", service.ErrTemplateNotFound, req.Template)
	}
	if !tpl.ReadyForClaim() {
		return nil, templatepkg.ErrTemplateNotReady
	}
	quota, err := s.effectiveResources(tpl.Spec, req.Config)
	if err != nil {
		return nil, err
	}
	securityClass, ok := v1alpha1.EffectiveSandboxSecurityClass(tpl.Spec.MainContainer.SecurityClass)
	if !ok {
		return nil, fmt.Errorf("%w: template security class is invalid", service.ErrInvalidClaimRequest)
	}
	runtimeClass, err := s.runtimeClasses.Resolve("", string(securityClass))
	if err != nil {
		return nil, fmt.Errorf("%w: resolve Nomad runtime class: %v", service.ErrDataPlaneNotReady, err)
	}
	resourceRequest, err := runtimeResourceRequest(quota)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", service.ErrInvalidClaimRequest, err)
	}
	sandboxID, err := naming.SandboxNameForOperation(runtimeClass.ClusterID, req.Template, req.OperationID)
	if err != nil {
		return nil, fmt.Errorf("derive sandbox ID: %w", err)
	}
	if req.SandboxID != "" && req.SandboxID != sandboxID {
		return nil, fmt.Errorf("%w: internal sandbox ID does not match operation", service.ErrClaimConflict)
	}
	req.SandboxID = sandboxID
	if req.RuntimeGeneration <= 0 {
		req.RuntimeGeneration = 1
	}
	if req.RuntimeGeneration != 1 {
		return nil, fmt.Errorf("%w: initial Nomad claim runtime generation must be 1", service.ErrInvalidClaimRequest)
	}

	policy, credentials, err := s.networkPolicy(tpl.Spec, &req)
	if err != nil {
		return nil, err
	}
	storeBindings := credentialbinding.ToStore(credentials)
	sanitizeNomadCredentialBindings(req.Config)
	assignment, err := runtimeAssignment(tpl.Spec, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: runtime assignment: %v", service.ErrInvalidClaimRequest, err)
	}
	if err := assignment.Validate(); err != nil {
		return nil, fmt.Errorf("%w: runtime assignment: %v", service.ErrInvalidClaimRequest, err)
	}
	rootFS, err := s.prepareRootFS(ctx, tpl, &req, runtimeClass.ArtifactPlatform)
	if err != nil {
		if errors.Is(err, sandboxstore.ErrRootFSBaseArtifactNotFound) {
			return nil, fmt.Errorf("%w: %v", service.ErrDataPlaneNotReady, err)
		}
		return nil, err
	}

	now := s.now().UTC()
	record := s.claimRecord(tpl, &req, runtimeClass, resourceRequest, now)
	if err := s.ensureClaimRecord(ctx, record, req.OperationID, storeBindings); err != nil {
		return nil, err
	}
	if err := s.initializeRootFS(ctx, &req, rootFS); err != nil {
		return nil, err
	}

	result, err := s.planner.Claim(ctx, runtimeslotclaim.Request{
		OperationID: req.OperationID, SandboxID: sandboxID,
		TeamID: req.TeamID, UserID: req.UserID,
		CompatibilityDigest: runtimeClass.CompatibilityDigest, ClusterID: runtimeClass.ClusterID,
		Resources:     resourceRequest,
		NetworkPolicy: policy, Runtime: assignment, StartedAt: req.StartedAt,
	})
	if errors.Is(err, sandboxstore.ErrRuntimeSlotUnavailable) {
		return nil, fmt.Errorf("%w: %v", service.ErrDataPlaneNotReady, err)
	}
	if err != nil {
		return nil, err
	}
	if result == nil || result.Slot == nil {
		return nil, fmt.Errorf("nomad slot planner returned no runtime binding")
	}
	_, err = s.store.CompleteSandboxClaim(ctx, &sandboxstore.CompleteSandboxClaimRequest{
		SandboxID: sandboxID, OperationID: req.OperationID, SlotID: result.Slot.ID,
		AllocationID: result.Slot.AllocationID, AllocationNamespace: result.Slot.AllocationNamespace,
		ResourceLeaseID:     result.Slot.ResourceLease.LeaseID,
		ResourceLeaseDigest: append([]byte(nil), result.Slot.ResourceLeaseDigest...),
	})
	if errors.Is(err, sandboxstore.ErrSandboxClaimCleanupPending) ||
		errors.Is(err, sandboxstore.ErrSandboxClaimReservationConflict) {
		return nil, fmt.Errorf("%w: %v", service.ErrClaimConflict, err)
	}
	if err != nil {
		return nil, fmt.Errorf("complete Nomad sandbox claim: %w", err)
	}
	s.logger.Info("Claimed Nomad sandbox",
		zap.String("sandboxID", sandboxID), zap.String("operationID", req.OperationID),
		zap.String("slotID", result.Slot.ID), zap.Duration("endToEndDuration", result.Duration),
	)
	clusterID := runtimeClass.ClusterID
	return &service.ClaimResponse{
		SandboxID: sandboxID, Status: "running", ProcdAddress: result.ProcdAddress,
		RuntimeID: result.Slot.AllocationID, Template: req.Template, ClusterId: &clusterID,
		CommandReadyDuration: result.Duration, CommandReadyWithinSLO: result.WithinSLO,
	}, nil
}

func (s *Service) effectiveResources(spec v1alpha1.SandboxTemplateSpec, config *sandboxstore.SandboxConfig) (v1alpha1.ResourceQuota, error) {
	return effectiveResources(s.resourcePolicy, spec, config)
}

func effectiveResources(
	resourcePolicy templatepkg.ResourcePolicy,
	spec v1alpha1.SandboxTemplateSpec,
	config *sandboxstore.SandboxConfig,
) (v1alpha1.ResourceQuota, error) {
	var memoryOverride *string
	if config != nil && config.Resources != nil {
		memoryOverride = &config.Resources.Memory
	}
	resolved, err := resourcePolicy.ResolveClaimResources(spec, memoryOverride)
	if err != nil {
		return v1alpha1.ResourceQuota{}, fmt.Errorf("%w: %v", service.ErrInvalidClaimRequest, err)
	}
	return resolved.Quota, nil
}

func runtimeResourceRequest(quota v1alpha1.ResourceQuota) (protocol.RuntimeResourceRequest, error) {
	cpu, err := quantity.Parse(quota.CPU)
	if err != nil {
		return protocol.RuntimeResourceRequest{}, fmt.Errorf("CPU is invalid: %w", err)
	}
	millicpu := cpu.MilliValue()
	if millicpu <= 0 || quantity.NewMilli(millicpu).Cmp(cpu) != 0 {
		return protocol.RuntimeResourceRequest{}, fmt.Errorf("CPU must be a positive exact millicore quantity")
	}
	memory, err := quantity.Parse(quota.Memory)
	if err != nil {
		return protocol.RuntimeResourceRequest{}, fmt.Errorf("memory is invalid: %w", err)
	}
	memoryBytes := memory.Value()
	if memoryBytes <= 0 || quantity.New(memoryBytes).Cmp(memory) != 0 {
		return protocol.RuntimeResourceRequest{}, fmt.Errorf("memory must be a positive exact byte quantity")
	}
	request := protocol.RuntimeResourceRequest{
		Version:       protocol.RuntimeResourceRequestVersion,
		CPUMillicores: millicpu, MemoryBytes: memoryBytes,
		PIDsLimit: protocol.DefaultRuntimePIDsLimit,
	}
	if err := request.Validate(); err != nil {
		return protocol.RuntimeResourceRequest{}, err
	}
	return request, nil
}

func (s *Service) networkPolicy(spec v1alpha1.SandboxTemplateSpec, req *service.ClaimRequest) (string, []v1alpha1.CredentialBinding, error) {
	var requestPolicy *v1alpha1.SandboxNetworkPolicy
	if req.Config != nil {
		requestPolicy = req.Config.Network
		if req.Config.Webhook != nil && strings.TrimSpace(req.Config.Webhook.URL) != "" {
			requestPolicy = service.AppendWebhookNetworkPolicy(requestPolicy, req.Config.Webhook.URL)
		}
	}
	request := &networkpolicy.BuildNetworkPolicyRequest{
		SandboxID: req.SandboxID, TeamID: req.TeamID,
		TemplateSpec: spec.Network, RequestSpec: requestPolicy,
		TemplateBindings: credentialBindings(spec.Network), RequestBindings: requestCredentialBindings(req.Config),
	}
	if err := s.networkPolicies.ValidateNetworkPolicyRequest(request); err != nil {
		return "", nil, fmt.Errorf("%w: invalid Nomad network policy: %v", service.ErrInvalidClaimRequest, err)
	}
	state := s.networkPolicies.BuildNetworkPolicyState(request)
	if state == nil || state.PolicySpec == nil {
		return "", nil, fmt.Errorf("build Nomad network policy")
	}
	annotation, err := v1alpha1.NetworkPolicyToAnnotation(state.PolicySpec)
	if err != nil {
		return "", nil, fmt.Errorf("serialize Nomad network policy: %w", err)
	}
	return annotation, state.CredentialBindings, nil
}

type rootFSPlan struct {
	snapshotID         string
	sourceRef          string
	sourceDigest       string
	baseArtifactDigest string
}

func (s *Service) prepareRootFS(
	ctx context.Context,
	tpl *templatepkg.Template,
	req *service.ClaimRequest,
	platform sandboxstore.RootFSArtifactPlatform,
) (rootFSPlan, error) {
	requirements, err := s.rootFSArtifactRequirements(tpl.Spec)
	if err != nil {
		return rootFSPlan{}, fmt.Errorf("%w: template RootFS requirements: %v", service.ErrInvalidClaimRequest, err)
	}
	if req.SnapshotID != "" {
		snapshotID := strings.TrimSpace(req.SnapshotID)
		if snapshotID != req.SnapshotID || templatepkg.IsBuildSnapshotID(snapshotID) {
			return rootFSPlan{}, sandboxstore.ErrRootFSSnapshotNotFound
		}
		snapshot, err := s.store.GetRootFSSnapshot(ctx, snapshotID, req.TeamID)
		if err != nil {
			return rootFSPlan{}, err
		}
		if snapshot == nil {
			return rootFSPlan{}, sandboxstore.ErrRootFSSnapshotNotFound
		}
		if _, err := s.validateSnapshotGeneration(ctx, snapshot); err != nil {
			return rootFSPlan{}, err
		}
		artifact, err := s.store.GetReadyRootFSBaseArtifactByDigest(
			ctx, snapshot.BaseArtifactDigest, platform, requirements,
		)
		if err != nil {
			return rootFSPlan{}, err
		}
		if artifact.SourceOCIDigest != snapshot.SourceOCIDigest || artifact.FormatGeneration != snapshot.FormatGeneration {
			return rootFSPlan{}, fmt.Errorf("%w: snapshot Base artifact attestation changed", sandboxstore.ErrRootFSBaseArtifactConflict)
		}
		return rootFSPlan{snapshotID: snapshotID}, nil
	}
	if tpl.RootFS != nil {
		source := *tpl.RootFS
		if err := source.Validate(); err != nil {
			return rootFSPlan{}, fmt.Errorf("%w: template RootFS attestation: %v", service.ErrInvalidClaimRequest, err)
		}
		templateDigest, err := digestPinnedImage(strings.TrimSpace(tpl.Spec.MainContainer.Image))
		if err != nil || templateDigest != source.SourceOCIDigest {
			return rootFSPlan{}, fmt.Errorf("%w: template image does not match its captured RootFS",
				service.ErrInvalidClaimRequest)
		}
		if source.Platform.OS != platform.OS || source.Platform.Architecture != platform.Architecture ||
			source.Platform.Variant != platform.Variant {
			return rootFSPlan{}, fmt.Errorf("%w: template RootFS platform does not match the warm-slot class",
				service.ErrDataPlaneNotReady)
		}
		snapshot, err := s.store.GetRootFSSnapshot(ctx, source.SnapshotID, req.TeamID)
		if err != nil {
			return rootFSPlan{}, err
		}
		if snapshot == nil || source.StorageFormat != templatepkg.RootFSTemplateStorageFormatBlockCOWV1 ||
			snapshot.HeadGenerationID != source.GenerationID ||
			snapshot.SourceOCIDigest != source.SourceOCIDigest ||
			snapshot.BaseArtifactDigest != source.BaseArtifactDigest ||
			snapshot.FormatGeneration != source.FormatGeneration {
			return rootFSPlan{}, fmt.Errorf("%w: template RootFS snapshot attestation changed",
				sandboxstore.ErrRootFSGenerationConflict)
		}
		generation, err := s.validateSnapshotGeneration(ctx, snapshot)
		if err != nil {
			return rootFSPlan{}, err
		}
		if generation.SourceOCIDigest != source.SourceOCIDigest ||
			generation.BaseArtifactDigest != source.BaseArtifactDigest ||
			generation.FormatGeneration != source.FormatGeneration {
			return rootFSPlan{}, fmt.Errorf("%w: template RootFS generation attestation changed",
				sandboxstore.ErrRootFSGenerationConflict)
		}
		artifact, err := s.store.GetReadyRootFSBaseArtifactByDigest(
			ctx, source.BaseArtifactDigest, platform, requirements,
		)
		if err != nil {
			return rootFSPlan{}, err
		}
		if artifact.SourceOCIDigest != source.SourceOCIDigest ||
			artifact.FormatGeneration != source.FormatGeneration {
			return rootFSPlan{}, fmt.Errorf("%w: template RootFS base artifact attestation changed",
				sandboxstore.ErrRootFSBaseArtifactConflict)
		}
		return rootFSPlan{snapshotID: source.SnapshotID}, nil
	}
	sourceRef := strings.TrimSpace(tpl.Spec.MainContainer.Image)
	sourceDigest, err := digestPinnedImage(sourceRef)
	if err != nil {
		return rootFSPlan{}, fmt.Errorf("%w: template image: %v", service.ErrInvalidClaimRequest, err)
	}
	artifact, err := s.store.GetReadyRootFSBaseArtifact(
		ctx,
		sourceDigest,
		platform,
		requirements,
	)
	if err != nil {
		return rootFSPlan{}, err
	}
	if artifact == nil {
		return rootFSPlan{}, sandboxstore.ErrRootFSBaseArtifactNotFound
	}
	return rootFSPlan{
		sourceRef: sourceRef, sourceDigest: sourceDigest,
		baseArtifactDigest: artifact.ArtifactDigest,
	}, nil
}

func (s *Service) rootFSArtifactRequirements(
	spec v1alpha1.SandboxTemplateSpec,
) (sandboxstore.ReadyRootFSArtifactRequirements, error) {
	logicalSize, err := templatepkg.ResolveRootFSLogicalSize(spec)
	if err != nil {
		return sandboxstore.ReadyRootFSArtifactRequirements{}, err
	}
	requirements := sandboxstore.ReadyRootFSArtifactRequirements{
		FormatGeneration: s.rootFSFormatGeneration,
		LogicalSizeBytes: logicalSize,
		ProcdProtocol:    s.rootFSProcdProtocol,
		ProcdDigest:      s.rootFSProcdDigest,
	}
	if err := requirements.Validate(); err != nil {
		return sandboxstore.ReadyRootFSArtifactRequirements{}, err
	}
	return requirements, nil
}

func (s *Service) validateSnapshotGeneration(
	ctx context.Context,
	snapshot *sandboxstore.RootFSSnapshot,
) (*sandboxstore.RootFSGeneration, error) {
	if snapshot == nil || snapshot.HeadGenerationID == "" {
		return nil, fmt.Errorf("%w: snapshot has no block generation", sandboxstore.ErrRootFSGenerationConflict)
	}
	generation, err := s.store.GetRootFSGeneration(ctx, snapshot.HeadGenerationID)
	if err != nil {
		return nil, fmt.Errorf("%w: load snapshot generation: %v",
			sandboxstore.ErrRootFSGenerationConflict, err)
	}
	if generation == nil || generation.ID != snapshot.HeadGenerationID ||
		generation.FilesystemID != snapshot.FilesystemID ||
		generation.SourceOCIDigest != snapshot.SourceOCIDigest ||
		generation.BaseArtifactDigest != snapshot.BaseArtifactDigest ||
		generation.FormatGeneration != snapshot.FormatGeneration ||
		(generation.DurabilityState != sandboxstore.RootFSGenerationStateCompositeDurable &&
			generation.DurabilityState != sandboxstore.RootFSGenerationStateS3Materialized) {
		return nil, fmt.Errorf("%w: snapshot generation attestation changed",
			sandboxstore.ErrRootFSGenerationConflict)
	}
	return generation, nil
}

func (s *Service) initializeRootFS(ctx context.Context, req *service.ClaimRequest, plan rootFSPlan) error {
	if plan.snapshotID != "" {
		_, err := s.store.RestoreRootFSFromSnapshot(ctx, &sandboxstore.RestoreRootFSFromSnapshotRequest{
			SandboxID: req.SandboxID, SnapshotID: plan.snapshotID, TeamID: req.TeamID,
			OperationID: req.OperationID + "/initial-restore",
		})
		return err
	}
	_, _, err := s.store.EnsureInitialRootFSGeneration(ctx, &sandboxstore.EnsureInitialRootFSGenerationRequest{
		SandboxID: req.SandboxID, TeamID: req.TeamID, SourceOCIRef: plan.sourceRef,
		SourceOCIDigest: plan.sourceDigest, BaseArtifactDigest: plan.baseArtifactDigest,
	})
	return err
}

func (s *Service) claimRecord(
	tpl *templatepkg.Template,
	req *service.ClaimRequest,
	runtimeClass RuntimeClass,
	resources protocol.RuntimeResourceRequest,
	now time.Time,
) *sandboxstore.SandboxRecord {
	config := service.CloneSandboxConfig(req.Config)
	if config == nil {
		config = &sandboxstore.SandboxConfig{}
	}
	if config.TTL == nil && s.defaultTTL > 0 {
		seconds := int32(s.defaultTTL / time.Second)
		config.TTL = &seconds
	}
	record := &sandboxstore.SandboxRecord{
		ID: req.SandboxID, TeamID: req.TeamID, UserID: req.UserID,
		TemplateID: tpl.TemplateID, TemplateName: tpl.TemplateID, TemplateNamespace: tpl.Scope,
		ClusterID:    runtimeClass.ClusterID,
		DesiredState: sandboxstore.SandboxDesiredStateActive,
		Config:       *config, TemplateSpec: tpl.Spec, RuntimeGeneration: req.RuntimeGeneration,
		ResourceMillicpu:  resources.CPUMillicores,
		ResourceMemoryMiB: bytesToMiBRoundUp(resources.MemoryBytes),
		ClaimedAt:         now, CreatedAt: now,
	}
	if req.Metadata != nil {
		record.OwnerKind = strings.TrimSpace(req.Metadata.OwnerKind)
	}
	if config.TTL != nil && *config.TTL > 0 {
		record.ExpiresAt = now.Add(time.Duration(*config.TTL) * time.Second)
	}
	if !req.HardExpiresAt.IsZero() {
		record.HardExpiresAt = req.HardExpiresAt.UTC()
	} else if config.HardTTL != nil && *config.HardTTL > 0 {
		record.HardExpiresAt = now.Add(time.Duration(*config.HardTTL) * time.Second)
	}
	return record
}

func (s *Service) ensureClaimRecord(
	ctx context.Context,
	expected *sandboxstore.SandboxRecord,
	operationID string,
	bindings []egressauthstore.CredentialBinding,
) error {
	existing, found, err := s.store.RetrySandboxClaim(ctx, &sandboxstore.RetrySandboxClaimRequest{
		Record: expected, OperationID: operationID, LeaseTTL: s.claimTTL,
		CredentialBindings: bindings,
	})
	if err != nil {
		return mapClaimReservationError("retry sandbox claim", err)
	}
	if found {
		return validateClaimRecord(existing, expected)
	}
	limit, err := s.quotaLimits.GetLimit(ctx, expected.TeamID, quota.DimensionActiveSandboxes)
	if err != nil {
		return fmt.Errorf("load active sandbox quota: %w", err)
	}
	var activeLimit *int64
	if limit != nil {
		if limit.TeamID != expected.TeamID || limit.Dimension != quota.DimensionActiveSandboxes {
			return fmt.Errorf("active sandbox quota identity does not match claim")
		}
		activeLimit = &limit.LimitValue
	}
	existing, err = s.store.ReserveSandboxClaim(ctx, &sandboxstore.ReserveSandboxClaimRequest{
		Record: expected, OperationID: operationID, LeaseTTL: s.claimTTL,
		ActiveSandboxLimit: activeLimit, CredentialBindings: bindings,
	})
	if err != nil {
		return mapClaimReservationError("reserve sandbox claim", err)
	}
	return validateClaimRecord(existing, expected)
}

func mapClaimReservationError(operation string, err error) error {
	switch {
	case errors.Is(err, sandboxstore.ErrActiveSandboxQuotaExceeded):
		return fmt.Errorf("%w: %v", service.ErrQuotaExceeded, err)
	case errors.Is(err, sandboxstore.ErrSandboxClaimReservationConflict),
		errors.Is(err, sandboxstore.ErrSandboxClaimCleanupPending):
		return fmt.Errorf("%w: %v", service.ErrClaimConflict, err)
	default:
		return fmt.Errorf("%s: %w", operation, err)
	}
}

func validateClaimRecord(existing, expected *sandboxstore.SandboxRecord) error {
	if !sameClaimRecord(existing, expected) {
		return fmt.Errorf("%w: operation sandbox identity is already bound to another claim", service.ErrClaimConflict)
	}
	return nil
}

func sameClaimRecord(actual, expected *sandboxstore.SandboxRecord) bool {
	return actual != nil && expected != nil && actual.DeletedAt.IsZero() &&
		actual.ID == expected.ID && actual.TeamID == expected.TeamID && actual.UserID == expected.UserID &&
		actual.TemplateID == expected.TemplateID && actual.ClusterID == expected.ClusterID &&
		actual.DesiredState == sandboxstore.SandboxDesiredStateActive &&
		actual.RuntimeGeneration == expected.RuntimeGeneration &&
		actual.OwnerKind == expected.OwnerKind &&
		actual.ResourceMillicpu == expected.ResourceMillicpu &&
		actual.ResourceMemoryMiB == expected.ResourceMemoryMiB &&
		reflect.DeepEqual(actual.Config, expected.Config) &&
		reflect.DeepEqual(actual.TemplateSpec, expected.TemplateSpec)
}

func bytesToMiBRoundUp(value int64) int64 {
	if value <= 0 {
		return 0
	}
	const mib = int64(1024 * 1024)
	return 1 + (value-1)/mib
}

func runtimeAssignment(spec v1alpha1.SandboxTemplateSpec, req *service.ClaimRequest) (runtimecontrol.Assignment, error) {
	environment := make(map[string]string, len(spec.EnvVars)+len(spec.MainContainer.Env)+2)
	for _, item := range spec.MainContainer.Env {
		environment[item.Name] = item.Value
	}
	for key, value := range spec.EnvVars {
		environment[key] = value
	}
	if req.Config != nil {
		for key, value := range req.Config.EnvVars {
			environment[key] = value
		}
	}
	environment[runtimecontrol.EnvSandboxID] = req.SandboxID
	securityClass, ok := v1alpha1.EffectiveSandboxSecurityClass(spec.MainContainer.SecurityClass)
	if !ok {
		return runtimecontrol.Assignment{}, fmt.Errorf("template security class is invalid")
	}
	resolvedMounts, err := templatepkg.ResolveEphemeralMounts(spec)
	if err != nil {
		return runtimecontrol.Assignment{}, err
	}
	ephemeralMounts := make([]runtimecontrol.EphemeralMount, 0, len(resolvedMounts))
	for _, mount := range resolvedMounts {
		ephemeralMounts = append(ephemeralMounts, runtimecontrol.EphemeralMount{
			MountPath: mount.MountPath, SizeBytes: mount.SizeBytes,
		})
	}
	assignment := runtimecontrol.Assignment{
		SandboxID: req.SandboxID, TeamID: req.TeamID,
		RuntimeGeneration: req.RuntimeGeneration, SecurityClass: string(securityClass),
		EphemeralMounts: ephemeralMounts, EnvVars: environment,
	}
	if req.Config != nil && req.Config.Webhook != nil {
		webhook := *req.Config.Webhook
		assignment.Webhook = &webhook
	}
	return assignment, nil
}

func digestPinnedImage(image string) (string, error) {
	if image == "" || image != strings.TrimSpace(image) {
		return "", fmt.Errorf("image must be non-empty and canonical")
	}
	named, err := distref.ParseNormalizedNamed(image)
	if err != nil {
		return "", err
	}
	digested, ok := named.(distref.Digested)
	if !ok {
		return "", fmt.Errorf("image must be pinned by OCI digest")
	}
	parsed, err := digest.Parse(digested.Digest().String())
	if err != nil || parsed.Algorithm() != digest.SHA256 || parsed.String() != digested.Digest().String() {
		return "", fmt.Errorf("image must use a canonical sha256 digest")
	}
	return parsed.String(), nil
}

func credentialBindings(policy *v1alpha1.SandboxNetworkPolicy) []v1alpha1.CredentialBinding {
	if policy == nil {
		return nil
	}
	return append([]v1alpha1.CredentialBinding(nil), policy.CredentialBindings...)
}

func requestCredentialBindings(config *sandboxstore.SandboxConfig) []v1alpha1.CredentialBinding {
	if config == nil {
		return nil
	}
	return credentialBindings(config.Network)
}

func sanitizeNomadCredentialBindings(config *sandboxstore.SandboxConfig) {
	if config == nil || config.Network == nil {
		return
	}
	config.Network.CredentialBindings = nil
}

var _ service.SandboxRuntime = (*Service)(nil)
var _ service.SandboxHardExpiryTerminator = (*Service)(nil)
