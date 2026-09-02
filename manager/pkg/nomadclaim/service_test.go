package nomadclaim

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/credentialbinding"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/egressauthstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/networkpolicy"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotclaim"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotreconciler"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/templatebuild"
	"github.com/sandbox0-ai/sandbox0/pkg/apierror"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsrebase"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	v1alpha1 "github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
	templatepkg "github.com/sandbox0-ai/sandbox0/pkg/template"
	templatestore "github.com/sandbox0-ai/sandbox0/pkg/template/store"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type fakeTemplateStore struct {
	templatestore.TemplateStore
	template *templatepkg.Template
}

func TestRuntimeAssignmentCarriesSecurityClassAndEphemeralMounts(t *testing.T) {
	spec := v1alpha1.TemplateSpec{
		MainContainer: v1alpha1.ContainerSpec{SecurityClass: v1alpha1.SandboxSecurityClassPrivileged},
		EphemeralMounts: []v1alpha1.EphemeralMountSpec{
			{MountPath: "/var/lib/docker", SizeLimit: "16Gi"},
		},
	}
	assignment, err := runtimeAssignment(spec, &service.ClaimRequest{
		SandboxID: "sandbox-1", TeamID: "team-1", RuntimeGeneration: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if assignment.SecurityClass != "privileged" || len(assignment.EphemeralMounts) != 1 ||
		assignment.EphemeralMounts[0].MountPath != "/var/lib/docker" ||
		assignment.EphemeralMounts[0].SizeBytes != 16<<30 {
		t.Fatalf("assignment = %#v", assignment)
	}
	if err := assignment.Validate(); err != nil {
		t.Fatalf("assignment validation = %v", err)
	}
}

func (f *fakeTemplateStore) GetTemplateForTeam(_ context.Context, teamID, templateID string) (*templatepkg.Template, error) {
	if f.template == nil || f.template.TemplateID != templateID ||
		(f.template.Scope == naming.ScopeTeam && f.template.TeamID != teamID) {
		return nil, nil
	}
	return f.template, nil
}

type fakeClaimStore struct {
	records                  map[string]*sandboxstore.SandboxRecord
	operations               map[string]string
	claimPhases              map[string]string
	credentialBindings       map[string][]egressauthstore.CredentialBinding
	credentialDigests        map[string]string
	artifact                 *sandboxstore.RootFSBaseArtifact
	ensureCalls              []*sandboxstore.EnsureInitialRootFSGenerationRequest
	restoreCalls             []*sandboxstore.RestoreRootFSFromSnapshotRequest
	cleanupCalls             []string
	cleanupErr               error
	pauseCandidate           *sandboxstore.NomadSandboxPauseCandidate
	pauseErr                 error
	pauseSources             []string
	pauseContinueErr         error
	pauseContinueCalls       []string
	pressurePause            *sandboxstore.NomadSandboxPauseCandidate
	pressurePauseErr         error
	pressureRequests         []*sandboxstore.RootFSWriterPressurePauseRequest
	resumeCandidate          *sandboxstore.NomadSandboxResumeCandidate
	resumeErr                error
	resumeRequested          bool
	resumeRetryErr           error
	resumeRetryRequests      []*sandboxstore.RetryNomadSandboxResumeRequest
	resumeRequests           []*sandboxstore.RequestNomadSandboxResumeRequest
	resumeAbortCalls         [][3]string
	resumeAbortErr           error
	resumeCompleteErr        error
	resumeCompleteCalls      []*sandboxstore.CompleteNomadSandboxResumeRequest
	forkCandidate            *sandboxstore.NomadSandboxRunningForkCandidate
	forkErr                  error
	forkRequests             []*sandboxstore.NomadSandboxForkRequest
	pausedForkErr            error
	pausedForkRequests       []*sandboxstore.NomadSandboxForkRequest
	pausedForkCompleted      map[string]*sandboxstore.SandboxRecord
	activeLifecycles         map[string]*sandboxstore.SandboxLifecycleTxn
	lifecyclesByID           map[string]*sandboxstore.SandboxLifecycleTxn
	forkAbortCalls           [][4]string
	forkAbortErr             error
	activeSlot               *sandboxstore.RuntimeSlot
	runtimeSlotErr           error
	quiesceCalls             []*sandboxstore.BeginRuntimeSlotQuiesceRequest
	snapshot                 *sandboxstore.RootFSSnapshot
	generation               *sandboxstore.RootFSGeneration
	createdSnapshots         []*sandboxstore.CreateRootFSSnapshotRequest
	deletedSnapshots         []string
	templateCaptureCandidate *sandboxstore.NomadTemplateCaptureCandidate
	templateCaptureRequests  []*sandboxstore.NomadRunningRootFSCaptureRequest
	rebaseCandidate          *sandboxstore.NomadPausedRebaseCandidate
	rebaseErr                error
	rebaseRequests           []*sandboxstore.NomadPausedRebaseRequest
	rebasePublishErr         error
	rebasePublishes          []*sandboxstore.PublishPausedRootFSRebaseRequest
	rebaseRejectErr          error
	rebaseRejects            [][]byte
	rebaseAckErr             error
	rebaseAcks               [][]byte
	writeCount               int
	pauseOrder               *[]string
}

func (f *fakeClaimStore) RequestNomadSandboxRunningFork(
	_ context.Context,
	request *sandboxstore.NomadSandboxForkRequest,
) (*sandboxstore.NomadSandboxRunningForkCandidate, error) {
	copyRequest := *request
	copyRequest.Target = cloneClaimRecord(request.Target)
	f.forkRequests = append(f.forkRequests, &copyRequest)
	if f.forkErr != nil {
		return nil, f.forkErr
	}
	if f.forkCandidate == nil {
		return nil, sandboxstore.ErrNomadSandboxForkNotReady
	}
	if f.forkCandidate.Source == nil {
		f.forkCandidate.Source = cloneClaimRecord(f.records[request.SourceSandboxID])
	}
	if f.forkCandidate.Target == nil {
		f.forkCandidate.Target = cloneClaimRecord(request.Target)
	}
	candidate := *f.forkCandidate
	candidate.Source = cloneClaimRecord(f.forkCandidate.Source)
	candidate.Target = cloneClaimRecord(f.forkCandidate.Target)
	if f.forkCandidate.Slot != nil {
		slot := *f.forkCandidate.Slot
		candidate.Slot = &slot
	}
	candidate.BindingDigest = append([]byte(nil), f.forkCandidate.BindingDigest...)
	if f.records == nil {
		f.records = make(map[string]*sandboxstore.SandboxRecord)
	}
	if f.records[request.Target.ID] == nil {
		f.records[request.Target.ID] = cloneClaimRecord(request.Target)
	}
	return &candidate, nil
}

func (f *fakeClaimStore) ForkNomadPausedSandbox(
	_ context.Context,
	request *sandboxstore.NomadSandboxForkRequest,
) (*sandboxstore.SandboxRecord, error) {
	copyRequest := *request
	copyRequest.Target = cloneClaimRecord(request.Target)
	f.pausedForkRequests = append(f.pausedForkRequests, &copyRequest)
	if f.pausedForkErr != nil {
		return nil, f.pausedForkErr
	}
	if f.forkCandidate != nil && f.forkCandidate.OperationID == request.OperationID {
		return nil, sandboxstore.ErrNomadSandboxRunningForkRequired
	}
	if completed := f.pausedForkCompleted[request.OperationID]; completed != nil {
		return cloneClaimRecord(completed), nil
	}
	source := f.records[request.SourceSandboxID]
	if source == nil {
		return nil, sandboxstore.ErrSandboxRecordNotFound
	}
	if source.DesiredState != sandboxstore.SandboxDesiredStatePaused {
		return nil, sandboxstore.ErrNomadSandboxRunningForkRequired
	}
	if f.records == nil {
		f.records = make(map[string]*sandboxstore.SandboxRecord)
	}
	if f.pausedForkCompleted == nil {
		f.pausedForkCompleted = make(map[string]*sandboxstore.SandboxRecord)
	}
	f.records[request.Target.ID] = cloneClaimRecord(request.Target)
	f.pausedForkCompleted[request.OperationID] = cloneClaimRecord(request.Target)
	return cloneClaimRecord(request.Target), nil
}

func (f *fakeClaimStore) GetSandbox(_ context.Context, sandboxID string) (*sandboxstore.SandboxRecord, error) {
	return cloneClaimRecord(f.records[sandboxID]), nil
}

func (f *fakeClaimStore) GetActiveLifecycleTxn(
	_ context.Context,
	sandboxID string,
) (*sandboxstore.SandboxLifecycleTxn, error) {
	return sandboxstore.CloneSandboxLifecycleTxn(f.activeLifecycles[sandboxID]), nil
}

func (f *fakeClaimStore) GetLifecycleTxn(
	_ context.Context,
	txnID string,
) (*sandboxstore.SandboxLifecycleTxn, error) {
	if txn := f.lifecyclesByID[txnID]; txn != nil {
		return sandboxstore.CloneSandboxLifecycleTxn(txn), nil
	}
	for _, txn := range f.activeLifecycles {
		if txn != nil && txn.ID == txnID {
			return sandboxstore.CloneSandboxLifecycleTxn(txn), nil
		}
	}
	return nil, nil
}

func (f *fakeClaimStore) GetPendingNomadPausedRebase(
	_ context.Context,
	sandboxID string,
) (*sandboxstore.SandboxLifecycleTxn, error) {
	if txn := f.activeLifecycles[sandboxID]; txn != nil && txn.Kind == sandboxstore.SandboxLifecycleKindRebase {
		return sandboxstore.CloneSandboxLifecycleTxn(txn), nil
	}
	for _, txn := range f.lifecyclesByID {
		if txn != nil && txn.SandboxID == sandboxID && txn.Kind == sandboxstore.SandboxLifecycleKindRebase &&
			(txn.Phase == sandboxstore.SandboxLifecyclePhaseCommitted ||
				txn.Phase == sandboxstore.SandboxLifecyclePhaseAborted) && txn.WorkerAcknowledgedAt.IsZero() {
			return sandboxstore.CloneSandboxLifecycleTxn(txn), nil
		}
	}
	return nil, nil
}

func (f *fakeClaimStore) RequestNomadPausedRebase(
	_ context.Context,
	request *sandboxstore.NomadPausedRebaseRequest,
) (*sandboxstore.NomadPausedRebaseCandidate, error) {
	copyRequest := *request
	f.rebaseRequests = append(f.rebaseRequests, &copyRequest)
	if f.rebaseErr != nil {
		return nil, f.rebaseErr
	}
	if f.rebaseCandidate == nil {
		return nil, sandboxstore.ErrNomadSandboxRebaseNotReady
	}
	if record := f.records[request.SandboxID]; record != nil {
		f.rebaseCandidate.Sandbox = cloneClaimRecord(record)
	}
	if f.lifecyclesByID == nil {
		f.lifecyclesByID = make(map[string]*sandboxstore.SandboxLifecycleTxn)
	}
	if f.activeLifecycles == nil {
		f.activeLifecycles = make(map[string]*sandboxstore.SandboxLifecycleTxn)
	}
	if f.lifecyclesByID[request.OperationID] == nil {
		txn := &sandboxstore.SandboxLifecycleTxn{
			ID: request.OperationID, SandboxID: request.SandboxID,
			Kind: sandboxstore.SandboxLifecycleKindRebase, Phase: sandboxstore.SandboxLifecyclePhasePreparing,
			Source:                   sandboxstore.SandboxLifecycleSourceManual,
			TargetGenerationID:       f.rebaseCandidate.TargetGenerationID,
			ExpectedGenerationID:     f.rebaseCandidate.SourceGeneration.ID,
			SourceBaseArtifactDigest: f.rebaseCandidate.SourceBaseArtifact.ArtifactDigest,
			TargetBaseArtifactDigest: request.TargetBaseArtifactDigest,
			RollbackExpiresAt:        request.RollbackExpiresAt,
			WorkerClusterID:          request.WorkerClusterID, WorkerNodeID: request.WorkerNodeID,
			WorkerNodeUID: request.WorkerNodeUID,
		}
		f.lifecyclesByID[request.OperationID] = txn
		f.activeLifecycles[request.SandboxID] = txn
	}
	return cloneNomadPausedRebaseCandidate(f.rebaseCandidate), nil
}

func (f *fakeClaimStore) PublishPausedRootFSRebase(
	_ context.Context,
	request *sandboxstore.PublishPausedRootFSRebaseRequest,
) (*sandboxstore.RootFSFilesystem, error) {
	copyRequest := *request
	if request.Generation != nil {
		generation := *request.Generation
		generation.Descriptor = append([]byte(nil), request.Generation.Descriptor...)
		copyRequest.Generation = &generation
	}
	copyRequest.HealthCheckDigest = append([]byte(nil), request.HealthCheckDigest...)
	copyRequest.WorkerProofDigest = append([]byte(nil), request.WorkerProofDigest...)
	f.rebasePublishes = append(f.rebasePublishes, &copyRequest)
	if f.rebasePublishErr != nil {
		if errors.Is(f.rebasePublishErr, sandboxstore.ErrNomadPausedRebaseTerminating) {
			if record := f.records[request.SandboxID]; record != nil {
				record.DesiredState = sandboxstore.SandboxDesiredStateTerminating
			}
			f.claimPhases[request.SandboxID] = sandboxstore.SandboxRuntimeClaimPhaseCleanupPending
		}
		return nil, f.rebasePublishErr
	}
	if f.rebaseCandidate != nil {
		f.rebaseCandidate.Completed = true
		f.rebaseCandidate.WorkerProofDigest = append([]byte(nil), request.WorkerProofDigest...)
		f.rebaseCandidate.TargetWriterEpoch = request.Generation.WriterEpoch
		f.rebaseCandidate.LifecyclePhase = sandboxstore.SandboxLifecyclePhaseCommitted
	}
	if txn := f.lifecyclesByID[request.OperationID]; txn != nil {
		txn.Phase = sandboxstore.SandboxLifecyclePhaseCommitted
		txn.PreparedGenerationID = request.Generation.ID
		txn.WorkerProofDigest = append([]byte(nil), request.WorkerProofDigest...)
		delete(f.activeLifecycles, request.SandboxID)
	}
	return &sandboxstore.RootFSFilesystem{ID: request.Generation.FilesystemID}, nil
}

func (f *fakeClaimStore) RejectNomadPausedRebaseWorker(
	_ context.Context,
	request *sandboxstore.NomadPausedRebaseRequest,
	proofDigest []byte,
) error {
	f.rebaseRejects = append(f.rebaseRejects, append([]byte(nil), proofDigest...))
	if f.rebaseRejectErr != nil {
		return f.rebaseRejectErr
	}
	if f.rebaseCandidate != nil {
		f.rebaseCandidate.Completed = false
		f.rebaseCandidate.Rejected = true
		f.rebaseCandidate.WorkerProofDigest = append([]byte(nil), proofDigest...)
		f.rebaseCandidate.LifecyclePhase = sandboxstore.SandboxLifecyclePhaseAborted
	}
	if txn := f.lifecyclesByID[request.OperationID]; txn != nil {
		txn.Phase = sandboxstore.SandboxLifecyclePhaseAborted
		txn.Error = "sandbox termination requested"
		txn.WorkerProofDigest = append([]byte(nil), proofDigest...)
		delete(f.activeLifecycles, request.SandboxID)
	}
	return nil
}

func (f *fakeClaimStore) AcknowledgeNomadPausedRebaseWorker(
	_ context.Context,
	_, _, _, _, _ string,
	proofDigest []byte,
) error {
	f.rebaseAcks = append(f.rebaseAcks, append([]byte(nil), proofDigest...))
	if f.rebaseAckErr != nil {
		return f.rebaseAckErr
	}
	if f.rebaseCandidate != nil {
		f.rebaseCandidate.WorkerAcknowledgedAt = time.Now().UTC()
	}
	for _, txn := range f.lifecyclesByID {
		if txn != nil && string(txn.WorkerProofDigest) == string(proofDigest) {
			txn.WorkerAcknowledgedAt = time.Now().UTC()
		}
	}
	return nil
}

func (f *fakeClaimStore) AbortNomadSandboxRunningFork(
	_ context.Context,
	operationID, sourceSandboxID, targetSandboxID, reason string,
) (bool, error) {
	f.forkAbortCalls = append(f.forkAbortCalls, [4]string{operationID, sourceSandboxID, targetSandboxID, reason})
	if f.forkAbortErr != nil {
		return false, f.forkAbortErr
	}
	delete(f.activeLifecycles, sourceSandboxID)
	return true, nil
}

func (f *fakeClaimStore) RetryNomadSandboxResume(
	_ context.Context,
	request *sandboxstore.RetryNomadSandboxResumeRequest,
) (*sandboxstore.NomadSandboxResumeCandidate, bool, error) {
	copyRequest := *request
	f.resumeRetryRequests = append(f.resumeRetryRequests, &copyRequest)
	if f.resumeRetryErr != nil {
		return nil, false, f.resumeRetryErr
	}
	record := f.records[request.SandboxID]
	if record == nil {
		return nil, false, sandboxstore.ErrSandboxRecordNotFound
	}
	if record.DesiredState == sandboxstore.SandboxDesiredStateActive &&
		record.RuntimeID != "" && record.RuntimeNamespace != "" {
		return &sandboxstore.NomadSandboxResumeCandidate{
			SandboxID: record.ID, AlreadyActive: true,
			RuntimeGeneration: record.RuntimeGeneration, Record: cloneClaimRecord(record),
		}, true, nil
	}
	if f.resumeRequested && f.resumeCandidate != nil {
		candidate := *f.resumeCandidate
		candidate.Record = cloneClaimRecord(record)
		return &candidate, true, nil
	}
	return nil, false, nil
}

func (f *fakeClaimStore) RequestNomadSandboxResume(
	_ context.Context,
	request *sandboxstore.RequestNomadSandboxResumeRequest,
) (*sandboxstore.NomadSandboxResumeCandidate, error) {
	copyRequest := *request
	if request.ActiveSandboxLimit != nil {
		limit := *request.ActiveSandboxLimit
		copyRequest.ActiveSandboxLimit = &limit
	}
	f.resumeRequests = append(f.resumeRequests, &copyRequest)
	if f.resumeErr != nil {
		return nil, f.resumeErr
	}
	record := f.records[request.SandboxID]
	if record == nil {
		return nil, sandboxstore.ErrSandboxRecordNotFound
	}
	if record.DesiredState == sandboxstore.SandboxDesiredStateActive &&
		record.RuntimeID != "" && record.RuntimeNamespace != "" {
		return &sandboxstore.NomadSandboxResumeCandidate{
			SandboxID: record.ID, AlreadyActive: true,
			RuntimeGeneration: record.RuntimeGeneration, Record: cloneClaimRecord(record),
		}, nil
	}
	if f.resumeCandidate == nil {
		return nil, sandboxstore.ErrNomadSandboxResumeNotReady
	}
	f.resumeRequested = true
	candidate := *f.resumeCandidate
	candidate.Record = cloneClaimRecord(record)
	return &candidate, nil
}

func (f *fakeClaimStore) AbortNomadSandboxResume(
	_ context.Context,
	sandboxID, operationID, reason string,
) (bool, error) {
	f.resumeAbortCalls = append(f.resumeAbortCalls, [3]string{sandboxID, operationID, reason})
	if f.resumeAbortErr != nil {
		return false, f.resumeAbortErr
	}
	f.resumeRequested = false
	return true, nil
}

func (f *fakeClaimStore) CompleteNomadSandboxResume(
	_ context.Context,
	request *sandboxstore.CompleteNomadSandboxResumeRequest,
) (*sandboxstore.SandboxRecord, error) {
	copyRequest := *request
	f.resumeCompleteCalls = append(f.resumeCompleteCalls, &copyRequest)
	if f.resumeCompleteErr != nil {
		return nil, f.resumeCompleteErr
	}
	if f.resumeCandidate == nil || request.OperationID != f.resumeCandidate.OperationID {
		return nil, sandboxstore.ErrNomadSandboxResumeConflict
	}
	record := cloneClaimRecord(f.records[request.SandboxID])
	if record == nil {
		return nil, sandboxstore.ErrSandboxRecordNotFound
	}
	record.DesiredState = sandboxstore.SandboxDesiredStateActive
	record.RuntimeID = request.AllocationID
	record.RuntimeNamespace = request.AllocationNamespace
	record.RuntimeGeneration = f.resumeCandidate.RuntimeGeneration
	f.records[request.SandboxID] = cloneClaimRecord(record)
	f.activeSlot = &sandboxstore.RuntimeSlot{
		ID: request.SlotID, SandboxID: request.SandboxID,
		AllocationID: request.AllocationID, AllocationNamespace: request.AllocationNamespace,
		State: sandboxstore.RuntimeSlotStateActive, ProcdInstanceID: "procd-resumed",
		ProcdAddress: "http://10.0.0.8:49983", CommandReadyDigest: make([]byte, sha256.Size),
		CommandReadyAt: time.Now(), AuthorityObservedAt: time.Now(), HeartbeatExpiresAt: time.Now().Add(time.Minute),
	}
	return cloneClaimRecord(record), nil
}

func (f *fakeClaimStore) GetRuntimeSlotBySandboxID(
	_ context.Context,
	sandboxID string,
) (*sandboxstore.RuntimeSlot, error) {
	if f.runtimeSlotErr != nil {
		return nil, f.runtimeSlotErr
	}
	if f.activeSlot == nil || f.activeSlot.SandboxID != sandboxID {
		return nil, sandboxstore.ErrRuntimeSlotNotFound
	}
	copy := *f.activeSlot
	copy.CommandReadyDigest = append([]byte(nil), f.activeSlot.CommandReadyDigest...)
	return &copy, nil
}

func (f *fakeClaimStore) RequestNomadSandboxPause(
	_ context.Context,
	sandboxID, source string,
) (*sandboxstore.NomadSandboxPauseCandidate, error) {
	f.pauseSources = append(f.pauseSources, sandboxID+":"+source)
	if f.pauseErr != nil {
		return nil, f.pauseErr
	}
	if f.pauseCandidate == nil {
		return nil, sandboxstore.ErrNomadSandboxPauseNotReady
	}
	copy := *f.pauseCandidate
	copy.BindingDigest = append([]byte(nil), f.pauseCandidate.BindingDigest...)
	return &copy, nil
}

func (f *fakeClaimStore) ContinueNomadSandboxPause(
	_ context.Context,
	sandboxID string,
) (*sandboxstore.NomadSandboxPauseCandidate, error) {
	f.pauseContinueCalls = append(f.pauseContinueCalls, sandboxID)
	if f.pauseContinueErr != nil {
		return nil, f.pauseContinueErr
	}
	if f.pauseCandidate == nil {
		return nil, sandboxstore.ErrNomadSandboxPauseNotPending
	}
	copy := *f.pauseCandidate
	copy.BindingDigest = append([]byte(nil), f.pauseCandidate.BindingDigest...)
	return &copy, nil
}

func (f *fakeClaimStore) RequestNomadSandboxTTLPause(
	ctx context.Context,
	sandboxID string,
) (*sandboxstore.NomadSandboxPauseCandidate, error) {
	return f.RequestNomadSandboxPause(ctx, sandboxID, sandboxstore.SandboxLifecycleSourceAuto)
}

func (f *fakeClaimStore) RequestNomadSandboxPressurePause(
	_ context.Context,
	request *sandboxstore.RootFSWriterPressurePauseRequest,
) (*sandboxstore.NomadSandboxPauseCandidate, error) {
	copyRequest := *request
	copyRequest.BindingDigest = append([]byte(nil), request.BindingDigest...)
	f.pressureRequests = append(f.pressureRequests, &copyRequest)
	if f.pressurePauseErr != nil {
		return nil, f.pressurePauseErr
	}
	if f.pressurePause == nil {
		return nil, sandboxstore.ErrNomadSandboxPauseNotReady
	}
	candidate := *f.pressurePause
	return &candidate, nil
}

func (f *fakeClaimStore) BeginRuntimeSlotQuiesce(
	_ context.Context,
	request *sandboxstore.BeginRuntimeSlotQuiesceRequest,
) (*sandboxstore.RuntimeSlot, error) {
	copy := *request
	f.quiesceCalls = append(f.quiesceCalls, &copy)
	if f.pauseOrder != nil {
		*f.pauseOrder = append(*f.pauseOrder, "quiesce")
	}
	return &sandboxstore.RuntimeSlot{ID: request.SlotID, State: sandboxstore.RuntimeSlotStateQuiescing}, nil
}

type fakeAllocationStopper struct {
	requests   []runtimeslotreconciler.AllocationPurgeRequest
	err        error
	pauseOrder *[]string
}

func (f *fakeAllocationStopper) Stop(
	_ context.Context,
	request runtimeslotreconciler.AllocationPurgeRequest,
) error {
	f.requests = append(f.requests, request)
	if f.pauseOrder != nil {
		*f.pauseOrder = append(*f.pauseOrder, "stop")
	}
	return f.err
}

func (f *fakeClaimStore) ReserveSandboxClaim(_ context.Context, request *sandboxstore.ReserveSandboxClaimRequest) (*sandboxstore.SandboxRecord, error) {
	if request == nil || request.Record == nil {
		return nil, errors.New("missing reservation record")
	}
	if existing := f.records[request.Record.ID]; existing != nil {
		if !sameClaimRecord(existing, request.Record) || f.operations[request.Record.ID] != request.OperationID {
			return nil, sandboxstore.ErrSandboxClaimReservationConflict
		}
		if !credentialbinding.EqualStoreSemantic(f.credentialBindings[request.Record.ID], request.CredentialBindings) {
			return nil, sandboxstore.ErrSandboxClaimReservationConflict
		}
		return cloneClaimRecord(existing), nil
	}
	if request.ActiveSandboxLimit != nil {
		var current int64
		for _, record := range f.records {
			if record.TeamID == request.Record.TeamID && record.DeletedAt.IsZero() &&
				record.DesiredState == sandboxstore.SandboxDesiredStateActive {
				current++
			}
		}
		if current >= *request.ActiveSandboxLimit {
			return nil, &sandboxstore.ActiveSandboxQuotaExceededError{
				TeamID: request.Record.TeamID, Current: current, Limit: *request.ActiveSandboxLimit,
			}
		}
	}
	if f.records == nil {
		f.records = make(map[string]*sandboxstore.SandboxRecord)
	}
	f.records[request.Record.ID] = cloneClaimRecord(request.Record)
	f.operations[request.Record.ID] = request.OperationID
	f.claimPhases[request.Record.ID] = sandboxstore.SandboxRuntimeClaimPhaseClaiming
	if f.credentialBindings == nil {
		f.credentialBindings = make(map[string][]egressauthstore.CredentialBinding)
		f.credentialDigests = make(map[string]string)
	}
	f.credentialBindings[request.Record.ID] = credentialbinding.CloneStore(request.CredentialBindings)
	f.credentialDigests[request.Record.ID] = credentialbinding.DigestStore(request.CredentialBindings)
	f.writeCount++
	return cloneClaimRecord(request.Record), nil
}

func (f *fakeClaimStore) RetrySandboxClaim(_ context.Context, request *sandboxstore.RetrySandboxClaimRequest) (*sandboxstore.SandboxRecord, bool, error) {
	if request == nil || request.Record == nil {
		return nil, false, errors.New("missing retry record")
	}
	existing := f.records[request.Record.ID]
	if existing == nil {
		return nil, false, nil
	}
	if !sameClaimRecord(existing, request.Record) || f.operations[request.Record.ID] != request.OperationID {
		return nil, true, sandboxstore.ErrSandboxClaimReservationConflict
	}
	if !credentialbinding.EqualStoreSemantic(f.credentialBindings[request.Record.ID], request.CredentialBindings) {
		return nil, true, sandboxstore.ErrSandboxClaimReservationConflict
	}
	phase := f.claimPhases[request.Record.ID]
	if phase == sandboxstore.SandboxRuntimeClaimPhaseCleanupPending || phase == sandboxstore.SandboxRuntimeClaimPhaseCleaned {
		return nil, true, sandboxstore.ErrSandboxClaimCleanupPending
	}
	return cloneClaimRecord(existing), true, nil
}

func (f *fakeClaimStore) GetNomadSandboxCredentialBindings(
	_ context.Context,
	_, sandboxID string,
) (*sandboxstore.NomadSandboxCredentialBindings, error) {
	bindings := credentialbinding.CloneStore(f.credentialBindings[sandboxID])
	digest := f.credentialDigests[sandboxID]
	if digest == "" {
		digest = credentialbinding.DigestStore(bindings)
	}
	return &sandboxstore.NomadSandboxCredentialBindings{Digest: digest, Bindings: bindings}, nil
}

func (f *fakeClaimStore) CompleteSandboxClaim(_ context.Context, request *sandboxstore.CompleteSandboxClaimRequest) (*sandboxstore.SandboxRecord, error) {
	if request == nil || f.operations[request.SandboxID] != request.OperationID {
		return nil, sandboxstore.ErrSandboxClaimReservationConflict
	}
	if phase := f.claimPhases[request.SandboxID]; phase == sandboxstore.SandboxRuntimeClaimPhaseCleanupPending ||
		phase == sandboxstore.SandboxRuntimeClaimPhaseCleaned {
		return nil, sandboxstore.ErrSandboxClaimCleanupPending
	}
	record := cloneClaimRecord(f.records[request.SandboxID])
	if record == nil {
		return nil, sandboxstore.ErrSandboxClaimReservationConflict
	}
	record.RuntimeID = request.AllocationID
	record.RuntimeNamespace = request.AllocationNamespace
	f.records[request.SandboxID] = cloneClaimRecord(record)
	f.claimPhases[request.SandboxID] = sandboxstore.SandboxRuntimeClaimPhaseReady
	f.writeCount++
	return record, nil
}

func (f *fakeClaimStore) RequestSandboxRuntimeClaimCleanup(
	_ context.Context,
	sandboxID, reason string,
) (*sandboxstore.SandboxClaimCleanupCandidate, error) {
	f.cleanupCalls = append(f.cleanupCalls, sandboxID+":"+reason)
	if f.cleanupErr != nil {
		return nil, f.cleanupErr
	}
	record := f.records[sandboxID]
	if record == nil {
		return nil, sandboxstore.ErrSandboxRecordNotFound
	}
	record.DesiredState = sandboxstore.SandboxDesiredStateTerminating
	f.claimPhases[sandboxID] = sandboxstore.SandboxRuntimeClaimPhaseCleanupPending
	return &sandboxstore.SandboxClaimCleanupCandidate{
		SandboxID: sandboxID, OperationID: f.operations[sandboxID],
		PhysicalStateRequired: record.RuntimeID != "",
	}, nil
}

func (f *fakeClaimStore) RequestHardExpiredSandboxRuntimeClaimCleanup(
	ctx context.Context,
	sandboxID, reason string,
) (*sandboxstore.SandboxClaimCleanupCandidate, error) {
	return f.RequestSandboxRuntimeClaimCleanup(ctx, sandboxID, reason)
}

type fakeQuotaLimitStore struct {
	limit *quota.Limit
	err   error
	calls []struct {
		teamID    string
		dimension quota.Dimension
	}
}

func (f *fakeQuotaLimitStore) GetLimit(_ context.Context, teamID string, dimension quota.Dimension) (*quota.Limit, error) {
	f.calls = append(f.calls, struct {
		teamID    string
		dimension quota.Dimension
	}{teamID: teamID, dimension: dimension})
	return f.limit, f.err
}

func (f *fakeClaimStore) GetReadyRootFSBaseArtifact(
	_ context.Context,
	source string,
	platform sandboxstore.RootFSArtifactPlatform,
	requirements sandboxstore.ReadyRootFSArtifactRequirements,
) (*sandboxstore.RootFSBaseArtifact, error) {
	if f.artifact == nil || f.artifact.SourceOCIDigest != source || f.artifact.Platform != platform ||
		f.artifact.FormatGeneration != requirements.FormatGeneration ||
		f.artifact.LogicalSizeBytes != requirements.LogicalSizeBytes ||
		f.artifact.ProcdProtocol != requirements.ProcdProtocol ||
		f.artifact.ProcdDigest != requirements.ProcdDigest {
		return nil, sandboxstore.ErrRootFSBaseArtifactNotFound
	}
	copy := *f.artifact
	return &copy, nil
}

func (f *fakeClaimStore) GetReadyRootFSBaseArtifactByDigest(
	_ context.Context,
	digest string,
	platform sandboxstore.RootFSArtifactPlatform,
	requirements sandboxstore.ReadyRootFSArtifactRequirements,
) (*sandboxstore.RootFSBaseArtifact, error) {
	if f.artifact == nil || f.artifact.ArtifactDigest != digest || f.artifact.Platform != platform ||
		f.artifact.FormatGeneration != requirements.FormatGeneration ||
		f.artifact.LogicalSizeBytes != requirements.LogicalSizeBytes ||
		f.artifact.ProcdProtocol != requirements.ProcdProtocol ||
		f.artifact.ProcdDigest != requirements.ProcdDigest {
		return nil, sandboxstore.ErrRootFSBaseArtifactNotFound
	}
	copy := *f.artifact
	return &copy, nil
}

func (f *fakeClaimStore) EnsureInitialRootFSGeneration(_ context.Context, request *sandboxstore.EnsureInitialRootFSGenerationRequest) (*sandboxstore.RootFSFilesystem, *sandboxstore.RootFSGeneration, error) {
	copy := *request
	f.ensureCalls = append(f.ensureCalls, &copy)
	return &sandboxstore.RootFSFilesystem{ID: request.SandboxID}, &sandboxstore.RootFSGeneration{ID: "generation-1"}, nil
}

func (f *fakeClaimStore) GetRootFSSnapshot(_ context.Context, snapshotID, teamID string) (*sandboxstore.RootFSSnapshot, error) {
	if f.snapshot == nil || f.snapshot.ID != snapshotID || f.snapshot.TeamID != teamID {
		return nil, sandboxstore.ErrRootFSSnapshotNotFound
	}
	copy := *f.snapshot
	return &copy, nil
}

func (f *fakeClaimStore) CreateRootFSSnapshot(
	_ context.Context,
	request *sandboxstore.CreateRootFSSnapshotRequest,
) (*sandboxstore.RootFSSnapshot, error) {
	copyRequest := *request
	f.createdSnapshots = append(f.createdSnapshots, &copyRequest)
	record := f.records[request.SandboxID]
	if record == nil || f.generation == nil {
		return nil, sandboxstore.ErrRootFSFilesystemNotFound
	}
	f.snapshot = &sandboxstore.RootFSSnapshot{
		ID: request.SnapshotID, FilesystemID: f.generation.FilesystemID,
		TeamID: record.TeamID, SourceSandboxID: request.SandboxID,
		HeadGenerationID:   f.generation.ID,
		BaseArtifactDigest: f.generation.BaseArtifactDigest,
		FormatGeneration:   f.generation.FormatGeneration, SourceOCIDigest: f.generation.SourceOCIDigest,
		Name: request.Name, Description: request.Description, CreatedAt: time.Unix(100, 0).UTC(),
	}
	copy := *f.snapshot
	return &copy, nil
}

func (f *fakeClaimStore) GetRootFSGeneration(_ context.Context, generationID string) (*sandboxstore.RootFSGeneration, error) {
	if f.generation == nil || f.generation.ID != generationID {
		return nil, sandboxstore.ErrRootFSGenerationConflict
	}
	copy := *f.generation
	copy.Descriptor = append([]byte(nil), f.generation.Descriptor...)
	return &copy, nil
}

func (f *fakeClaimStore) DeleteRootFSSnapshot(_ context.Context, snapshotID, teamID string) error {
	if f.snapshot == nil || f.snapshot.ID != snapshotID || f.snapshot.TeamID != teamID {
		return sandboxstore.ErrRootFSSnapshotNotFound
	}
	f.deletedSnapshots = append(f.deletedSnapshots, snapshotID)
	f.snapshot = nil
	return nil
}

func (f *fakeClaimStore) DeleteTemplateBuildRootFSCapture(ctx context.Context, snapshotID, teamID string) error {
	return f.DeleteRootFSSnapshot(ctx, snapshotID, teamID)
}

func (f *fakeClaimStore) RequestNomadRunningRootFSCapture(
	_ context.Context,
	request *sandboxstore.NomadRunningRootFSCaptureRequest,
) (*sandboxstore.NomadTemplateCaptureCandidate, error) {
	copyRequest := *request
	f.templateCaptureRequests = append(f.templateCaptureRequests, &copyRequest)
	if f.templateCaptureCandidate == nil {
		return nil, sandboxstore.ErrNomadTemplateCaptureNotReady
	}
	copy := *f.templateCaptureCandidate
	copy.TeamID = request.TeamID
	copy.CaptureKind = request.CaptureKind
	copy.Name = request.Name
	copy.Description = request.Description
	copy.ExpiresAt = request.ExpiresAt
	if copy.Source == nil && f.records[request.SourceSandboxID] != nil {
		source := *f.records[request.SourceSandboxID]
		copy.Source = &source
	}
	copy.BindingDigest = append([]byte(nil), f.templateCaptureCandidate.BindingDigest...)
	if f.templateCaptureCandidate.Slot != nil {
		slot := *f.templateCaptureCandidate.Slot
		copy.Slot = &slot
	}
	if f.templateCaptureCandidate.Snapshot != nil {
		snapshot := *f.templateCaptureCandidate.Snapshot
		copy.Snapshot = &snapshot
	}
	return &copy, nil
}

func (f *fakeClaimStore) ContinueNomadRunningRootFSCapture(
	ctx context.Context,
	sourceSandboxID string,
) (*sandboxstore.NomadTemplateCaptureCandidate, error) {
	if len(f.templateCaptureRequests) == 0 {
		return nil, nil
	}
	request := f.templateCaptureRequests[len(f.templateCaptureRequests)-1]
	if request.SourceSandboxID != sourceSandboxID {
		return nil, nil
	}
	return f.RequestNomadRunningRootFSCapture(ctx, request)
}

func (f *fakeClaimStore) RestoreRootFSFromSnapshot(_ context.Context, request *sandboxstore.RestoreRootFSFromSnapshotRequest) (*sandboxstore.RootFSFilesystem, error) {
	copy := *request
	f.restoreCalls = append(f.restoreCalls, &copy)
	return &sandboxstore.RootFSFilesystem{ID: request.SandboxID}, nil
}

type fakePlanner struct {
	requests []runtimeslotclaim.Request
	err      error
	result   *runtimeslotclaim.Result
}

func (f *fakePlanner) Claim(_ context.Context, request runtimeslotclaim.Request) (*runtimeslotclaim.Result, error) {
	f.requests = append(f.requests, request)
	if f.err != nil {
		return nil, f.err
	}
	if f.result != nil {
		return f.result, nil
	}
	return &runtimeslotclaim.Result{
		Slot: &sandboxstore.RuntimeSlot{
			ID: "slot-1", AllocationID: "allocation-1", AllocationNamespace: "default",
		},
		Grant:        &sandboxstore.RootFSWriterGrant{ID: "grant-1"},
		Stage:        rootfshandoff.StageRequest{},
		ProcdAddress: "http://10.0.0.8:49983", Duration: 420 * time.Millisecond, WithinSLO: true,
	}, nil
}

func TestServiceClaimsRetryStableNomadSlotEndToEnd(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	startedAt := fixture.now.Add(-200 * time.Millisecond)
	request := &service.ClaimRequest{
		TeamID: "team-1", UserID: "user-1", Template: "DEFAULT",
		OperationID: "operation-1", StartedAt: startedAt,
		Config: &sandboxstore.SandboxConfig{
			EnvVars: map[string]string{"REQUEST": "yes", "SHARED": "request"},
			Network: &v1alpha1.SandboxNetworkPolicy{Mode: v1alpha1.NetworkModeBlockAll},
		},
	}

	response, err := fixture.service.ClaimSandbox(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	expectedID, err := naming.SandboxNameForOperation("cluster-1", "default", "operation-1")
	if err != nil {
		t.Fatal(err)
	}
	if response.SandboxID != expectedID || response.ProcdAddress != "http://10.0.0.8:49983" ||
		response.RuntimeID != "allocation-1" || response.ClusterId == nil || *response.ClusterId != "cluster-1" ||
		response.CommandReadyDuration != 420*time.Millisecond || !response.CommandReadyWithinSLO {
		t.Fatalf("response = %+v", response)
	}
	if len(fixture.planner.requests) != 1 {
		t.Fatalf("planner calls = %d", len(fixture.planner.requests))
	}
	planned := fixture.planner.requests[0]
	if planned.OperationID != "operation-1" || planned.SandboxID != expectedID ||
		planned.CompatibilityDigest != fixture.runtimeClass.CompatibilityDigest ||
		!planned.StartedAt.Equal(startedAt) {
		t.Fatalf("planner request = %+v", planned)
	}
	if planned.Runtime.EnvVars["TEMPLATE"] != "yes" || planned.Runtime.EnvVars["MAIN"] != "yes" ||
		planned.Runtime.EnvVars["REQUEST"] != "yes" || planned.Runtime.EnvVars["SHARED"] != "request" ||
		planned.Runtime.EnvVars[runtimecontrol.EnvSandboxID] != expectedID {
		t.Fatalf("runtime environment = %+v", planned.Runtime.EnvVars)
	}
	policy, err := v1alpha1.ParseNetworkPolicyFromAnnotationStrict(planned.NetworkPolicy)
	if err != nil {
		t.Fatal(err)
	}
	if policy.SandboxID != expectedID || policy.TeamID != "team-1" || policy.Mode != v1alpha1.NetworkModeBlockAll {
		t.Fatalf("network policy = %+v", policy)
	}
	if len(fixture.store.ensureCalls) != 1 || fixture.store.ensureCalls[0].SandboxID != expectedID ||
		fixture.store.ensureCalls[0].BaseArtifactDigest != fixture.store.artifact.ArtifactDigest {
		t.Fatalf("initial RootFS calls = %+v", fixture.store.ensureCalls)
	}
	record := fixture.store.records[expectedID]
	if record == nil || record.RuntimeID != "allocation-1" || record.RuntimeNamespace != "default" ||
		record.RuntimeGeneration != 1 || record.ExpiresAt.Sub(fixture.now) != time.Hour {
		t.Fatalf("persisted sandbox = %+v", record)
	}

	retried, err := fixture.service.ClaimSandbox(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if retried.SandboxID != response.SandboxID || len(fixture.planner.requests) != 2 ||
		fixture.planner.requests[1].OperationID != fixture.planner.requests[0].OperationID {
		t.Fatalf("retry response=%+v requests=%+v", retried, fixture.planner.requests)
	}
}

func TestServiceClaimsAndResumesWithExternalCredentialBindings(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	binding := v1alpha1.CredentialBinding{
		Ref: "api-auth", SourceRef: "team-api-source",
		Projection: v1alpha1.ProjectionSpec{
			Type: v1alpha1.CredentialProjectionTypeHTTPHeaders,
			HTTPHeaders: &v1alpha1.HTTPHeadersProjection{Headers: []v1alpha1.ProjectedHeader{{
				Name: "Authorization", ValueTemplate: "Bearer {{.token}}",
			}}},
		},
	}
	request := &service.ClaimRequest{
		TeamID: "team-1", UserID: "user-1", Template: "default", OperationID: "operation-credential",
		Config: &sandboxstore.SandboxConfig{Network: &v1alpha1.SandboxNetworkPolicy{
			Mode: v1alpha1.NetworkModeBlockAll,
			Egress: &v1alpha1.NetworkEgressPolicy{CredentialRules: []v1alpha1.EgressCredentialRule{{
				Name: "api", CredentialRef: binding.Ref, Protocol: v1alpha1.EgressAuthProtocolHTTP,
				Domains: []string{"api.example.com"},
			}}},
			CredentialBindings: []v1alpha1.CredentialBinding{binding},
		}},
	}
	response, err := fixture.service.ClaimSandbox(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	record := fixture.store.records[response.SandboxID]
	if record == nil || record.Config.Network == nil || record.Config.Network.CredentialBindings != nil {
		t.Fatalf("persisted sandbox credential policy = %+v", record)
	}
	stored := fixture.store.credentialBindings[response.SandboxID]
	if len(stored) != 1 || stored[0].SourceRef != binding.SourceRef {
		t.Fatalf("external credential bindings = %+v", stored)
	}
	parsed, err := v1alpha1.ParseNetworkPolicyFromAnnotationStrict(fixture.planner.requests[0].NetworkPolicy)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.CredentialBindingDigest != credentialbinding.DigestPublic([]v1alpha1.CredentialBinding{binding}) {
		t.Fatalf("network credential digest = %q", parsed.CredentialBindingDigest)
	}

	changed := *request
	changed.Config = service.CloneSandboxConfig(request.Config)
	changed.Config.Network = request.Config.Network.DeepCopy()
	changed.Config.Network.CredentialBindings[0].SourceRef = "another-source"
	if _, err := fixture.service.ClaimSandbox(context.Background(), &changed); !errors.Is(err, service.ErrClaimConflict) {
		t.Fatalf("changed credential retry error = %v", err)
	}

	record.DesiredState = sandboxstore.SandboxDesiredStatePaused
	record.RuntimeID = ""
	record.RuntimeNamespace = ""
	fixture.store.records[response.SandboxID] = record
	fixture.store.resumeCandidate = &sandboxstore.NomadSandboxResumeCandidate{
		SandboxID: response.SandboxID, OperationID: "resume-credential", RuntimeGeneration: 2,
		LifecyclePhase: sandboxstore.SandboxLifecyclePhasePreparing,
		FilesystemID:   "filesystem-credential", SourceGenerationID: "generation-credential",
		Record: cloneClaimRecord(record),
	}
	fixture.planner.requests = nil
	if _, err := fixture.service.ResumePausedSandboxRuntime(context.Background(), response.SandboxID); err != nil {
		t.Fatal(err)
	}
	if len(fixture.planner.requests) != 1 {
		t.Fatalf("resume planner requests = %d", len(fixture.planner.requests))
	}
	resumedPolicy, err := v1alpha1.ParseNetworkPolicyFromAnnotationStrict(fixture.planner.requests[0].NetworkPolicy)
	if err != nil {
		t.Fatal(err)
	}
	if resumedPolicy.CredentialBindingDigest != parsed.CredentialBindingDigest {
		t.Fatalf("resume credential digest = %q, want %q",
			resumedPolicy.CredentialBindingDigest, parsed.CredentialBindingDigest)
	}
}

func TestServiceRejectsChangedRetryBinding(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	request := &service.ClaimRequest{
		TeamID: "team-1", UserID: "user-1", Template: "default", OperationID: "operation-1",
		Config: &sandboxstore.SandboxConfig{EnvVars: map[string]string{"VALUE": "first"}},
	}
	if _, err := fixture.service.ClaimSandbox(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	request.Config.EnvVars["VALUE"] = "changed"
	_, err := fixture.service.ClaimSandbox(context.Background(), request)
	if !errors.Is(err, service.ErrClaimConflict) {
		t.Fatalf("changed retry error = %v, want claim conflict", err)
	}
	if len(fixture.planner.requests) != 1 {
		t.Fatalf("planner calls = %d, want 1", len(fixture.planner.requests))
	}
}

func TestServiceTerminateSandboxUsesDurableClaimCleanup(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	response, err := fixture.service.ClaimSandbox(context.Background(), &service.ClaimRequest{
		TeamID: "team-1", UserID: "user-1", Template: "default", OperationID: "operation-delete",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.service.TerminateSandbox(context.Background(), response.SandboxID); err != nil {
		t.Fatal(err)
	}
	if got := fixture.store.cleanupCalls; len(got) != 1 || got[0] != response.SandboxID+":sandbox deletion requested" {
		t.Fatalf("cleanup calls = %v", got)
	}
	if record := fixture.store.records[response.SandboxID]; record == nil || record.DesiredState != sandboxstore.SandboxDesiredStateTerminating ||
		fixture.store.claimPhases[response.SandboxID] != sandboxstore.SandboxRuntimeClaimPhaseCleanupPending {
		t.Fatalf("record=%+v phase=%q", record, fixture.store.claimPhases[response.SandboxID])
	}

	fixture.store.cleanupErr = errors.New("database unavailable")
	if err := fixture.service.TerminateSandbox(context.Background(), response.SandboxID); err == nil ||
		!strings.Contains(err.Error(), "request Nomad sandbox cleanup") {
		t.Fatalf("termination error = %v", err)
	}
}

type recordingPauseEnqueuer struct {
	sandboxIDs []string
}

func (r *recordingPauseEnqueuer) EnqueueSandboxPause(sandboxID string) {
	r.sandboxIDs = append(r.sandboxIDs, sandboxID)
}

func TestServiceRequestsPlannedPauseAndStopsExactAllocation(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	fixture.store.pauseCandidate = &sandboxstore.NomadSandboxPauseCandidate{
		SandboxID: "sandbox-1", OperationID: "retire-1", Source: sandboxstore.SandboxLifecycleSourceManual,
		ClaimOperationID: "claim-operation-1", ClaimID: "claim-1", SlotID: "slot-1",
		ClusterID: "cluster-1", AllocationID: "allocation-1", AllocationNamespace: "nomad", NodeID: "node-1",
		NodeUID: "node-uid-1", NodeBootID: "boot-1", WriterGrantID: "grant-1", WriterEpoch: 7,
		BindingVersion: sandboxstore.RootFSWriterBindingVersion, BindingDigest: bytes.Repeat([]byte{0x41}, 32),
	}
	enqueuer := &recordingPauseEnqueuer{}
	fixture.service.SetPauseEnqueuer(enqueuer)

	response, err := fixture.service.PauseSandboxAndWait(context.Background(), "sandbox-1")
	if err != nil {
		t.Fatal(err)
	}
	if response.Paused || response.Status != managerapi.SandboxStatusStarting {
		t.Fatalf("pause response = %+v", response)
	}
	if len(enqueuer.sandboxIDs) != 1 || enqueuer.sandboxIDs[0] != "sandbox-1" {
		t.Fatalf("pause enqueues = %v", enqueuer.sandboxIDs)
	}
	require.Equal(t, []string{"sandbox-1", "sandbox-1"}, fixture.store.pauseContinueCalls)
	if len(fixture.allocation.requests) != 1 {
		t.Fatalf("allocation stops = %+v", fixture.allocation.requests)
	}
	if len(fixture.plannedRetire.requests) != 1 || len(fixture.store.quiesceCalls) != 1 {
		t.Fatalf("planned-retire/quiesce calls = %+v / %+v", fixture.plannedRetire.requests, fixture.store.quiesceCalls)
	}
	if !reflect.DeepEqual(*fixture.pauseOrder, []string{"plan", "quiesce", "stop"}) {
		t.Fatalf("unsafe pause order = %v", *fixture.pauseOrder)
	}
	stop := fixture.allocation.requests[0]
	if stop.OperationID != "retire-1" || stop.Target.AllocationID != "allocation-1" ||
		stop.Target.AllocationNamespace != "nomad" || stop.Target.NodeID != "node-1" {
		t.Fatalf("allocation stop = %+v", stop)
	}
}

func TestServiceDropsStalePauseCompletionAfterResume(t *testing.T) {
	for name, staleErr := range map[string]error{
		"resumed": sandboxstore.ErrNomadSandboxPauseNotPending,
		"deleted": sandboxstore.ErrSandboxRecordNotFound,
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newClaimServiceFixture(t)
			fixture.store.pauseContinueErr = staleErr

			require.NoError(t, fixture.service.CompletePausingSandboxRuntime(t.Context(), "sandbox-1"))
			require.Equal(t, []string{"sandbox-1"}, fixture.store.pauseContinueCalls)
			require.Empty(t, fixture.store.pauseSources)
			require.Empty(t, fixture.plannedRetire.requests)
			require.Empty(t, fixture.store.quiesceCalls)
			require.Empty(t, fixture.allocation.requests)
		})
	}
}

func TestServiceDoesNotQuiesceAfterPlannedRetireResponseLoss(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	fixture.store.pauseCandidate = &sandboxstore.NomadSandboxPauseCandidate{
		SandboxID: "sandbox-1", OperationID: "retire-1", Source: sandboxstore.SandboxLifecycleSourceManual,
		ClaimOperationID: "claim-operation-1", ClaimID: "claim-1", SlotID: "slot-1",
		ClusterID: "cluster-1", AllocationID: "allocation-1", AllocationNamespace: "nomad", NodeID: "node-1",
		NodeUID: "node-uid-1", NodeBootID: "boot-1", WriterGrantID: "grant-1", WriterEpoch: 7,
		BindingVersion: sandboxstore.RootFSWriterBindingVersion, BindingDigest: bytes.Repeat([]byte{0x43}, 32),
	}
	fixture.plannedRetire.err = errors.New("planned-retire response lost")

	err := fixture.service.CompletePausingSandboxRuntime(t.Context(), "sandbox-1")
	require.ErrorContains(t, err, "planned-retire response lost")
	require.Len(t, fixture.plannedRetire.requests, 1)
	require.Empty(t, fixture.store.quiesceCalls)
	require.Empty(t, fixture.allocation.requests)
	require.Equal(t, []string{"plan"}, *fixture.pauseOrder)

	fixture.plannedRetire.err = nil
	require.ErrorIs(t, fixture.service.CompletePausingSandboxRuntime(t.Context(), "sandbox-1"), errNomadSandboxPausePending)
	require.Len(t, fixture.plannedRetire.requests, 2)
	require.Equal(t, fixture.plannedRetire.requests[0], fixture.plannedRetire.requests[1])
	require.Equal(t, fixture.plannedRetire.targets[0], fixture.plannedRetire.targets[1])
	require.Len(t, fixture.store.quiesceCalls, 1)
	require.Len(t, fixture.allocation.requests, 1)
	require.Equal(t, []string{"plan", "plan", "quiesce", "stop"}, *fixture.pauseOrder)
}

func TestServiceRetriesExactPauseAfterAllocationStopResponseLoss(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	fixture.store.pauseCandidate = &sandboxstore.NomadSandboxPauseCandidate{
		SandboxID: "sandbox-1", OperationID: "retire-1", Source: sandboxstore.SandboxLifecycleSourceManual,
		ClaimOperationID: "claim-operation-1", ClaimID: "claim-1", SlotID: "slot-1",
		ClusterID: "cluster-1", AllocationID: "allocation-1", AllocationNamespace: "nomad", NodeID: "node-1",
		NodeUID: "node-uid-1", NodeBootID: "boot-1", WriterGrantID: "grant-1", WriterEpoch: 7,
		BindingVersion: sandboxstore.RootFSWriterBindingVersion, BindingDigest: bytes.Repeat([]byte{0x44}, 32),
	}
	fixture.allocation.err = errors.New("allocation stop response lost")

	err := fixture.service.CompletePausingSandboxRuntime(t.Context(), "sandbox-1")
	require.ErrorContains(t, err, "allocation stop response lost")
	require.Len(t, fixture.plannedRetire.requests, 1)
	require.Len(t, fixture.store.quiesceCalls, 1)
	require.Len(t, fixture.allocation.requests, 1)
	require.Equal(t, []string{"plan", "quiesce", "stop"}, *fixture.pauseOrder)

	fixture.allocation.err = nil
	require.ErrorIs(t, fixture.service.CompletePausingSandboxRuntime(t.Context(), "sandbox-1"), errNomadSandboxPausePending)
	require.Len(t, fixture.plannedRetire.requests, 2)
	require.Equal(t, fixture.plannedRetire.requests[0], fixture.plannedRetire.requests[1])
	require.Len(t, fixture.store.quiesceCalls, 2)
	require.Equal(t, fixture.store.quiesceCalls[0], fixture.store.quiesceCalls[1])
	require.Len(t, fixture.allocation.requests, 2)
	require.Equal(t, fixture.allocation.requests[0], fixture.allocation.requests[1])
	require.Equal(t, []string{"plan", "quiesce", "stop", "plan", "quiesce", "stop"}, *fixture.pauseOrder)
}

func TestServiceAutomaticPausePersistsAutoSourceBeforeStop(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	fixture.store.pauseCandidate = &sandboxstore.NomadSandboxPauseCandidate{
		SandboxID: "sandbox-1", OperationID: "retire-auto-1", Source: sandboxstore.SandboxLifecycleSourceAuto,
		ClaimOperationID: "claim-operation-1", ClaimID: "claim-1", SlotID: "slot-1",
		ClusterID: "cluster-1", AllocationID: "allocation-1", AllocationNamespace: "nomad", NodeID: "node-1",
		NodeUID: "node-uid-1", NodeBootID: "boot-1", WriterGrantID: "grant-1", WriterEpoch: 7,
		BindingVersion: sandboxstore.RootFSWriterBindingVersion, BindingDigest: bytes.Repeat([]byte{0x42}, 32),
	}
	enqueuer := &recordingPauseEnqueuer{}
	fixture.service.SetPauseEnqueuer(enqueuer)

	if err := fixture.service.PauseSandboxByID(context.Background(), "sandbox-1"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fixture.store.pauseSources, []string{"sandbox-1:auto"}) {
		t.Fatalf("pause sources = %v", fixture.store.pauseSources)
	}
	if len(enqueuer.sandboxIDs) != 1 || len(fixture.allocation.requests) != 1 {
		t.Fatalf("enqueues=%v allocation stops=%+v", enqueuer.sandboxIDs, fixture.allocation.requests)
	}
}

func TestServiceAutomaticPauseTreatsRefreshedTTLAsNoOp(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	fixture.store.pauseErr = sandboxstore.ErrNomadSandboxTTLNotExpired
	enqueuer := &recordingPauseEnqueuer{}
	fixture.service.SetPauseEnqueuer(enqueuer)

	require.NoError(t, fixture.service.PauseSandboxByID(t.Context(), "sandbox-1"))
	require.Empty(t, enqueuer.sandboxIDs)
	require.Empty(t, fixture.allocation.requests)
}

func TestServiceAutomaticPauseSurfacesHardTTLRace(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	fixture.store.pauseErr = sandboxstore.ErrNomadSandboxHardTTLExpired

	err := fixture.service.PauseSandboxByID(t.Context(), "sandbox-1")
	require.ErrorIs(t, err, sandboxstore.ErrNomadSandboxHardTTLExpired)
}

func TestServiceHardExpiryTerminationUsesExactStoreBoundary(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	fixture.store.records["sandbox-1"] = &sandboxstore.SandboxRecord{
		ID: "sandbox-1", DesiredState: sandboxstore.SandboxDesiredStateActive,
	}
	fixture.store.cleanupErr = sandboxstore.ErrNomadSandboxHardTTLNotExpired
	require.NoError(t, fixture.service.TerminateHardExpiredSandbox(t.Context(), "sandbox-1"))

	fixture.store.cleanupErr = nil
	require.NoError(t, fixture.service.TerminateHardExpiredSandbox(t.Context(), "sandbox-1"))
	require.Equal(t, []string{
		"sandbox-1:sandbox hard TTL expired",
		"sandbox-1:sandbox hard TTL expired",
	}, fixture.store.cleanupCalls)
}

func TestServicePressurePauseReturnsDurableOperationBeforeNomadStop(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	fixture.store.pressurePause = &sandboxstore.NomadSandboxPauseCandidate{
		SandboxID: "sandbox-1", OperationID: "retire-pressure-1",
		WriterGrantID: "grant-1", WriterGrantState: sandboxstore.RootFSWriterGrantStateConsumed,
	}
	enqueuer := &recordingPauseEnqueuer{}
	fixture.service.SetPauseEnqueuer(enqueuer)
	request := &sandboxstore.RootFSWriterPressurePauseRequest{
		SandboxID: "sandbox-1", GrantID: "grant-1", WriterEpoch: 7,
		BindingVersion: sandboxstore.RootFSWriterBindingVersion,
		BindingDigest:  make([]byte, 32), NodeUID: "node-uid",
	}
	operationID, err := fixture.service.RequestRootFSWriterPressurePause(context.Background(), request)
	if err != nil || operationID != "retire-pressure-1" {
		t.Fatalf("operation=%q error=%v", operationID, err)
	}
	if len(enqueuer.sandboxIDs) != 1 || enqueuer.sandboxIDs[0] != "sandbox-1" {
		t.Fatalf("pause enqueues = %v", enqueuer.sandboxIDs)
	}
	if len(fixture.allocation.requests) != 0 {
		t.Fatalf("regional response did not precede external Nomad stop: %+v", fixture.allocation.requests)
	}
	if len(fixture.store.pressureRequests) != 1 ||
		fixture.store.pressureRequests[0].GrantID != request.GrantID ||
		fixture.store.pressureRequests[0].WriterEpoch != request.WriterEpoch {
		t.Fatalf("pressure requests = %+v", fixture.store.pressureRequests)
	}
}

func TestServiceKeepsCommittedPausePendingUntilSlotIsTerminal(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	fixture.store.pauseCandidate = &sandboxstore.NomadSandboxPauseCandidate{
		SandboxID: "sandbox-1", AlreadyPaused: true,
		ClaimOperationID: "claim-operation-1", ClaimID: "claim-1", SlotID: "slot-1",
	}

	require.ErrorIs(t, fixture.service.CompletePausingSandboxRuntime(context.Background(), "sandbox-1"), errNomadSandboxPausePending)
	if len(fixture.store.quiesceCalls) != 1 || fixture.store.quiesceCalls[0].SlotID != "slot-1" {
		t.Fatalf("quiesce calls = %+v", fixture.store.quiesceCalls)
	}
	if len(fixture.allocation.requests) != 0 {
		t.Fatalf("committed pause stopped allocation again: %+v", fixture.allocation.requests)
	}

	fixture.store.pauseCandidate.SlotID = ""
	require.NoError(t, fixture.service.CompletePausingSandboxRuntime(context.Background(), "sandbox-1"))
}

func TestServiceResumesPausedNomadSandboxThroughDurableSlotClaim(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	sandboxID := preparePausedNomadResume(t, fixture)
	fixture.quotaLimits.limit = &quota.Limit{
		TeamID: "team-1", Dimension: quota.DimensionActiveSandboxes, LimitValue: 7,
	}

	response, err := fixture.service.ResumeSandboxAndWait(context.Background(), sandboxID)
	if err != nil {
		t.Fatal(err)
	}
	if response.SandboxID != sandboxID || !response.Resumed {
		t.Fatalf("resume response = %+v", response)
	}
	if len(fixture.store.resumeRequests) != 1 || fixture.store.resumeRequests[0].SandboxID != sandboxID ||
		fixture.store.resumeRequests[0].ExpectedTeamID != "team-1" ||
		fixture.store.resumeRequests[0].ActiveSandboxLimit == nil ||
		*fixture.store.resumeRequests[0].ActiveSandboxLimit != 7 {
		t.Fatalf("resume requests = %+v", fixture.store.resumeRequests)
	}
	if len(fixture.planner.requests) != 1 {
		t.Fatalf("planner requests = %+v", fixture.planner.requests)
	}
	planned := fixture.planner.requests[0]
	if planned.OperationID != fixture.store.resumeCandidate.OperationID || planned.SandboxID != sandboxID ||
		planned.TeamID != "team-1" || planned.UserID != "user-1" ||
		planned.ClusterID != fixture.runtimeClass.ClusterID ||
		planned.CompatibilityDigest != fixture.runtimeClass.CompatibilityDigest ||
		planned.Runtime.RuntimeGeneration != 2 || planned.Runtime.ResetCopiedSessionState ||
		!planned.StartedAt.Equal(fixture.now) {
		t.Fatalf("resume planner request = %+v", planned)
	}
	if planned.Runtime.EnvVars["TEMPLATE"] != "yes" || planned.Runtime.EnvVars["MAIN"] != "yes" ||
		planned.Runtime.EnvVars[runtimecontrol.EnvSandboxID] != sandboxID {
		t.Fatalf("resume runtime assignment = %+v", planned.Runtime)
	}
	policy, err := v1alpha1.ParseNetworkPolicyFromAnnotationStrict(planned.NetworkPolicy)
	if err != nil {
		t.Fatal(err)
	}
	if policy.SandboxID != sandboxID || policy.TeamID != "team-1" || policy.Mode != v1alpha1.NetworkModeAllowAll {
		t.Fatalf("resume network policy = %+v", policy)
	}
	if len(fixture.store.resumeCompleteCalls) != 1 ||
		fixture.store.resumeCompleteCalls[0].OperationID != fixture.store.resumeCandidate.OperationID ||
		fixture.store.resumeCompleteCalls[0].SlotID != "slot-1" {
		t.Fatalf("resume completions = %+v", fixture.store.resumeCompleteCalls)
	}
	record := fixture.store.records[sandboxID]
	if record.DesiredState != sandboxstore.SandboxDesiredStateActive || record.RuntimeGeneration != 2 ||
		record.RuntimeID != "allocation-1" || record.RuntimeNamespace != "default" {
		t.Fatalf("resumed record = %+v", record)
	}
}

func TestServiceResetsCopiedSessionStateOnFirstForkResume(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	sandboxID := preparePausedNomadResume(t, fixture)
	fixture.store.records[sandboxID].RuntimeGeneration = 0
	fixture.store.resumeCandidate.Record.RuntimeGeneration = 0
	fixture.store.resumeCandidate.RuntimeGeneration = 1

	response, err := fixture.service.ResumeSandboxAndWait(context.Background(), sandboxID)
	if err != nil {
		t.Fatal(err)
	}
	if response.SandboxID != sandboxID || len(fixture.planner.requests) != 1 {
		t.Fatalf("resume response=%+v planner=%+v", response, fixture.planner.requests)
	}
	planned := fixture.planner.requests[0]
	if planned.Runtime.RuntimeGeneration != 1 || !planned.Runtime.ResetCopiedSessionState {
		t.Fatalf("first fork resume assignment = %+v", planned.Runtime)
	}
	if fixture.store.records[sandboxID].RuntimeGeneration != 1 {
		t.Fatalf("fork runtime generation = %d", fixture.store.records[sandboxID].RuntimeGeneration)
	}
}

func TestServiceProjectsResumedAndAlreadyActiveNomadRuntime(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	sandboxID := preparePausedNomadResume(t, fixture)

	resumed, err := fixture.service.ResumePausedSandboxRuntime(context.Background(), sandboxID)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.ID != sandboxID || resumed.Status != managerapi.SandboxStatusRunning || resumed.Paused ||
		resumed.InternalAddr != "http://10.0.0.8:49983" || resumed.RuntimeID != "allocation-1" ||
		resumed.RuntimeGeneration != 2 {
		t.Fatalf("resumed projection = %+v", resumed)
	}

	alreadyActive, err := fixture.service.ResumePausedSandboxRuntime(context.Background(), sandboxID)
	if err != nil {
		t.Fatal(err)
	}
	if alreadyActive.InternalAddr != resumed.InternalAddr || alreadyActive.RuntimeGeneration != 2 ||
		len(fixture.planner.requests) != 1 || len(fixture.store.resumeCompleteCalls) != 1 ||
		len(fixture.store.resumeRequests) != 1 || len(fixture.store.resumeRetryRequests) != 2 {
		t.Fatalf("already-active projection=%+v planner=%d complete=%d request=%d retry=%d",
			alreadyActive, len(fixture.planner.requests), len(fixture.store.resumeCompleteCalls),
			len(fixture.store.resumeRequests), len(fixture.store.resumeRetryRequests))
	}
}

func TestServiceAbortsFailedNomadResumeBeforeStartingANewAttempt(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	sandboxID := preparePausedNomadResume(t, fixture)
	fixture.quotaLimits.limit = &quota.Limit{
		TeamID: "team-1", Dimension: quota.DimensionActiveSandboxes, LimitValue: 1,
	}
	fixture.planner.err = errors.New("node channel unavailable")
	if _, err := fixture.service.ResumeSandboxAndWait(context.Background(), sandboxID); err == nil {
		t.Fatal("initial resume unexpectedly succeeded")
	}
	if len(fixture.store.resumeAbortCalls) != 1 ||
		fixture.store.resumeAbortCalls[0][0] != sandboxID ||
		fixture.store.resumeAbortCalls[0][1] != "nomad-resume-operation-1" ||
		!strings.Contains(fixture.store.resumeAbortCalls[0][2], "node channel unavailable") {
		t.Fatalf("resume abort calls = %+v", fixture.store.resumeAbortCalls)
	}
	fixture.planner.err = nil
	fixture.store.resumeCandidate.OperationID = "nomad-resume-operation-2"

	response, err := fixture.service.ResumeSandboxAndWait(context.Background(), sandboxID)
	if err != nil {
		t.Fatal(err)
	}
	if response.SandboxID != sandboxID || len(fixture.store.resumeRequests) != 2 ||
		len(fixture.store.resumeRetryRequests) != 2 || len(fixture.quotaLimits.calls) != 2 ||
		len(fixture.planner.requests) != 2 || len(fixture.store.resumeCompleteCalls) != 1 {
		t.Fatalf("response=%+v requests=%d retries=%d quota=%d planner=%d complete=%d",
			response, len(fixture.store.resumeRequests), len(fixture.store.resumeRetryRequests),
			len(fixture.quotaLimits.calls), len(fixture.planner.requests), len(fixture.store.resumeCompleteCalls))
	}
}

func TestServiceSurfacesResumeAbortFailureWithPlannerError(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	sandboxID := preparePausedNomadResume(t, fixture)
	fixture.planner.err = errors.New("command-ready probe failed")
	fixture.store.resumeAbortErr = errors.New("database unavailable")

	_, err := fixture.service.ResumeSandboxAndWait(context.Background(), sandboxID)
	if err == nil || !strings.Contains(err.Error(), "command-ready probe failed") ||
		!strings.Contains(err.Error(), "database unavailable") {
		t.Fatalf("joined resume failure = %v", err)
	}
	if len(fixture.store.resumeAbortCalls) != 1 || len(fixture.store.resumeCompleteCalls) != 0 {
		t.Fatalf("abort=%+v complete=%+v", fixture.store.resumeAbortCalls, fixture.store.resumeCompleteCalls)
	}
}

func TestServiceTruncatesResumeAbortReasonAtAValidUTF8Boundary(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	sandboxID := preparePausedNomadResume(t, fixture)
	fixture.planner.err = errors.New(strings.Repeat("界", 1_000))

	if _, err := fixture.service.ResumeSandboxAndWait(context.Background(), sandboxID); err == nil {
		t.Fatal("resume unexpectedly succeeded")
	}
	if len(fixture.store.resumeAbortCalls) != 1 {
		t.Fatalf("resume abort calls = %+v", fixture.store.resumeAbortCalls)
	}
	reason := fixture.store.resumeAbortCalls[0][2]
	if len(reason) > 2_048 || !utf8.ValidString(reason) {
		t.Fatalf("resume abort reason bytes=%d validUTF8=%t", len(reason), utf8.ValidString(reason))
	}
}

func TestServiceMapsNomadResumeUnavailableConflictAndQuotaErrors(t *testing.T) {
	t.Run("warm slot unavailable", func(t *testing.T) {
		fixture := newClaimServiceFixture(t)
		sandboxID := preparePausedNomadResume(t, fixture)
		fixture.planner.err = sandboxstore.ErrRuntimeSlotUnavailable
		_, err := fixture.service.ResumeSandboxAndWait(context.Background(), sandboxID)
		if !errors.Is(err, service.ErrSandboxLifecycleUnavailable) {
			t.Fatalf("resume error = %v", err)
		}
		if len(fixture.store.resumeCompleteCalls) != 0 {
			t.Fatalf("resume completed without a slot: %+v", fixture.store.resumeCompleteCalls)
		}
		if len(fixture.store.resumeAbortCalls) != 1 {
			t.Fatalf("resume abort calls = %+v", fixture.store.resumeAbortCalls)
		}
	})

	t.Run("planner returned no exact binding", func(t *testing.T) {
		fixture := newClaimServiceFixture(t)
		sandboxID := preparePausedNomadResume(t, fixture)
		fixture.planner.result = &runtimeslotclaim.Result{}
		_, err := fixture.service.ResumeSandboxAndWait(context.Background(), sandboxID)
		if !errors.Is(err, service.ErrSandboxLifecycleUnavailable) {
			t.Fatalf("resume error = %v", err)
		}
		if len(fixture.store.resumeAbortCalls) != 1 || len(fixture.store.resumeCompleteCalls) != 0 {
			t.Fatalf("abort=%+v complete=%+v", fixture.store.resumeAbortCalls, fixture.store.resumeCompleteCalls)
		}
	})

	t.Run("durable conflict", func(t *testing.T) {
		fixture := newClaimServiceFixture(t)
		sandboxID := preparePausedNomadResume(t, fixture)
		fixture.store.resumeErr = sandboxstore.ErrNomadSandboxResumeConflict
		_, err := fixture.service.ResumeSandboxAndWait(context.Background(), sandboxID)
		if !apierror.IsConflict(err) {
			t.Fatalf("resume error = %v, want conflict", err)
		}
		if len(fixture.planner.requests) != 0 {
			t.Fatalf("planner requests = %+v", fixture.planner.requests)
		}
	})

	t.Run("quota exceeded", func(t *testing.T) {
		fixture := newClaimServiceFixture(t)
		sandboxID := preparePausedNomadResume(t, fixture)
		fixture.store.resumeErr = &sandboxstore.ActiveSandboxQuotaExceededError{
			TeamID: "team-1", Current: 1, Limit: 1,
		}
		_, err := fixture.service.ResumeSandboxAndWait(context.Background(), sandboxID)
		if !errors.Is(err, service.ErrQuotaExceeded) {
			t.Fatalf("resume error = %v, want quota exceeded", err)
		}
	})
}

func TestServiceValidatesStoredNomadResumeBeforeDurableReservation(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	sandboxID := preparePausedNomadResume(t, fixture)
	fixture.store.records[sandboxID].Config.Services = []managerapi.SandboxAppService{{ID: "unsupported"}}

	_, err := fixture.service.ResumeSandboxAndWait(context.Background(), sandboxID)
	if !errors.Is(err, service.ErrSandboxLifecycleUnavailable) {
		t.Fatalf("resume error = %v, want lifecycle unavailable", err)
	}
	if len(fixture.store.resumeRequests) != 0 || len(fixture.planner.requests) != 0 {
		t.Fatalf("resume reservation=%+v planner=%+v", fixture.store.resumeRequests, fixture.planner.requests)
	}
}

func TestServiceRejectsMismatchedResumeQuotaIdentityBeforeReservation(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	sandboxID := preparePausedNomadResume(t, fixture)
	fixture.quotaLimits.limit = &quota.Limit{
		TeamID: "team-other", Dimension: quota.DimensionActiveSandboxes, LimitValue: 7,
	}

	_, err := fixture.service.ResumeSandboxAndWait(context.Background(), sandboxID)
	if err == nil || !strings.Contains(err.Error(), "quota identity") {
		t.Fatalf("resume error = %v", err)
	}
	if len(fixture.store.resumeRequests) != 0 {
		t.Fatalf("resume requests = %+v", fixture.store.resumeRequests)
	}
}

func TestServiceFailsBeforePersistenceWithoutReadyBaseArtifact(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	fixture.store.artifact = nil
	_, err := fixture.service.ClaimSandbox(context.Background(), &service.ClaimRequest{
		TeamID: "team-1", UserID: "user-1", Template: "default", OperationID: "operation-1",
	})
	if !errors.Is(err, service.ErrDataPlaneNotReady) {
		t.Fatalf("claim error = %v, want data plane not ready", err)
	}
	if fixture.store.writeCount != 0 || len(fixture.planner.requests) != 0 {
		t.Fatalf("side effects: writes=%d planner=%d", fixture.store.writeCount, len(fixture.planner.requests))
	}
}

func TestServiceRejectsBaseArtifactFromDifferentPlatform(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	fixture.store.artifact.Platform.Architecture = "arm64"
	_, err := fixture.service.ClaimSandbox(context.Background(), &service.ClaimRequest{
		TeamID: "team-1", UserID: "user-1", Template: "default", OperationID: "operation-1",
	})
	if !errors.Is(err, service.ErrDataPlaneNotReady) {
		t.Fatalf("claim error = %v, want data plane not ready", err)
	}
	if fixture.store.writeCount != 0 || len(fixture.planner.requests) != 0 {
		t.Fatalf("side effects: writes=%d planner=%d", fixture.store.writeCount, len(fixture.planner.requests))
	}
}

func TestServiceRestoresBlockSnapshotBeforeClaim(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	fixture.store.snapshot = &sandboxstore.RootFSSnapshot{
		ID: "snapshot-1", FilesystemID: "snapshot-filesystem", TeamID: "team-1",
		HeadGenerationID:   "snapshot-generation",
		BaseArtifactDigest: fixture.store.artifact.ArtifactDigest,
		SourceOCIDigest:    fixture.store.artifact.SourceOCIDigest,
		FormatGeneration:   fixture.store.artifact.FormatGeneration,
	}
	fixture.store.generation = &sandboxstore.RootFSGeneration{
		ID: fixture.store.snapshot.HeadGenerationID, FilesystemID: fixture.store.snapshot.FilesystemID,
		SourceOCIDigest:    fixture.store.snapshot.SourceOCIDigest,
		BaseArtifactDigest: fixture.store.snapshot.BaseArtifactDigest,
		FormatGeneration:   fixture.store.snapshot.FormatGeneration,
		DurabilityState:    sandboxstore.RootFSGenerationStateS3Materialized,
	}
	response, err := fixture.service.ClaimSandbox(context.Background(), &service.ClaimRequest{
		TeamID: "team-1", UserID: "user-1", Template: "default",
		OperationID: "operation-snapshot", SnapshotID: "snapshot-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.SandboxID == "" || len(fixture.store.restoreCalls) != 1 ||
		fixture.store.restoreCalls[0].SandboxID != response.SandboxID ||
		fixture.store.restoreCalls[0].OperationID != "operation-snapshot/initial-restore" ||
		fixture.store.restoreCalls[0].InitialClaimOperationID != "operation-snapshot" ||
		len(fixture.store.ensureCalls) != 0 {
		t.Fatalf("restore calls = %+v ensure calls = %+v", fixture.store.restoreCalls, fixture.store.ensureCalls)
	}
	if len(fixture.planner.requests) != 1 || !fixture.planner.requests[0].Runtime.ResetCopiedSessionState {
		t.Fatalf("snapshot runtime assignment = %+v", fixture.planner.requests)
	}
}

func TestServiceRestoresAttestedTemplateRootFSBeforeClaim(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	tpl := fixture.service.templates.(*fakeTemplateStore).template
	tpl.RootFS = &templatepkg.RootFSTemplateSource{
		StorageFormat: templatepkg.RootFSTemplateStorageFormatBlockCOWV1,
		SnapshotID:    "template-build-capture", GenerationID: "captured-generation",
		SourceOCIDigest:    fixture.store.artifact.SourceOCIDigest,
		BaseArtifactDigest: fixture.store.artifact.ArtifactDigest,
		FormatGeneration:   fixture.store.artifact.FormatGeneration,
		Platform:           ocispec.Platform{OS: "linux", Architecture: "amd64"},
	}
	fixture.store.snapshot = &sandboxstore.RootFSSnapshot{
		ID: tpl.RootFS.SnapshotID, FilesystemID: "captured-filesystem",
		TeamID: "team-1", SourceSandboxID: "source-sandbox",
		HeadGenerationID:   tpl.RootFS.GenerationID,
		BaseArtifactDigest: tpl.RootFS.BaseArtifactDigest,
		SourceOCIDigest:    tpl.RootFS.SourceOCIDigest,
		FormatGeneration:   tpl.RootFS.FormatGeneration,
	}
	fixture.store.generation = &sandboxstore.RootFSGeneration{
		ID: tpl.RootFS.GenerationID, FilesystemID: fixture.store.snapshot.FilesystemID,
		SourceOCIDigest:    tpl.RootFS.SourceOCIDigest,
		BaseArtifactDigest: tpl.RootFS.BaseArtifactDigest,
		FormatGeneration:   tpl.RootFS.FormatGeneration,
		DurabilityState:    sandboxstore.RootFSGenerationStateCompositeDurable,
	}

	response, err := fixture.service.ClaimSandbox(context.Background(), &service.ClaimRequest{
		TeamID: "team-1", UserID: "user-1", Template: "default", OperationID: "operation-template-rootfs",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(fixture.store.restoreCalls) != 1 ||
		fixture.store.restoreCalls[0].SandboxID != response.SandboxID ||
		fixture.store.restoreCalls[0].SnapshotID != tpl.RootFS.SnapshotID ||
		len(fixture.store.ensureCalls) != 0 {
		t.Fatalf("restore calls = %+v ensure calls = %+v", fixture.store.restoreCalls, fixture.store.ensureCalls)
	}
	if len(fixture.planner.requests) != 1 || !fixture.planner.requests[0].Runtime.ResetCopiedSessionState {
		t.Fatalf("template RootFS runtime assignment = %+v", fixture.planner.requests)
	}
}

func TestServiceRejectsChangedTemplateRootFSAttestationBeforePersistence(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	tpl := fixture.service.templates.(*fakeTemplateStore).template
	tpl.RootFS = &templatepkg.RootFSTemplateSource{
		StorageFormat: templatepkg.RootFSTemplateStorageFormatBlockCOWV1,
		SnapshotID:    "template-build-capture", GenerationID: "captured-generation",
		SourceOCIDigest:    fixture.store.artifact.SourceOCIDigest,
		BaseArtifactDigest: fixture.store.artifact.ArtifactDigest,
		FormatGeneration:   fixture.store.artifact.FormatGeneration,
		Platform:           ocispec.Platform{OS: "linux", Architecture: "amd64"},
	}
	fixture.store.snapshot = &sandboxstore.RootFSSnapshot{
		ID: tpl.RootFS.SnapshotID, TeamID: "team-1",
		HeadGenerationID: "substituted-generation", BaseArtifactDigest: tpl.RootFS.BaseArtifactDigest,
		SourceOCIDigest: tpl.RootFS.SourceOCIDigest, FormatGeneration: tpl.RootFS.FormatGeneration,
	}

	_, err := fixture.service.ClaimSandbox(context.Background(), &service.ClaimRequest{
		TeamID: "team-1", UserID: "user-1", Template: "default", OperationID: "operation-changed-rootfs",
	})
	if !errors.Is(err, sandboxstore.ErrRootFSGenerationConflict) {
		t.Fatalf("claim error = %v, want generation conflict", err)
	}
	if fixture.store.writeCount != 0 || len(fixture.planner.requests) != 0 {
		t.Fatalf("side effects: writes=%d planner=%d", fixture.store.writeCount, len(fixture.planner.requests))
	}
}

func TestServiceRejectsTemplateSnapshotGenerationFilesystemMismatch(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	tpl := fixture.service.templates.(*fakeTemplateStore).template
	tpl.RootFS = &templatepkg.RootFSTemplateSource{
		StorageFormat: templatepkg.RootFSTemplateStorageFormatBlockCOWV1,
		SnapshotID:    "template-build-capture", GenerationID: "captured-generation",
		SourceOCIDigest:    fixture.store.artifact.SourceOCIDigest,
		BaseArtifactDigest: fixture.store.artifact.ArtifactDigest,
		FormatGeneration:   fixture.store.artifact.FormatGeneration,
		Platform:           ocispec.Platform{OS: "linux", Architecture: "amd64"},
	}
	fixture.store.snapshot = &sandboxstore.RootFSSnapshot{
		ID: tpl.RootFS.SnapshotID, FilesystemID: "substituted-filesystem", TeamID: "team-1",
		HeadGenerationID:   tpl.RootFS.GenerationID,
		BaseArtifactDigest: tpl.RootFS.BaseArtifactDigest,
		SourceOCIDigest:    tpl.RootFS.SourceOCIDigest,
		FormatGeneration:   tpl.RootFS.FormatGeneration,
	}
	fixture.store.generation = &sandboxstore.RootFSGeneration{
		ID: tpl.RootFS.GenerationID, FilesystemID: "captured-filesystem",
		SourceOCIDigest:    tpl.RootFS.SourceOCIDigest,
		BaseArtifactDigest: tpl.RootFS.BaseArtifactDigest,
		FormatGeneration:   tpl.RootFS.FormatGeneration,
		DurabilityState:    sandboxstore.RootFSGenerationStateS3Materialized,
	}

	_, err := fixture.service.ClaimSandbox(context.Background(), &service.ClaimRequest{
		TeamID: "team-1", UserID: "user-1", Template: "default",
		OperationID: "operation-cross-filesystem-rootfs",
	})
	if !errors.Is(err, sandboxstore.ErrRootFSGenerationConflict) {
		t.Fatalf("claim error = %v, want generation conflict", err)
	}
	if fixture.store.writeCount != 0 || len(fixture.planner.requests) != 0 {
		t.Fatalf("side effects: writes=%d planner=%d", fixture.store.writeCount, len(fixture.planner.requests))
	}
}

func TestServiceCapturesPausedNomadTemplateAsBlockGeneration(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	sourceSpec := fixture.service.templates.(*fakeTemplateStore).template.Spec
	fixture.store.records["source-sandbox"] = &sandboxstore.SandboxRecord{
		ID: "source-sandbox", TeamID: "team-1", ClusterID: "cluster-1",
		DesiredState: sandboxstore.SandboxDesiredStatePaused,
		TemplateSpec: sourceSpec,
	}
	fixture.store.generation = &sandboxstore.RootFSGeneration{
		ID: "generation-captured", FilesystemID: "source-sandbox",
		SourceOCIDigest:    fixture.store.artifact.SourceOCIDigest,
		BaseArtifactDigest: fixture.store.artifact.ArtifactDigest,
		FormatGeneration:   fixture.store.artifact.FormatGeneration,
		DurabilityState:    sandboxstore.RootFSGenerationStateCompositeDurable,
	}
	snapshotID := templatepkg.BuildSnapshotID("11111111-1111-1111-1111-111111111111")
	capture, err := fixture.service.EnsureTemplateBuildCapture(
		context.Background(), "source-sandbox", "team-1", snapshotID, sourceSpec,
	)
	if err != nil {
		t.Fatal(err)
	}
	if capture.Version != templatebuild.CaptureMetadataVersion ||
		capture.SnapshotID != snapshotID || capture.HeadGenerationID != fixture.store.generation.ID ||
		capture.SourceOCIDigest != fixture.store.artifact.SourceOCIDigest ||
		capture.BaseArtifactDigest != fixture.store.artifact.ArtifactDigest ||
		capture.Platform.Architecture != "amd64" {
		t.Fatalf("capture = %#v", capture)
	}
	if len(fixture.store.createdSnapshots) != 1 {
		t.Fatalf("snapshot creates = %+v", fixture.store.createdSnapshots)
	}
	if err := fixture.service.DeleteTemplateBuildCapture(context.Background(), snapshotID, "team-1"); err != nil {
		t.Fatal(err)
	}
	if len(fixture.store.deletedSnapshots) != 1 || fixture.store.deletedSnapshots[0] != snapshotID {
		t.Fatalf("snapshot deletions = %v", fixture.store.deletedSnapshots)
	}
}

func TestServiceCapturesActiveNomadTemplateThroughExactWriter(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	sourceSpec := fixture.service.templates.(*fakeTemplateStore).template.Spec
	fixture.store.records["source-sandbox"] = &sandboxstore.SandboxRecord{
		ID: "source-sandbox", TeamID: "team-1", ClusterID: "cluster-1",
		DesiredState:      sandboxstore.SandboxDesiredStateActive,
		RuntimeGeneration: 3, RuntimeNamespace: "nomad", RuntimeID: "allocation-1",
		TemplateSpec: sourceSpec,
	}
	snapshotID := templatepkg.BuildSnapshotID("22222222-2222-2222-2222-222222222222")
	operationID := "nomad-template-capture-" + strings.TrimPrefix(snapshotID, "template-build-")
	targetFilesystemID := sandboxstore.NomadTemplateCaptureFilesystemID(operationID, snapshotID)
	targetGenerationID := sandboxstore.NomadTemplateCaptureGenerationID(operationID, snapshotID)
	binding := sha256.Sum256([]byte("template-capture-binding"))
	fixture.store.templateCaptureCandidate = &sandboxstore.NomadTemplateCaptureCandidate{
		OperationID: operationID, SnapshotID: snapshotID,
		TargetFilesystemID: targetFilesystemID, TargetGenerationID: targetGenerationID,
		Slot: &sandboxstore.RuntimeSlot{
			ID: "slot-1", ClusterID: "cluster-1", AllocationID: "allocation-1",
			NodeID: "node-1", NodeUID: "node-uid-1", NodeBootID: "boot-1",
		},
		SourceFilesystemID: "source-filesystem", SourceGenerationID: "source-generation",
		SourceWriterGrantID: "writer-grant", SourceWriterEpoch: 7,
		BindingVersion: rootfshandoff.WriterBindingVersion, BindingDigest: binding[:],
	}
	currentHead := digest.FromString("template-capture-head").String()
	baseRoot := digest.FromString("template-capture-base").String()
	descriptor := testNomadRebaseDescriptor(t, "template-capture", currentHead)
	proof := rootfshandoff.RunningForkCheckpointProof{
		Version:     rootfshandoff.RunningForkCheckpointVersion,
		OperationID: operationID, SourceSandboxID: "source-sandbox",
		SourceFilesystemID: "source-filesystem", TargetSandboxID: targetFilesystemID,
		SourceWriterGrantID: "writer-grant", SourceWriterEpoch: 7,
		BindingVersion: rootfshandoff.WriterBindingVersion, BindingDigest: hex.EncodeToString(binding[:]),
		ExpectedSourceGenerationID: "source-generation", CheckpointGenerationID: targetGenerationID,
		CheckpointSequence: 1, CheckpointDescriptorDigest: digest.FromBytes(descriptor).String(),
	}
	proofDigest, err := proof.Digest()
	if err != nil {
		t.Fatal(err)
	}
	fixture.runningFork.result = rootfshandoff.RunningForkCheckpointResult{
		Generation: rootfshandoff.GenerationDescriptor{
			Version:      rootfshandoff.GenerationDescriptorVersion,
			GenerationID: targetGenerationID, FilesystemID: targetFilesystemID,
			SourceOCIDigest:    fixture.store.artifact.SourceOCIDigest,
			BaseArtifactDigest: fixture.store.artifact.ArtifactDigest,
			BaseBlockRoot:      baseRoot, CurrentBlockHead: currentHead,
			WriterEpoch: 7, FormatGeneration: fixture.store.artifact.FormatGeneration,
			DurabilityState: sandboxstore.RootFSGenerationStateS3Materialized,
			LocatorVersion:  2, Descriptor: descriptor,
		},
		Proof: proof, ProofDigest: hex.EncodeToString(proofDigest[:]),
	}
	fixture.runningFork.onCall = func() {
		fixture.store.generation = &sandboxstore.RootFSGeneration{
			ID: targetGenerationID, FilesystemID: targetFilesystemID,
			SourceOCIDigest:    fixture.store.artifact.SourceOCIDigest,
			BaseArtifactDigest: fixture.store.artifact.ArtifactDigest,
			BaseBlockRoot:      baseRoot, CurrentBlockHead: currentHead,
			WriterEpoch: 7, FormatGeneration: fixture.store.artifact.FormatGeneration,
			DurabilityState: sandboxstore.RootFSGenerationStateS3Materialized,
			LocatorVersion:  2, Descriptor: descriptor,
		}
		fixture.store.snapshot = &sandboxstore.RootFSSnapshot{
			ID: snapshotID, FilesystemID: targetFilesystemID, TeamID: "team-1",
			SourceSandboxID: "source-sandbox", HeadGenerationID: targetGenerationID,
			BaseArtifactDigest: fixture.store.artifact.ArtifactDigest,
			SourceOCIDigest:    fixture.store.artifact.SourceOCIDigest,
			FormatGeneration:   fixture.store.artifact.FormatGeneration,
			CreatedAt:          time.Unix(200, 0).UTC(),
		}
		fixture.store.templateCaptureCandidate.Completed = true
		fixture.store.templateCaptureCandidate.Snapshot = fixture.store.snapshot
	}

	capture, err := fixture.service.EnsureTemplateBuildCapture(
		context.Background(), "source-sandbox", "team-1", snapshotID, sourceSpec,
	)
	if err != nil {
		t.Fatal(err)
	}
	if capture.HeadGenerationID != targetGenerationID || capture.SnapshotID != snapshotID ||
		len(fixture.runningFork.requests) != 1 || len(fixture.store.templateCaptureRequests) != 2 {
		t.Fatalf("capture=%#v requests=%d store requests=%d", capture,
			len(fixture.runningFork.requests), len(fixture.store.templateCaptureRequests))
	}
	nodeRequest := fixture.runningFork.requests[0]
	if nodeRequest.Fork.TargetSandboxID != targetFilesystemID ||
		nodeRequest.Fork.TargetGenerationID != targetGenerationID ||
		nodeRequest.SourceWriterGrantID != "writer-grant" {
		t.Fatalf("node capture request = %#v", nodeRequest)
	}
}

func TestServiceSnapshotsActiveSandboxAndRecoversLostResponse(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	sourceID := "source-running-snapshot"
	operationID := "operation-running-snapshot"
	snapshotID := runningRootFSSnapshotID(sourceID, operationID)
	expiresAt := fixture.now.Add(24 * time.Hour)
	fixture.store.records[sourceID] = &sandboxstore.SandboxRecord{
		ID: sourceID, TeamID: "team-1", ClusterID: "cluster-1",
		DesiredState:      sandboxstore.SandboxDesiredStateActive,
		RuntimeGeneration: 4, RuntimeNamespace: "nomad", RuntimeID: "allocation-snapshot",
		TemplateSpec: fixture.service.templates.(*fakeTemplateStore).template.Spec,
	}
	targetFilesystemID, targetGenerationID := prepareRunningRootFSCaptureCheckpoint(
		t, &fixture, sourceID, operationID, snapshotID, "release checkpoint", "before migration", expiresAt,
	)
	request := &service.CreateSandboxRootFSSnapshotRequest{
		OperationID: operationID, StartedAt: fixture.now,
		Name: "release checkpoint", Description: "before migration", ExpiresAt: expiresAt,
	}

	// The node committed no callback before the response was lost. PostgreSQL
	// retains the exact writer intent for controller recovery.
	fixture.runningFork.err = errdefs.ErrUnavailable
	publish := fixture.runningFork.onCall
	fixture.runningFork.onCall = nil
	_, err := fixture.service.CreateRunningSandboxRootFSSnapshot(
		t.Context(), sourceID, "team-1", request,
	)
	if !errors.Is(err, service.ErrSandboxCheckpointRequiresCtld) {
		t.Fatalf("initial snapshot error = %v, want checkpoint backend unavailable", err)
	}
	if fixture.store.templateCaptureCandidate.Completed {
		t.Fatal("snapshot published before checkpoint callback")
	}

	fixture.runningFork.err = nil
	fixture.runningFork.onCall = publish
	if err := fixture.service.CompleteSandboxRootFSSnapshot(t.Context(), sourceID); err != nil {
		t.Fatal(err)
	}
	snapshot := fixture.store.templateCaptureCandidate.Snapshot
	if snapshot == nil || snapshot.ID != snapshotID || snapshot.FilesystemID != targetFilesystemID ||
		snapshot.HeadGenerationID != targetGenerationID || snapshot.SourceSandboxID != sourceID ||
		snapshot.Name != request.Name || snapshot.Description != request.Description ||
		!snapshot.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("recovered snapshot = %#v", snapshot)
	}
	if fixture.store.records[sourceID].DesiredState != sandboxstore.SandboxDesiredStateActive ||
		fixture.store.records[sourceID].RuntimeID != "allocation-snapshot" {
		t.Fatalf("source runtime changed during snapshot: %#v", fixture.store.records[sourceID])
	}
	if len(fixture.runningFork.requests) != 2 {
		t.Fatalf("node checkpoint requests = %d, want initial dispatch and recovery", len(fixture.runningFork.requests))
	}

	// An exact ingress retry returns the durable snapshot and never dispatches
	// another writer checkpoint.
	retry, err := fixture.service.CreateRunningSandboxRootFSSnapshot(
		t.Context(), sourceID, "team-1", request,
	)
	if err != nil {
		t.Fatal(err)
	}
	if retry == nil || retry.ID != snapshotID || len(fixture.runningFork.requests) != 2 {
		t.Fatalf("snapshot retry = %#v, node requests = %d", retry, len(fixture.runningFork.requests))
	}
}

func prepareRunningRootFSCaptureCheckpoint(
	t *testing.T,
	fixture *claimServiceFixture,
	sourceID, operationID, snapshotID, name, description string,
	expiresAt time.Time,
) (string, string) {
	t.Helper()
	targetFilesystemID := sandboxstore.NomadTemplateCaptureFilesystemID(operationID, snapshotID)
	targetGenerationID := sandboxstore.NomadTemplateCaptureGenerationID(operationID, snapshotID)
	binding := sha256.Sum256([]byte("running-snapshot-binding-" + operationID))
	fixture.store.templateCaptureCandidate = &sandboxstore.NomadTemplateCaptureCandidate{
		OperationID: operationID, SnapshotID: snapshotID,
		TargetFilesystemID: targetFilesystemID, TargetGenerationID: targetGenerationID,
		Slot: &sandboxstore.RuntimeSlot{
			ID: "slot-snapshot", ClusterID: "cluster-1", AllocationID: "allocation-snapshot",
			NodeID: "node-snapshot", NodeUID: "node-uid-snapshot", NodeBootID: "boot-snapshot",
		},
		SourceFilesystemID: "source-filesystem", SourceGenerationID: "source-generation",
		SourceWriterGrantID: "writer-grant", SourceWriterEpoch: 8,
		BindingVersion: rootfshandoff.WriterBindingVersion, BindingDigest: binding[:],
	}
	currentHead := digest.FromString("running-snapshot-head-" + operationID).String()
	baseRoot := digest.FromString("running-snapshot-base-" + operationID).String()
	descriptor := testNomadRebaseDescriptor(t, "running-snapshot", currentHead)
	proof := rootfshandoff.RunningForkCheckpointProof{
		Version:     rootfshandoff.RunningForkCheckpointVersion,
		OperationID: operationID, SourceSandboxID: sourceID,
		SourceFilesystemID: "source-filesystem", TargetSandboxID: targetFilesystemID,
		SourceWriterGrantID: "writer-grant", SourceWriterEpoch: 8,
		BindingVersion: rootfshandoff.WriterBindingVersion, BindingDigest: hex.EncodeToString(binding[:]),
		ExpectedSourceGenerationID: "source-generation", CheckpointGenerationID: targetGenerationID,
		CheckpointSequence: 1, CheckpointDescriptorDigest: digest.FromBytes(descriptor).String(),
	}
	proofDigest, err := proof.Digest()
	if err != nil {
		t.Fatal(err)
	}
	fixture.runningFork.result = rootfshandoff.RunningForkCheckpointResult{
		Generation: rootfshandoff.GenerationDescriptor{
			Version:      rootfshandoff.GenerationDescriptorVersion,
			GenerationID: targetGenerationID, FilesystemID: targetFilesystemID,
			SourceOCIDigest:    fixture.store.artifact.SourceOCIDigest,
			BaseArtifactDigest: fixture.store.artifact.ArtifactDigest,
			BaseBlockRoot:      baseRoot, CurrentBlockHead: currentHead,
			WriterEpoch: 8, FormatGeneration: fixture.store.artifact.FormatGeneration,
			DurabilityState: sandboxstore.RootFSGenerationStateS3Materialized,
			LocatorVersion:  2, Descriptor: descriptor,
		},
		Proof: proof, ProofDigest: hex.EncodeToString(proofDigest[:]),
	}
	fixture.runningFork.onCall = func() {
		fixture.store.generation = &sandboxstore.RootFSGeneration{
			ID: targetGenerationID, FilesystemID: targetFilesystemID,
			SourceOCIDigest:    fixture.store.artifact.SourceOCIDigest,
			BaseArtifactDigest: fixture.store.artifact.ArtifactDigest,
			BaseBlockRoot:      baseRoot, CurrentBlockHead: currentHead,
			WriterEpoch: 8, FormatGeneration: fixture.store.artifact.FormatGeneration,
			DurabilityState: sandboxstore.RootFSGenerationStateS3Materialized,
			LocatorVersion:  2, Descriptor: descriptor,
		}
		fixture.store.snapshot = &sandboxstore.RootFSSnapshot{
			ID: snapshotID, FilesystemID: targetFilesystemID, TeamID: "team-1",
			SourceSandboxID: sourceID, HeadGenerationID: targetGenerationID,
			BaseArtifactDigest: fixture.store.artifact.ArtifactDigest,
			SourceOCIDigest:    fixture.store.artifact.SourceOCIDigest,
			FormatGeneration:   fixture.store.artifact.FormatGeneration,
			Name:               name, Description: description, CreatedAt: fixture.now, ExpiresAt: expiresAt,
		}
		fixture.store.templateCaptureCandidate.Completed = true
		fixture.store.templateCaptureCandidate.Snapshot = fixture.store.snapshot
	}
	return targetFilesystemID, targetGenerationID
}

func TestServiceMapsUnavailableWarmPoolToRetryableError(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	fixture.planner.err = sandboxstore.ErrRuntimeSlotUnavailable
	_, err := fixture.service.ClaimSandbox(context.Background(), &service.ClaimRequest{
		TeamID: "team-1", UserID: "user-1", Template: "default", OperationID: "operation-1",
	})
	if !errors.Is(err, service.ErrDataPlaneNotReady) {
		t.Fatalf("claim error = %v, want data plane not ready", err)
	}
}

func TestServiceRejectsRetryAfterAbandonedClaimCleanupFence(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	fixture.planner.err = errors.New("node channel unavailable")
	request := &service.ClaimRequest{
		TeamID: "team-1", UserID: "user-1", Template: "default", OperationID: "operation-1",
	}
	if _, err := fixture.service.ClaimSandbox(context.Background(), request); err == nil {
		t.Fatal("initial failed claim returned no error")
	}
	if len(fixture.store.records) != 1 {
		t.Fatalf("reserved records = %d", len(fixture.store.records))
	}
	for sandboxID := range fixture.store.records {
		fixture.store.claimPhases[sandboxID] = sandboxstore.SandboxRuntimeClaimPhaseCleanupPending
	}
	fixture.planner.err = nil
	_, err := fixture.service.ClaimSandbox(context.Background(), request)
	if !errors.Is(err, service.ErrClaimConflict) {
		t.Fatalf("cleanup-fenced retry error = %v, want claim conflict", err)
	}
	if len(fixture.planner.requests) != 1 {
		t.Fatalf("planner calls = %d, want 1", len(fixture.planner.requests))
	}
}

func TestServiceEnforcesActiveSandboxQuotaBeforeRootFSInitialization(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	fixture.quotaLimits.limit = &quota.Limit{
		TeamID: "team-1", Dimension: quota.DimensionActiveSandboxes, LimitValue: 0,
	}
	_, err := fixture.service.ClaimSandbox(context.Background(), &service.ClaimRequest{
		TeamID: "team-1", UserID: "user-1", Template: "default", OperationID: "operation-1",
	})
	if !errors.Is(err, service.ErrQuotaExceeded) {
		t.Fatalf("claim error = %v, want quota exceeded", err)
	}
	if fixture.store.writeCount != 0 || len(fixture.store.ensureCalls) != 0 || len(fixture.planner.requests) != 0 {
		t.Fatalf("side effects: reservations=%d rootfs=%d planner=%d",
			fixture.store.writeCount, len(fixture.store.ensureCalls), len(fixture.planner.requests))
	}
}

func TestServiceAllowsExactRetryAfterQuotaBecomesFullOrUnavailable(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	fixture.quotaLimits.limit = &quota.Limit{
		TeamID: "team-1", Dimension: quota.DimensionActiveSandboxes, LimitValue: 1,
	}
	request := &service.ClaimRequest{
		TeamID: "team-1", UserID: "user-1", Template: "default", OperationID: "operation-1",
	}
	first, err := fixture.service.ClaimSandbox(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := fixture.service.ClaimSandbox(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	fixture.quotaLimits.err = errors.New("quota database unavailable")
	retryDuringOutage, err := fixture.service.ClaimSandbox(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if retry.SandboxID != first.SandboxID || retryDuringOutage.SandboxID != first.SandboxID ||
		fixture.store.writeCount != 4 {
		// One initial reservation plus one runtime-binding completion write per successful planner call.
		t.Fatalf("retries=%+v/%+v first=%+v writes=%d",
			retry, retryDuringOutage, first, fixture.store.writeCount)
	}
}

func TestServiceFailsClosedWhenQuotaPolicyCannotBeLoaded(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	fixture.quotaLimits.err = errors.New("quota database unavailable")
	_, err := fixture.service.ClaimSandbox(context.Background(), &service.ClaimRequest{
		TeamID: "team-1", UserID: "user-1", Template: "default", OperationID: "operation-1",
	})
	if err == nil || !strings.Contains(err.Error(), "load active sandbox quota") {
		t.Fatalf("claim error = %v", err)
	}
	if fixture.store.writeCount != 0 || len(fixture.store.ensureCalls) != 0 || len(fixture.planner.requests) != 0 {
		t.Fatalf("side effects: reservations=%d rootfs=%d planner=%d",
			fixture.store.writeCount, len(fixture.store.ensureCalls), len(fixture.planner.requests))
	}
}

func TestServiceRunningForkRecoversPublicationAfterNodeResponseLoss(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	claimed, err := fixture.service.ClaimSandbox(t.Context(), &service.ClaimRequest{
		TeamID: "team-1", UserID: "user-1", Template: "default", OperationID: "operation-fork-source",
	})
	if err != nil {
		t.Fatal(err)
	}
	source := fixture.store.records[claimed.SandboxID]
	if source == nil {
		t.Fatalf("claimed source %s was not persisted", claimed.SandboxID)
		return
	}
	operationID := "operation-running-fork-1"
	targetID, err := naming.SandboxNameForOperation(source.ClusterID, source.TemplateID, operationID)
	if err != nil {
		t.Fatal(err)
	}
	fixture.store.forkCandidate = &sandboxstore.NomadSandboxRunningForkCandidate{
		OperationID:        operationID,
		TargetGenerationID: sandboxstore.NomadSandboxRunningForkGenerationID(operationID, targetID),
		Slot: &sandboxstore.RuntimeSlot{
			ID: "slot-fork-source", SandboxID: source.ID, ClusterID: source.ClusterID,
			AllocationID: source.RuntimeID, AllocationNamespace: source.RuntimeNamespace,
			NodeID: "node-1", NodeUID: "node-uid-1", NodeBootID: "boot-1",
		},
		SourceFilesystemID: "filesystem-source", SourceGenerationID: "generation-source",
		SourceWriterGrantID: "writer-source", SourceWriterEpoch: 7,
		BindingVersion: rootfshandoff.WriterBindingVersion, BindingDigest: make([]byte, sha256.Size),
	}
	fixture.runningFork.err = errdefs.ErrUnavailable
	fixture.runningFork.onCall = func() { fixture.store.forkCandidate.Completed = true }
	ttl, hardTTL := int32(30), int32(120)
	response, err := fixture.service.ForkSandbox(t.Context(), source.ID, source.TeamID, "user-2", &service.ForkSandboxRequest{
		OperationID: operationID, StartedAt: fixture.now,
		Config: &service.ForkSandboxConfig{TTL: &ttl, HardTTL: &hardTTL},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response == nil || response.Sandbox == nil || response.SourceSandboxID != source.ID ||
		response.Sandbox.ID != targetID || response.Sandbox.Status != managerapi.SandboxStatusPaused ||
		!response.Sandbox.Paused || response.Sandbox.RuntimeGeneration != 0 {
		t.Fatalf("fork response = %+v", response)
	}
	if len(fixture.store.forkRequests) != 2 || len(fixture.runningFork.requests) != 1 ||
		len(fixture.runningFork.targets) != 1 {
		t.Fatalf("fork calls = store %d, node %d", len(fixture.store.forkRequests), len(fixture.runningFork.requests))
	}
	storedTarget := fixture.store.forkRequests[0].Target
	if storedTarget == nil || storedTarget.ID != targetID || storedTarget.UserID != "user-2" ||
		storedTarget.Config.TTL == nil || *storedTarget.Config.TTL != ttl ||
		storedTarget.Config.HardTTL == nil || *storedTarget.Config.HardTTL != hardTTL ||
		!storedTarget.ExpiresAt.Equal(fixture.now.Add(30*time.Second)) ||
		!storedTarget.HardExpiresAt.Equal(fixture.now.Add(120*time.Second)) {
		t.Fatalf("fork target = %+v", storedTarget)
	}
	nodeRequest := fixture.runningFork.requests[0]
	if nodeRequest.Fork.OperationID != operationID || nodeRequest.Fork.SourceSandboxID != source.ID ||
		nodeRequest.Fork.TargetSandboxID != targetID ||
		nodeRequest.Fork.TargetGenerationID != fixture.store.forkCandidate.TargetGenerationID ||
		nodeRequest.SourceWriterGrantID != fixture.store.forkCandidate.SourceWriterGrantID ||
		fixture.runningFork.targets[0].SlotID != fixture.store.forkCandidate.Slot.ID {
		t.Fatalf("node running-fork request = %+v target=%+v", nodeRequest, fixture.runningFork.targets[0])
	}

	// The same signed operation recovers the committed target directly and
	// never freezes the source a second time, even if source termination was
	// requested independently since checkpoint publication.
	source.DesiredState = sandboxstore.SandboxDesiredStateTerminating
	_, err = fixture.service.ForkSandbox(t.Context(), source.ID, source.TeamID, "user-2", &service.ForkSandboxRequest{
		OperationID: operationID, StartedAt: fixture.now,
		Config: &service.ForkSandboxConfig{TTL: &ttl, HardTTL: &hardTTL},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(fixture.runningFork.requests) != 1 || len(fixture.store.forkRequests) != 3 {
		t.Fatalf("completed retry dispatched node=%d store=%d",
			len(fixture.runningFork.requests), len(fixture.store.forkRequests))
	}
}

func TestServicePausedForkCommitsWithoutNodeDispatchAndRetriesAfterSourceResume(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	claimed, err := fixture.service.ClaimSandbox(t.Context(), &service.ClaimRequest{
		TeamID: "team-1", UserID: "user-1", Template: "default", OperationID: "operation-paused-fork-source",
	})
	if err != nil {
		t.Fatal(err)
	}
	source := fixture.store.records[claimed.SandboxID]
	source.DesiredState = sandboxstore.SandboxDesiredStatePaused
	source.RuntimeID = ""
	source.RuntimeNamespace = ""
	operationID := "operation-paused-fork-1"
	ttl := int32(45)
	request := &service.ForkSandboxRequest{
		OperationID: operationID, StartedAt: fixture.now,
		Config: &service.ForkSandboxConfig{TTL: &ttl},
	}
	response, err := fixture.service.ForkSandbox(
		t.Context(), source.ID, source.TeamID, "user-paused-fork", request,
	)
	if err != nil {
		t.Fatal(err)
	}
	targetID, err := naming.SandboxNameForOperation(source.ClusterID, source.TemplateID, operationID)
	if err != nil {
		t.Fatal(err)
	}
	if response == nil || response.Sandbox == nil || response.Sandbox.ID != targetID ||
		response.Sandbox.Status != managerapi.SandboxStatusPaused || response.Sandbox.RuntimeGeneration != 0 {
		t.Fatalf("paused fork response = %+v", response)
	}
	if len(fixture.store.pausedForkRequests) != 1 || len(fixture.store.forkRequests) != 0 ||
		len(fixture.runningFork.requests) != 0 {
		t.Fatalf("paused fork calls = paused %d running-store %d node %d",
			len(fixture.store.pausedForkRequests), len(fixture.store.forkRequests), len(fixture.runningFork.requests))
	}
	storedTarget := fixture.store.pausedForkRequests[0].Target
	if storedTarget == nil || storedTarget.ID != targetID || storedTarget.RuntimeGeneration != 0 ||
		storedTarget.Config.TTL == nil || *storedTarget.Config.TTL != ttl ||
		!storedTarget.ExpiresAt.Equal(fixture.now.Add(45*time.Second)) {
		t.Fatalf("paused fork target = %+v", storedTarget)
	}

	// The durable committed operation remains retryable even after the source
	// has independently resumed into another physical runtime generation.
	source.DesiredState = sandboxstore.SandboxDesiredStateActive
	source.RuntimeID = "allocation-after-paused-fork"
	source.RuntimeNamespace = "default"
	source.RuntimeGeneration++
	retry, err := fixture.service.ForkSandbox(
		t.Context(), source.ID, source.TeamID, "user-paused-fork", request,
	)
	if err != nil {
		t.Fatal(err)
	}
	if retry.Sandbox.ID != targetID || len(fixture.store.pausedForkRequests) != 2 ||
		len(fixture.store.forkRequests) != 0 || len(fixture.runningFork.requests) != 0 {
		t.Fatalf("paused fork retry = %+v calls paused=%d running=%d node=%d",
			retry, len(fixture.store.pausedForkRequests), len(fixture.store.forkRequests), len(fixture.runningFork.requests))
	}
}

func TestServiceForkRecoveryReplaysDurableNodeDispatch(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	claimed, err := fixture.service.ClaimSandbox(t.Context(), &service.ClaimRequest{
		TeamID: "team-1", UserID: "user-1", Template: "default", OperationID: "operation-fork-recovery-source",
	})
	if err != nil {
		t.Fatal(err)
	}
	source := fixture.store.records[claimed.SandboxID]
	operationID := "operation-fork-recovery"
	targetID, err := naming.SandboxNameForOperation(source.ClusterID, source.TemplateID, operationID)
	if err != nil {
		t.Fatal(err)
	}
	target := cloneClaimRecord(source)
	target.ID = targetID
	target.DesiredState = sandboxstore.SandboxDesiredStatePaused
	target.RuntimeID = ""
	target.RuntimeNamespace = ""
	target.RuntimeGeneration = 0
	fixture.store.records[targetID] = target
	fixture.store.activeLifecycles = map[string]*sandboxstore.SandboxLifecycleTxn{
		source.ID: {
			ID: operationID, SandboxID: source.ID, Kind: sandboxstore.SandboxLifecycleKindFork,
			Phase: sandboxstore.SandboxLifecyclePhasePublishing, Source: sandboxstore.SandboxLifecycleSourceManual,
			FromRuntimeNamespace: source.RuntimeNamespace, FromRuntimeID: source.RuntimeID,
			TargetSandboxID:    targetID,
			TargetGenerationID: sandboxstore.NomadSandboxRunningForkGenerationID(operationID, targetID),
			UpdatedAt:          fixture.now,
		},
	}
	fixture.store.forkCandidate = &sandboxstore.NomadSandboxRunningForkCandidate{
		OperationID:        operationID,
		TargetGenerationID: sandboxstore.NomadSandboxRunningForkGenerationID(operationID, targetID),
		Slot: &sandboxstore.RuntimeSlot{
			ID: "slot-fork-recovery", SandboxID: source.ID, ClusterID: source.ClusterID,
			AllocationID: source.RuntimeID, AllocationNamespace: source.RuntimeNamespace,
			NodeID: "node-1", NodeUID: "node-uid-1", NodeBootID: "boot-1",
		},
		SourceFilesystemID: "filesystem-source", SourceGenerationID: "generation-source",
		SourceWriterGrantID: "writer-source", SourceWriterEpoch: 7,
		BindingVersion: rootfshandoff.WriterBindingVersion, BindingDigest: make([]byte, sha256.Size),
	}
	fixture.runningFork.onCall = func() { fixture.store.forkCandidate.Completed = true }

	if err := fixture.service.CompleteSandboxFork(t.Context(), source.ID); err != nil {
		t.Fatal(err)
	}
	if len(fixture.runningFork.requests) != 1 || len(fixture.store.forkRequests) != 2 ||
		len(fixture.store.forkAbortCalls) != 0 {
		t.Fatalf("recovery calls = node %d store %d abort %d",
			len(fixture.runningFork.requests), len(fixture.store.forkRequests), len(fixture.store.forkAbortCalls))
	}
}

func TestServiceForkRecoveryKeepsStaleExactLiveWriterRetryable(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	claimed, err := fixture.service.ClaimSandbox(t.Context(), &service.ClaimRequest{
		TeamID: "team-1", UserID: "user-1", Template: "default", OperationID: "operation-live-fork-source",
	})
	if err != nil {
		t.Fatal(err)
	}
	source := fixture.store.records[claimed.SandboxID]
	operationID := "operation-stale-live-running-fork"
	targetID, err := naming.SandboxNameForOperation(source.ClusterID, source.TemplateID, operationID)
	if err != nil {
		t.Fatal(err)
	}
	target := cloneClaimRecord(source)
	target.ID = targetID
	target.DesiredState = sandboxstore.SandboxDesiredStatePaused
	target.RuntimeID = ""
	target.RuntimeNamespace = ""
	target.RuntimeGeneration = 0
	targetDigest, err := sandboxstore.NomadSandboxForkTargetRecordDigest(target)
	if err != nil {
		t.Fatal(err)
	}
	fixture.store.records[targetID] = target
	fixture.store.activeLifecycles = map[string]*sandboxstore.SandboxLifecycleTxn{
		source.ID: {
			ID: operationID, SandboxID: source.ID, Kind: sandboxstore.SandboxLifecycleKindFork,
			Phase: sandboxstore.SandboxLifecyclePhasePublishing, Source: sandboxstore.SandboxLifecycleSourceManual,
			FromGeneration: source.RuntimeGeneration, ToGeneration: source.RuntimeGeneration,
			FromRuntimeNamespace: source.RuntimeNamespace, FromRuntimeID: source.RuntimeID,
			TargetSandboxID:    targetID,
			TargetGenerationID: sandboxstore.NomadSandboxRunningForkGenerationID(operationID, targetID),
			TargetRecordDigest: targetDigest, ExpectedGenerationID: "generation-source",
			UpdatedAt: fixture.now.Add(-defaultNomadRunningForkRecoveryTimeout - time.Second),
		},
	}
	fixture.store.activeSlot = &sandboxstore.RuntimeSlot{
		ID: "slot-stale-live-fork", ClusterID: source.ClusterID, SandboxID: source.ID,
		AllocationID: source.RuntimeID, AllocationNamespace: source.RuntimeNamespace,
		NodeID: "node-1", NodeUID: "node-uid-1", NodeBootID: "boot-1",
		State: sandboxstore.RuntimeSlotStateActive, FilesystemID: "filesystem-source",
		SourceGenerationID: "generation-source", WriterGrantID: "writer-source",
		ProcdInstanceID: "procd-live", CommandReadyDigest: make([]byte, sha256.Size),
		CommandReadyAt: fixture.now, AuthorityObservedAt: fixture.now,
		HeartbeatExpiresAt: fixture.now.Add(time.Minute),
	}
	fixture.store.forkErr = errdefs.ErrUnavailable

	err = fixture.service.CompleteSandboxFork(t.Context(), source.ID)
	if !errors.Is(err, errdefs.ErrUnavailable) {
		t.Fatalf("stale exact fork recovery error = %v, want unavailable", err)
	}
	if len(fixture.store.forkAbortCalls) != 0 || len(fixture.runningFork.requests) != 0 {
		t.Fatalf("stale exact fork aborts=%+v node=%d",
			fixture.store.forkAbortCalls, len(fixture.runningFork.requests))
	}
}

func TestServiceForkRecoveryDoesNotAbortOnRuntimeSlotReadFailure(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	claimed, err := fixture.service.ClaimSandbox(t.Context(), &service.ClaimRequest{
		TeamID: "team-1", UserID: "user-1", Template: "default", OperationID: "operation-slot-read-source",
	})
	if err != nil {
		t.Fatal(err)
	}
	source := fixture.store.records[claimed.SandboxID]
	operationID := "operation-stale-slot-read-fork"
	targetID, err := naming.SandboxNameForOperation(source.ClusterID, source.TemplateID, operationID)
	if err != nil {
		t.Fatal(err)
	}
	target := cloneClaimRecord(source)
	target.ID = targetID
	target.DesiredState = sandboxstore.SandboxDesiredStatePaused
	target.RuntimeID = ""
	target.RuntimeNamespace = ""
	target.RuntimeGeneration = 0
	fixture.store.records[targetID] = target
	fixture.store.activeLifecycles = map[string]*sandboxstore.SandboxLifecycleTxn{
		source.ID: {
			ID: operationID, SandboxID: source.ID, Kind: sandboxstore.SandboxLifecycleKindFork,
			Phase: sandboxstore.SandboxLifecyclePhasePublishing, Source: sandboxstore.SandboxLifecycleSourceManual,
			FromRuntimeNamespace: source.RuntimeNamespace, FromRuntimeID: source.RuntimeID,
			TargetSandboxID:    targetID,
			TargetGenerationID: sandboxstore.NomadSandboxRunningForkGenerationID(operationID, targetID),
			UpdatedAt:          fixture.now.Add(-defaultNomadRunningForkRecoveryTimeout - time.Second),
		},
	}
	fixture.store.runtimeSlotErr = errors.New("database unavailable")

	err = fixture.service.CompleteSandboxFork(t.Context(), source.ID)
	if err == nil || !strings.Contains(err.Error(), "source slot for recovery") {
		t.Fatalf("slot read recovery error = %v", err)
	}
	if len(fixture.store.forkAbortCalls) != 0 {
		t.Fatalf("slot read failure aborts = %+v", fixture.store.forkAbortCalls)
	}
}

func TestServiceForkRecoveryAbortsStaleNeverPublishedTarget(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	claimed, err := fixture.service.ClaimSandbox(t.Context(), &service.ClaimRequest{
		TeamID: "team-1", UserID: "user-1", Template: "default", OperationID: "operation-stale-fork-source",
	})
	if err != nil {
		t.Fatal(err)
	}
	source := fixture.store.records[claimed.SandboxID]
	operationID := "operation-stale-running-fork"
	targetID, err := naming.SandboxNameForOperation(source.ClusterID, source.TemplateID, operationID)
	if err != nil {
		t.Fatal(err)
	}
	target := cloneClaimRecord(source)
	target.ID = targetID
	target.DesiredState = sandboxstore.SandboxDesiredStatePaused
	target.RuntimeID = ""
	target.RuntimeNamespace = ""
	target.RuntimeGeneration = 0
	fixture.store.records[targetID] = target
	fixture.store.activeLifecycles = map[string]*sandboxstore.SandboxLifecycleTxn{
		source.ID: {
			ID: operationID, SandboxID: source.ID, Kind: sandboxstore.SandboxLifecycleKindFork,
			Phase: sandboxstore.SandboxLifecyclePhasePublishing, Source: sandboxstore.SandboxLifecycleSourceManual,
			FromRuntimeNamespace: source.RuntimeNamespace, FromRuntimeID: source.RuntimeID,
			TargetSandboxID:    targetID,
			TargetGenerationID: sandboxstore.NomadSandboxRunningForkGenerationID(operationID, targetID),
			UpdatedAt:          fixture.now.Add(-defaultNomadRunningForkRecoveryTimeout - time.Second),
		},
	}

	if err := fixture.service.CompleteSandboxFork(t.Context(), source.ID); err != nil {
		t.Fatal(err)
	}
	if len(fixture.store.forkAbortCalls) != 1 || fixture.store.forkAbortCalls[0][0] != operationID ||
		fixture.store.forkAbortCalls[0][1] != source.ID || fixture.store.forkAbortCalls[0][2] != targetID ||
		len(fixture.runningFork.requests) != 0 {
		t.Fatalf("stale fork recovery aborts=%+v node=%d",
			fixture.store.forkAbortCalls, len(fixture.runningFork.requests))
	}
}

func TestServicePausedRootFSRebasePublishesAcknowledgesAndRetriesExactly(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	operationID := "operation-paused-rebase"
	sandboxID, targetDigest := preparePausedNomadRebase(t, &fixture, operationID, time.Hour)
	request := &service.RebaseSandboxRootFSRequest{
		OperationID: operationID, StartedAt: fixture.now,
		TargetBaseArtifactDigest: targetDigest, RollbackTTL: int32Pointer(3600),
	}

	response, err := fixture.service.RebaseSandboxRootFS(
		t.Context(), sandboxID, "team-1", request,
	)
	if err != nil {
		t.Fatal(err)
	}
	if response == nil || response.SandboxID != sandboxID ||
		response.GenerationID != fixture.store.rebaseCandidate.TargetGenerationID ||
		response.BaseArtifactDigest != targetDigest || response.Status != managerapi.SandboxStatusPaused ||
		!response.RollbackExpiresAt.Equal(fixture.now.Add(time.Hour)) {
		t.Fatalf("rebase response = %+v", response)
	}
	if len(fixture.pausedRebase.selectCalls) != 1 || len(fixture.pausedRebase.resolveCalls) != 0 ||
		len(fixture.pausedRebase.executeCalls) != 1 || len(fixture.store.rebasePublishes) != 1 ||
		len(fixture.pausedRebase.ackCalls) != 1 || len(fixture.store.rebaseAcks) != 1 ||
		len(fixture.store.rebaseRequests) != 2 {
		t.Fatalf("rebase calls select=%d resolve=%d execute=%d publish=%d node-ack=%d pg-ack=%d request=%d",
			len(fixture.pausedRebase.selectCalls), len(fixture.pausedRebase.resolveCalls),
			len(fixture.pausedRebase.executeCalls), len(fixture.store.rebasePublishes),
			len(fixture.pausedRebase.ackCalls), len(fixture.store.rebaseAcks), len(fixture.store.rebaseRequests))
	}
	worker := fixture.pausedRebase.executeCalls[0].Worker
	if worker.OperationID != operationID || worker.SandboxID != sandboxID ||
		worker.TargetBaseArtifactDigest != targetDigest || worker.MaxChangedBlocks != rootfsrebase.MaxWorkerChangedBlocks {
		t.Fatalf("worker request = %+v", worker)
	}

	// A completed exact retry uses PostgreSQL and never re-executes or re-acks.
	retry, err := fixture.service.RebaseSandboxRootFS(t.Context(), sandboxID, "team-1", request)
	if err != nil {
		t.Fatal(err)
	}
	if retry.GenerationID != response.GenerationID || len(fixture.pausedRebase.executeCalls) != 1 ||
		len(fixture.store.rebasePublishes) != 1 || len(fixture.pausedRebase.ackCalls) != 1 {
		t.Fatalf("exact retry = %+v execute=%d publish=%d ack=%d", retry,
			len(fixture.pausedRebase.executeCalls), len(fixture.store.rebasePublishes), len(fixture.pausedRebase.ackCalls))
	}
}

func TestServicePausedRootFSRebaseRecoversCommittedPendingAcknowledgement(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	operationID := "operation-paused-rebase-ack-loss"
	sandboxID, targetDigest := preparePausedNomadRebase(t, &fixture, operationID, time.Hour)
	fixture.pausedRebase.ackErr = errdefs.ErrUnavailable
	response, err := fixture.service.RebaseSandboxRootFS(t.Context(), sandboxID, "team-1",
		&service.RebaseSandboxRootFSRequest{
			OperationID: operationID, StartedAt: fixture.now,
			TargetBaseArtifactDigest: targetDigest, RollbackTTL: int32Pointer(3600),
		})
	if err != nil || response == nil {
		t.Fatalf("committed response with pending ack = %+v, %v", response, err)
	}
	if len(fixture.store.rebaseAcks) != 0 || len(fixture.pausedRebase.ackCalls) != 1 {
		t.Fatalf("failed ack calls node=%d pg=%d", len(fixture.pausedRebase.ackCalls), len(fixture.store.rebaseAcks))
	}

	fixture.pausedRebase.ackErr = nil
	if err := fixture.service.CompleteSandboxRootFSRebase(t.Context(), sandboxID); err != nil {
		t.Fatal(err)
	}
	if len(fixture.pausedRebase.resolveCalls) != 1 || len(fixture.pausedRebase.executeCalls) != 1 ||
		len(fixture.store.rebasePublishes) != 1 || len(fixture.pausedRebase.ackCalls) != 2 ||
		len(fixture.store.rebaseAcks) != 1 {
		t.Fatalf("ack recovery resolve=%d execute=%d publish=%d node-ack=%d pg-ack=%d",
			len(fixture.pausedRebase.resolveCalls), len(fixture.pausedRebase.executeCalls),
			len(fixture.store.rebasePublishes), len(fixture.pausedRebase.ackCalls), len(fixture.store.rebaseAcks))
	}
}

func TestServicePausedRootFSRebaseRetriesNodeResultAfterResponseLoss(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	operationID := "operation-paused-rebase-response-loss"
	sandboxID, targetDigest := preparePausedNomadRebase(t, &fixture, operationID, time.Hour)
	request := &service.RebaseSandboxRootFSRequest{
		OperationID: operationID, StartedAt: fixture.now,
		TargetBaseArtifactDigest: targetDigest, RollbackTTL: int32Pointer(3600),
	}
	fixture.pausedRebase.executeErr = errdefs.ErrUnavailable
	_, err := fixture.service.RebaseSandboxRootFS(t.Context(), sandboxID, "team-1", request)
	if !errors.Is(err, service.ErrSandboxLifecycleUnavailable) || len(fixture.store.rebasePublishes) != 0 {
		t.Fatalf("response-loss error = %v publishes=%d", err, len(fixture.store.rebasePublishes))
	}

	fixture.pausedRebase.executeErr = nil
	response, err := fixture.service.RebaseSandboxRootFS(t.Context(), sandboxID, "team-1", request)
	if err != nil || response == nil {
		t.Fatalf("response-loss retry = %+v, %v", response, err)
	}
	if len(fixture.pausedRebase.selectCalls) != 1 || len(fixture.pausedRebase.resolveCalls) != 1 ||
		len(fixture.pausedRebase.executeCalls) != 2 || len(fixture.store.rebasePublishes) != 1 {
		t.Fatalf("response-loss calls select=%d resolve=%d execute=%d publish=%d",
			len(fixture.pausedRebase.selectCalls), len(fixture.pausedRebase.resolveCalls),
			len(fixture.pausedRebase.executeCalls), len(fixture.store.rebasePublishes))
	}
}

func TestServicePausedRootFSRebaseRejectsWorkerBeforeClaimCleanup(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	operationID := "operation-paused-rebase-delete-race"
	sandboxID, targetDigest := preparePausedNomadRebase(t, &fixture, operationID, time.Hour)
	request := &service.RebaseSandboxRootFSRequest{
		OperationID: operationID, StartedAt: fixture.now,
		TargetBaseArtifactDigest: targetDigest, RollbackTTL: int32Pointer(3600),
	}
	fixture.pausedRebase.executeErr = errdefs.ErrUnavailable
	_, err := fixture.service.RebaseSandboxRootFS(t.Context(), sandboxID, "team-1", request)
	if !errors.Is(err, service.ErrSandboxLifecycleUnavailable) {
		t.Fatalf("initial worker dispatch error = %v", err)
	}
	if err := fixture.service.TerminateSandbox(t.Context(), sandboxID); err != nil {
		t.Fatal(err)
	}
	fixture.pausedRebase.executeErr = nil
	if err := fixture.service.CompleteSandboxRootFSRebase(t.Context(), sandboxID); err != nil {
		t.Fatal(err)
	}
	if !fixture.store.rebaseCandidate.Rejected || fixture.store.rebaseCandidate.Completed ||
		len(fixture.pausedRebase.executeCalls) != 1 || len(fixture.pausedRebase.rejectCalls) != 1 ||
		len(fixture.store.rebaseRejects) != 1 || len(fixture.pausedRebase.ackCalls) != 1 ||
		len(fixture.store.rebaseAcks) != 1 {
		t.Fatalf("delete-race state rejected=%t completed=%t execute=%d reject=%d pg-reject=%d node-ack=%d pg-ack=%d",
			fixture.store.rebaseCandidate.Rejected, fixture.store.rebaseCandidate.Completed,
			len(fixture.pausedRebase.executeCalls), len(fixture.pausedRebase.rejectCalls),
			len(fixture.store.rebaseRejects), len(fixture.pausedRebase.ackCalls), len(fixture.store.rebaseAcks))
	}
}

func TestServicePausedRootFSRebaseRejectsCachedResultWhenDeleteWinsPublication(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	operationID := "operation-paused-rebase-delete-after-output"
	sandboxID, targetDigest := preparePausedNomadRebase(t, &fixture, operationID, time.Hour)
	fixture.store.rebasePublishErr = sandboxstore.ErrNomadPausedRebaseTerminating
	_, err := fixture.service.RebaseSandboxRootFS(t.Context(), sandboxID, "team-1",
		&service.RebaseSandboxRootFSRequest{
			OperationID: operationID, StartedAt: fixture.now,
			TargetBaseArtifactDigest: targetDigest, RollbackTTL: int32Pointer(3600),
		})
	if !apierror.IsConflict(err) {
		t.Fatalf("delete-won publication error = %v", err)
	}
	if len(fixture.pausedRebase.executeCalls) != 1 || len(fixture.store.rebasePublishes) != 1 ||
		len(fixture.pausedRebase.rejectCalls) != 1 || len(fixture.store.rebaseRejects) != 1 ||
		len(fixture.pausedRebase.ackCalls) != 1 || len(fixture.store.rebaseAcks) != 1 {
		t.Fatalf("delete-won calls execute=%d publish=%d reject=%d pg-reject=%d node-ack=%d pg-ack=%d",
			len(fixture.pausedRebase.executeCalls), len(fixture.store.rebasePublishes),
			len(fixture.pausedRebase.rejectCalls), len(fixture.store.rebaseRejects),
			len(fixture.pausedRebase.ackCalls), len(fixture.store.rebaseAcks))
	}
}

func TestServicePausedRootFSRebaseRejectsChangedRetryAndMissingWorker(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	operationID := "operation-paused-rebase-conflict"
	sandboxID, targetDigest := preparePausedNomadRebase(t, &fixture, operationID, time.Hour)
	request := &service.RebaseSandboxRootFSRequest{
		OperationID: operationID, StartedAt: fixture.now,
		TargetBaseArtifactDigest: targetDigest, RollbackTTL: int32Pointer(3600),
	}
	fixture.pausedRebase.executeErr = errdefs.ErrUnavailable
	_, _ = fixture.service.RebaseSandboxRootFS(t.Context(), sandboxID, "team-1", request)

	changed := *request
	changed.RollbackTTL = int32Pointer(7200)
	_, err := fixture.service.RebaseSandboxRootFS(t.Context(), sandboxID, "team-1", &changed)
	if !apierror.IsConflict(err) {
		t.Fatalf("changed retry error = %v, want conflict", err)
	}

	missing := newClaimServiceFixture(t)
	missingID, missingTarget := preparePausedNomadRebase(t, &missing, "operation-no-worker", time.Hour)
	missing.pausedRebase.selectErr = errdefs.ErrUnavailable
	_, err = missing.service.RebaseSandboxRootFS(t.Context(), missingID, "team-1",
		&service.RebaseSandboxRootFSRequest{
			OperationID: "operation-no-worker", StartedAt: missing.now,
			TargetBaseArtifactDigest: missingTarget, RollbackTTL: int32Pointer(3600),
		})
	if !errors.Is(err, service.ErrSandboxLifecycleUnavailable) || len(missing.store.rebaseRequests) != 0 {
		t.Fatalf("missing worker error = %v requests=%d", err, len(missing.store.rebaseRequests))
	}
}

func preparePausedNomadRebase(
	t *testing.T,
	fixture *claimServiceFixture,
	operationID string,
	rollbackTTL time.Duration,
) (string, string) {
	t.Helper()
	claimed, err := fixture.service.ClaimSandbox(t.Context(), &service.ClaimRequest{
		TeamID: "team-1", UserID: "user-1", Template: "default",
		OperationID: "claim-" + operationID, StartedAt: fixture.now,
	})
	if err != nil {
		t.Fatal(err)
	}
	record := fixture.store.records[claimed.SandboxID]
	record.DesiredState = sandboxstore.SandboxDesiredStatePaused
	record.RuntimeID = ""
	record.RuntimeNamespace = ""
	sourceBaseRoot := digest.FromString(operationID + "-source-base").String()
	sourceHead := digest.FromString(operationID + "-source-head").String()
	targetBaseRoot := digest.FromString(operationID + "-target-base").String()
	sourceArtifactDigest := digest.FromString(operationID + "-source-artifact").String()
	targetArtifactDigest := digest.FromString(operationID + "-target-artifact").String()
	sourceDescriptor := testNomadRebaseDescriptor(t, operationID+"-source", sourceHead)
	sourceBaseDescriptor := testNomadRebaseDescriptor(t, operationID+"-source-base", sourceBaseRoot)
	targetBaseDescriptor := testNomadRebaseDescriptor(t, operationID+"-target-base", targetBaseRoot)
	deadline := fixture.now.Add(rollbackTTL).UTC().Truncate(time.Microsecond)
	fixture.store.rebaseCandidate = &sandboxstore.NomadPausedRebaseCandidate{
		LifecyclePhase: sandboxstore.SandboxLifecyclePhasePreparing,
		Sandbox:        cloneClaimRecord(record),
		Filesystem: &sandboxstore.RootFSFilesystem{
			ID: "filesystem-" + operationID, TeamID: record.TeamID,
			HeadGenerationID:   "generation-source-" + operationID,
			WriterEpoch:        7,
			BaseArtifactDigest: sourceArtifactDigest, FormatGeneration: 1,
		},
		SourceGeneration: &sandboxstore.RootFSGeneration{
			ID: "generation-source-" + operationID, FilesystemID: "filesystem-" + operationID,
			SourceOCIDigest:    digest.FromString(operationID + "-source-oci").String(),
			BaseArtifactDigest: sourceArtifactDigest, BaseBlockRoot: sourceBaseRoot,
			CurrentBlockHead: sourceHead, WriterEpoch: 7, FormatGeneration: 1,
			DurabilityState: sandboxstore.RootFSGenerationStateS3Materialized,
			LocatorVersion:  4, Descriptor: sourceDescriptor,
		},
		SourceBaseArtifact: &sandboxstore.RootFSBaseArtifact{
			ArtifactDigest:  sourceArtifactDigest,
			SourceOCIDigest: digest.FromString(operationID + "-source-oci").String(),
			BaseBlockRoot:   sourceBaseRoot, FormatGeneration: 1, Descriptor: sourceBaseDescriptor,
		},
		TargetBaseArtifact: &sandboxstore.RootFSBaseArtifact{
			ArtifactDigest:  targetArtifactDigest,
			SourceOCIDigest: digest.FromString(operationID + "-target-oci").String(),
			BaseBlockRoot:   targetBaseRoot, FormatGeneration: 1, Descriptor: targetBaseDescriptor,
		},
		TargetGenerationID: sandboxstore.NomadPausedRebaseGenerationID(
			operationID, record.ID, "generation-source-"+operationID, targetArtifactDigest,
		),
		TargetWriterEpoch: 8, RollbackExpiresAt: deadline,
		WorkerClusterID: record.ClusterID, WorkerNodeID: fixture.pausedRebase.target.NodeID,
		WorkerNodeUID: fixture.pausedRebase.target.NodeUID,
	}
	workerRequest, err := nomadPausedRebaseWorkerRequest(operationID, fixture.store.rebaseCandidate)
	if err != nil {
		t.Fatal(err)
	}
	requestDigest, err := workerRequest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	health := sha256.Sum256([]byte(operationID + "-health"))
	apply := rootfsrebase.ApplyResult{
		Version: rootfsrebase.ApplyResultVersion, TargetNodeCount: 1,
		OldManifestDigest:    testHexDigest(operationID + "-old"),
		SourceManifestDigest: testHexDigest(operationID + "-source"),
		DiffDigest:           testHexDigest(operationID + "-diff"),
		TargetManifestDigest: testHexDigest(operationID + "-target"),
		HealthProof:          hex.EncodeToString(health[:]),
	}
	fixture.pausedRebase.result = rootfsrebase.WorkerResult{
		Version: rootfsrebase.WorkerProtocolVersion, RequestDigest: requestDigest,
		GenerationID: workerRequest.TargetGenerationID, FilesystemID: workerRequest.FilesystemID,
		ParentGenerationID: workerRequest.SourceGenerationID,
		SourceOCIDigest:    workerRequest.TargetSourceOCIDigest,
		BaseArtifactDigest: workerRequest.TargetBaseArtifactDigest,
		BaseBlockRoot:      workerRequest.TargetBaseBlockRoot, CurrentBlockHead: workerRequest.TargetBaseBlockRoot,
		WriterEpoch: workerRequest.TargetWriterEpoch, FormatGeneration: workerRequest.TargetFormatGeneration,
		DurabilityState: rootfsblock.DurabilityS3, LocatorVersion: workerRequest.SourceLocatorVersion + 1,
		Descriptor:        append([]byte(nil), workerRequest.TargetBaseDescriptor...),
		HealthCheckDigest: health[:], Apply: apply,
	}
	if err := fixture.pausedRebase.result.SealProof(); err != nil {
		t.Fatal(err)
	}
	return record.ID, targetArtifactDigest
}

func testNomadRebaseDescriptor(t *testing.T, suffix, root string) []byte {
	t.Helper()
	payload, err := rootfsblock.EncodeDescriptor(rootfsblock.Descriptor{
		Version: rootfsblock.DescriptorVersion, LogicalSizeBytes: 8 * rootfsblock.LogicalBlockSize,
		BlockSizeBytes: rootfsblock.LogicalBlockSize,
		MappingRoot: rootfsblock.MappingRootLocator{
			Version: rootfsblock.MappingPageVersion, RootDigest: root,
			Object: rootfsblock.ObjectRange{
				Key: "rootfs/rebase/" + suffix + "/map", Length: 4096,
				Checksum: digest.FromString(suffix + "-map").String(),
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func testHexDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func int32Pointer(value int32) *int32 {
	return &value
}

func preparePausedNomadResume(t *testing.T, fixture claimServiceFixture) string {
	t.Helper()
	claimed, err := fixture.service.ClaimSandbox(context.Background(), &service.ClaimRequest{
		TeamID: "team-1", UserID: "user-1", Template: "default", OperationID: "operation-resume-source",
	})
	if err != nil {
		t.Fatal(err)
	}
	record := fixture.store.records[claimed.SandboxID]
	if record == nil {
		t.Fatalf("claimed sandbox %s was not persisted", claimed.SandboxID)
		return ""
	}
	record.DesiredState = sandboxstore.SandboxDesiredStatePaused
	record.RuntimeID = ""
	record.RuntimeNamespace = ""
	fixture.store.resumeCandidate = &sandboxstore.NomadSandboxResumeCandidate{
		SandboxID: claimed.SandboxID, OperationID: "nomad-resume-operation-1",
		LifecyclePhase:    sandboxstore.SandboxLifecyclePhasePreparing,
		RuntimeGeneration: record.RuntimeGeneration + 1,
		FilesystemID:      "filesystem-1", SourceGenerationID: "generation-paused-1",
		Record: cloneClaimRecord(record),
	}
	fixture.store.activeSlot = nil
	fixture.store.resumeRequests = nil
	fixture.store.resumeRetryRequests = nil
	fixture.store.resumeRequested = false
	fixture.store.resumeCompleteCalls = nil
	fixture.planner.requests = nil
	fixture.quotaLimits.calls = nil
	return claimed.SandboxID
}

type claimServiceFixture struct {
	service       *Service
	store         *fakeClaimStore
	planner       *fakePlanner
	allocation    *fakeAllocationStopper
	plannedRetire *fakePlannedRetireController
	runningFork   *fakeRunningForkController
	pausedRebase  *fakePausedRebaseController
	quotaLimits   *fakeQuotaLimitStore
	runtimeClass  RuntimeClass
	now           time.Time
	pauseOrder    *[]string
}

type fakePlannedRetireController struct {
	requests   []protocol.NodePlannedRetireControlRequest
	targets    []protocol.NodeChannelTarget
	err        error
	pauseOrder *[]string
}

func (f *fakePlannedRetireController) PlannedRetire(
	_ context.Context,
	target protocol.NodeChannelTarget,
	request protocol.NodePlannedRetireControlRequest,
) (protocol.NodePlannedRetireControlProof, error) {
	f.targets = append(f.targets, target)
	f.requests = append(f.requests, request)
	if f.pauseOrder != nil {
		*f.pauseOrder = append(*f.pauseOrder, "plan")
	}
	if f.err != nil {
		return protocol.NodePlannedRetireControlProof{}, f.err
	}
	return protocol.NewNodePlannedRetireControlProof(request)
}

type fakeRunningForkController struct {
	result   rootfshandoff.RunningForkCheckpointResult
	err      error
	onCall   func()
	targets  []protocol.NodeChannelTarget
	requests []protocol.NodeRunningForkControlRequest
}

type fakePausedRebaseController struct {
	target       protocol.NodeChannelTarget
	selectErr    error
	resolveErr   error
	result       rootfsrebase.WorkerResult
	executeErr   error
	rejectErr    error
	ackErr       error
	selectCalls  [][2]string
	resolveCalls [][3]string
	executeCalls []protocol.NodePausedRebaseControlRequest
	rejectCalls  []protocol.NodePausedRebaseControlRequest
	ackCalls     []protocol.NodePausedRebaseControlRequest
}

func (f *fakePausedRebaseController) SelectPausedRebaseNode(
	_ context.Context,
	clusterID, operationID string,
) (protocol.NodeChannelTarget, error) {
	f.selectCalls = append(f.selectCalls, [2]string{clusterID, operationID})
	return f.target, f.selectErr
}

func (f *fakePausedRebaseController) ResolvePausedRebaseNode(
	_ context.Context,
	clusterID, nodeID, nodeUID string,
) (protocol.NodeChannelTarget, error) {
	f.resolveCalls = append(f.resolveCalls, [3]string{clusterID, nodeID, nodeUID})
	return f.target, f.resolveErr
}

func (f *fakePausedRebaseController) PausedRebase(
	_ context.Context,
	_ protocol.NodeChannelTarget,
	request protocol.NodePausedRebaseControlRequest,
) (rootfsrebase.WorkerResult, error) {
	f.executeCalls = append(f.executeCalls, request)
	return f.result, f.executeErr
}

func (f *fakePausedRebaseController) RejectPausedRebase(
	_ context.Context,
	_ protocol.NodeChannelTarget,
	request protocol.NodePausedRebaseControlRequest,
) (rootfsrebase.WorkerRejection, error) {
	f.rejectCalls = append(f.rejectCalls, request)
	if f.rejectErr != nil {
		return rootfsrebase.WorkerRejection{}, f.rejectErr
	}
	return rootfsrebase.RejectWithoutResult(request.Worker)
}

func (f *fakePausedRebaseController) AcknowledgePausedRebase(
	_ context.Context,
	_ protocol.NodeChannelTarget,
	request protocol.NodePausedRebaseControlRequest,
) (rootfsrebase.WorkerAcknowledgement, error) {
	f.ackCalls = append(f.ackCalls, request)
	if f.ackErr != nil {
		return rootfsrebase.WorkerAcknowledgement{}, f.ackErr
	}
	requestDigest, err := request.Worker.Digest()
	if err != nil {
		return rootfsrebase.WorkerAcknowledgement{}, err
	}
	return rootfsrebase.WorkerAcknowledgement{
		RequestDigest: requestDigest, ProofDigest: request.AcknowledgeProofDigest,
	}, nil
}

func (f *fakeRunningForkController) RunningFork(
	_ context.Context,
	target protocol.NodeChannelTarget,
	request protocol.NodeRunningForkControlRequest,
) (rootfshandoff.RunningForkCheckpointResult, error) {
	f.targets = append(f.targets, target)
	f.requests = append(f.requests, request)
	if f.onCall != nil {
		f.onCall()
	}
	return f.result, f.err
}

func newClaimServiceFixture(t *testing.T) claimServiceFixture {
	t.Helper()
	imageDigest := digest.FromString("procd-image").String()
	artifactDigest := digest.FromString("base-artifact").String()
	template := &templatepkg.Template{
		TemplateID: "default", Scope: naming.ScopePublic,
		Spec: v1alpha1.SandboxTemplateSpec{
			MainContainer: v1alpha1.ContainerSpec{
				Image: "example.com/sandbox0/procd@" + imageDigest,
				Env:   []v1alpha1.EnvVar{{Name: "MAIN", Value: "yes"}, {Name: "SHARED", Value: "main"}},
				Resources: v1alpha1.ResourceQuota{
					CPU: "1", Memory: "1Gi",
				},
			},
			EnvVars: map[string]string{"TEMPLATE": "yes", "SHARED": "template"},
			Network: &v1alpha1.SandboxNetworkPolicy{Mode: v1alpha1.NetworkModeAllowAll},
		},
	}
	compatibility := protocol.RuntimeCompatibility{
		Version: protocol.RuntimeCompatibilityVersion, Architecture: "amd64",
		DriverVersion: "0.1.0", RunscVersion: "runsc-1", Platform: "systrap",
		Overlay2: "none", FileAccess: "shared", DirectFS: true,
		Command: "/procd", ProcdPort: protocol.NomadProcdPort,
		RuntimeMode:   runtimecontrol.ControlModeStatic,
		SecurityClass: "standard",
	}
	compatibilityDigest, err := compatibility.Digest()
	if err != nil {
		t.Fatal(err)
	}
	runtimeClass := RuntimeClass{
		Name: "one", ClusterID: "cluster-1",
		ArtifactPlatform: sandboxstore.RootFSArtifactPlatform{OS: "linux", Architecture: "amd64"},
		Compatibility:    compatibility, CompatibilityDigest: compatibilityDigest,
	}
	store := &fakeClaimStore{
		records: make(map[string]*sandboxstore.SandboxRecord), operations: make(map[string]string),
		claimPhases: make(map[string]string),
		artifact: &sandboxstore.RootFSBaseArtifact{
			ArtifactDigest: artifactDigest, SourceOCIRef: template.Spec.MainContainer.Image,
			SourceOCIDigest: imageDigest, FormatGeneration: 1, LogicalSizeBytes: 8 << 30,
			ProcdProtocol: "sandbox0.procd.test.v1",
			ProcdDigest:   "sha256:" + strings.Repeat("f", 64),
			Platform:      sandboxstore.RootFSArtifactPlatform{OS: "linux", Architecture: "amd64"},
		},
	}
	planner := &fakePlanner{}
	pauseOrder := &[]string{}
	store.pauseOrder = pauseOrder
	allocation := &fakeAllocationStopper{pauseOrder: pauseOrder}
	plannedRetire := &fakePlannedRetireController{pauseOrder: pauseOrder}
	runningFork := &fakeRunningForkController{}
	pausedRebase := &fakePausedRebaseController{target: protocol.NodeChannelTarget{
		ClusterID: "cluster-1", NodeID: "node-rebase-1", NodeUID: "node-rebase-uid-1", NodeBootID: "boot-rebase-1",
	}}
	quotaLimits := &fakeQuotaLimitStore{}
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	claimService, err := New(Config{
		Store: store, Templates: &fakeTemplateStore{template: template},
		RuntimeClasses: &RuntimeClassCatalog{classes: []RuntimeClass{runtimeClass}}, Planner: planner, Allocation: allocation,
		PlannedRetire:          plannedRetire,
		RunningFork:            runningFork,
		PausedRebase:           pausedRebase,
		QuotaLimits:            quotaLimits,
		NetworkPolicies:        networkpolicy.NewNetworkPolicyService(zap.NewNop()),
		ResourcePolicy:         templatepkg.NewResourcePolicy("1Gi", "8Gi"),
		RootFSFormatGeneration: 1,
		RootFSProcdProtocol:    "sandbox0.procd.test.v1",
		RootFSProcdDigest:      "sha256:" + strings.Repeat("f", 64),
		ClaimTTL:               15 * time.Second,
		DefaultTTL:             time.Hour, Now: func() time.Time { return now }, Logger: zap.NewNop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return claimServiceFixture{
		service: claimService, store: store, planner: planner, allocation: allocation,
		plannedRetire: plannedRetire, runningFork: runningFork,
		pausedRebase: pausedRebase,
		quotaLimits:  quotaLimits, runtimeClass: runtimeClass, now: now, pauseOrder: pauseOrder,
	}
}

func cloneClaimRecord(record *sandboxstore.SandboxRecord) *sandboxstore.SandboxRecord {
	if record == nil {
		return nil
	}
	copy := *record
	if config := service.CloneSandboxConfig(&record.Config); config != nil {
		copy.Config = *config
	}
	copy.TemplateSpec = *record.TemplateSpec.DeepCopy()
	return &copy
}

func cloneNomadPausedRebaseCandidate(
	candidate *sandboxstore.NomadPausedRebaseCandidate,
) *sandboxstore.NomadPausedRebaseCandidate {
	if candidate == nil {
		return nil
	}
	copy := *candidate
	copy.Sandbox = cloneClaimRecord(candidate.Sandbox)
	if candidate.Filesystem != nil {
		filesystem := *candidate.Filesystem
		copy.Filesystem = &filesystem
	}
	if candidate.SourceGeneration != nil {
		generation := *candidate.SourceGeneration
		generation.Descriptor = append([]byte(nil), candidate.SourceGeneration.Descriptor...)
		copy.SourceGeneration = &generation
	}
	if candidate.SourceBaseArtifact != nil {
		artifact := *candidate.SourceBaseArtifact
		artifact.Descriptor = append([]byte(nil), candidate.SourceBaseArtifact.Descriptor...)
		copy.SourceBaseArtifact = &artifact
	}
	if candidate.TargetBaseArtifact != nil {
		artifact := *candidate.TargetBaseArtifact
		artifact.Descriptor = append([]byte(nil), candidate.TargetBaseArtifact.Descriptor...)
		copy.TargetBaseArtifact = &artifact
	}
	copy.WorkerProofDigest = append([]byte(nil), candidate.WorkerProofDigest...)
	return &copy
}
