package nomadclaim

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/networkpolicy"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotclaim"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	templatepkg "github.com/sandbox0-ai/sandbox0/pkg/template"
	templatestore "github.com/sandbox0-ai/sandbox0/pkg/template/store"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/resource"
)

type fakeTemplateStore struct {
	templatestore.TemplateStore
	template *templatepkg.Template
}

func (f *fakeTemplateStore) GetTemplateForTeam(_ context.Context, teamID, templateID string) (*templatepkg.Template, error) {
	if f.template == nil || f.template.TemplateID != templateID ||
		(f.template.Scope == naming.ScopeTeam && f.template.TeamID != teamID) {
		return nil, nil
	}
	return f.template, nil
}

type fakeClaimStore struct {
	records      map[string]*sandboxstore.SandboxRecord
	operations   map[string]string
	claimPhases  map[string]string
	artifact     *sandboxstore.RootFSBaseArtifact
	ensureCalls  []*sandboxstore.EnsureInitialRootFSGenerationRequest
	restoreCalls []*sandboxstore.RestoreRootFSFromSnapshotRequest
	cleanupCalls []string
	cleanupErr   error
	snapshot     *sandboxstore.RootFSSnapshot
	writeCount   int
}

func (f *fakeClaimStore) ReserveSandboxClaim(_ context.Context, request *sandboxstore.ReserveSandboxClaimRequest) (*sandboxstore.SandboxRecord, error) {
	if request == nil || request.Record == nil {
		return nil, errors.New("missing reservation record")
	}
	if existing := f.records[request.Record.ID]; existing != nil {
		if !sameClaimRecord(existing, request.Record) || f.operations[request.Record.ID] != request.OperationID {
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
	phase := f.claimPhases[request.Record.ID]
	if phase == sandboxstore.SandboxRuntimeClaimPhaseCleanupPending || phase == sandboxstore.SandboxRuntimeClaimPhaseCleaned {
		return nil, true, sandboxstore.ErrSandboxClaimCleanupPending
	}
	return cloneClaimRecord(existing), true, nil
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
	record.CurrentPodName = request.AllocationID
	record.CurrentPodNamespace = request.AllocationNamespace
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
		PhysicalStateRequired: record.CurrentPodName != "",
	}, nil
}

type fakeQuotaLimitStore struct {
	limit *quota.Limit
	err   error
}

func (f *fakeQuotaLimitStore) GetLimit(_ context.Context, _ string, _ quota.Dimension) (*quota.Limit, error) {
	return f.limit, f.err
}

func (f *fakeClaimStore) GetReadyRootFSBaseArtifact(
	_ context.Context,
	source string,
	platform sandboxstore.RootFSArtifactPlatform,
	format int,
) (*sandboxstore.RootFSBaseArtifact, error) {
	if f.artifact == nil || f.artifact.SourceOCIDigest != source || f.artifact.Platform != platform || format != 0 {
		return nil, sandboxstore.ErrRootFSBaseArtifactNotFound
	}
	copy := *f.artifact
	return &copy, nil
}

func (f *fakeClaimStore) GetReadyRootFSBaseArtifactByDigest(
	_ context.Context,
	digest string,
	platform sandboxstore.RootFSArtifactPlatform,
) (*sandboxstore.RootFSBaseArtifact, error) {
	if f.artifact == nil || f.artifact.ArtifactDigest != digest || f.artifact.Platform != platform {
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

func (f *fakeClaimStore) RestoreRootFSFromSnapshot(_ context.Context, request *sandboxstore.RestoreRootFSFromSnapshotRequest) (*sandboxstore.RootFSFilesystem, error) {
	copy := *request
	f.restoreCalls = append(f.restoreCalls, &copy)
	return &sandboxstore.RootFSFilesystem{ID: request.SandboxID}, nil
}

type fakePlanner struct {
	requests []runtimeslotclaim.Request
	err      error
}

func (f *fakePlanner) Claim(_ context.Context, request runtimeslotclaim.Request) (*runtimeslotclaim.Result, error) {
	f.requests = append(f.requests, request)
	if f.err != nil {
		return nil, f.err
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
		response.PodName != "allocation-1" || response.ClusterId == nil || *response.ClusterId != "cluster-1" {
		t.Fatalf("response = %+v", response)
	}
	if len(fixture.planner.requests) != 1 {
		t.Fatalf("planner calls = %d", len(fixture.planner.requests))
	}
	planned := fixture.planner.requests[0]
	if planned.OperationID != "operation-1" || planned.SandboxID != expectedID ||
		planned.CompatibilityDigest != fixture.profile.CompatibilityDigest ||
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
	if record == nil || record.CurrentPodName != "allocation-1" || record.CurrentPodNamespace != "default" ||
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

func TestServiceFailsClosedForUnconnectedPauseAndResume(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	if _, err := fixture.service.PauseSandboxAndWait(context.Background(), "sandbox-1"); !errors.Is(err, service.ErrSandboxLifecycleUnavailable) {
		t.Fatalf("pause error = %v", err)
	}
	if _, err := fixture.service.ResumeSandboxAndWait(context.Background(), "sandbox-1"); !errors.Is(err, service.ErrSandboxLifecycleUnavailable) {
		t.Fatalf("resume error = %v", err)
	}
	if err := fixture.service.PauseSandboxByID(context.Background(), "sandbox-1"); !errors.Is(err, service.ErrSandboxLifecycleUnavailable) {
		t.Fatalf("automatic pause error = %v", err)
	}
	if len(fixture.store.cleanupCalls) != 0 {
		t.Fatalf("unavailable lifecycle mutated cleanup state: %v", fixture.store.cleanupCalls)
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
		ID: "snapshot-1", TeamID: "team-1", StorageFormat: sandboxstore.RootFSStorageFormatBlockCOWV1,
		BaseArtifactDigest: fixture.store.artifact.ArtifactDigest,
		SourceOCIDigest:    fixture.store.artifact.SourceOCIDigest,
		FormatGeneration:   fixture.store.artifact.FormatGeneration,
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
		len(fixture.store.ensureCalls) != 0 {
		t.Fatalf("restore calls = %+v ensure calls = %+v", fixture.store.restoreCalls, fixture.store.ensureCalls)
	}
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

type claimServiceFixture struct {
	service     *Service
	store       *fakeClaimStore
	planner     *fakePlanner
	quotaLimits *fakeQuotaLimitStore
	profile     Profile
	now         time.Time
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
					CPU: resource.MustParse("1"), Memory: resource.MustParse("1Gi"),
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
		RuntimeMode: runtimecontrol.ControlModeStatic,
		CPUPeriod:   100000, CPUQuota: 100000, CPUShares: 1024, MemoryLimitBytes: 1 << 30,
	}
	compatibilityDigest, err := compatibility.Digest()
	if err != nil {
		t.Fatal(err)
	}
	profile := Profile{
		Name: "one", ClusterID: "cluster-1",
		TemplateCPU: resource.MustParse("1"), TemplateMemory: resource.MustParse("1Gi"),
		ArtifactPlatform: sandboxstore.RootFSArtifactPlatform{OS: "linux", Architecture: "amd64"},
		Compatibility:    compatibility, CompatibilityDigest: compatibilityDigest,
	}
	store := &fakeClaimStore{
		records: make(map[string]*sandboxstore.SandboxRecord), operations: make(map[string]string),
		claimPhases: make(map[string]string),
		artifact: &sandboxstore.RootFSBaseArtifact{
			ArtifactDigest: artifactDigest, SourceOCIRef: template.Spec.MainContainer.Image,
			SourceOCIDigest: imageDigest, FormatGeneration: 1,
			Platform: sandboxstore.RootFSArtifactPlatform{OS: "linux", Architecture: "amd64"},
		},
	}
	planner := &fakePlanner{}
	quotaLimits := &fakeQuotaLimitStore{}
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	claimService, err := New(Config{
		Store: store, Templates: &fakeTemplateStore{template: template},
		Profiles: &ProfileCatalog{profiles: []Profile{profile}}, Planner: planner,
		QuotaLimits:     quotaLimits,
		NetworkPolicies: networkpolicy.NewNetworkPolicyService(zap.NewNop()),
		ResourcePolicy:  templatepkg.NewResourcePolicy("1Gi", "8Gi"),
		ClaimTTL:        15 * time.Second,
		DefaultTTL:      time.Hour, Now: func() time.Time { return now }, Logger: zap.NewNop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return claimServiceFixture{
		service: claimService, store: store, planner: planner, quotaLimits: quotaLimits,
		profile: profile, now: now,
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

func mustQuantity(value string) resource.Quantity {
	return resource.MustParse(value)
}
