package functionapi

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/functionruntime"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/functions"
	"go.uber.org/zap"
)

func TestTryLockFunctionRevisionPublishSerializesFunction(t *testing.T) {
	handler := &Handler{}

	release, ok := handler.tryLockFunctionRevisionPublish("team-a", "function-a")
	if !ok {
		t.Fatal("first publish lock attempt failed")
	}
	if secondRelease, ok := handler.tryLockFunctionRevisionPublish("team-a", "function-a"); ok {
		secondRelease()
		t.Fatal("second publish lock attempt for same function succeeded")
	}

	otherRelease, ok := handler.tryLockFunctionRevisionPublish("team-a", "function-b")
	if !ok {
		t.Fatal("publish lock for different function failed")
	}
	otherRelease()

	release()

	reacquired, ok := handler.tryLockFunctionRevisionPublish("team-a", "function-a")
	if !ok {
		t.Fatal("publish lock was not reusable after release")
	}
	reacquired()
}

func TestPrepareRevisionFromSourceAcceptsRevisionSpec(t *testing.T) {
	service := mgr.SandboxAppService{
		ID:          "api",
		DisplayName: "API",
		Port:        3000,
		Runtime: &mgr.SandboxAppServiceRuntime{
			Type:    mgr.SandboxAppServiceRuntimeCMD,
			Command: []string{"node", "server.js"},
		},
		Ingress: mgr.SandboxAppServiceIngress{
			Public: true,
			Routes: []mgr.SandboxAppServiceRoute{{
				ID:     "root",
				Resume: true,
			}},
		},
	}
	serviceBytes, err := json.Marshal(service)
	if err != nil {
		t.Fatalf("marshal service: %v", err)
	}

	rev, name, cleanup, err := (&Handler{}).prepareRevisionFromSource(context.Background(), &authn.AuthContext{
		TeamID: "team-1",
		UserID: "user-1",
	}, functionSourceRequest{
		Type: functions.RevisionSourceTypeRevisionSpec,
		RevisionSpec: &functions.FunctionRevisionSpec{
			TemplateID:     "node-template",
			RuntimeService: serviceBytes,
			Mounts: []functions.FunctionRevisionMount{{
				MountPoint: "/workspace/app",
				Source: functions.FunctionRevisionMountSource{
					Type:            functions.FunctionRevisionMountSourceSandboxVolume,
					SandboxVolumeID: "revision-volume",
				},
			}},
		},
	}, functionruntime.Metadata{})
	if err != nil {
		t.Fatalf("prepareRevisionFromSource() error = %v", err)
	}
	if cleanup != nil {
		t.Fatal("revision_spec source should not return sandbox restore cleanup")
	}
	if name != "API" {
		t.Fatalf("name = %q, want API", name)
	}
	if rev.SourceType != functions.RevisionSourceTypeRevisionSpec {
		t.Fatalf("source_type = %q, want revision_spec", rev.SourceType)
	}
	if rev.Spec.TemplateID != "node-template" || len(rev.Spec.Mounts) != 1 {
		t.Fatalf("revision spec = %+v, want template and mount", rev.Spec)
	}
	if len(rev.ServiceSnapshot) == 0 {
		t.Fatal("service snapshot compatibility mirror is empty")
	}
}

func TestPrepareRevisionFromSourceMaterializesArtifactMount(t *testing.T) {
	service := mgr.SandboxAppService{
		ID:   "api",
		Port: 3000,
		Runtime: &mgr.SandboxAppServiceRuntime{
			Type:    mgr.SandboxAppServiceRuntimeCMD,
			Command: []string{"node", "server.js"},
		},
		Ingress: mgr.SandboxAppServiceIngress{
			Public: true,
			Routes: []mgr.SandboxAppServiceRoute{{ID: "root", Resume: true}},
		},
	}
	serviceBytes, err := json.Marshal(service)
	if err != nil {
		t.Fatalf("marshal service: %v", err)
	}
	volumeStore := &recordingRevisionVolumeStore{
		artifactMounts: []functions.FunctionRevisionMount{{
			MountPoint: "/workspace/app",
			Mode:       functions.FunctionRevisionMountModeReadOnly,
			Source: functions.FunctionRevisionMountSource{
				Type:            functions.FunctionRevisionMountSourceArtifact,
				ArtifactID:      "artifact-1",
				SandboxVolumeID: "prepared-volume",
			},
		}},
		artifactCleanup: []functions.RestoreMount{{
			SandboxVolumeID: "prepared-volume",
			MountPoint:      "/workspace/app",
		}},
	}
	handler := &Handler{volumeStore: volumeStore, logger: zap.NewNop()}

	rev, _, cleanup, err := handler.prepareRevisionFromSource(context.Background(), &authn.AuthContext{
		TeamID: "team-1",
		UserID: "user-1",
	}, functionSourceRequest{
		Type: functions.RevisionSourceTypeRevisionSpec,
		RevisionSpec: &functions.FunctionRevisionSpec{
			TemplateID:     "node-template",
			RuntimeService: serviceBytes,
			Mounts: []functions.FunctionRevisionMount{{
				MountPoint: "/workspace/app",
				Source: functions.FunctionRevisionMountSource{
					Type:       functions.FunctionRevisionMountSourceArtifact,
					ArtifactID: "artifact-1",
				},
			}},
		},
	}, functionruntime.Metadata{FunctionID: "fn-1", FunctionRevisionID: "rev-1"})
	if err != nil {
		t.Fatalf("prepareRevisionFromSource() error = %v", err)
	}
	if cleanup == nil {
		t.Fatal("cleanup = nil, want prepared artifact cleanup")
	}
	if !volumeStore.prepareArtifactCalled {
		t.Fatal("PrepareArtifactMounts was not called")
	}
	if len(rev.Spec.Mounts) != 1 || rev.Spec.Mounts[0].Source.SandboxVolumeID != "prepared-volume" {
		t.Fatalf("revision mounts = %+v, want prepared volume", rev.Spec.Mounts)
	}
	if len(rev.RestoreMounts) != 1 || rev.RestoreMounts[0].SandboxVolumeID != "prepared-volume" {
		t.Fatalf("restore mounts = %+v, want prepared artifact cleanup mount", rev.RestoreMounts)
	}
	cleanup()
	if len(volumeStore.deletedMounts) != 1 || volumeStore.deletedMounts[0].SandboxVolumeID != "prepared-volume" {
		t.Fatalf("deleted mounts = %+v, want prepared volume cleanup", volumeStore.deletedMounts)
	}
}

func TestPrepareRevisionFromSourceRejectsArtifactMountWithoutStore(t *testing.T) {
	service := mgr.SandboxAppService{
		ID:   "api",
		Port: 3000,
		Runtime: &mgr.SandboxAppServiceRuntime{
			Type:    mgr.SandboxAppServiceRuntimeCMD,
			Command: []string{"node", "server.js"},
		},
		Ingress: mgr.SandboxAppServiceIngress{
			Public: true,
			Routes: []mgr.SandboxAppServiceRoute{{ID: "root", Resume: true}},
		},
	}
	serviceBytes, err := json.Marshal(service)
	if err != nil {
		t.Fatalf("marshal service: %v", err)
	}

	_, _, _, err = (&Handler{}).prepareRevisionFromSource(context.Background(), &authn.AuthContext{
		TeamID: "team-1",
		UserID: "user-1",
	}, functionSourceRequest{
		Type: functions.RevisionSourceTypeRevisionSpec,
		RevisionSpec: &functions.FunctionRevisionSpec{
			TemplateID:     "node-template",
			RuntimeService: serviceBytes,
			Mounts: []functions.FunctionRevisionMount{{
				MountPoint: "/workspace/app",
				Source: functions.FunctionRevisionMountSource{
					Type:       functions.FunctionRevisionMountSourceArtifact,
					ArtifactID: "artifact-1",
				},
			}},
		},
	}, functionruntime.Metadata{})
	if err == nil {
		t.Fatal("prepareRevisionFromSource() error = nil, want missing artifact store")
	}
}

func TestRuntimeStatusReportsFailedInstance(t *testing.T) {
	now := time.Now().UTC()
	message := "health check returned HTTP 500"
	fn := &functions.Function{ID: "fn-1", TeamID: "team-1", Enabled: true}
	rev := &functions.Revision{ID: "rev-1", FunctionID: "fn-1", TeamID: "team-1", RevisionNumber: 2}
	status := runtimeStatus(fn, rev, []*functions.RuntimeInstance{{
		ID:             "inst-1",
		TeamID:         "team-1",
		FunctionID:     "fn-1",
		RevisionID:     "rev-1",
		SandboxID:      "sb-1",
		State:          functions.RuntimeInstanceStateFailed,
		ReadinessState: functions.RuntimeReadinessStateFailed,
		LastError:      &message,
		LastErrorAt:    &now,
		FailedAt:       &now,
		UpdatedAt:      now,
	}}, nil)

	if status.State != functions.RuntimeStateIdle {
		t.Fatalf("state = %q, want %q", status.State, functions.RuntimeStateIdle)
	}
	if status.Phase != functions.RuntimePhaseFailed {
		t.Fatalf("phase = %q, want %q", status.Phase, functions.RuntimePhaseFailed)
	}
	if status.ReadinessState != functions.RuntimeReadinessStateFailed {
		t.Fatalf("readiness = %q, want %q", status.ReadinessState, functions.RuntimeReadinessStateFailed)
	}
	if status.LastError == nil || *status.LastError != message {
		t.Fatalf("last_error = %v, want %q", status.LastError, message)
	}
}

func TestRuntimeStatusReportsCurrentProvisioningEvent(t *testing.T) {
	now := time.Now().UTC()
	fn := &functions.Function{ID: "fn-1", TeamID: "team-1", Enabled: true}
	rev := &functions.Revision{ID: "rev-1", FunctionID: "fn-1", TeamID: "team-1", RevisionNumber: 2}
	status := runtimeStatus(fn, rev, nil, []*functions.RuntimeEvent{{
		ID:             "event-1",
		TeamID:         "team-1",
		FunctionID:     "fn-1",
		RevisionID:     "rev-1",
		Phase:          functions.RuntimePhaseProvisioning,
		ReadinessState: functions.RuntimeReadinessStateChecking,
		Reason:         "claim_runtime",
		CreatedAt:      now,
	}})

	if status.State != functions.RuntimeStateIdle {
		t.Fatalf("state = %q, want %q", status.State, functions.RuntimeStateIdle)
	}
	if status.Phase != functions.RuntimePhaseProvisioning {
		t.Fatalf("phase = %q, want %q", status.Phase, functions.RuntimePhaseProvisioning)
	}
	if status.ReadinessState != functions.RuntimeReadinessStateChecking {
		t.Fatalf("readiness = %q, want %q", status.ReadinessState, functions.RuntimeReadinessStateChecking)
	}
	if len(status.RecentEvents) != 1 {
		t.Fatalf("recent_events = %d, want 1", len(status.RecentEvents))
	}
}

func TestRuntimeStatusReadyInstanceOverridesOlderFailure(t *testing.T) {
	now := time.Now().UTC()
	earlier := now.Add(-time.Minute)
	message := "previous startup failed"
	contextID := "ctx-1"
	fn := &functions.Function{ID: "fn-1", TeamID: "team-1", Enabled: true}
	rev := &functions.Revision{ID: "rev-1", FunctionID: "fn-1", TeamID: "team-1", RevisionNumber: 2}
	status := runtimeStatus(fn, rev, []*functions.RuntimeInstance{
		{
			ID:             "failed",
			TeamID:         "team-1",
			FunctionID:     "fn-1",
			RevisionID:     "rev-1",
			SandboxID:      "sb-old",
			State:          functions.RuntimeInstanceStateFailed,
			ReadinessState: functions.RuntimeReadinessStateFailed,
			LastError:      &message,
			FailedAt:       &earlier,
			UpdatedAt:      earlier,
		},
		{
			ID:             "ready",
			TeamID:         "team-1",
			FunctionID:     "fn-1",
			RevisionID:     "rev-1",
			SandboxID:      "sb-ready",
			ContextID:      &contextID,
			State:          functions.RuntimeInstanceStateReady,
			ReadinessState: functions.RuntimeReadinessStateReady,
			ReadyAt:        &now,
			UpdatedAt:      now,
		},
	}, nil)

	if status.State != functions.RuntimeStateActive {
		t.Fatalf("state = %q, want %q", status.State, functions.RuntimeStateActive)
	}
	if status.Phase != functions.RuntimePhaseReady {
		t.Fatalf("phase = %q, want %q", status.Phase, functions.RuntimePhaseReady)
	}
	if status.ReadinessState != functions.RuntimeReadinessStateReady {
		t.Fatalf("readiness = %q, want %q", status.ReadinessState, functions.RuntimeReadinessStateReady)
	}
}

func TestDeleteRuntimeSandboxesForRevisionKeepsMappingWhenDeleteFails(t *testing.T) {
	deleteErr := errors.New("delete failed")
	runtime := &recordingRuntimeController{errors: map[string]error{"sb-2": deleteErr}}
	handler := &Handler{runtime: runtime, logger: zap.NewNop()}
	rev := &functions.Revision{ID: "rev-1", FunctionID: "fn-1", TeamID: "team-1"}

	complete := handler.deleteRuntimeSandboxesForRevision(context.Background(), &authn.AuthContext{TeamID: "team-1"}, rev, map[string]struct{}{
		"sb-1": {},
		"sb-2": {},
	})

	if complete {
		t.Fatal("deleteRuntimeSandboxesForRevision() complete = true, want false")
	}
	if !runtime.deleted["sb-1"] || !runtime.deleted["sb-2"] {
		t.Fatalf("deleted sandboxes = %#v, want both attempted", runtime.deleted)
	}
}

func TestDeleteRuntimeSandboxesForRevisionCompletesWhenDeletesSucceed(t *testing.T) {
	runtime := &recordingRuntimeController{}
	handler := &Handler{runtime: runtime, logger: zap.NewNop()}
	rev := &functions.Revision{ID: "rev-1", FunctionID: "fn-1", TeamID: "team-1"}

	complete := handler.deleteRuntimeSandboxesForRevision(context.Background(), &authn.AuthContext{TeamID: "team-1"}, rev, map[string]struct{}{
		"sb-1": {},
	})

	if !complete {
		t.Fatal("deleteRuntimeSandboxesForRevision() complete = false, want true")
	}
	if !runtime.deleted["sb-1"] {
		t.Fatalf("deleted sandboxes = %#v, want sb-1", runtime.deleted)
	}
}

type recordingRuntimeController struct {
	deleted map[string]bool
	errors  map[string]error
}

type recordingRevisionVolumeStore struct {
	prepareArtifactCalled bool
	artifactMounts        []functions.FunctionRevisionMount
	artifactCleanup       []functions.RestoreMount
	deletedMounts         []functions.RestoreMount
}

func (r *recordingRevisionVolumeStore) PrepareRestoreMounts(ctx context.Context, authCtx *authn.AuthContext, sandbox *mgr.Sandbox, metadata functionruntime.Metadata) ([]functions.RestoreMount, error) {
	return nil, nil
}

func (r *recordingRevisionVolumeStore) DeleteRestoreMounts(ctx context.Context, authCtx *authn.AuthContext, sandbox *mgr.Sandbox, mounts []functions.RestoreMount) error {
	r.deletedMounts = append(r.deletedMounts, mounts...)
	return nil
}

func (r *recordingRevisionVolumeStore) PrepareArtifactMounts(ctx context.Context, authCtx *authn.AuthContext, revisionSpec functions.FunctionRevisionSpec, metadata functionruntime.Metadata) ([]functions.FunctionRevisionMount, []functions.RestoreMount, error) {
	r.prepareArtifactCalled = true
	return r.artifactMounts, r.artifactCleanup, nil
}

func (r *recordingRuntimeController) DeleteRuntimeSandbox(_ context.Context, _ *authn.AuthContext, sandboxID string) error {
	if r.deleted == nil {
		r.deleted = make(map[string]bool)
	}
	r.deleted[sandboxID] = true
	if r.errors != nil {
		return r.errors[sandboxID]
	}
	return nil
}
