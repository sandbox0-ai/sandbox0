package functionapi

import (
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/functions"
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
