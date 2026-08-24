package runtimecontroller

import (
	"context"
	"testing"

	ctxpkg "github.com/sandbox0-ai/sandbox0/manager/procd/pkg/context"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/session"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
	"go.uber.org/zap"
)

func TestControllerActivatesImmutableAssignment(t *testing.T) {
	controller := newTestController(t)
	if result := controller.Probe(sandboxprobe.KindReadiness); result.Status != sandboxprobe.StatusSuspended {
		t.Fatalf("pending readiness = %#v", result)
	}
	if ready, _ := controller.CanServe(); ready {
		t.Fatal("pending runtime was externally serveable")
	}

	assignment := runtimecontrol.Assignment{
		SandboxID:         "sandbox-1",
		RuntimeGeneration: 2,
		EnvVars:           map[string]string{runtimecontrol.EnvSandboxID: "sandbox-1", "MODE": "test"},
	}
	if err := controller.Activate(context.Background(), assignment); err != nil {
		t.Fatalf("Activate() error = %v", err)
	}
	if ready, reason := controller.CanServe(); !ready {
		t.Fatalf("active runtime is not serveable: %s", reason)
	}
	if result := controller.Probe(sandboxprobe.KindReadiness); result.Status != sandboxprobe.StatusPassed {
		t.Fatalf("active readiness = %#v", result)
	}
	revision, err := assignment.Revision()
	if err != nil {
		t.Fatal(err)
	}
	state := controller.State()
	if state.Phase != PhaseReady || state.Revision != revision || state.RuntimeGeneration != 2 {
		t.Fatalf("state = %#v", state)
	}
	if err := controller.Activate(context.Background(), assignment); err != nil {
		t.Fatalf("idempotent Activate() error = %v", err)
	}
	assignment.RuntimeGeneration++
	if err := controller.Activate(context.Background(), assignment); err == nil {
		t.Fatal("Activate() accepted a changed assignment")
	}
}

func TestControllerFailsClosedOnCopiedSessionStateWithoutExplicitReset(t *testing.T) {
	stateDir := t.TempDir()
	sourceStore, err := session.NewFileStore(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sourceStore.BindSandbox("source-sandbox"); err != nil {
		t.Fatal(err)
	}
	supervisor, err := session.NewSupervisor(sourceStore, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = supervisor.Close() })
	controller := New(ctxpkg.NewManagerWithSupervisor(supervisor), supervisor, nil, nil, 49983, zap.NewNop())

	assignment := runtimecontrol.Assignment{
		SandboxID:         "target-sandbox",
		RuntimeGeneration: 1,
	}
	if err := controller.Activate(context.Background(), assignment); err == nil {
		t.Fatal("Activate() accepted copied state without an explicit reset")
	}
	if ready, _ := controller.CanServe(); ready {
		t.Fatal("failed copied state was serveable")
	}
	if result := controller.Probe(sandboxprobe.KindReadiness); result.Status != sandboxprobe.StatusFailed {
		t.Fatalf("failed activation readiness = %#v", result)
	}
	if result := controller.Probe(sandboxprobe.KindLiveness); result.Status != sandboxprobe.StatusPassed {
		t.Fatalf("failed activation liveness = %#v", result)
	}
}

func TestFreshProcdRecoversImmutableAssignmentWithoutExternalRequest(t *testing.T) {
	stateDir := t.TempDir()
	assignment := runtimecontrol.Assignment{
		SandboxID:         "sandbox-1",
		RuntimeGeneration: 4,
	}

	activate := func() {
		store, storeErr := session.NewFileStore(stateDir)
		if storeErr != nil {
			t.Fatal(storeErr)
		}
		supervisor, supervisorErr := session.NewSupervisor(store, zap.NewNop())
		if supervisorErr != nil {
			t.Fatal(supervisorErr)
		}
		controller := New(ctxpkg.NewManagerWithSupervisor(supervisor), supervisor, nil, nil, 49983, zap.NewNop())
		if activateErr := controller.Activate(context.Background(), assignment); activateErr != nil {
			t.Fatal(activateErr)
		}
		if controller.State().Phase != PhaseReady {
			t.Fatalf("state = %#v, want ready", controller.State())
		}
		if closeErr := supervisor.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
	}

	activate()
	activate()
}

func newTestController(t *testing.T) *Controller {
	t.Helper()
	store, err := session.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	supervisor, err := session.NewSupervisor(store, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = supervisor.Close() })
	return New(ctxpkg.NewManagerWithSupervisor(supervisor), supervisor, nil, nil, 49983, zap.NewNop())
}
