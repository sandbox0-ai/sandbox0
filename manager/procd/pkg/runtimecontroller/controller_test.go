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

func TestControllerTransitionsStandbyWaitingAndReady(t *testing.T) {
	controller := newTestController(t)
	var observations []runtimecontrol.Observation
	report := func(observation runtimecontrol.Observation) error {
		observations = append(observations, observation)
		return nil
	}

	if err := controller.HandleSnapshot(context.Background(), runtimecontrol.Snapshot{
		State: runtimecontrol.DesiredStandby,
	}, report); err != nil {
		t.Fatalf("standby HandleSnapshot() error = %v", err)
	}
	if result := controller.Probe(sandboxprobe.KindReadiness); result.Status != sandboxprobe.StatusPassed {
		t.Fatalf("standby readiness = %#v", result)
	}
	if ready, _ := controller.CanServe(); ready {
		t.Fatal("standby runtime was externally serveable")
	}

	assignment := runtimecontrol.Assignment{
		SandboxID:         "sandbox-1",
		RuntimeGeneration: 2,
		EnvVars:           map[string]string{"MODE": "test"},
	}
	revision, err := assignment.Revision()
	if err != nil {
		t.Fatal(err)
	}
	waiting := runtimecontrol.Snapshot{
		State:      runtimecontrol.DesiredWaitingStorage,
		Revision:   revision,
		Assignment: &assignment,
	}
	if err := controller.HandleSnapshot(context.Background(), waiting, report); err != nil {
		t.Fatalf("waiting HandleSnapshot() error = %v", err)
	}
	if result := controller.Probe(sandboxprobe.KindReadiness); result.Status != sandboxprobe.StatusSuspended {
		t.Fatalf("waiting readiness = %#v", result)
	}
	if result := controller.Probe(sandboxprobe.KindLiveness); result.Status != sandboxprobe.StatusPassed {
		t.Fatalf("waiting liveness = %#v", result)
	}

	active := waiting
	active.State = runtimecontrol.DesiredActive
	if err := controller.HandleSnapshot(context.Background(), active, report); err != nil {
		t.Fatalf("active HandleSnapshot() error = %v", err)
	}
	if ready, reason := controller.CanServe(); !ready {
		t.Fatalf("active runtime is not serveable: %s", reason)
	}
	if got := observations[len(observations)-1]; got.State != runtimecontrol.ObservedReady ||
		got.Revision != revision || got.RuntimeGeneration != 2 {
		t.Fatalf("last observation = %#v", got)
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
	revision, err := assignment.Revision()
	if err != nil {
		t.Fatal(err)
	}
	var last runtimecontrol.Observation
	err = controller.HandleSnapshot(context.Background(), runtimecontrol.Snapshot{
		State:      runtimecontrol.DesiredActive,
		Revision:   revision,
		Assignment: &assignment,
	}, func(observation runtimecontrol.Observation) error {
		last = observation
		return nil
	})
	if err != nil {
		t.Fatalf("HandleSnapshot() error = %v", err)
	}
	if last.State != runtimecontrol.ObservedFailed {
		t.Fatalf("last observation = %#v", last)
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

func TestFreshProcdRecoversActiveManifestWithoutExternalRequest(t *testing.T) {
	stateDir := t.TempDir()
	assignment := runtimecontrol.Assignment{
		SandboxID:         "sandbox-1",
		RuntimeGeneration: 4,
	}
	revision, err := assignment.Revision()
	if err != nil {
		t.Fatal(err)
	}
	snapshot := runtimecontrol.Snapshot{
		State:      runtimecontrol.DesiredActive,
		Revision:   revision,
		Assignment: &assignment,
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
		var last runtimecontrol.Observation
		if handleErr := controller.HandleSnapshot(context.Background(), snapshot, func(observation runtimecontrol.Observation) error {
			last = observation
			return nil
		}); handleErr != nil {
			t.Fatal(handleErr)
		}
		if last.State != runtimecontrol.ObservedReady {
			t.Fatalf("last observation = %#v, want ready", last)
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
