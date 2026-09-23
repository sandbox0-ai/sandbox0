package runtimecontroller

import (
	"context"
	"maps"
	"os"
	"path/filepath"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/session"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/webhook"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func checkpointControllerFixture(t *testing.T, c *Controller) runtimecontrol.CheckpointRestoreAssignment {
	t.Helper()
	source := runtimecontrol.Assignment{SandboxID: "source", TeamID: "team", RuntimeGeneration: 1,
		SecurityClass: "standard", EnvVars: map[string]string{runtimecontrol.EnvSandboxID: "source"}}
	if err := c.Activate(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	capture, err := runtimecontrol.NewCheckpointCaptureAssignment("capture", source)
	if err != nil {
		t.Fatal(err)
	}
	target := source
	target.SandboxID = "child"
	target.EnvVars = maps.Clone(source.EnvVars)
	target.EnvVars[runtimecontrol.EnvSandboxID] = target.SandboxID
	return runtimecontrol.CheckpointRestoreAssignment{OperationID: "fork", Capture: capture,
		Kind: runtimecontrol.CheckpointFork, Target: target}
}

func TestCheckpointControllerForkChoosesTargetAfterPreparation(t *testing.T) {
	c := New(nil, nil, nil, nil, 49983, zap.NewNop())
	request := checkpointControllerFixture(t, c)
	if err := c.RestoreCheckpoint(request); err == nil {
		t.Fatal("unprepared image was restored")
	}
	for range 2 {
		if err := c.PrepareCheckpoint(request.Capture); err != nil {
			t.Fatal(err)
		}
	}
	if ready, _ := c.CanServe(); ready {
		t.Fatal("prepared source still serves APIs")
	}
	if err := c.Activate(context.Background(), request.Target); err == nil {
		t.Fatal("ordinary activation bypassed checkpoint")
	}
	for range 2 {
		if err := c.RestoreCheckpoint(request); err != nil {
			t.Fatal(err)
		}
	}
	if ready, _ := c.CanServe(); !ready {
		t.Fatal("restored fork is not ready")
	}
	if err := c.CancelPreparedCheckpoint(request.Capture); err == nil {
		t.Fatal("restored fork reopened source")
	}
	other := request
	other.OperationID = "another-restore"
	if err := c.RestoreCheckpoint(other); err == nil {
		t.Fatal("another restore took ownership of restored process")
	}
	if err := c.PrepareCheckpoint(request.Capture); err == nil {
		t.Fatal("old capture gated restored child")
	}
	// The child can be independently paused and resumed using its own identity.
	capture, err := runtimecontrol.NewCheckpointCaptureAssignment("child-pause", request.Target)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.PrepareCheckpoint(capture); err != nil {
		t.Fatal(err)
	}
	next := runtimecontrol.CheckpointRestoreAssignment{OperationID: "child-resume",
		Capture: capture, Kind: runtimecontrol.CheckpointResume, Target: request.Target}
	next.Target.RuntimeGeneration++
	if err := c.RestoreCheckpoint(next); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointControllerCanceledCaptureCannotRestore(t *testing.T) {
	for _, prepare := range []bool{false, true} {
		c := New(nil, nil, nil, nil, 49983, zap.NewNop())
		request := checkpointControllerFixture(t, c)
		if prepare {
			if err := c.PrepareCheckpoint(request.Capture); err != nil {
				t.Fatal(err)
			}
		}
		for range 2 {
			if err := c.CancelPreparedCheckpoint(request.Capture); err != nil {
				t.Fatal(err)
			}
		}
		if err := c.PrepareCheckpoint(request.Capture); err == nil {
			t.Fatal("prepare overtook cancellation tombstone")
		}
		if err := c.RestoreCheckpoint(request); err == nil {
			t.Fatal("restore overtook cancellation")
		}
	}
}

func TestCheckpointControllerPartialForkCannotChangeTarget(t *testing.T) {
	dir := t.TempDir()
	store, err := session.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	supervisor, err := session.NewSupervisor(store, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = supervisor.Close() })
	c := New(nil, supervisor, nil, nil, 49983, zap.NewNop())
	request := checkpointControllerFixture(t, c)
	value, _, err := supervisor.Create(session.SessionSpec{Command: []string{"/bin/true"},
		Lifecycle: session.LifecycleSpec{DesiredState: session.DesiredStateStopped}}, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.PrepareCheckpoint(request.Capture); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(dir, value.ID, "state.json")
	if err := os.Remove(statePath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(statePath, 0700); err != nil {
		t.Fatal(err)
	}
	if err := c.RestoreCheckpoint(request); err == nil {
		t.Fatal("fork ignored failed persistence")
	}
	if ready, _ := c.CanServe(); ready {
		t.Fatal("failed fork published readiness")
	}
	if err := c.CancelPreparedCheckpoint(request.Capture); err == nil {
		t.Fatal("partial fork reopened source")
	}
	other := request
	other.OperationID = "replacement"
	if err := c.RestoreCheckpoint(other); err == nil {
		t.Fatal("partial fork allowed a different operation")
	}
	if err := os.Remove(statePath); err != nil {
		t.Fatal(err)
	}
	if err := c.RestoreCheckpoint(request); err != nil {
		t.Fatal(err)
	}
	if ready, _ := c.CanServe(); !ready {
		t.Fatal("exact retry did not finish fork")
	}
}

func TestCheckpointControllerCancellationCannotReleaseMigration(t *testing.T) {
	c := New(nil, nil, nil, nil, 49983, zap.NewNop())
	request := checkpointControllerFixture(t, c)
	if err := c.CancelPreparedCheckpoint(request.Capture); err != nil {
		t.Fatal(err)
	}
	target := request.Target
	target.SandboxID = request.Capture.SandboxID
	target.EnvVars = maps.Clone(target.EnvVars)
	target.EnvVars[runtimecontrol.EnvSandboxID] = target.SandboxID
	target.RuntimeGeneration = request.Capture.RuntimeGeneration + 1
	migration := runtimecontrol.MigrationAssignment{OperationID: "migration",
		SourceGeneration: request.Capture.RuntimeGeneration, SourceRevision: request.Capture.Revision, Target: target}
	if err := c.PrepareMigration(migration); err != nil {
		t.Fatal(err)
	}
	if err := c.CancelPreparedCheckpoint(request.Capture); err == nil {
		t.Fatal("stale checkpoint cancellation reopened migration")
	}
	if err := c.CancelPreparedMigration(migration); err != nil {
		t.Fatal(err)
	}
	next := request.Capture
	next.OperationID = "next-checkpoint"
	if err := c.PrepareCheckpoint(next); err != nil {
		t.Fatal(err)
	}
	if err := c.CancelPreparedMigration(migration); err == nil {
		t.Fatal("stale migration cancellation reopened checkpoint")
	}
	if ready, _ := c.CanServe(); ready {
		t.Fatal("conflicting cancellation reopened API admission")
	}
}

func TestCheckpointControllerPreservesWebhookGateAcrossCanceledPreparation(t *testing.T) {
	d := webhook.NewDispatcher(webhook.Options{OutboxDir: t.TempDir()}, nil)
	t.Cleanup(func() { _ = d.Shutdown(context.Background()) })
	c := New(nil, nil, nil, d, 49983, zap.NewNop())
	request := checkpointControllerFixture(t, c)
	require.NoError(t, c.PrepareCheckpointContext(t.Context(), request.Capture))
	require.NoError(t, c.CancelPreparedCheckpoint(request.Capture))
	require.Error(t, d.RebindCheckpointIdentity("source", "child", "team"), "cancellation must reopen delivery")
	request.Capture.OperationID = "new-capture"
	require.NoError(t, c.PrepareCheckpointContext(t.Context(), request.Capture))
	require.NoError(t, c.RestoreCheckpoint(request))
	require.Error(t, d.RebindCheckpointIdentity("source", "another-child", "team"), "successful handover must reopen delivery only once")
}

func TestCheckpointControllerRejectsVolatileWebhookBeforeGatingRuntime(t *testing.T) {
	d := webhook.NewDispatcher(webhook.Options{}, nil)
	t.Cleanup(func() { _ = d.Shutdown(context.Background()) })
	c := New(nil, nil, nil, d, 49983, zap.NewNop())
	request := checkpointControllerFixture(t, c)
	d.SetConfig("http://127.0.0.1:1", "secret")
	require.ErrorContains(t, c.PrepareCheckpoint(request.Capture), "durable outbox")
	ready, _ := c.CanServe()
	require.True(t, ready, "unsupported capture must not close a healthy runtime")
}

func TestCheckpointControllerRestoreAfterFailedAttemptUsesCapturedProcess(t *testing.T) {
	for _, kind := range []runtimecontrol.CheckpointRestoreKind{runtimecontrol.CheckpointResume, runtimecontrol.CheckpointFork} {
		t.Run(string(kind), func(t *testing.T) {
			store, err := session.NewFileStore(t.TempDir())
			require.NoError(t, err)
			supervisor, err := session.NewSupervisor(store, zap.NewNop())
			require.NoError(t, err)
			t.Cleanup(func() { _ = supervisor.Close() })
			c := New(nil, supervisor, nil, nil, 49983, zap.NewNop())
			request := checkpointControllerFixture(t, c)
			value, _, err := supervisor.Create(session.SessionSpec{Command: []string{"/bin/true"},
				Lifecycle: session.LifecycleSpec{DesiredState: session.DesiredStateStopped}}, "preserved-session")
			require.NoError(t, err)
			request.Kind = kind
			if kind == runtimecontrol.CheckpointResume {
				request.Target.SandboxID = request.Capture.SandboxID
				request.Target.EnvVars[runtimecontrol.EnvSandboxID] = request.Target.SandboxID
			}
			request.FromGeneration = request.Capture.RuntimeGeneration + 2
			request.Target.RuntimeGeneration = request.FromGeneration + 1
			require.NoError(t, c.PrepareCheckpoint(request.Capture))
			require.NoError(t, c.RestoreCheckpoint(request))
			require.NoError(t, c.RestoreCheckpoint(request))
			ready, _ := c.CanServe()
			require.True(t, ready)
			retained, err := supervisor.Get(value.ID)
			require.NoError(t, err)
			require.Equal(t, request.Target.RuntimeGeneration, retained.RuntimeGeneration)
			require.Equal(t, value.Attempt, retained.Attempt)
			other := request
			other.OperationID = "late-failed-attempt"
			other.FromGeneration--
			other.Target.RuntimeGeneration--
			require.Error(t, c.RestoreCheckpoint(other))
		})
	}
}
