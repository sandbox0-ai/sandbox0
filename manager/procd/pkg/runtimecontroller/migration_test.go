package runtimecontroller

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/session"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
	"go.uber.org/zap"
)

func testMigrationAssignment(t *testing.T, controller *Controller) runtimecontrol.MigrationAssignment {
	t.Helper()
	source := runtimecontrol.Assignment{SandboxID: "sandbox-1", TeamID: "team-1",
		RuntimeGeneration: 1, SecurityClass: "standard"}
	if err := controller.Activate(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	revision, _ := source.Revision()
	target := source
	target.RuntimeGeneration = 2
	return runtimecontrol.MigrationAssignment{OperationID: "migration-1", SourceGeneration: 1,
		SourceRevision: revision, Target: target}
}

func TestControllerMigrationGatesUntilExactHandover(t *testing.T) {
	c := newTestController(t)
	request := testMigrationAssignment(t, c)
	if err := c.RestoreMigration(request); err == nil {
		t.Fatal("unprepared restore accepted")
	}
	for range 2 {
		if err := c.PrepareMigration(request); err != nil {
			t.Fatal(err)
		}
	}
	if ready, _ := c.CanServe(); ready {
		t.Fatal("prepared source still serves user APIs")
	}
	if probe := c.Probe(sandboxprobe.KindReadiness); probe.Status != sandboxprobe.StatusSuspended || probe.Reason != "RuntimeMigrating" {
		t.Fatalf("migration readiness = %#v", probe)
	}
	if probe := c.Probe(sandboxprobe.KindLiveness); probe.Status != sandboxprobe.StatusPassed {
		t.Fatalf("migration liveness = %#v", probe)
	}
	if err := c.Activate(context.Background(), request.Target); err == nil {
		t.Fatal("activation bypassed migration gate")
	}
	other := request
	other.OperationID = "other-operation"
	if err := c.PrepareMigration(other); err == nil {
		t.Fatal("another prepare replaced migration")
	}
	if err := c.RestoreMigration(other); err == nil {
		t.Fatal("another restore replaced migration")
	}
	if err := c.CancelPreparedMigration(other); err == nil {
		t.Fatal("another cancellation reopened source")
	}
	for range 2 {
		if err := c.RestoreMigration(request); err != nil {
			t.Fatal(err)
		}
	}
	if ready, _ := c.CanServe(); !ready {
		t.Fatal("restored target remains gated")
	}
	if got := c.State().RuntimeGeneration; got != 2 {
		t.Fatalf("generation = %d", got)
	}
	if err := c.CancelPreparedMigration(request); err == nil {
		t.Fatal("cancellation rolled back restored target")
	}
	if err := c.PrepareMigration(request); err == nil {
		t.Fatal("stale prepare acknowledged a source cut after restoration")
	}
}

func TestControllerMigrationCancellationPreservesSource(t *testing.T) {
	c := newTestController(t)
	request := testMigrationAssignment(t, c)
	if err := c.PrepareMigration(request); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := c.CancelPreparedMigration(request); err != nil {
			t.Fatal(err)
		}
	}
	if ready, _ := c.CanServe(); !ready || c.State().RuntimeGeneration != 1 {
		t.Fatal("cancellation did not reopen source")
	}
	if err := c.RestoreMigration(request); err == nil {
		t.Fatal("restore after cancellation was accepted")
	}
	if err := c.PrepareMigration(request); err == nil {
		t.Fatal("delayed preparation reclosed a canceled migration")
	}
}

func TestControllerMigrationCancellationBeforePreparation(t *testing.T) {
	c := newTestController(t)
	request := testMigrationAssignment(t, c)
	for range 2 {
		if err := c.CancelPreparedMigration(request); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.PrepareMigration(request); err == nil {
		t.Fatal("preparation overtook cancellation tombstone")
	}
	if ready, _ := c.CanServe(); !ready || c.State().RuntimeGeneration != 1 {
		t.Fatal("preparation-free cancellation changed source execution")
	}
	next := request
	next.OperationID = "next-migration"
	if err := c.PrepareMigration(next); err != nil {
		t.Fatal(err)
	}
}

func TestControllerMigrationPersistenceFailureCannotReopenSource(t *testing.T) {
	directory := t.TempDir()
	store, err := session.NewFileStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	supervisor, err := session.NewSupervisor(store, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = supervisor.Close() })
	c := New(nil, supervisor, nil, nil, 49983, zap.NewNop())
	request := testMigrationAssignment(t, c)
	value, _, err := supervisor.Create(session.SessionSpec{Command: []string{"/bin/true"},
		Lifecycle: session.LifecycleSpec{DesiredState: session.DesiredStateStopped}}, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.PrepareMigration(request); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, value.ID, "state.json")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := c.RestoreMigration(request); err == nil {
		t.Fatal("restore ignored failed session persistence")
	}
	if ready, _ := c.CanServe(); ready {
		t.Fatal("failed persistence published readiness")
	}
	if err := c.CancelPreparedMigration(request); err == nil {
		t.Fatal("partial restoration reopened the source generation")
	}
	if err := c.Activate(context.Background(), request.Target); err == nil {
		t.Fatal("activation bypassed restore retry")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := c.RestoreMigration(request); err != nil {
		t.Fatal(err)
	}
	if ready, _ := c.CanServe(); !ready {
		t.Fatal("exact retry did not restore readiness")
	}
	stored, err := store.Load()
	if err != nil || len(stored) != 1 || stored[0].RuntimeGeneration != 2 {
		t.Fatalf("durable target sessions = %#v, %v", stored, err)
	}
}
