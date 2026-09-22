package session

import (
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestMigrationPreservesLiveAttemptAndInputReceipts(t *testing.T) {
	supervisor := newTestSupervisor(t)
	value, _, err := supervisor.Create(SessionSpec{
		Command: []string{"/bin/sh", "-c", "while IFS= read -r line; do printf 'out:%s\\n' \"$line\"; done"},
		IO:      IOSpec{Mode: IOModePipes},
	}, "create-before-migration")
	if err != nil {
		t.Fatal(err)
	}
	waitForSessionPhase(t, supervisor, value.ID, PhaseRunning)
	value, _ = supervisor.Get(value.ID)
	input := InputRequest{InputID: "before", ExpectedAttemptID: value.Attempt.ID,
		DataBase64: base64.StdEncoding.EncodeToString([]byte("before\n"))}
	if _, err := supervisor.WriteInput(value.ID, input); err != nil {
		t.Fatal(err)
	}
	waitForMigrationOutput(t, supervisor, value.ID, "out:before\n")
	before, _ := supervisor.Get(value.ID)
	supervisor.mu.RLock()
	managed := supervisor.sessions[value.ID]
	managed.mu.Lock()
	runtime, journal := managed.runtime, managed.journal
	managed.mu.Unlock()
	supervisor.mu.RUnlock()

	for range 2 {
		if err := supervisor.RebindRuntimeGeneration("migration-1", "sandbox-1", 1, 2); err != nil {
			t.Fatal(err)
		}
	}
	after, _ := supervisor.Get(value.ID)
	if after.RuntimeGeneration != 2 || !reflect.DeepEqual(before.Attempt, after.Attempt) ||
		before.Cursor != after.Cursor || !reflect.DeepEqual(before.InputReceipts, after.InputReceipts) {
		t.Fatalf("handover replaced execution state: before=%#v after=%#v", before, after)
	}
	managed.mu.Lock()
	preserved := managed.runtime == runtime && managed.journal == journal
	managed.mu.Unlock()
	if !preserved {
		t.Fatal("handover replaced process or journal handles")
	}
	if receipt, err := supervisor.WriteInput(value.ID, input); err != nil || !receipt.Duplicate {
		t.Fatalf("pre-migration receipt was lost: %#v, %v", receipt, err)
	}
	input.InputID = "after"
	input.DataBase64 = base64.StdEncoding.EncodeToString([]byte("after\n"))
	input.EOF = true
	if _, err := supervisor.WriteInput(value.ID, input); err != nil {
		t.Fatal(err)
	}
	waitForSessionPhase(t, supervisor, value.ID, PhaseExited)
	waitForMigrationOutput(t, supervisor, value.ID, "out:before\nout:after\n")
	created, duplicate, err := supervisor.Create(value.Spec, "create-before-migration")
	if err != nil || !duplicate || created.ID != value.ID {
		t.Fatalf("migration lost creation key: %#v, %t, %v", created, duplicate, err)
	}
	if err := supervisor.RebindRuntimeGeneration("different-operation", "sandbox-1", 1, 2); err == nil {
		t.Fatal("different operation replay was accepted")
	}
	if err := supervisor.Activate(Activation{SandboxID: "sandbox-1", RuntimeGeneration: 1}); err == nil {
		t.Fatal("ordinary activation rolled back the migrated generation")
	}
	if err := supervisor.RebindRuntimeGeneration("migration-2", "sandbox-1", 2, 3); err != nil {
		t.Fatal(err)
	}
	if err := supervisor.RebindRuntimeGeneration("migration-1", "sandbox-1", 1, 2); err == nil {
		t.Fatal("stale operation rolled back a later migration")
	}
}

func TestMigrationPartialSessionSaveStaysRetryable(t *testing.T) {
	supervisor := newTestSupervisor(t)
	for range 2 {
		if _, _, err := supervisor.Create(SessionSpec{Command: []string{"/bin/true"},
			Lifecycle: LifecycleSpec{DesiredState: DesiredStateStopped}}, ""); err != nil {
			t.Fatal(err)
		}
	}
	values := supervisor.List()
	statePath := filepath.Join(supervisor.store.root, values[0].ID, sessionStateFile)
	backup := statePath + ".saved"
	if err := os.Rename(statePath, backup); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(statePath, 0700); err != nil {
		t.Fatal(err)
	}
	repaired := false
	repair := func() {
		if repaired {
			return
		}
		if err := os.Remove(statePath); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(backup, statePath); err != nil {
			t.Fatal(err)
		}
		repaired = true
	}
	t.Cleanup(repair)
	if err := supervisor.RebindRuntimeGeneration("migration-1", "sandbox-1", 1, 2); err == nil {
		t.Fatal("handover ignored persistence failure")
	}
	for _, value := range supervisor.List() {
		if value.RuntimeGeneration != 2 {
			t.Fatalf("mixed in-memory generation: %#v", value)
		}
	}
	if err := supervisor.Activate(Activation{SandboxID: "sandbox-1", RuntimeGeneration: 2}); err == nil {
		t.Fatal("activation bypassed incomplete handover")
	}
	if err := supervisor.RebindRuntimeGeneration("migration-2", "sandbox-1", 2, 3); err == nil {
		t.Fatal("new handover bypassed incomplete persistence")
	}
	repair()
	if err := supervisor.RebindRuntimeGeneration("migration-1", "sandbox-1", 1, 2); err != nil {
		t.Fatal(err)
	}
	stored, err := supervisor.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range stored {
		if value.RuntimeGeneration != 2 {
			t.Fatalf("retry left stale durable generation: %#v", value)
		}
	}
}

func TestMigrationConcurrentSessionCreationUsesCommittedGeneration(t *testing.T) {
	supervisor := newTestSupervisor(t)
	const count = 16
	start := make(chan struct{})
	errCh := make(chan error, count+1)
	var wg sync.WaitGroup
	for i := range count {
		wg.Go(func() {
			<-start
			_, _, err := supervisor.Create(SessionSpec{Command: []string{"/bin/true"}}, fmt.Sprint(i))
			errCh <- err
		})
	}
	wg.Go(func() {
		<-start
		errCh <- supervisor.RebindRuntimeGeneration("migration-1", "sandbox-1", 1, 2)
	})
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, value := range supervisor.List() {
		if value.RuntimeGeneration != 2 {
			t.Fatalf("late source-generation attempt: %#v", value)
		}
	}
}

func waitForMigrationOutput(t *testing.T, supervisor *Supervisor, id, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var got string
	for time.Now().Before(deadline) {
		page, err := supervisor.Events(id, 0, 1000)
		if err != nil {
			t.Fatal(err)
		}
		got = ""
		for _, event := range page.Events {
			if event.Stream == "stdout" {
				data, err := base64.StdEncoding.DecodeString(event.DataBase64)
				if err != nil {
					t.Fatal(err)
				}
				got += string(data)
			}
		}
		if got == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("output = %q, want %q", got, want)
}
