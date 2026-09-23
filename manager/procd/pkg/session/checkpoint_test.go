package session

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestCheckpointForkPreservesLiveAttemptAtEqualGeneration(t *testing.T) {
	s := newTestSupervisor(t)
	value, _, err := s.Create(SessionSpec{
		Command: []string{"/bin/sh", "-c", "while IFS= read -r line; do printf 'out:%s\\n' \"$line\"; done"},
		IO:      IOSpec{Mode: IOModePipes},
	}, "creation-before-fork")
	if err != nil {
		t.Fatal(err)
	}
	waitForSessionPhase(t, s, value.ID, PhaseRunning)
	before, _ := s.Get(value.ID)
	s.mu.RLock()
	managed := s.sessions[value.ID]
	managed.mu.Lock()
	process, journal := managed.runtime, managed.journal
	managed.mu.Unlock()
	s.mu.RUnlock()
	for range 2 {
		if err := s.RebindRuntimeIdentity("fork-1", "sandbox-1", "child-1", 1, 1); err != nil {
			t.Fatal(err)
		}
	}
	after, _ := s.Get(value.ID)
	if !reflect.DeepEqual(before.Attempt, after.Attempt) {
		t.Fatal("fork restarted or replaced the live attempt")
	}
	managed.mu.Lock()
	preserved := managed.runtime == process && managed.journal == journal
	managed.mu.Unlock()
	if !preserved {
		t.Fatal("fork replaced execution or event journal handles")
	}
	owner, err := os.ReadFile(filepath.Join(s.store.root, sandboxIDFile))
	if err != nil || string(owner) != "child-1\n" {
		t.Fatalf("fork owner = %q, %v", owner, err)
	}
	if err := s.Activate(Activation{SandboxID: "sandbox-1", RuntimeGeneration: 1}); err == nil {
		t.Fatal("source identity reactivated the fork")
	}
	if err := s.RebindRuntimeIdentity("fork-2", "sandbox-1", "child-2", 1, 1); err == nil {
		t.Fatal("a second target reused the restored process")
	}
	if _, err := s.WriteInput(value.ID, InputRequest{InputID: "after-fork", ExpectedAttemptID: before.Attempt.ID,
		DataBase64: base64.StdEncoding.EncodeToString([]byte("alive\n")), EOF: true}); err != nil {
		t.Fatal(err)
	}
	waitForSessionPhase(t, s, value.ID, PhaseExited)
	waitForMigrationOutput(t, s, value.ID, "out:alive\n")
	duplicate, found, err := s.Create(value.Spec, "creation-before-fork")
	if err != nil || !found || duplicate.ID != value.ID {
		t.Fatal("fork lost the session creation receipt")
	}
	if err := s.RebindRuntimeGeneration("move-child", "child-1", 1, 2); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointForkOwnerFailureKeepsExactRetry(t *testing.T) {
	s := newTestSupervisor(t)
	value, _, err := s.Create(SessionSpec{Command: []string{"/bin/true"},
		Lifecycle: LifecycleSpec{DesiredState: DesiredStateStopped}}, "")
	if err != nil {
		t.Fatal(err)
	}
	ownerPath := filepath.Join(s.store.root, sandboxIDFile)
	if err := os.Remove(ownerPath); err != nil {
		t.Fatal(err)
	}
	if err := s.RebindRuntimeIdentity("fork", "sandbox-1", "child", 1, 1); err == nil {
		t.Fatal("fork adopted missing store ownership")
	}
	if err := s.Activate(Activation{SandboxID: "child", RuntimeGeneration: 1}); err == nil {
		t.Fatal("activation bypassed incomplete owner persistence")
	}
	if err := s.RebindRuntimeIdentity("other", "child", "other-child", 1, 1); err == nil {
		t.Fatal("another fork bypassed the pending owner write")
	}
	if err := os.WriteFile(ownerPath, []byte("sandbox-1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.RebindRuntimeIdentity("fork", "sandbox-1", "child", 1, 1); err != nil {
		t.Fatal(err)
	}
	stored, err := s.store.Load()
	if err != nil || len(stored) != 1 || stored[0].ID != value.ID {
		t.Fatal("fork discarded persisted sessions")
	}
	if err := s.Activate(Activation{SandboxID: "child", RuntimeGeneration: 1}); err != nil {
		t.Fatal(err)
	}
}
