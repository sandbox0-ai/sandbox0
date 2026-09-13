package session

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestSupervisorActivationOwnerPolicy(t *testing.T) {
	for _, tc := range []struct {
		name        string
		owner       string
		resetCopied bool
		wantError   bool
		wantSession bool
	}{
		{name: "legacy_unowned_is_adopted", wantSession: true},
		{name: "copied_unowned_is_cleared", resetCopied: true},
		{name: "foreign_owner_requires_reset", owner: "source-sandbox", wantError: true},
		{name: "foreign_owner_is_cleared", owner: "source-sandbox", resetCopied: true},
		{name: "matching_owner_is_preserved", owner: "target-sandbox", wantSession: true},
		{name: "matching_owner_survives_repeated_reset_assignment", owner: "target-sandbox", resetCopied: true, wantSession: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := NewFileStore(filepath.Join(t.TempDir(), "sessions"))
			if err != nil {
				t.Fatal(err)
			}
			if tc.owner != "" {
				if _, err := store.BindSandbox(tc.owner); err != nil {
					t.Fatal(err)
				}
			}
			record := Session{
				ID: "ses-existing", CreationKey: "existing-key",
				Spec: normalizeSpec(SessionSpec{Command: []string{"/bin/true"},
					Lifecycle: LifecycleSpec{DesiredState: DesiredStateStopped}}),
				SpecVersion: 1, Phase: PhaseStopped, RuntimeGeneration: 1,
			}
			if err := store.Save(record); err != nil {
				t.Fatal(err)
			}
			supervisor, err := NewSupervisor(store, zap.NewNop())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { cleanupSupervisor(t, supervisor) })
			err = supervisor.Activate(Activation{SandboxID: "target-sandbox", RuntimeGeneration: 2, ResetCopiedSessionState: tc.resetCopied})
			if (err != nil) != tc.wantError {
				t.Fatalf("activation error = %v, wantError %t", err, tc.wantError)
			}
			owner, readErr := os.ReadFile(filepath.Join(store.root, sandboxIDFile))
			if readErr != nil {
				t.Fatal(readErr)
			}
			if tc.wantError {
				if strings.TrimSpace(string(owner)) != tc.owner {
					t.Fatal("rejected activation changed owner")
				}
				if _, err := os.Stat(filepath.Join(store.root, record.ID, sessionStateFile)); err != nil {
					t.Fatal("rejected activation removed source state")
				}
				return
			}
			if strings.TrimSpace(string(owner)) != "target-sandbox" {
				t.Fatalf("owner = %q", owner)
			}
			_, err = supervisor.Get(record.ID)
			if tc.wantSession {
				if err != nil {
					t.Fatal(err)
				}
				value, duplicate, err := supervisor.Create(record.Spec, record.CreationKey)
				if err != nil || !duplicate || value.ID != record.ID {
					t.Fatalf("preserved idempotency key: value=%#v duplicate=%t error=%v", value, duplicate, err)
				}
			} else {
				if !errors.Is(err, ErrSessionNotFound) {
					t.Fatalf("copied session was exposed: %v", err)
				}
				if _, err := os.Stat(filepath.Join(store.root, record.ID)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("copied directory remains: %v", err)
				}
				_, duplicate, err := supervisor.Create(record.Spec, record.CreationKey)
				if err != nil || duplicate {
					t.Fatalf("copied idempotency key was retained: duplicate=%t error=%v", duplicate, err)
				}
			}
		})
	}
}

func TestSupervisorCopiedUnownedStateIsClearedBeforeDecode(t *testing.T) {
	store, err := NewFileStore(filepath.Join(t.TempDir(), "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(store.root, "ses-copied")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, sessionStateFile), []byte("invalid copied state"), 0600); err != nil {
		t.Fatal(err)
	}
	supervisor, err := NewSupervisor(store, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupSupervisor(t, supervisor) })
	if err := supervisor.Activate(Activation{SandboxID: "target-sandbox", RuntimeGeneration: 1, ResetCopiedSessionState: true}); err != nil {
		t.Fatal(err)
	}
	if len(supervisor.List()) != 0 {
		t.Fatal("copied state became visible")
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("copied directory remains: %v", err)
	}
}

func TestSupervisorCopiedUnownedSessionDoesNotStartAttempt(t *testing.T) {
	store, err := NewFileStore(filepath.Join(t.TempDir(), "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	record := Session{
		ID: "ses-copied-running", SpecVersion: 1, Phase: PhaseRunning, RuntimeGeneration: 1,
		Spec: normalizeSpec(SessionSpec{Command: []string{"/bin/true"}, Lifecycle: LifecycleSpec{
			DesiredState: DesiredStateRunning, RuntimeRecovery: RuntimeRecoveryRestart}}),
		Attempt: &Attempt{ID: "att-source", Number: 1, RuntimeGeneration: 1, StartedAt: &now},
	}
	if err := store.Save(record); err != nil {
		t.Fatal(err)
	}
	supervisor, err := NewSupervisor(store, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupSupervisor(t, supervisor) })
	if err := supervisor.Activate(Activation{SandboxID: "target-sandbox", RuntimeGeneration: 1, ResetCopiedSessionState: true}); err != nil {
		t.Fatal(err)
	}
	if got := supervisor.List(); len(got) != 0 {
		t.Fatalf("copied session recovered into target: %#v", got)
	}
}

func TestSupervisorCopiedResetRecreatesRemovedStateDirectory(t *testing.T) {
	store, err := NewFileStore(filepath.Join(t.TempDir(), "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(store.root); err != nil {
		t.Fatal(err)
	}
	supervisor, err := NewSupervisor(store, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupSupervisor(t, supervisor) })
	if err := supervisor.Activate(Activation{SandboxID: "target-sandbox", RuntimeGeneration: 1, ResetCopiedSessionState: true}); err != nil {
		t.Fatal(err)
	}
	owner, err := os.ReadFile(filepath.Join(store.root, sandboxIDFile))
	if err != nil || strings.TrimSpace(string(owner)) != "target-sandbox" {
		t.Fatalf("recreated owner = %q, error %v", owner, err)
	}
}

func TestSupervisorOwnerReadFailurePreservesSessionState(t *testing.T) {
	store, err := NewFileStore(filepath.Join(t.TempDir(), "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(store.root, sandboxIDFile), 0700); err != nil {
		t.Fatal(err)
	}
	stateDir := filepath.Join(store.root, "ses-retained")
	if err := os.Mkdir(stateDir, 0700); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(stateDir, sessionStateFile)
	const originalState = "must not decode or remove before resolving ownership"
	if err := os.WriteFile(statePath, []byte(originalState), 0600); err != nil {
		t.Fatal(err)
	}
	supervisor, err := NewSupervisor(store, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupSupervisor(t, supervisor) })
	if err := supervisor.Activate(Activation{SandboxID: "target-sandbox", RuntimeGeneration: 1, ResetCopiedSessionState: true}); err == nil {
		t.Fatal("activation accepted an unreadable owner")
	}
	data, err := os.ReadFile(statePath)
	if err != nil || string(data) != originalState {
		t.Fatalf("state changed after ownership error: %q %v", data, err)
	}
	if supervisor.active || len(supervisor.List()) != 0 {
		t.Fatal("ownership failure published active sessions")
	}
}
