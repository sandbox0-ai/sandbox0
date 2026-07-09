package context

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/process"
)

func TestFileStateStoreRoundTripAndRecoveryRequest(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "contexts")
	store, err := NewFileStateStore(dir)
	if err != nil {
		t.Fatalf("NewFileStateStore() error = %v", err)
	}
	record := persistedContext{
		ID:           "ctx-test",
		Config:       process.ProcessConfig{Type: process.ProcessTypeCMD, Command: []string{"/bin/sleep", "10"}},
		DesiredState: process.ProcessStateRunning,
		CreatedAt:    time.Now().UTC(),
	}
	if err := store.Save(record); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	records, err := store.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(records) != 1 || records[0].ID != record.ID || records[0].DesiredState != process.ProcessStateRunning {
		t.Fatalf("Load() = %#v, want running %s", records, record.ID)
	}

	if err := os.WriteFile(store.requestPath, []byte("requested\n"), 0o600); err != nil {
		t.Fatalf("write recovery request: %v", err)
	}
	requested, err := store.RecoveryRequested()
	if err != nil || !requested {
		t.Fatalf("RecoveryRequested() = %v, %v, want true, nil", requested, err)
	}
	consumed, err := store.ConsumeRecoveryRequest()
	if err != nil || !consumed {
		t.Fatalf("ConsumeRecoveryRequest() = %v, %v, want true, nil", consumed, err)
	}
	requested, err = store.RecoveryRequested()
	if err != nil || requested {
		t.Fatalf("RecoveryRequested() after consume = %v, %v, want false, nil", requested, err)
	}
}

func TestFileStateStoreClearRemovesMalformedRecords(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "contexts")
	store, err := NewFileStateStore(dir)
	if err != nil {
		t.Fatalf("NewFileStateStore() error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "broken.json"), []byte("{"), 0o600); err != nil {
		t.Fatalf("write malformed record: %v", err)
	}
	if err := store.Clear(); err != nil {
		t.Fatalf("Clear() error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "broken.json")); !os.IsNotExist(err) {
		t.Fatalf("malformed record still exists: %v", err)
	}
}

func TestManagerRestoreContextsReplaysRunningAndPausedDefinitions(t *testing.T) {
	tests := []struct {
		name   string
		paused bool
	}{
		{name: "running"},
		{name: "paused", paused: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, err := NewFileStateStore(filepath.Join(t.TempDir(), "contexts"))
			if err != nil {
				t.Fatalf("NewFileStateStore() error = %v", err)
			}
			first := NewManager()
			first.SetStateStore(store)
			created, err := first.CreateContext(process.ProcessConfig{
				Type:    process.ProcessTypeCMD,
				Command: []string{"/bin/sh", "-c", "while :; do sleep 1; done"},
				EnvVars: map[string]string{"RECOVERED": "true"},
			})
			if err != nil {
				t.Fatalf("CreateContext() error = %v", err)
			}
			if tt.paused {
				if err := first.PauseAll(); err != nil {
					t.Fatalf("PauseAll() error = %v", err)
				}
			}
			first.CleanupPreservingState()

			second := NewManager()
			second.SetStateStore(store)
			restored, err := second.RestoreContexts()
			if err != nil {
				t.Fatalf("RestoreContexts() error = %v", err)
			}
			t.Cleanup(second.Cleanup)
			if len(restored) != 1 {
				t.Fatalf("len(restored) = %d, want 1", len(restored))
			}
			if restored[0].ID != created.ID {
				t.Fatalf("restored ID = %q, want %q", restored[0].ID, created.ID)
			}
			if restored[0].EnvVars["RECOVERED"] != "true" {
				t.Fatalf("restored env = %#v", restored[0].EnvVars)
			}
			if tt.paused && !restored[0].IsPaused() {
				t.Fatal("restored context is not paused")
			}
			if !tt.paused && !restored[0].IsRunning() {
				t.Fatal("restored context is not running")
			}
		})
	}
}

func TestManagerRestoreContextsSkipsFinishedContext(t *testing.T) {
	store, err := NewFileStateStore(filepath.Join(t.TempDir(), "contexts"))
	if err != nil {
		t.Fatalf("NewFileStateStore() error = %v", err)
	}
	first := NewManager()
	first.SetStateStore(store)
	_, err = first.CreateContext(process.ProcessConfig{
		Type:    process.ProcessTypeCMD,
		Command: []string{"/bin/true"},
	})
	if err != nil {
		t.Fatalf("CreateContext() error = %v", err)
	}
	waitForCondition(t, func() bool {
		records, loadErr := store.Load()
		return loadErr == nil && len(records) == 1 &&
			records[0].DesiredState != process.ProcessStateRunning &&
			records[0].DesiredState != process.ProcessStatePaused
	})
	first.CleanupPreservingState()

	second := NewManager()
	second.SetStateStore(store)
	restored, err := second.RestoreContexts()
	if err != nil {
		t.Fatalf("RestoreContexts() error = %v", err)
	}
	if len(restored) != 0 {
		second.Cleanup()
		t.Fatalf("len(restored) = %d, want 0", len(restored))
	}
}

func TestManagerDeleteContextDoesNotRecreateStateFromAsyncExit(t *testing.T) {
	store, err := NewFileStateStore(filepath.Join(t.TempDir(), "contexts"))
	if err != nil {
		t.Fatalf("NewFileStateStore() error = %v", err)
	}
	manager := NewManager()
	manager.SetStateStore(store)
	ctx, err := manager.CreateContext(process.ProcessConfig{
		Type:    process.ProcessTypeCMD,
		Command: []string{"/bin/sh", "-c", "while :; do sleep 1; done"},
	})
	if err != nil {
		t.Fatalf("CreateContext() error = %v", err)
	}
	if err := manager.DeleteContext(ctx.ID); err != nil {
		t.Fatalf("DeleteContext() error = %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	records, err := store.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("persisted records = %#v, want none", records)
	}
}

func waitForCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}
