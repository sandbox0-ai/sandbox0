package main

import (
	"os"
	"path/filepath"
	"testing"

	ctxpkg "github.com/sandbox0-ai/sandbox0/manager/procd/pkg/context"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/process"
	"github.com/sandbox0-ai/sandbox0/pkg/procdstate"
	"go.uber.org/zap"
)

func TestInitializeContextRecoveryRequiresMarker(t *testing.T) {
	for _, tt := range []struct {
		name        string
		withMarker  bool
		wantContext int
	}{
		{name: "ordinary restart clears definitions"},
		{name: "ctld recovery replays definitions", withMarker: true, wantContext: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			store, err := ctxpkg.NewFileStateStore(filepath.Join(root, "contexts"))
			if err != nil {
				t.Fatalf("NewFileStateStore() error = %v", err)
			}
			first := ctxpkg.NewManager()
			first.SetStateStore(store)
			if _, err := first.CreateContext(process.ProcessConfig{
				Type:    process.ProcessTypeCMD,
				Command: []string{"/bin/sh", "-c", "while :; do sleep 1; done"},
			}); err != nil {
				t.Fatalf("CreateContext() error = %v", err)
			}
			first.CleanupPreservingState()

			if tt.withMarker {
				markerPath := filepath.Join(root, procdstate.RecoveryRequestFilename)
				if err := os.WriteFile(markerPath, []byte("{}"), 0o600); err != nil {
					t.Fatalf("write recovery marker: %v", err)
				}
			}
			second := ctxpkg.NewManager()
			second.SetStateStore(store)
			if err := initializeContextRecovery(second, store, zap.NewNop()); err != nil {
				t.Fatalf("initializeContextRecovery() error = %v", err)
			}
			t.Cleanup(second.Cleanup)
			if got := len(second.ListContexts()); got != tt.wantContext {
				t.Fatalf("context count = %d, want %d", got, tt.wantContext)
			}
		})
	}
}
