package runtimecontroller

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/session"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
	"go.uber.org/zap"
)

func TestControllerClearsCopiedUnownedSessionsBeforeReadiness(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		name := "valid"
		if corrupt {
			name = "corrupt"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			store, err := session.NewFileStore(root)
			if err != nil {
				t.Fatal(err)
			}
			const sessionID = "ses-copied"
			if err := store.Save(session.Session{
				ID: sessionID, Phase: session.PhaseStopped, SpecVersion: 1,
				Spec: session.SessionSpec{Command: []string{"/bin/true"},
					Lifecycle: session.LifecycleSpec{DesiredState: session.DesiredStateStopped}},
			}); err != nil {
				t.Fatal(err)
			}
			if corrupt {
				if err := os.WriteFile(filepath.Join(root, sessionID, "state.json"), []byte("bad copied state"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			supervisor, err := session.NewSupervisor(store, zap.NewNop())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = supervisor.Close() })
			controller := New(nil, supervisor, nil, nil, 49983, zap.NewNop())
			assignment := runtimecontrol.Assignment{
				SandboxID: "target-sandbox", RuntimeGeneration: 1, SecurityClass: "standard",
				ResetCopiedSessionState: true,
			}
			if err := controller.Activate(context.Background(), assignment); err != nil {
				t.Fatal(err)
			}
			if ready, reason := controller.CanServe(); !ready {
				t.Fatalf("cleared runtime is not ready: %s", reason)
			}
			if result := controller.Probe(sandboxprobe.KindReadiness); result.Status != sandboxprobe.StatusPassed {
				t.Fatalf("readiness = %#v", result)
			}
			if len(supervisor.List()) != 0 {
				t.Fatal("runtime published readiness with copied sessions")
			}
			if _, err := os.Stat(filepath.Join(root, sessionID)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("runtime published readiness before copied state removal: %v", err)
			}
		})
	}
}
