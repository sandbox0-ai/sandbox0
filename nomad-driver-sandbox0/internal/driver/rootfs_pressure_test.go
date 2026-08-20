package driver

import (
	"context"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	"github.com/stretchr/testify/require"
)

func TestSessionDaemonPlansPressuredWriterBeforeTerminalTrigger(t *testing.T) {
	pressure := rootfssession.DirtyTailPressureSession{
		Stage: rootfshandoff.StageRequest{
			Parent: "parent-1",
			Identity: rootfshandoff.Identity{
				WriterGrantID: "grant-1", WriterEpoch: 7,
			},
		},
		Pressure: rootfsblock.DirtyTailPressure{
			Scope: "node", UsedBytes: 4096, RequestedBytes: 4096, LimitBytes: 4096,
		},
	}
	runtime := &fakeRootFSRuntime{
		pressureSignal: make(chan struct{}, 1),
		pressures:      []rootfssession.DirtyTailPressureSession{pressure},
	}
	daemon := &rootFSSessionDaemon{
		runtime: runtime, logger: hclog.NewNullLogger(),
		inflight: make(map[string]bool), trigger: make(chan string, 1),
	}
	daemon.scanDirtyTailPressures(context.Background())
	require.Eventually(t, func() bool {
		runtime.mu.Lock()
		defer runtime.mu.Unlock()
		return len(runtime.pressurePlans) == 1
	}, time.Second, 10*time.Millisecond)
	select {
	case parent := <-daemon.trigger:
		require.Equal(t, pressure.Stage.Parent, parent)
	case <-time.After(time.Second):
		t.Fatal("planned pressure retirement did not trigger terminal reconciliation")
	}
	daemon.wg.Wait()
}

func TestSessionDaemonNeverCrashAbandonsDurablePressurePendingWriter(t *testing.T) {
	stage := rootfshandoff.StageRequest{
		Parent:   "parent-1",
		Identity: rootfshandoff.Identity{WriterGrantID: "grant-1", WriterEpoch: 7},
	}
	runtime := &fakeRootFSRuntime{recoverySessions: []rootfssession.RecoverySession{{
		Stage: stage, Kind: rootfssession.RecoveryCrashAbandon,
		PressureOperationID: rootfshandoff.PlannedRetireOperationID(
			stage.Parent, stage.Identity.WriterGrantID, stage.Identity.WriterEpoch,
		),
	}}}
	daemon := &rootFSSessionDaemon{
		runtime: runtime, logger: hclog.NewNullLogger(),
		inflight: make(map[string]bool), trigger: make(chan string, 1),
	}
	daemon.scan(context.Background(), stage.Parent)
	daemon.wg.Wait()
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	require.Zero(t, runtime.crashCalls)
	require.Zero(t, runtime.retireCalls)
}
