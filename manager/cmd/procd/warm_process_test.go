package main

import (
	"os"
	"sync/atomic"
	"testing"
	"time"

	ctxpkg "github.com/sandbox0-ai/sandbox0/manager/procd/pkg/context"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestMain(m *testing.M) {
	warmProcessExit.Store(func(int) {})
	os.Exit(m.Run())
}

func TestParseWarmProcesses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     string
		wantErr string
	}{
		{
			name: "empty",
			raw:  "",
		},
		{
			name: "cmd",
			raw:  `[{"type":"cmd","command":["/bin/sh","-lc","sleep 3600"],"cwd":"/workspace","envVars":{"MODE":"warm"}}]`,
		},
		{
			name: "repl",
			raw:  `[{"type":"repl","alias":"bash"}]`,
		},
		{
			name:    "cmd missing command",
			raw:     `[{"type":"cmd"}]`,
			wantErr: "warmProcesses[0].command[0] is required",
		},
		{
			name:    "repl rejects command",
			raw:     `[{"type":"repl","command":["/bin/bash"]}]`,
			wantErr: "warmProcesses[0].command is only valid",
		},
		{
			name:    "invalid type",
			raw:     `[{"type":"daemon"}]`,
			wantErr: "warmProcesses[0].type must be one of",
		},
		{
			name:    "invalid probe",
			raw:     `[{"type":"repl","probes":{"readiness":{}}}]`,
			wantErr: "warmProcesses[0].probes.readiness must configure one of",
		},
		{
			name:    "invalid probe port",
			raw:     `[{"type":"repl","probes":{"liveness":{"tcpSocket":{"port":"http"}}}}]`,
			wantErr: "probe named ports are not supported",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseWarmProcesses(tt.raw)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			if tt.raw == "" {
				require.Empty(t, got)
				return
			}
			require.Len(t, got, 1)
		})
	}
}

func TestStartWarmProcessesKeepsContextsReady(t *testing.T) {
	t.Setenv(warmProcessesEnvVar, `[{"type":"cmd","command":["/bin/sh","-lc","sleep 5"],"envVars":{"MODE":"warm"}}]`)

	manager := ctxpkg.NewManager()
	manager.SetDefaultCleanupPolicy(ctxpkg.CleanupPolicy{IdleTimeout: time.Nanosecond})
	defer manager.Cleanup()

	processes, err := startWarmProcesses(manager, zap.NewNop())
	require.NoError(t, err)
	require.Len(t, processes, 1)

	ctx, err := manager.GetContext(processes[0].ContextID)
	require.NoError(t, err)
	require.True(t, ctx.IsRunning())
	require.Equal(t, ctxpkg.CleanupPolicy{}, ctx.CleanupPolicy)

	prober := &warmProcessProber{manager: manager, processes: processes}
	require.Equal(t, sandboxprobe.StatusPassed, prober.Probe(sandboxprobe.KindLiveness).Status)
	require.Equal(t, sandboxprobe.StatusPassed, prober.Probe(sandboxprobe.KindReadiness).Status)

	require.NoError(t, manager.DeleteContext(processes[0].ContextID))
	result := prober.Probe(sandboxprobe.KindReadiness)
	require.Equal(t, sandboxprobe.StatusFailed, result.Status)
	require.Equal(t, "WarmProcessMissing", result.Reason)
}

func TestWarmProcessHealthAllowsPausedContext(t *testing.T) {
	t.Setenv(warmProcessesEnvVar, `[{"type":"cmd","command":["/bin/sh","-lc","sleep 5"]}]`)

	manager := ctxpkg.NewManager()
	defer manager.Cleanup()

	processes, err := startWarmProcesses(manager, zap.NewNop())
	require.NoError(t, err)
	require.Len(t, processes, 1)

	ctx, err := manager.GetContext(processes[0].ContextID)
	require.NoError(t, err)
	require.NoError(t, ctx.Pause())
	defer func() { _ = ctx.Resume() }()

	prober := &warmProcessProber{manager: manager, processes: processes}
	liveness := prober.Probe(sandboxprobe.KindLiveness)
	require.Equal(t, sandboxprobe.StatusSuspended, liveness.Status)
	require.Equal(t, "WarmProcessPaused", liveness.Reason)

	readiness := prober.Probe(sandboxprobe.KindReadiness)
	require.Equal(t, sandboxprobe.StatusFailed, readiness.Status)
	require.Equal(t, "WarmProcessNotRunning", readiness.Reason)
}

func TestWarmProcessLivenessFailureExitsProcd(t *testing.T) {
	previousExit, _ := warmProcessExit.Load().(func(int))
	var exitCode atomic.Int32
	warmProcessExit.Store(func(code int) { exitCode.Store(int32(code)) })
	t.Cleanup(func() { warmProcessExit.Store(previousExit) })

	prober := &warmProcessProber{
		exitOnFailedLiveness: true,
		processes: []warmProcessRuntime{{
			Spec: warmProcessSpec{
				Name:   "hung-helper",
				Probes: &warmProbeSet{Liveness: &warmProbeSpec{Exec: &execProbeSpec{Command: []string{"/bin/sh", "-lc", "exit 7"}}}},
			},
			ContextID: "ctx-1",
			StartedAt: time.Now(),
		}},
	}

	result := prober.Probe(sandboxprobe.KindLiveness)
	require.Equal(t, sandboxprobe.StatusFailed, result.Status)
	require.Equal(t, "ExecProbeFailed", result.Reason)
	require.Eventually(t, func() bool { return exitCode.Load() == 1 }, time.Second, 10*time.Millisecond)
}
