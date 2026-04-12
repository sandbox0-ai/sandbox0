package main

import (
	"os"
	"testing"
	"time"

	ctxpkg "github.com/sandbox0-ai/sandbox0/manager/procd/pkg/context"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestMain(m *testing.M) {
	warmProcessExit = func(int) {}
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

	contextIDs, err := startWarmProcesses(manager, zap.NewNop())
	require.NoError(t, err)
	require.Len(t, contextIDs, 1)

	ctx, err := manager.GetContext(contextIDs[0])
	require.NoError(t, err)
	require.True(t, ctx.IsRunning())
	require.Equal(t, ctxpkg.CleanupPolicy{}, ctx.CleanupPolicy)

	ready := warmProcessChecker{manager: manager, contextIDs: contextIDs}
	require.NoError(t, ready.CheckHealth())
	require.NoError(t, ready.Check())

	require.NoError(t, manager.DeleteContext(contextIDs[0]))
	require.ErrorContains(t, ready.Check(), "warm process context")
}

func TestWarmProcessHealthAllowsPausedContext(t *testing.T) {
	t.Setenv(warmProcessesEnvVar, `[{"type":"cmd","command":["/bin/sh","-lc","sleep 5"]}]`)

	manager := ctxpkg.NewManager()
	defer manager.Cleanup()

	contextIDs, err := startWarmProcesses(manager, zap.NewNop())
	require.NoError(t, err)
	require.Len(t, contextIDs, 1)

	ctx, err := manager.GetContext(contextIDs[0])
	require.NoError(t, err)
	require.NoError(t, ctx.Pause())
	defer func() { _ = ctx.Resume() }()

	checker := warmProcessChecker{manager: manager, contextIDs: contextIDs}
	require.NoError(t, checker.CheckHealth())
	require.ErrorContains(t, checker.Check(), "is not running")
}
