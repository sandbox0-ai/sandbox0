package runtimeslotterminal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotreconciler"
	"github.com/stretchr/testify/require"
)

type terminalFunc func(context.Context) (runtimeslotreconciler.Result, error)

func (f terminalFunc) RunOnce(ctx context.Context) (runtimeslotreconciler.Result, error) {
	return f(ctx)
}

type refillFunc func(context.Context, string, string) (int, string, error)

func (f refillFunc) RefillFailedCarriers(ctx context.Context, cluster, cursor string) (int, string, error) {
	return f(ctx, cluster, cursor)
}

func TestRefillIsPacedRotatesClustersAndCannotReplaceTerminalWork(t *testing.T) {
	now := time.Now()
	terminalCalls := 0
	var clusters, cursors []string
	terminalErr := errors.New("terminal failure")
	runner := &terminalAndRefill{
		terminal: terminalFunc(func(context.Context) (runtimeslotreconciler.Result, error) {
			terminalCalls++
			return runtimeslotreconciler.Result{Completed: 3}, terminalErr
		}),
		refiller: refillFunc(func(ctx context.Context, cluster, cursor string) (int, string, error) {
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.LessOrEqual(t, time.Until(deadline), 5*time.Second)
			clusters = append(clusters, cluster)
			cursors = append(cursors, cursor)
			return 1, "next-" + cluster, nil
		}),
		clusters: []string{"a", "b"}, cursors: make(map[string]string), now: func() time.Time { return now },
	}
	for i := range 4 {
		result, err := runner.RunOnce(t.Context())
		require.ErrorIs(t, err, terminalErr)
		require.Equal(t, 3, result.Completed)
		if i == 1 {
			require.Zero(t, result.RefillRequested)
		} else {
			require.Equal(t, 1, result.RefillRequested)
		}
		if i > 0 {
			now = now.Add(30 * time.Second)
		}
	}
	require.Equal(t, 4, terminalCalls)
	require.Equal(t, []string{"a", "b", "a"}, clusters)
	require.Equal(t, []string{"", "", "next-a"}, cursors)
}

func TestRefillRetainsFailedCursorAndSkipsCanceledPass(t *testing.T) {
	now := time.Now()
	refillCalls := 0
	runner := &terminalAndRefill{
		terminal: terminalFunc(func(context.Context) (runtimeslotreconciler.Result, error) {
			return runtimeslotreconciler.Result{Completed: 1}, nil
		}),
		refiller: refillFunc(func(context.Context, string, string) (int, string, error) {
			refillCalls++
			return 1, "uncommitted", errors.New("uncertain stop")
		}),
		clusters: []string{"a"}, cursors: map[string]string{"a": "previous"}, now: func() time.Time { return now },
	}
	result, err := runner.RunOnce(t.Context())
	require.Error(t, err)
	require.Equal(t, 1, result.Completed)
	require.Equal(t, 1, result.RefillRequested)
	require.Equal(t, "previous", runner.cursors["a"])
	now = now.Add(time.Minute)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _ = runner.RunOnce(ctx)
	require.Equal(t, 1, refillCalls)
}
