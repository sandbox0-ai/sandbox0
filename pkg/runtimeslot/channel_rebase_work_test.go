package runtimeslot

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsrebase"
	"github.com/stretchr/testify/require"
)

type retainedRebaseTestExecutor struct {
	*testNodeChannelExecutor
	started chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (e *retainedRebaseTestExecutor) PausedRebase(ctx context.Context, _ NodeChannelTarget, _ NodePausedRebaseControlRequest) (rootfsrebase.WorkerResult, error) {
	if e.calls.Add(1) == 1 {
		close(e.started)
	}
	select {
	case <-e.release:
		return e.pausedRebase, nil
	case <-ctx.Done():
		return rootfsrebase.WorkerResult{}, ctx.Err()
	}
}

func TestRetainedRebaseJoinsExactWorkAfterTransportCancellation(t *testing.T) {
	request, result := testNodeChannelPausedRebase(t)
	command, err := NewNodeChannelPausedRebaseCommand(testNodeChannelRebaseTarget(), request)
	require.NoError(t, err)
	executor := &retainedRebaseTestExecutor{testNodeChannelExecutor: &testNodeChannelExecutor{pausedRebase: result}, started: make(chan struct{}), release: make(chan struct{})}
	agent := &NodeChannelAgent{config: NodeChannelAgentConfig{Executor: executor, PausedRebaseExecutor: executor, PausedRebaseTimeout: time.Second, MaxConcurrent: 1}}
	owner, cancel := context.WithCancel(t.Context())
	defer func() { cancel(); agent.rebaseWG.Wait() }()
	work, err := agent.retainedRebase(owner, command)
	require.NoError(t, err)
	<-executor.started
	// A rotated transport stops waiting; the same exact request on its new
	// stream joins the authorized job instead of canceling/re-executing it.
	transport, stopTransport := context.WithCancel(owner)
	stopTransport()
	require.Error(t, transport.Err())
	again, err := agent.retainedRebase(owner, command)
	require.NoError(t, err)
	require.Same(t, work, again)
	require.EqualValues(t, 1, executor.calls.Load())
	close(executor.release)
	select {
	case <-again.done:
	case <-time.After(time.Second):
		t.Fatal("retained rebase did not complete")
	}
	require.NoError(t, again.result.ValidateFor(command))
	require.Empty(t, again.result.Error)
	retry, err := agent.retainedRebase(owner, command)
	require.NoError(t, err)
	require.Same(t, work, retry)
	require.EqualValues(t, 1, executor.calls.Load())
	changed := command
	changed.Target.NodeBootID = "other-boot"
	_, err = agent.retainedRebase(owner, changed)
	require.Error(t, err, "a changed target cannot reuse the request identity")
}

func TestRetainedRebaseOwnerCancellationAndGlobalConcurrency(t *testing.T) {
	request, result := testNodeChannelPausedRebase(t)
	command, err := NewNodeChannelPausedRebaseCommand(testNodeChannelRebaseTarget(), request)
	require.NoError(t, err)
	executor := &retainedRebaseTestExecutor{testNodeChannelExecutor: &testNodeChannelExecutor{pausedRebase: result}, started: make(chan struct{}), release: make(chan struct{})}
	agent := &NodeChannelAgent{config: NodeChannelAgentConfig{Executor: executor, PausedRebaseExecutor: executor, PausedRebaseTimeout: time.Second, MaxConcurrent: 1}}
	owner, cancel := context.WithCancel(t.Context())
	defer func() { cancel(); agent.rebaseWG.Wait() }()
	work, err := agent.retainedRebase(owner, command)
	require.NoError(t, err)
	<-executor.started
	require.Equal(t, 1, len(agent.nodeWorkPermit()))
	other := request
	other.Reject = true
	otherCommand, err := NewNodeChannelPausedRebaseCommand(testNodeChannelRebaseTarget(), other)
	require.NoError(t, err)
	_, err = agent.retainedRebase(owner, otherCommand)
	require.Error(t, err, "reconnecting cannot reset the active-job bound")
	cancel()
	select {
	case <-work.done:
	case <-time.After(time.Second):
		t.Fatal("owner cancellation did not join retained work")
	}
	require.Equal(t, NodeChannelErrorUnavailable, work.result.ErrorClass)
	require.Zero(t, len(agent.nodeWorkPermit()))
}
