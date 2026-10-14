package runtimeslotnomad

import (
	"bytes"
	"context"
	"errors"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotreconciler"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestProviderAbsentServerRetirementNeverCallsDeadClient(t *testing.T) {
	target := runtimeslotreconciler.AllocationTarget{ClusterID: "cluster", AllocationID: "allocation", AllocationNamespace: "sandbox0", NodeID: "original-node"}
	api := &fakeAPI{allocation: &Allocation{ID: target.AllocationID, Namespace: target.AllocationNamespace, NodeID: target.NodeID, DesiredStatus: "run", ClientStatus: "running"}, clientErr: errors.New("dead node")}
	c, err := New(api)
	require.NoError(t, err)
	request := runtimeslotreconciler.AllocationPurgeRequest{Target: target, OperationID: "provider-retire"}
	provider := bytes.Repeat([]byte{3}, 32)
	_, err = c.RetireProviderAbsent(context.Background(), request, provider)
	require.ErrorIs(t, err, ErrProviderAllocationNotTerminal)
	require.Len(t, api.stopCalls, 1)
	require.Empty(t, api.evaluateCalls)
	require.Zero(t, api.clientCalls)
	require.Zero(t, api.gcCalls)
	api.allocation.ClientStatus = "lost"
	proof, err := c.RetireProviderAbsent(context.Background(), request, provider)
	require.NoError(t, err)
	require.Len(t, proof, 32)
	require.Len(t, api.evaluateCalls, 1)
	again, err := c.RetireProviderAbsent(context.Background(), request, provider)
	require.NoError(t, err)
	require.Equal(t, proof, again)
	changed, err := c.RetireProviderAbsent(context.Background(), request, bytes.Repeat([]byte{4}, 32))
	require.NoError(t, err)
	require.NotEqual(t, proof, changed)
	require.Zero(t, api.clientCalls)
	require.Zero(t, api.gcCalls)
	api.allocation.NodeID = "replacement-node"
	_, err = c.RetireProviderAbsent(context.Background(), request, provider)
	require.Error(t, err)
	api.allocation = nil
	api.serverErr = errors.New("server unavailable")
	_, err = c.RetireProviderAbsent(context.Background(), request, provider)
	require.Error(t, err)
}
