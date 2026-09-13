package runtimeslotnomad

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotreconciler"
	"github.com/stretchr/testify/require"
)

func TestControllerPurgeRequiresEvaluationAfterObservedTerminalState(t *testing.T) {
	api := &fakeAPI{allocation: testAllocation(), client: true}
	controller, err := New(api)
	require.NoError(t, err)
	request := runtimeslotreconciler.AllocationPurgeRequest{OperationID: "purge-operation", Target: testTarget()}
	// Nomad Stop accepts a migration request. Its first system-job evaluation
	// may only stop the old allocation, leaving no replacement or queued work.
	require.ErrorIs(t, controller.Purge(t.Context(), request), runtimeslotreconciler.ErrAllocationStillPresent)
	require.Len(t, api.stopCalls, 1)
	require.Zero(t, api.gcCalls, "a stop acknowledgement does not prove the scheduler observed terminal state")

	api.allocation.DesiredStatus = "stop"
	require.NoError(t, controller.Purge(t.Context(), request))
	require.Equal(t, []string{request.OperationID, request.OperationID}, api.stopCalls,
		"enqueue another exact-allocation evaluation after terminal state is observed")
	require.Equal(t, 1, api.gcCalls)
}

func TestControllerPurgeStillSchedulesAfterClientArtifactsDisappear(t *testing.T) {
	for _, status := range []string{"complete", "failed", "lost"} {
		t.Run(status, func(t *testing.T) {
			api := &fakeAPI{allocation: testAllocation(), client: false}
			api.allocation.ClientStatus = status
			controller, err := New(api)
			require.NoError(t, err)
			request := runtimeslotreconciler.AllocationPurgeRequest{OperationID: "purge-operation", Target: testTarget()}
			require.NoError(t, controller.Purge(t.Context(), request))
			require.Len(t, api.stopCalls, 1)
			require.Zero(t, api.gcCalls)
		})
	}
}

func TestControllerPurgeDoesNotLosePostTerminalEvaluationOnResponseLoss(t *testing.T) {
	api := &fakeAPI{allocation: testAllocation(), client: false}
	api.allocation.DesiredStatus = "stop"
	api.stopErr = runtimeslotreconciler.ErrAllocationStillPresent
	controller, err := New(api)
	require.NoError(t, err)
	request := runtimeslotreconciler.AllocationPurgeRequest{OperationID: "purge-operation", Target: testTarget()}
	require.ErrorIs(t, controller.Purge(t.Context(), request), api.stopErr)
	require.Len(t, api.stopCalls, 1)
	api.stopErr = nil
	require.NoError(t, controller.Purge(t.Context(), request))
	require.Len(t, api.stopCalls, 2)
}
