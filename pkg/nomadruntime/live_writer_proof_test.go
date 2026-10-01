package nomadruntime

import (
	"context"
	"fmt"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
	"github.com/stretchr/testify/require"
)

func TestLiveWriterProofUsesBoundedBatchesAndExactGrantMatching(t *testing.T) {
	requests := make([]rootfshandoff.StageRequest, 512)
	for i := range requests {
		requests[i] = batchTestStage(fmt.Sprint(i))
	}
	calls := 0
	authority := batchTestAuthority{batch: func(_ context.Context, stages []rootfshandoff.StageRequest) (protocol.BatchRenewResponse, error) {
		calls++
		require.LessOrEqual(t, len(stages), protocol.MaxBatchRenewItems)
		for _, stage := range stages {
			require.Empty(t, stage.Identity.WriterGrantToken)
		}
		return batchTestResponse(stages, batchTestObservation()), nil
	}}
	proofs, err := proveLiveWriters(t.Context(), authority, requests)
	require.NoError(t, err)
	require.Equal(t, 2, calls)
	require.Len(t, proofs, len(requests))
	for i, proof := range proofs {
		require.Equal(t, requests[i], proof.request)
	}
	authority.batch = func(_ context.Context, stages []rootfshandoff.StageRequest) (protocol.BatchRenewResponse, error) {
		response := batchTestResponse(stages, batchTestObservation())
		response.Results[0].GrantID = "foreign"
		return response, nil
	}
	_, err = proveLiveWriters(t.Context(), authority, requests)
	require.Error(t, err)
	authority.batch = func(_ context.Context, stages []rootfshandoff.StageRequest) (protocol.BatchRenewResponse, error) {
		observation := batchTestObservation()
		observation.LeaseExpiresAt = observation.RenewAfter
		return batchTestResponse(stages, observation), nil
	}
	_, err = proveLiveWriters(t.Context(), authority, requests)
	require.Error(t, err)
}
