package nomadruntime

import (
	"context"
	"fmt"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
)

type liveWriterProof struct {
	request     rootfshandoff.StageRequest
	observation protocol.LeaseObservation
	started     time.Time
}

// Use the existing bounded regional batch protocol, so an occupied node does
// not perform hundreds of sequential round trips while its devices are paused.
func proveLiveWriters(ctx context.Context, authority rootFSWriterAuthority, requests []rootfshandoff.StageRequest) ([]liveWriterProof, error) {
	proofs := make([]liveWriterProof, 0, len(requests))
	for offset := 0; offset < len(requests); {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		started := time.Now()
		if batch, ok := authority.(writerBatchAuthority); ok {
			end := min(offset+protocol.MaxBatchRenewItems, len(requests))
			stages := make([]rootfshandoff.StageRequest, end-offset)
			for i, request := range requests[offset:end] {
				stages[i] = request.WithoutWriterGrantToken()
			}
			response, err := batch.RenewWriterGrants(ctx, stages)
			if err != nil {
				return nil, err
			}
			if err := response.Validate(len(stages)); err != nil {
				return nil, err
			}
			results := make(map[string]protocol.BatchRenewResult, len(response.Results))
			for _, result := range response.Results {
				results[result.GrantID] = result
			}
			for _, request := range requests[offset:end] {
				result, exists := results[request.Identity.WriterGrantID]
				if !exists {
					return nil, fmt.Errorf("live writer proof returned another grant")
				}
				if result.ErrorCode != "" {
					return nil, writerBatchResultError(result.ErrorCode)
				}
				proofs = append(proofs, liveWriterProof{request, *result.Observation, started})
				delete(results, request.Identity.WriterGrantID)
			}
			if len(results) != 0 {
				return nil, fmt.Errorf("live writer proof contains unrequested grants")
			}
			offset = end
		} else {
			observation, err := authority.RenewWriterGrant(ctx, requests[offset])
			if err != nil {
				return nil, err
			}
			proofs = append(proofs, liveWriterProof{requests[offset], observation, started})
			offset++
		}
	}
	for _, proof := range proofs {
		_, expires, err := localWriterLeaseSchedule(proof.observation, proof.started)
		if err != nil || time.Until(expires) < 20*time.Second {
			return nil, fmt.Errorf("live writer has insufficient proven lease time: %w", err)
		}
	}
	return proofs, nil
}
