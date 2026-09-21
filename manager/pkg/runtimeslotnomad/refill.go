package runtimeslotnomad

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotreconciler"
	"github.com/sandbox0-ai/sandbox0/pkg/nomadinventory"
)

// RefillFailedCarriers requests fresh one-shot allocations after startup fails
// before regional registration. Nomad system jobs do not reschedule themselves.
// This only evaluates jobs with already-failed carriers. It never invokes client
// GC, releases a resource/writer lease, or treats the catalog as physical proof.
func (a *HTTPAPI) RefillFailedCarriers(ctx context.Context, clusterID, after string) (int, string, error) {
	if err := validateID("cluster_id", clusterID); err != nil {
		return 0, after, err
	}
	endpoint, err := a.resolver.ServerEndpoint(ctx, clusterID)
	if err != nil {
		return 0, after, err
	}
	if endpoint.ClusterID != clusterID || endpoint.NodeID != "" {
		return 0, after, fmt.Errorf("carrier refill server identity mismatch")
	}
	client, baseURL, err := newNomadHTTPClient(endpoint)
	if err != nil {
		return 0, after, err
	}
	token, err := readNomadToken(endpoint.TokenFile)
	if err != nil {
		return 0, after, err
	}
	headers := http.Header{"X-Nomad-Token": {token}}
	rows, next, err := nomadinventory.FailedWarmPage(ctx, client, baseURL, nomadinventory.DefaultWarmJobID, after, headers)
	if err != nil {
		return 0, after, err
	}
	requested := 0
	for _, candidate := range rows {
		// Re-read exact identity and terminal state immediately before evaluation.
		// Another replica or terminal reconciliation may already have acted.
		current, err := nomadinventory.Get(ctx, client, baseURL, candidate.ID, candidate.NodeID, candidate.Namespace, headers)
		if err != nil {
			return requested, after, err
		}
		if current == nil || current.ClientStatus != "failed" || current.DesiredStatus != "run" || current.NextAllocation != "" {
			continue
		}
		if current.JobID != candidate.JobID {
			return requested, after, fmt.Errorf("carrier refill job identity changed")
		}
		target := runtimeslotreconciler.AllocationTarget{
			ClusterID: clusterID, AllocationID: current.ID,
			NodeID: current.NodeID, AllocationNamespace: current.Namespace,
		}
		// Retries reconcile the same desired carrier counts. Evaluation does
		// not advance the finished allocation version or force live restarts.
		digest := sha256.Sum256([]byte(clusterID + "\x00" + current.NodeID + "\x00" + current.Namespace + "\x00" + current.ID))
		if err := a.EvaluateTerminalAllocation(ctx, target, fmt.Sprintf("carrier-refill-%x", digest)); err != nil {
			return requested, after, err
		}
		requested++
	}
	return requested, next, nil
}
