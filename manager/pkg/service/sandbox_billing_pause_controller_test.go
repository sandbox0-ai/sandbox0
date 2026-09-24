package service

import (
	"context"
	"errors"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/stretchr/testify/require"
)

type billingPauseLister struct {
	entries []sandboxstore.BillingPauseCandidate
	after   []string
}

func (l *billingPauseLister) ListBillingPauseCandidates(_ context.Context, clusterID, after string, limit int) ([]sandboxstore.BillingPauseCandidate, error) {
	if clusterID != "cluster-a" {
		return nil, errors.New("unexpected cluster")
	}
	l.after = append(l.after, after)
	var result []sandboxstore.BillingPauseCandidate
	for _, entry := range l.entries {
		if entry.SandboxID > after {
			result = append(result, entry)
		}
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

type billingPauseRecorder struct {
	requests []sandboxstore.BillingPauseCandidate
	failID   string
}

func (r *billingPauseRecorder) PauseSandboxForBilling(_ context.Context, id string, version int64) error {
	r.requests = append(r.requests, sandboxstore.BillingPauseCandidate{SandboxID: id, AdmissionVersion: version})
	if id == r.failID {
		return errors.New("temporary pause failure")
	}
	return nil
}

func TestSandboxBillingPauseControllerCyclesPastFailures(t *testing.T) {
	lister := &billingPauseLister{entries: []sandboxstore.BillingPauseCandidate{
		{SandboxID: "a", AdmissionVersion: 4},
		{SandboxID: "b", AdmissionVersion: 5},
		{SandboxID: "c", AdmissionVersion: 6},
	}}
	pauser := &billingPauseRecorder{failID: "a"}
	controller, err := NewSandboxBillingPauseController(lister, pauser, "cluster-a", 0, nil)
	require.NoError(t, err)
	controller.batchSize = 2
	require.NoError(t, controller.runOnce(t.Context()))
	require.Equal(t, "b", controller.afterSandboxID)
	require.NoError(t, controller.runOnce(t.Context()))
	require.Empty(t, controller.afterSandboxID)
	require.Equal(t, []string{"", "b"}, lister.after)
	require.Equal(t, lister.entries, pauser.requests)
	require.NoError(t, controller.runOnce(t.Context()))
	require.Equal(t, 5, len(pauser.requests), "a failing candidate is retried after a complete scan")
}
