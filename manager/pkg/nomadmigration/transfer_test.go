package nomadmigration

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type schedulingStore struct {
	Store
	ids    []string
	reads  []string
	fail   string
	cancel context.CancelFunc
}

func (s *schedulingStore) ListNomadMigrationTransfers(_ context.Context, after string, limit int) ([]string, error) {
	var ids []string
	for _, id := range s.ids {
		if id > after && len(ids) < limit {
			ids = append(ids, id)
		}
	}
	return ids, nil
}
func (s *schedulingStore) GetNomadMigrationTransfer(_ context.Context, id string) (*Transfer, error) {
	s.reads = append(s.reads, id)
	if s.cancel != nil {
		cancel := s.cancel
		s.cancel = nil
		cancel()
	}
	if id == s.fail {
		return nil, errors.New("injected unavailable operation")
	}
	return nil, nil // Another replica already completed it.
}

type unexpectedNode struct{ Node }

func TestTransferScanTraversesFailedFullBatchAndWraps(t *testing.T) {
	s := &schedulingStore{fail: "00"}
	for i := range 11 {
		s.ids = append(s.ids, fmt.Sprintf("%02d", i))
	}
	c, err := New(s, unexpectedNode{})
	require.NoError(t, err)
	r, err := c.RunOnce(t.Context())
	require.Error(t, err)
	require.Equal(t, 8, r.Candidates)
	require.Equal(t, 1, r.Failed)
	require.Equal(t, 7, r.Skipped)
	r, err = c.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 3, r.Skipped)
	require.Equal(t, s.ids, s.reads)
	_, err = c.RunOnce(t.Context())
	require.Error(t, err)
	require.Equal(t, s.ids[:8], s.reads[11:])
}

func TestTransferCancellationPreservesUnvisitedSuffix(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	s := &schedulingStore{ids: []string{"a", "b", "c"}, cancel: cancel}
	c, err := New(s, unexpectedNode{})
	require.NoError(t, err)
	_, err = c.RunOnce(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []string{"a"}, s.reads)
	_, err = c.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, s.ids, s.reads)
}

func TestTransferWorkerStopsAndReportsOnePass(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	c, err := New(&schedulingStore{ids: []string{"a"}}, unexpectedNode{})
	require.NoError(t, err)
	calls := 0
	err = c.Run(ctx, func(r Report) { calls++; require.NoError(t, r.Error); require.Equal(t, 1, r.Result.Skipped); cancel() })
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls)
}
