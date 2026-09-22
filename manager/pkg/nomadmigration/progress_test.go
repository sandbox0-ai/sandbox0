package nomadmigration

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCommittedProgressWakesEveryDependentLane(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var progress Progress
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		ready := false
		reads := [2]int{}
		for i := range reads {
			lane := &Coordinator{progress: &progress, list: func(context.Context, string, int) ([]string, error) {
				reads[i]++
				if ready {
					return []string{"operation"}, nil
				}
				return nil, nil
			}, step: func(context.Context, string) (bool, error) { return false, nil }}
			go func() { _ = lane.Run(ctx, nil) }()
		}
		synctest.Wait()
		require.Equal(t, [2]int{1, 1}, reads)
		started := time.Now()
		ready = true
		progress.notify()
		synctest.Wait()
		require.Equal(t, [2]int{2, 2}, reads)
		require.Zero(t, time.Since(started), "progress must not wait for the reconciliation tick")
		cancel()
		synctest.Wait()
	})
}

func TestWorkerImmediatelyAdvancesCommittedStages(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		steps := 0
		lane := &Coordinator{list: func(context.Context, string, int) ([]string, error) { return []string{"operation"}, nil },
			step: func(context.Context, string) (bool, error) { steps++; return true, nil }}
		started := time.Now()
		err := lane.Run(ctx, func(Report) {
			if steps == 3 {
				cancel()
			}
		})
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 3, steps)
		require.Zero(t, time.Since(started))
	})
}

func TestFailedLaneRetainsBackoffDespiteOtherProgress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var progress Progress
		var steps atomic.Int32
		lane := &Coordinator{progress: &progress, list: func(context.Context, string, int) ([]string, error) { return []string{"operation"}, nil },
			step: func(context.Context, string) (bool, error) {
				steps.Add(1)
				return false, errors.New("node unavailable")
			}}
		go func() { _ = lane.Run(ctx, nil) }()
		synctest.Wait()
		for range 20 {
			progress.notify()
			synctest.Wait()
		}
		require.Equal(t, int32(1), steps.Load())
		time.Sleep(time.Second)
		synctest.Wait()
		require.Equal(t, int32(2), steps.Load())
		cancel()
		synctest.Wait()
	})
}

func TestProgressDuringScanIsNotLost(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var progress Progress
		passes := 0
		lane := &Coordinator{progress: &progress, list: func(context.Context, string, int) ([]string, error) {
			passes++
			if passes == 1 {
				progress.notify()
			} else {
				cancel()
			}
			return nil, nil
		}}
		started := time.Now()
		require.ErrorIs(t, lane.Run(ctx, nil), context.Canceled)
		require.Equal(t, 2, passes)
		require.Zero(t, time.Since(started))
	})
}
