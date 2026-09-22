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

type migrationDrainWatchFunc func(context.Context, func()) error

func (f migrationDrainWatchFunc) WatchNomadMigrationDrains(ctx context.Context, wake func()) error {
	return f(ctx, wake)
}

func TestDrainNotificationWakesDiscoveryBeforePeriodicScan(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		notifications := make(chan struct{})
		listenerStopped := false
		watcher := migrationDrainWatchFunc(func(ctx context.Context, wake func()) error {
			defer func() { listenerStopped = true }()
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-notifications:
					wake()
				}
			}
		})
		var reads atomic.Int32
		lane := &Coordinator{progress: &Progress{}, drainWatcher: watcher,
			list: func(context.Context, string, int) ([]string, error) { reads.Add(1); return nil, nil }}
		done := make(chan error, 1)
		go func() { done <- lane.Run(ctx, nil) }()
		synctest.Wait()
		require.Equal(t, int32(1), reads.Load())
		started := time.Now()
		notifications <- struct{}{}
		synctest.Wait()
		require.Equal(t, int32(2), reads.Load())
		require.Zero(t, time.Since(started))
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
		require.True(t, listenerStopped, "Run joins the listener before returning")
	})
}

func TestDrainListenerFailurePreservesPeriodicRecoveryAndBoundedRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var attempts, reads, reports atomic.Int32
		lane := &Coordinator{progress: &Progress{},
			drainWatcher: migrationDrainWatchFunc(func(context.Context, func()) error {
				attempts.Add(1)
				return errors.New("connection lost")
			}), list: func(context.Context, string, int) ([]string, error) { reads.Add(1); return nil, nil }}
		done := make(chan error, 1)
		go func() {
			done <- lane.Run(ctx, func(r Report) {
				if r.Error != nil {
					reports.Add(1)
				}
			})
		}()
		synctest.Wait()
		require.Equal(t, int32(1), attempts.Load())
		require.Equal(t, int32(1), reads.Load())
		for range 10 {
			lane.progress.notify()
			synctest.Wait()
		}
		require.Equal(t, int32(1), attempts.Load(), "other progress cannot spin the failed listener")
		time.Sleep(time.Second)
		synctest.Wait()
		require.Equal(t, int32(2), attempts.Load())
		require.Equal(t, int32(12), reads.Load(), "fallback scanning continues without notifications")
		require.Equal(t, int32(2), reports.Load())
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
	})
}
