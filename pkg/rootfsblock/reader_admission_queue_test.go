package rootfsblock

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
)

func requireReadAdmissionUsage(t *testing.T, a *sourceReadAdmission, active, waiting, owners int) {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	count := 0
	for _, owner := range a.owners {
		count += owner.waiters.Len()
	}
	require.Equal(t, []int{active, waiting, owners}, []int{a.active, count, len(a.owners)})
	require.Equal(t, owners, a.order.Len())
}

func holdReadAdmission(t *testing.T, cache *ReadCache) []func() {
	t.Helper()
	reader := &Reader{cache: cache}
	held := make([]func(), maxConcurrentSourceReads)
	for index := range held {
		var err error
		held[index], err = reader.acquireSourceSlot()
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		for _, release := range held {
			release()
		}
	})
	return held
}

type readAdmissionGrant struct {
	owner   int
	release func()
	err     error
}

func TestReadAdmissionRotatesReaderOwners(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cache, err := NewReadCache(0)
		require.NoError(t, err)
		held := holdReadAdmission(t, cache)
		grants := make(chan readAdmissionGrant, 12)
		for owner := range 3 {
			reader := &Reader{cache: cache}
			for range 4 {
				go func() {
					release, err := reader.acquireSourceSlot()
					grants <- readAdmissionGrant{owner: owner, release: release, err: err}
				}()
			}
			synctest.Wait()
		}
		requireReadAdmissionUsage(t, &cache.sourceSlots, 8, 12, 3)
		held[0]()
		var order []int
		for range 12 {
			grant := <-grants
			require.NoError(t, grant.err)
			order = append(order, grant.owner)
			grant.release()
		}
		require.Equal(t, []int{0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2}, order)
		requireReadAdmissionUsage(t, &cache.sourceSlots, 7, 0, 0)
	})
}

// Opening a generation's actual mapping root must not inherit the entire
// queued backlog of an older Reader. This asserts order, not an SLO.
func TestReadAdmissionNewMappingBypassesOldReaderBacklog(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cache, err := NewReadCache(0)
		require.NoError(t, err)
		source, _, descriptor, _ := adaptiveFixture(t, bulkReadThreshold, 1024)
		held := holdReadAdmission(t, cache)
		old := &Reader{cache: cache}
		grants := make(chan readAdmissionGrant, 64)
		for range 64 {
			go func() {
				release, err := old.acquireSourceSlot()
				grants <- readAdmissionGrant{release: release, err: err}
			}()
		}
		synctest.Wait()
		opened := make(chan error, 1)
		go func() {
			_, err := NewReaderWithCache(source, descriptor, cache)
			opened <- err
		}()
		synctest.Wait()
		requireReadAdmissionUsage(t, &cache.sourceSlots, 8, 65, 2)
		require.Empty(t, opened)
		held[0]()
		first := <-grants
		require.NoError(t, first.err)
		first.release()
		synctest.Wait()
		require.Len(t, opened, 1, "the new mapping must load after at most one old Reader grant")
		require.NoError(t, <-opened)
		for range 63 {
			grant := <-grants
			require.NoError(t, grant.err)
			grant.release()
		}
		requireReadAdmissionUsage(t, &cache.sourceSlots, 7, 0, 0)
	})
}

func TestReadAdmissionCancellationReclaimsOwnersWithoutWaitingForIO(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cache, err := NewReadCache(0)
		require.NoError(t, err)
		held := holdReadAdmission(t, cache)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 512)
		for owner := range 64 {
			lifetime := t.Context()
			if owner%2 == 0 {
				lifetime = ctx
			}
			reader := &Reader{cache: cache, lifetime: lifetime}
			for range 8 {
				go func() {
					release, err := reader.acquireSourceSlot()
					if release != nil {
						release()
						release()
					}
					done <- err
				}()
			}
		}
		synctest.Wait()
		requireReadAdmissionUsage(t, &cache.sourceSlots, 8, 512, 64)
		cancel()
		synctest.Wait()
		require.Len(t, done, 256)
		requireReadAdmissionUsage(t, &cache.sourceSlots, 8, 256, 32)
		for _, release := range held {
			release()
			release()
		}
		canceled, success := 0, 0
		for range 512 {
			err := <-done
			if errors.Is(err, context.Canceled) {
				canceled++
			} else {
				require.NoError(t, err)
				success++
			}
		}
		require.Equal(t, 256, canceled)
		require.Equal(t, 256, success)
		requireReadAdmissionUsage(t, &cache.sourceSlots, 0, 0, 0)
	})
}

func TestReadAdmissionCancellationRacesGrant(t *testing.T) {
	for range 128 {
		synctest.Test(t, func(t *testing.T) {
			cache, err := NewReadCache(0)
			require.NoError(t, err)
			held := holdReadAdmission(t, cache)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			reader := &Reader{cache: cache, lifetime: ctx}
			done := make(chan readAdmissionGrant, 1)
			go func() {
				release, err := reader.acquireSourceSlot()
				done <- readAdmissionGrant{release: release, err: err}
			}()
			synctest.Wait()
			requireReadAdmissionUsage(t, &cache.sourceSlots, 8, 1, 1)
			var racers sync.WaitGroup
			racers.Add(2)
			go func() { defer racers.Done(); held[0]() }()
			go func() { defer racers.Done(); cancel() }()
			racers.Wait()
			grant := <-done
			if grant.err != nil {
				require.ErrorIs(t, grant.err, context.Canceled)
				require.Nil(t, grant.release)
			} else {
				grant.release()
				grant.release()
			}
			requireReadAdmissionUsage(t, &cache.sourceSlots, 7, 0, 0)
		})
	}
}

func TestReadAdmissionSeparateCachesHaveIndependentBudgets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for range 2 {
			cache, err := NewReadCache(0)
			require.NoError(t, err)
			holdReadAdmission(t, cache)
			requireReadAdmissionUsage(t, &cache.sourceSlots, 8, 0, 0)
		}
	})
}
