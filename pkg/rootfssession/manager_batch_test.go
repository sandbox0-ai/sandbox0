package session

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestManagerConcurrentIndependentEnsureBatchesDurableJournalCommits(t *testing.T) {
	const workers = 32
	base := t.TempDir()
	objects := newSessionObjectStore()
	runtime := newFakeHostRuntime(objects)
	runtime.devicePaths = make([]string, workers)
	requests := make([]rootfshandoff.StageRequest, workers)
	for index := range workers {
		runtime.devicePaths[index] = fmt.Sprintf("/dev/fake%d", index)
		requests[index] = testStageRequest(t, objects, fmt.Sprintf("independent-%d", index))
	}
	manager, err := New(Config{
		StatePath:  filepath.Join(base, "state", "sessions.db"),
		BranchRoot: filepath.Join(base, "branches"), MountRoot: filepath.Join(base, "mounts"),
		Source: objects, Publisher: objects, Runtime: runtime,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, manager.Close()) }()

	// A longer test-only window makes commit coalescing deterministic without
	// asserting wall-clock timings. Production uses the bounded 1 ms window.
	manager.db.MaxBatchDelay = 25 * time.Millisecond
	before := journalTransactionID(t, manager.db)
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wait sync.WaitGroup
	for index := range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, ensureErr := manager.Ensure(t.Context(), requests[index])
			errs <- ensureErr
		}()
	}
	close(start)
	wait.Wait()
	close(errs)
	for ensureErr := range errs {
		require.NoError(t, ensureErr)
	}
	after := journalTransactionID(t, manager.db)

	require.Less(t, after-before, workers,
		"five durable states per claim must be group-committed instead of forming a global fsync queue")
	require.Equal(t, workers, runtime.count("attach"))
	require.Equal(t, workers, runtime.count("mount-xfs"))
	require.Equal(t, workers, runtime.count("mount-overlay"))
}

func journalTransactionID(t *testing.T, db *bolt.DB) int {
	t.Helper()
	var id int
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		id = tx.ID()
		return nil
	}))
	return id
}
