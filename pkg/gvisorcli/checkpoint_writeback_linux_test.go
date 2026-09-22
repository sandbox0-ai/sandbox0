//go:build linux

package gvisorcli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newCheckpointWritebackTest(t *testing.T) (*checkpointWriteback, string) {
	t.Helper()
	directory := t.TempDir()
	root, err := os.OpenRoot(directory)
	require.NoError(t, err)
	t.Cleanup(func() { _ = root.Close() })
	w := &checkpointWriteback{root: root, files: make(map[string]*checkpointWritebackFile), flush: func(*os.File) error { return nil }, sync: func(*os.File) error { return nil }}
	t.Cleanup(func() {
		for _, f := range w.files {
			_ = f.file.Close()
		}
	})
	return w, directory
}

func TestCheckpointWritebackRetainsDescriptorAndRejectsReplacement(t *testing.T) {
	w, directory := newCheckpointWritebackTest(t)
	path := filepath.Join(directory, "pages.img")
	require.NoError(t, os.WriteFile(path, nil, 0600))
	require.NoError(t, os.Truncate(path, checkpointWritebackBatch+1))
	var held *os.File
	w.flush = func(file *os.File) error { held = file; return nil }
	require.NoError(t, w.scan(t.Context()))
	require.NotNil(t, held)
	info, err := held.Stat()
	require.NoError(t, err)
	require.EqualValues(t, checkpointWritebackBatch+1, info.Size())
	require.NoError(t, os.Rename(path, path+".old"))
	require.NoError(t, os.WriteFile(path, []byte("replacement"), 0600))
	require.ErrorContains(t, w.scan(t.Context()), "replaced")
}

func TestCheckpointWritebackFailureIsReturnedAfterJoin(t *testing.T) {
	w, directory := newCheckpointWritebackTest(t)
	path := filepath.Join(directory, "pages.img")
	require.NoError(t, os.WriteFile(path, nil, 0600))
	require.NoError(t, os.Truncate(path, checkpointWritebackBatch))
	entered, release := make(chan struct{}), make(chan struct{})
	injected := errors.New("asynchronous writeback failure")
	w.flush = func(*os.File) error { close(entered); <-release; return injected }
	syncFailure := errors.New("retained descriptor sync failure")
	w.sync = func(*os.File) error { return syncFailure }
	stop := w.start(t.Context())
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("writeback did not start")
	}
	result := make(chan error, 1)
	go func() { result <- stop() }()
	select {
	case <-result:
		t.Fatal("join returned before writeback exited")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-result:
		require.ErrorIs(t, err, injected)
		require.ErrorIs(t, err, syncFailure)
	case <-time.After(time.Second):
		t.Fatal("writeback did not join")
	}
	for _, file := range w.files {
		_, err := file.file.Stat()
		require.ErrorIs(t, err, os.ErrClosed)
	}
}

func TestCheckpointWritebackRejectsUnsafeInventoryAndBoundsDescriptors(t *testing.T) {
	t.Run("symlink", func(t *testing.T) {
		w, directory := newCheckpointWritebackTest(t)
		require.NoError(t, os.Symlink("/dev/null", filepath.Join(directory, "pages.img")))
		require.ErrorContains(t, w.scan(t.Context()), "regular")
		require.Empty(t, w.files)
	})
	t.Run("truncation", func(t *testing.T) {
		w, directory := newCheckpointWritebackTest(t)
		path := filepath.Join(directory, "pages.img")
		require.NoError(t, os.WriteFile(path, nil, 0600))
		require.NoError(t, os.Truncate(path, checkpointWritebackBatch))
		require.NoError(t, w.scan(t.Context()))
		require.NoError(t, os.Truncate(path, 0))
		require.ErrorContains(t, w.scan(t.Context()), "truncated")
	})
	t.Run("limit", func(t *testing.T) {
		w, directory := newCheckpointWritebackTest(t)
		for i := 0; i <= checkpointWritebackFileLimit; i++ {
			require.NoError(t, os.WriteFile(filepath.Join(directory, strconv.Itoa(i)), nil, 0600))
		}
		require.ErrorContains(t, w.scan(t.Context()), "limit")
		require.Empty(t, w.files)
	})
	t.Run("canceled", func(t *testing.T) {
		w, _ := newCheckpointWritebackTest(t)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		require.NoError(t, w.start(ctx)())
		require.Empty(t, w.files)
	})
}
