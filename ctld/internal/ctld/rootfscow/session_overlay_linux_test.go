package rootfscow

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfsstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestSessionCapturesOverlayMergedCopyUpWrite(t *testing.T) {
	lower := t.TempDir()
	upper := t.TempDir()
	work := t.TempDir()
	merged := t.TempDir()
	path := ".bashrc"
	require.NoError(t, os.WriteFile(filepath.Join(lower, path), []byte("base\n"), 0o644))
	options := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", lower, upper, work)
	if err := unix.Mount("overlay", merged, "overlay", 0, options); err != nil {
		if errors.Is(err, unix.EPERM) || errors.Is(err, unix.EACCES) || errors.Is(err, unix.ENODEV) || errors.Is(err, unix.EINVAL) {
			t.Skipf("overlay mount is unavailable: %v", err)
		}
		require.NoError(t, err)
	}
	t.Cleanup(func() { require.NoError(t, unix.Unmount(merged, 0)) })

	session, store, prefix := newTestSessionWithEventRoot(t, upper, merged)
	require.NoError(t, session.WaitInitial(testContext(t)))
	content := []byte("export ROOTFS_COW=durable\n")
	file, err := os.OpenFile(filepath.Join(merged, path), os.O_WRONLY|os.O_TRUNC, 0)
	require.NoError(t, err)
	_, err = file.Write(content)
	require.NoError(t, err)
	require.NoError(t, file.Sync())
	require.NoError(t, file.Close())
	require.Eventually(t, func() bool {
		session.mu.Lock()
		defer session.mu.Unlock()
		version, ok := session.known[path]
		return ok && version.Size == int64(len(content))
	}, 5*time.Second, 10*time.Millisecond)

	result, err := session.Seal(testContext(t), SealRequest{HeadID: "head-overlay-event", Base: testBaseIdentity()})
	require.NoError(t, err)
	head, err := rootfsstore.LoadHead(context.Background(), store, result.Reference)
	require.NoError(t, err)
	entry := mustFindEntry(t, store, prefix, head.Root, path)
	assert.Equal(t, content, readFileEntry(t, store, prefix, entry))
}
