//go:build linux

package rootfsartifact

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

// This is an opt-in isolated-host filesystem test, not a claim benchmark. Its
// publisher is an in-memory transport around the production block builder.
func TestPrivilegedXFSDataRangesFullImageRoundTrip(t *testing.T) {
	if os.Getenv("SANDBOX0_PRIVILEGED_XFS_BUILDER") != "1" {
		t.Skip("set SANDBOX0_PRIVILEGED_XFS_BUILDER=1 on an isolated Linux host")
	}
	require.Equal(t, 0, os.Geteuid())
	for _, command := range []string{"mkfs.xfs", "mount", "cp", "umount", "xfs_repair", "findmnt"} {
		_, err := exec.LookPath(command)
		require.NoError(t, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	work, err := os.MkdirTemp("", "s0-xfs-data-ranges-")
	require.NoError(t, err)
	t.Cleanup(func() {
		// Never recursively remove a directory containing an undetached mount,
		// including an image retained by ErrXFSImageStillMounted.
		output, err := exec.Command("findmnt", "-rn", "-o", "TARGET").Output()
		if err != nil {
			t.Errorf("retain %s: cannot prove mount absence: %v", work, err)
			return
		}
		for _, path := range strings.Fields(string(output)) {
			if path == work || strings.HasPrefix(path, work+"/") {
				t.Errorf("retain mounted test root %s", work)
				return
			}
		}
		require.NoError(t, os.RemoveAll(work))
	})
	source := filepath.Join(work, "source")
	require.NoError(t, os.Mkdir(source, 0o700))
	payload := make([]byte, 256<<10)
	_, err = rand.New(rand.NewSource(3127)).Read(payload)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(source, "dense"), payload, 0o600))
	require.NoError(t, os.Link(filepath.Join(source, "dense"), filepath.Join(source, "hardlink")))
	require.NoError(t, os.WriteFile(filepath.Join(source, "shared"), payload, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(source, "small"), []byte("preserve me"), 0o600))
	require.NoError(t, os.Symlink("/outside-the-import-root", filepath.Join(source, "external")))
	sparse, err := os.OpenFile(filepath.Join(source, "sparse"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	require.NoError(t, err)
	require.NoError(t, sparse.Truncate(2<<20))
	_, err = sparse.WriteAt(payload[:128<<10], 4096)
	require.NoError(t, err)
	_, err = sparse.WriteAt(payload[:128<<10], (512<<10)+4096)
	require.NoError(t, err)
	require.NoError(t, sparse.Close())
	frozen := false
	runner := xfsInspectRunner(func(ctx context.Context, name string, args ...string) error {
		if err := (execRunner{}).Run(ctx, name, args...); err != nil {
			return err
		}
		if name == "cp" {
			lower := args[len(args)-1]
			original, err := os.Open(filepath.Join(lower, "shared"))
			if err != nil {
				return err
			}
			defer original.Close()
			clone, err := os.OpenFile(filepath.Join(lower, "clone"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
			if err != nil {
				return err
			}
			defer clone.Close()
			if err := unix.IoctlFileClone(int(clone.Fd()), int(original.Fd())); err != nil {
				return err
			}
			unwritten, err := os.OpenFile(filepath.Join(lower, "unwritten"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
			if err != nil {
				return err
			}
			defer unwritten.Close()
			if err := unix.Fallocate(int(unwritten.Fd()), 0, 0, 256<<10); err != nil {
				return err
			}
			_, err = CollectReadOnlyXFSDataRanges(ctx, lower, MinimumLogicalSizeBytes, 64<<10)
			if err == nil || !strings.Contains(err.Error(), "read-only") {
				return fmt.Errorf("writable XFS was not rejected: %v", err)
			}
		}
		if name == "mount" && slices.Contains(args, "remount,ro") {
			frozen = true
			probe := filepath.Join(args[len(args)-1], "lower", "must-not-create")
			file, err := os.OpenFile(probe, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
			if file != nil {
				file.Close()
			}
			if !errors.Is(err, unix.EROFS) {
				return fmt.Errorf("XFS was not frozen: %v", err)
			}
		}
		return nil
	})
	destination := filepath.Join(work, "base.xfs")
	plan, err := (XFSBuilder{Runner: runner}).BuildWithDataRanges(ctx, source, destination, MinimumLogicalSizeBytes, 64<<10)
	require.NoError(t, err)
	require.True(t, frozen)
	require.NotNil(t, plan.Layout)
	require.EqualValues(t, 7, plan.Stats.RegularPaths)
	require.EqualValues(t, 5, plan.Stats.FilesScanned)
	require.EqualValues(t, 1, plan.Stats.HardlinksSkipped)
	require.GreaterOrEqual(t, plan.Stats.SharedExtents, int64(2))
	require.Greater(t, plan.Stats.FlaggedExtents, plan.Stats.SharedExtents)
	require.GreaterOrEqual(t, plan.Stats.PreferredBytes, int64(256<<10))
	leftovers, err := filepath.Glob(filepath.Join(work, "xfs-mount-*"))
	require.NoError(t, err)
	require.Empty(t, leftovers, "raw reads must only begin after clean unmount")
	image, err := os.Open(destination)
	require.NoError(t, err)
	defer image.Close()
	store := &xfsLayoutTestStore{objects: make(map[string][]byte)}
	built, err := rootfsblock.BuildMaterializedGenerationWithLayout(ctx, image, MinimumLogicalSizeBytes, store,
		rootfsblock.BuildOptions{FormatVersion: 2, DataRangeBytes: 64 << 10, PackBytes: 256 << 10, PageEntries: 4}, plan.Layout)
	require.NoError(t, err)
	sparseStore := &xfsLayoutTestStore{objects: make(map[string][]byte)}
	sparseBuilt, err := rootfsblock.BuildMaterializedFileGeneration(ctx, image, MinimumLogicalSizeBytes, sparseStore,
		rootfsblock.BuildOptions{FormatVersion: 2, DataRangeBytes: 64 << 10, PackBytes: 256 << 10, PageEntries: 4}, plan.Layout)
	require.NoError(t, err)
	require.Equal(t, built, sparseBuilt, "host hole discovery must preserve the exact XFS publication")
	require.Equal(t, store.objects, sparseStore.objects)
	reader, err := rootfsblock.NewReader(store, built.Descriptor, 4<<20)
	require.NoError(t, err)
	want, got := make([]byte, 1<<20), make([]byte, 1<<20)
	for offset := int64(0); offset < MinimumLogicalSizeBytes; offset += int64(len(want)) {
		require.NoError(t, ctx.Err())
		n, err := image.ReadAt(want, offset)
		require.NoError(t, err)
		require.Equal(t, len(want), n)
		n, err = reader.ReadAt(got, offset)
		require.NoError(t, err)
		require.Equal(t, len(got), n)
		require.True(t, bytes.Equal(want, got), "raw image differs at offset %d", offset)
	}
	t.Logf("full_image_bytes=%d stats=%+v published_objects=%d published_bytes=%d", MinimumLogicalSizeBytes, plan.Stats, built.Objects, built.Bytes)
}

type xfsLayoutTestStore struct{ objects map[string][]byte }

func (s *xfsLayoutTestStore) PutImmutable(_ context.Context, key string, payload []byte) error {
	if old, exists := s.objects[key]; exists && !bytes.Equal(old, payload) {
		return fmt.Errorf("immutable object conflict")
	}
	s.objects[key] = bytes.Clone(payload)
	return nil
}

func (s *xfsLayoutTestStore) Get(key string, offset, length int64) (io.ReadCloser, error) {
	payload, exists := s.objects[key]
	if !exists || offset < 0 || length < 0 || offset > int64(len(payload)) || length > int64(len(payload))-offset {
		return nil, fmt.Errorf("invalid object range")
	}
	return io.NopCloser(bytes.NewReader(payload[offset : offset+length])), nil
}
