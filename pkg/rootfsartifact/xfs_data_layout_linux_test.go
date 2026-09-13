//go:build linux

package rootfsartifact

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

func TestXFSDataRangesFiemapABI(t *testing.T) {
	require.Equal(t, uintptr(32), unsafe.Sizeof(fiemapHeader{}))
	require.Equal(t, uintptr(56), unsafe.Sizeof(fiemapExtent{}))
	require.Equal(t, uintptr(40), unsafe.Offsetof(fiemapExtent{}.Flags))
}

func TestXFSDataRangesFilePhaseHolesAndTail(t *testing.T) {
	const unit = uint64(64 << 10)
	c := xfsRangeCollector{logicalSize: 8 << 20, unit: unit, maxExtents: 100}
	// A hole precedes the first extent. The file-relative 64KiB boundary
	// occurs 60KiB into that extent, at a non-global physical phase.
	query := func(_ context.Context, _ *os.File, start, length uint64) ([]fiemapExtent, error) {
		require.Zero(t, start)
		require.Equal(t, 5*unit+100, length)
		return []fiemapExtent{
			{Logical: 4096, Physical: 2 * unit, Length: 3 * unit},
			{Logical: 4 * unit, Physical: 10*unit + 4096, Length: 2 * unit, Flags: fiemapExtentLast},
		}, nil
	}
	require.NoError(t, c.scanFile(t.Context(), nil, 5*unit+100, query))
	require.Equal(t, []rootfsblock.DataRangeSpan{
		{Start: int64(3*unit - 4096), End: int64(5*unit - 4096)},
		{Start: int64(10*unit + 4096), End: int64(11*unit + 4096)},
	}, c.spans)
	require.EqualValues(t, 3*unit, c.stats.PreferredBytes)
	require.EqualValues(t, 2, c.stats.Extents)
}

func TestXFSDataRangesPaginationAndFirstOverlap(t *testing.T) {
	const unit = uint64(64 << 10)
	c := xfsRangeCollector{logicalSize: 8 << 20, unit: unit, maxExtents: 100}
	calls := 0
	query := func(_ context.Context, _ *os.File, start, _ uint64) ([]fiemapExtent, error) {
		calls++
		if calls == 1 {
			require.Zero(t, start)
			return []fiemapExtent{{Physical: 4096, Length: unit}}, nil
		}
		require.Equal(t, unit, start)
		// The API permits an extent to begin before the requested cursor.
		return []fiemapExtent{{Physical: 4096, Length: 3 * unit, Flags: fiemapExtentLast}}, nil
	}
	require.NoError(t, c.scanFile(t.Context(), nil, 3*unit, query))
	require.Equal(t, 2, calls)
	require.Equal(t, []rootfsblock.DataRangeSpan{{Start: 4096, End: int64(unit + 4096)}, {Start: int64(unit + 4096), End: int64(3*unit + 4096)}}, c.spans)
	_, err := rootfsblock.NewDataRangeLayout(8<<20, int(unit), c.spans)
	require.NoError(t, err)
}

func TestXFSDataRangesSkipEveryNonLastFlag(t *testing.T) {
	for bit := uint(1); bit < 32; bit++ {
		t.Run(fmt.Sprint(bit), func(t *testing.T) {
			c := xfsRangeCollector{logicalSize: 1 << 20, unit: 64 << 10, maxExtents: 10}
			query := func(context.Context, *os.File, uint64, uint64) ([]fiemapExtent, error) {
				return []fiemapExtent{{Physical: math.MaxUint64, Length: 1 << 20, Flags: 1<<bit | fiemapExtentLast}}, nil
			}
			require.NoError(t, c.scanFile(t.Context(), nil, 1<<20, query))
			require.Empty(t, c.spans, "flagged physical contents must not become file-aligned ranges")
			require.EqualValues(t, 1, c.stats.FlaggedExtents)
			require.Equal(t, bit == 13, c.stats.SharedExtents == 1)
		})
	}
}

func TestXFSDataRangesRejectMalformedGeometryAndBudget(t *testing.T) {
	for name, extents := range map[string][]fiemapExtent{
		"zero":              {{Length: 0}},
		"overflow":          {{Logical: math.MaxUint64 - 10, Length: 20}},
		"outside-file":      {{Logical: 1 << 20, Length: 4096}},
		"outside-image":     {{Physical: 1 << 20, Length: 4096}},
		"overflow-physical": {{Physical: math.MaxUint64 - 4095, Length: 4096}},
		"unaligned":         {{Physical: 1, Length: 4096}},
		"unordered":         {{Logical: 4096, Length: 4096}, {Length: 4096}},
		"overlap":           {{Length: 8192}, {Logical: 4096, Length: 8192}},
		"early-last":        {{Length: 4096, Flags: fiemapExtentLast}, {Logical: 4096, Length: 4096}},
	} {
		t.Run(name, func(t *testing.T) {
			c := xfsRangeCollector{logicalSize: 1 << 20, unit: 64 << 10, maxExtents: 10}
			require.Error(t, c.scanFile(t.Context(), nil, 1<<20, func(context.Context, *os.File, uint64, uint64) ([]fiemapExtent, error) { return extents, nil }))
		})
	}
	c := xfsRangeCollector{logicalSize: 1 << 20, unit: 64 << 10, maxExtents: 1}
	query := func(context.Context, *os.File, uint64, uint64) ([]fiemapExtent, error) {
		return []fiemapExtent{{Length: 64 << 10}}, nil
	}
	require.ErrorContains(t, c.scanFile(t.Context(), nil, 1<<20, query), "limit")
	c.maxExtents = 10
	c.stats.Extents = 0
	require.ErrorContains(t, c.scanFile(t.Context(), nil, 1<<20, query), "non-progressing")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, c.scanFile(ctx, nil, 1<<20, query), context.Canceled)
	ctx, cancel = context.WithCancel(t.Context())
	require.ErrorIs(t, c.scanFile(ctx, nil, 1<<20, func(context.Context, *os.File, uint64, uint64) ([]fiemapExtent, error) {
		cancel()
		return nil, nil
	}), context.Canceled)
}

func collectTestTree(t *testing.T, path string, query fiemapQuery) (XFSDataRangePlan, error) {
	t.Helper()
	root, err := os.OpenRoot(path)
	require.NoError(t, err)
	defer root.Close()
	dir, err := root.Open(".")
	require.NoError(t, err)
	defer dir.Close()
	return collectXFSDataRanges(t.Context(), root, dir, 32<<20, 64<<10, query)
}

func TestXFSDataRangesBoundedTraversalHardlinksAndIsolation(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(root, "sub"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(root, "sub", "data"), make([]byte, 192<<10), 0o600))
	require.NoError(t, os.Link(filepath.Join(root, "sub", "data"), filepath.Join(root, "hardlink")))
	require.NoError(t, os.Symlink(filepath.Join(t.TempDir(), "outside"), filepath.Join(root, "external-link")))
	require.NoError(t, os.Symlink("sub", filepath.Join(root, "directory-link")))
	require.NoError(t, unix.Mkfifo(filepath.Join(root, "fifo"), 0o600))
	for i := range 270 {
		require.NoError(t, os.WriteFile(filepath.Join(root, fmt.Sprint(i)), []byte("small"), 0o600))
	}
	calls := 0
	plan, err := collectTestTree(t, root, func(_ context.Context, file *os.File, _, _ uint64) ([]fiemapExtent, error) {
		calls++
		info, err := file.Stat()
		require.NoError(t, err)
		require.EqualValues(t, 192<<10, info.Size())
		return []fiemapExtent{{Physical: 4096, Length: 192 << 10, Flags: fiemapExtentLast}}, nil
	})
	require.NoError(t, err)
	require.NotNil(t, plan.Layout)
	require.Equal(t, 1, calls)
	require.Equal(t, XFSDataRangeStats{Entries: 276, RegularPaths: 272, FilesScanned: 1, HardlinksSkipped: 1, Extents: 1, PreferredSpans: 1, PreferredBytes: 192 << 10}, plan.Stats)
}

func TestXFSDataRangesRejectPhysicalOverlapAndMutation(t *testing.T) {
	for _, mutate := range []bool{false, true} {
		t.Run(fmt.Sprint(mutate), func(t *testing.T) {
			root := t.TempDir()
			for _, name := range []string{"one", "two"} {
				require.NoError(t, os.WriteFile(filepath.Join(root, name), make([]byte, 64<<10), 0o600))
			}
			plan, err := collectTestTree(t, root, func(_ context.Context, file *os.File, _, _ uint64) ([]fiemapExtent, error) {
				if mutate {
					require.NoError(t, os.Truncate(filepath.Join(root, filepath.Base(file.Name())), 1))
				}
				return []fiemapExtent{{Physical: 4096, Length: 64 << 10, Flags: fiemapExtentLast}}, nil
			})
			require.Error(t, err)
			require.Nil(t, plan.Layout)
			if mutate {
				require.ErrorContains(t, err, "changed")
			} else {
				require.ErrorContains(t, err, "overlap")
			}
		})
	}
}

func TestXFSDataRangesRejectExcessDepthAndNonXFS(t *testing.T) {
	root := t.TempDir()
	path := root
	for range xfsScanMaxDepth + 1 {
		path = filepath.Join(path, "d")
		require.NoError(t, os.Mkdir(path, 0o700))
	}
	plan, err := collectTestTree(t, root, nil)
	require.ErrorContains(t, err, "depth limit")
	require.Nil(t, plan.Layout)
	plan, err = CollectReadOnlyXFSDataRanges(t.Context(), root, 1<<20, 64<<10)
	require.ErrorContains(t, err, "read-only 4KiB-block XFS")
	require.Nil(t, plan.Layout)
}

type xfsInspectRunner func(context.Context, string, ...string) error

func (r xfsInspectRunner) Run(ctx context.Context, name string, args ...string) error {
	return r(ctx, name, args...)
}

func TestXFSDataRangesBuilderFreezesInspectsAndCleans(t *testing.T) {
	for _, failure := range []string{"", "remount", "inspect", "xfs_repair"} {
		t.Run(failure, func(t *testing.T) {
			var calls []string
			runner := xfsInspectRunner(func(_ context.Context, name string, args ...string) error {
				if name == "mount" && slices.Contains(args, "remount,ro") {
					name = "remount"
				}
				calls = append(calls, name)
				if name == failure {
					return errCommandFailure
				}
				return nil
			})
			destination := filepath.Join(t.TempDir(), "base.xfs")
			err := (XFSBuilder{Runner: runner}).build(t.Context(), t.TempDir(), destination, MinimumLogicalSizeBytes, func(_ context.Context, lower string) error {
				require.Equal(t, "remount", calls[len(calls)-1])
				require.Equal(t, "lower", filepath.Base(lower))
				calls = append(calls, "inspect")
				if failure == "inspect" {
					return errCommandFailure
				}
				return nil
			})
			if failure == "" {
				require.NoError(t, err)
				require.Equal(t, []string{"mkfs.xfs", "mount", "cp", "remount", "inspect", "umount", "xfs_repair"}, calls)
			} else {
				require.ErrorIs(t, err, errCommandFailure)
				_, err := os.Stat(destination)
				require.True(t, errors.Is(err, os.ErrNotExist))
				require.Contains(t, calls, "umount")
			}
		})
	}
	destination := filepath.Join(t.TempDir(), "base.xfs")
	runner := &recordingRunner{}
	plan, err := (XFSBuilder{Runner: runner}).BuildWithDataRanges(t.Context(), t.TempDir(), destination, MinimumLogicalSizeBytes, 1)
	require.Error(t, err)
	require.Nil(t, plan.Layout)
	require.Empty(t, runner.calls)
}
