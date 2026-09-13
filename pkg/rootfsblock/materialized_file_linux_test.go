//go:build linux

package rootfsblock

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func createSparseMaterializedFile(t *testing.T, size int64, writes map[int64][]byte) (*os.File, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "owned-image")
	writer, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	require.NoError(t, err)
	require.NoError(t, writer.Truncate(size))
	for offset, data := range writes {
		_, err := writer.WriteAt(data, offset)
		require.NoError(t, err)
	}
	require.NoError(t, writer.Sync())
	require.NoError(t, writer.Close())
	reader, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })
	return reader, path
}

func TestMaterializedFilePublicationPreservesCanonicalBytesAndPosition(t *testing.T) {
	const size = 17*CompressedDataRangeBytes + LogicalBlockSize
	writes := map[int64][]byte{
		0:                              []byte{1},
		3*CompressedDataRangeBytes - 1: bytes.Repeat([]byte{5}, 2*LogicalBlockSize+1),
		7*CompressedDataRangeBytes + LogicalBlockSize: bytes.Repeat([]byte{0}, 3*LogicalBlockSize),
		size - 3: {9, 8, 7},
	}
	for _, version := range []int{0, 2} {
		for _, shifted := range []bool{false, true} {
			if version == 0 && shifted {
				continue
			}
			t.Run(fmt.Sprintf("format-%d/shifted-%t", version, shifted), func(t *testing.T) {
				file, _ := createSparseMaterializedFile(t, size, writes)
				var plan *DataRangeLayout
				if shifted {
					var err error
					plan, err = NewDataRangeLayout(size, CompressedDataRangeBytes, []DataRangeSpan{{LogicalBlockSize, 5*CompressedDataRangeBytes + LogicalBlockSize}})
					require.NoError(t, err)
				}
				options := BuildOptions{FormatVersion: version, DataRangeBytes: CompressedDataRangeBytes, PackBytes: 2 * CompressedDataRangeBytes, PageEntries: 2}
				plain, sparse := newBuildTestStore(), newBuildTestStore()
				want, err := buildMaterializedGeneration(t.Context(), file, size, plain, options, plan)
				require.NoError(t, err)
				_, err = file.Seek(17, io.SeekStart)
				require.NoError(t, err)
				got, err := BuildMaterializedFileGeneration(t.Context(), file, size, sparse, options, plan)
				require.NoError(t, err)
				require.Equal(t, want, got)
				require.Equal(t, plain.objects, sparse.objects)
				position, err := file.Seek(0, io.SeekCurrent)
				require.NoError(t, err)
				require.Equal(t, int64(17), position)
			})
		}
	}
}

type materializedCallbackPublisher struct {
	*buildTestStore
	once func() error
}

func (p *materializedCallbackPublisher) PutImmutable(ctx context.Context, key string, payload []byte) error {
	if p.once != nil {
		fn := p.once
		p.once = nil
		if err := fn(); err != nil {
			return err
		}
	}
	return p.buildTestStore.PutImmutable(ctx, key, payload)
}

func TestMaterializedFileRejectsMutationAndPreservesFailure(t *testing.T) {
	const size = 4 * CompressedDataRangeBytes
	for _, mutation := range []string{"rewrite", "truncate", "permissions", "publish-error", "close-reader"} {
		t.Run(mutation, func(t *testing.T) {
			file, path := createSparseMaterializedFile(t, size, map[int64][]byte{0: bytes.Repeat([]byte{1}, size)})
			publisher := &materializedCallbackPublisher{buildTestStore: newBuildTestStore(), once: func() error {
				switch mutation {
				case "rewrite":
					writer, err := os.OpenFile(path, os.O_WRONLY, 0)
					if err != nil {
						return err
					}
					_, err = writer.WriteAt([]byte{2}, size-1)
					return errors.Join(err, writer.Close())
				case "truncate":
					return os.Truncate(path, size/2)
				case "permissions":
					return os.Chmod(path, 0o400)
				case "publish-error":
					return errors.New("injected immutable publication failure")
				case "close-reader":
					return file.Close()
				}
				return nil
			}}
			result, err := BuildMaterializedFileGeneration(t.Context(), file, size, publisher, BuildOptions{FormatVersion: 2, PackBytes: CompressedDataRangeBytes}, nil)
			require.Error(t, err)
			require.Empty(t, result)
			if mutation == "rewrite" || mutation == "truncate" || mutation == "permissions" {
				require.ErrorContains(t, err, "changed during publication")
			}
			if mutation == "publish-error" {
				require.ErrorContains(t, err, "injected immutable publication failure")
			}
		})
	}
}

func TestMaterializedFileRejectsUnsafeInputs(t *testing.T) {
	const size = 2 * CompressedDataRangeBytes
	file, path := createSparseMaterializedFile(t, size, nil)
	writer, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer writer.Close()
	dir, err := os.Open(filepath.Dir(path))
	require.NoError(t, err)
	defer dir.Close()
	closed, err := os.Open(path)
	require.NoError(t, err)
	require.NoError(t, closed.Close())
	for _, tc := range []struct {
		name string
		file *os.File
		size int64
	}{
		{"nil", nil, size}, {"writable", writer, size}, {"directory", dir, size},
		{"closed", closed, size}, {"wrong-size", file, size + LogicalBlockSize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newBuildTestStore()
			got, err := BuildMaterializedFileGeneration(t.Context(), tc.file, tc.size, store, BuildOptions{FormatVersion: 2}, nil)
			require.Error(t, err)
			require.Empty(t, got)
			require.Empty(t, store.objects)
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	got, err := BuildMaterializedFileGeneration(ctx, file, size, newBuildTestStore(), BuildOptions{}, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, got)
}

func TestMaterializedSparseOneTiBFileDoesNotReadLogicalHoles(t *testing.T) {
	const size = int64(1) << 40
	const unit = CompressedDataRangeBytes
	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprint(empty), func(t *testing.T) {
			writes := map[int64][]byte{}
			if !empty {
				writes[0] = []byte{3}
				writes[size-1] = []byte{7}
			}
			file, _ := createSparseMaterializedFile(t, size, writes)
			// Do not accidentally run a TiB full-scan fallback on a filesystem
			// that conservatively reports every byte as data.
			hole, err := file.Seek(unit, unix.SEEK_HOLE)
			if err != nil || hole != unit {
				t.Skip("backing filesystem does not expose the test's sparse hole")
			}
			calls := 0
			input := &sparseCountReader{ReaderAt: file}
			reader := &materializedFileReader{ReaderAt: input, size: size, seek: func(offset int64, hole bool) (int64, error) {
				calls++
				return seekMaterializedFile(file, offset, hole)
			}}
			store := newBuildTestStore()
			result, err := BuildMaterializedGeneration(t.Context(), reader, size, store, BuildOptions{FormatVersion: 2})
			require.NoError(t, err)
			wantReads := int64(2 * unit)
			if empty {
				wantReads = 0
			}
			require.Equal(t, wantReads, input.bytes)
			require.LessOrEqual(t, calls, 5)
			publicResult, err := BuildMaterializedFileGeneration(t.Context(), file, size, store, BuildOptions{FormatVersion: 2}, nil)
			require.NoError(t, err)
			require.Equal(t, result, publicResult)
			view, err := NewReader(store, result.Descriptor, 0)
			require.NoError(t, err)
			for _, offset := range []int64{0, unit, size / 2, size - 1} {
				got := make([]byte, 1)
				_, err := view.ReadAt(got, offset)
				require.NoError(t, err)
				want := byte(0)
				if !empty && offset == 0 {
					want = 3
				}
				if !empty && offset == size-1 {
					want = 7
				}
				require.Equal(t, want, got[0])
			}
			info, err := file.Stat()
			require.NoError(t, err)
			t.Logf("logical_bytes=%d allocated_bytes=%d read_bytes=%d seek_calls=%d objects=%d; sparse storage model, not populated RootFS or startup acceptance", size, info.Sys().(*syscall.Stat_t).Blocks*512, input.bytes, calls, result.Objects)
		})
	}
}
