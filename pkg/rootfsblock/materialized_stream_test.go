package rootfsblock

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func TestMaterializedMappingPublishedBeforeImageEOF(t *testing.T) {
	for _, version := range []int{0, CompressedFormatVersion} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			const size = 40 * LogicalBlockSize
			input := &streamPatternReader{size: size}
			publisher := &streamInspectPublisher{store: newBuildTestStore(), onMap: func() error {
				require.Less(t, input.readBytes, int64(size), "mapping leaves must not wait for the complete image")
				return errors.New("mapping publication stopped")
			}}
			_, err := BuildMaterializedGeneration(t.Context(), input, size, publisher, BuildOptions{
				FormatVersion: version, DataRangeBytes: LogicalBlockSize, PackBytes: 4 * LogicalBlockSize, PageEntries: 2,
			})
			require.ErrorContains(t, err, "mapping publication stopped")
			require.Equal(t, 1, publisher.maps)
			require.Less(t, input.readBytes, int64(size), "failed publication must stop consuming input")
		})
	}
}

// These identities were captured from the pre-streaming implementation. They
// cover empty roots, fanout boundaries, sparse gaps, trailing zeros and both
// formats, independently of the new tree construction algorithm.
func TestMaterializedGenerationPreservesGoldenPublication(t *testing.T) {
	var cases []struct {
		Version       int    `json:"version"`
		Fanout        int    `json:"fanout"`
		NonzeroRanges int    `json:"nonzero_ranges"`
		DescriptorSHA string `json:"descriptor_sha256"`
		ReferencesSHA string `json:"references_sha256"`
		Objects       int    `json:"objects"`
		Bytes         int64  `json:"bytes"`
	}
	raw, err := os.ReadFile("testdata/materialized_golden.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &cases))
	require.Len(t, cases, 72)
	for _, tc := range cases {
		t.Run(fmt.Sprintf("version-%d/fanout-%d/entries-%d", tc.Version, tc.Fanout, tc.NonzeroRanges), func(t *testing.T) {
			payload := make([]byte, (tc.NonzeroRanges*3+7)*LogicalBlockSize)
			for index := 0; index < tc.NonzeroRanges; index++ {
				for j := (index*3 + 2) * LogicalBlockSize; j < (index*3+3)*LogicalBlockSize; j++ {
					payload[j] = byte(index%251 + 1)
				}
			}
			store := newBuildTestStore()
			options := BuildOptions{FormatVersion: tc.Version, DataRangeBytes: LogicalBlockSize, PackBytes: 4 * LogicalBlockSize, PageEntries: tc.Fanout}
			result, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(payload), int64(len(payload)), store, options)
			require.NoError(t, err)
			refs, err := json.Marshal(result.References)
			require.NoError(t, err)
			require.Equal(t, tc.DescriptorSHA, digest.FromBytes(result.Payload).String())
			require.Equal(t, tc.ReferencesSHA, digest.FromBytes(refs).String())
			require.Equal(t, tc.Objects, result.Objects)
			require.Equal(t, tc.Bytes, result.Bytes)
			reader, err := NewReader(store, result.Descriptor, DefaultReadCacheBytes)
			require.NoError(t, err)
			actual := make([]byte, len(payload))
			_, err = reader.ReadAt(actual, 0)
			require.NoError(t, err)
			require.Equal(t, payload, actual)
			retried, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(payload), int64(len(payload)), store, options)
			require.NoError(t, err)
			require.Equal(t, result, retried)
		})
	}
}

type streamPatternReader struct {
	size      int64
	readBytes int64
	zero      bool
}

func (r *streamPatternReader) ReadAt(dst []byte, offset int64) (int, error) {
	if offset < 0 || offset >= r.size {
		return 0, io.EOF
	}
	n := min(int64(len(dst)), r.size-offset)
	for index := range dst[:int(n)] {
		if r.zero {
			dst[index] = 0
		} else {
			dst[index] = byte((offset+int64(index))/LogicalBlockSize%251 + 1)
		}
	}
	r.readBytes += n
	if n < int64(len(dst)) {
		return int(n), io.EOF
	}
	return int(n), nil
}

type streamInspectPublisher struct {
	store *buildTestStore
	maps  int
	onMap func() error
}

func (p *streamInspectPublisher) PutImmutable(ctx context.Context, key string, data []byte) error {
	if strings.Contains(key, "/maps/") {
		p.maps++
		if p.onMap != nil {
			if err := p.onMap(); err != nil {
				return err
			}
		}
	}
	return p.store.PutImmutable(ctx, key, data)
}

type streamDiscardPublisher struct{}

func (streamDiscardPublisher) PutImmutable(context.Context, string, []byte) error { return nil }

func BenchmarkMaterializedCompressedImport(b *testing.B) {
	const size = 64 << 20
	b.SetBytes(size)
	b.ReportAllocs()
	for b.Loop() {
		_, err := BuildMaterializedGeneration(b.Context(), &streamPatternReader{size: size}, size, streamDiscardPublisher{}, BuildOptions{FormatVersion: CompressedFormatVersion})
		if err != nil {
			b.Fatal(err)
		}
	}
}
