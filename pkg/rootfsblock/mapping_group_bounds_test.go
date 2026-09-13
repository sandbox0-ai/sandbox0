package rootfsblock

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func TestMappingGroupLazyDecodeAdmission(t *testing.T) {
	store, d, children, pack := groupFixture(t, true, "decode-admission")
	synctest.Test(t, func(t *testing.T) {
		source := &groupProbeSource{RangeSource: store, pack: pack}
		r, err := NewReader(source, d, DefaultReadCacheBytes)
		require.NoError(t, err)
		require.NoError(t, groupRead(r, r.root, 0))
		require.EqualValues(t, 2, r.cache.decodes.Load())
		_, ok := r.cache.getPage(rangeCacheKey(children[1].Object))
		require.False(t, ok)
		_, ok = r.cache.get(rangeCacheKey(children[1].Object))
		require.True(t, ok)
		var release []func()
		for range 8 {
			f, err := r.acquireSourceSlot()
			require.NoError(t, err)
			release = append(release, f)
		}
		defer func() {
			for _, f := range release {
				f()
			}
		}()
		source.reset()
		done := make(chan error, 1)
		go func() { done <- groupRead(r, r.root, children[1].LogicalStart) }()
		synctest.Wait()
		require.Empty(t, done)
		require.Empty(t, source.requests())
		requireReadAdmissionUsage(t, &r.cache.sourceSlots, 8, 1, 1)
		release[0]()
		require.NoError(t, <-done)
		requireReadAdmissionUsage(t, &r.cache.sourceSlots, 7, 0, 0)
		require.EqualValues(t, 3, r.cache.decodes.Load())
	})
}

type groupSparseReader struct {
	data  []byte
	reads int64
}

func (r *groupSparseReader) ReadAt(dst []byte, off int64) (int, error) {
	r.reads += int64(len(dst))
	return bytes.NewReader(r.data).ReadAt(dst, off)
}
func (r *groupSparseReader) nextDataOffset(ctx context.Context, off int64) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	for off < int64(len(r.data)) {
		if r.data[off] != 0 {
			return off, nil
		}
		off++
	}
	return int64(len(r.data)), nil
}

func TestMappingPublicationSparseScanEquivalence(t *testing.T) {
	payload := make([]byte, 200*LogicalBlockSize)
	for i := range 50 {
		copy(payload[(i*3+1)*LogicalBlockSize:], bytes.Repeat([]byte{byte(i + 1)}, LogicalBlockSize))
	}
	opts := BuildOptions{FormatVersion: 2, DataRangeBytes: LogicalBlockSize, PageEntries: 3, PackBytes: 8 * LogicalBlockSize, MappingGroupPolicy: ContiguousMappingV1}
	full, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(payload), int64(len(payload)), newBuildTestStore(), opts)
	require.NoError(t, err)
	sparse := &groupSparseReader{data: payload}
	store := newBuildTestStore()
	got, err := buildMaterializedGeneration(t.Context(), sparse, int64(len(payload)), store, opts, nil)
	require.NoError(t, err)
	require.Equal(t, full, got)
	require.Less(t, sparse.reads, int64(len(payload)))
	mappingPublicationInventory(t, store, got)
}

func TestMappingPublicationByteBoundsAndOversizeFallback(t *testing.T) {
	for _, kind := range []string{"stored", "decoded", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			store := newBuildTestStore()
			opts, err := NormalizeBuildOptions(BuildOptions{FormatVersion: 2, MappingGroupPolicy: ContiguousMappingV1})
			require.NoError(t, err)
			b := generationBuilder{ctx: t.Context(), publisher: store, options: opts}
			defer b.encoder.close()
			tree := newMappingStream(&b, 4096)
			rng := rand.New(rand.NewPCG(3, 71))
			keyLen := 800
			if kind == "stored" {
				keyLen = 256
			}
			if kind == "oversized" {
				keyLen = 1000
			}
			for p := range 3 {
				page := MappingPage{Version: 2, StartBlock: uint64(p * 1024), BlockCount: 1024}
				for i := range 1024 {
					key := strings.Repeat("k", keyLen)
					if kind == "stored" {
						keyBytes := make([]byte, keyLen)
						for j := range keyBytes {
							keyBytes[j] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[rng.IntN(62)]
						}
						key = string(keyBytes)
					}
					page.Entries = append(page.Entries, MappingEntry{Kind: MappingEntryData, LogicalStart: uint64(p*1024 + i), BlockCount: 1, Object: ObjectRange{Key: "packs/" + key, Length: LogicalBlockSize, Checksum: digest.FromString(fmt.Sprint(i)).String()}})
				}
				require.NoError(t, tree.queueMappingPage(0, page))
				for _, pending := range tree.pending {
					var stored, decoded int
					for _, v := range pending {
						stored += len(v.stored)
						decoded += len(v.payload)
					}
					require.LessOrEqual(t, stored, mappingPublishStoredBytes)
					require.LessOrEqual(t, decoded, mappingPublishDecodedBytes)
				}
			}
			require.NoError(t, tree.flushPendingMappingGroups())
			require.Len(t, tree.levels[0], 3)
			for _, page := range tree.levels[0] {
				require.Zero(t, page.object.Offset)
				require.NotEmpty(t, store.objects[page.object.Key])
			}
			require.Len(t, store.objects, 3, "each oversized pair falls back to independently addressed pages")
		})
	}
}

var _ io.ReaderAt = (*groupSparseReader)(nil)
