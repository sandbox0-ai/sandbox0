package rootfsblock

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/stretchr/testify/require"
)

// This diagnostic measures existing byte/request amplification, not an SLO.
// The in-memory object backend deliberately excludes network timing.
func TestColdReadDiagnosticAmplification(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		name := "plaintext"
		if encrypted {
			name = "encrypted"
		}
		t.Run(name, func(t *testing.T) {
			base := &diagnosticCountingStore{Store: objectstore.NewMemoryStore(t.Name())}
			var source objectstore.Store = base
			if encrypted {
				source = objectstore.Encrypting(base, objectstore.EncryptionConfig{
					Enabled: true, KeyEncryptor: diagnosticKeyWrapper{},
				})
			}
			put := func(key string, payload []byte) ObjectRange {
				require.NoError(t, source.Put(key, bytes.NewReader(payload)))
				return ObjectRange{Key: key, Length: int64(len(payload)), Checksum: digest.FromBytes(payload).String()}
			}
			payload := bytes.Repeat([]byte{0x53}, DefaultDataRangeBytes)
			data := put("data-pack", payload)
			blocks := int64(len(payload) / LogicalBlockSize)
			mapping, err := EncodeMappingPage(MappingPage{
				StartBlock: 0, BlockCount: uint64(blocks),
				Entries: []MappingEntry{{LogicalStart: 0, BlockCount: uint32(blocks), Kind: MappingEntryData, Object: data}},
			})
			require.NoError(t, err)
			root := put("mapping-root", mapping)
			reader, err := NewReader(source, testReaderDescriptor(root, blocks), DefaultReadCacheBytes)
			require.NoError(t, err)
			base.calls, base.bytes = 0, 0
			read := make([]byte, LogicalBlockSize)
			n, err := reader.ReadAt(read, LogicalBlockSize)
			require.NoError(t, err)
			require.Equal(t, payload[:LogicalBlockSize], read)
			wantCalls := 1
			if encrypted {
				wantCalls = 2
			}
			require.Equal(t, wantCalls, base.calls)
			require.GreaterOrEqual(t, base.bytes, int64(DefaultDataRangeBytes))
			t.Logf("cold: logical_bytes=%d underlying_gets=%d fetched_bytes=%d amplification=%.2f", n, base.calls, base.bytes, float64(base.bytes)/float64(n))
			base.calls, base.bytes = 0, 0
			_, err = reader.ReadAt(read, LogicalBlockSize)
			require.NoError(t, err)
			require.Zero(t, base.calls)
			t.Logf("repeat: underlying_gets=%d fetched_bytes=%d", base.calls, base.bytes)
		})
	}
}

type diagnosticCountingStore struct {
	objectstore.Store
	mu    sync.Mutex
	calls int
	bytes int64
}

func (s *diagnosticCountingStore) Get(key string, offset, length int64) (io.ReadCloser, error) {
	body, err := s.Store.Get(key, offset, length)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	payload, err := io.ReadAll(body)
	s.mu.Lock()
	s.calls++
	s.bytes += int64(len(payload))
	s.mu.Unlock()
	return io.NopCloser(bytes.NewReader(payload)), err
}

// Compare supported format-v1 layouts without changing the production default
// or prefetching any user data. Sparse demand saves bytes with 1 MiB ranges;
// a full scan also demonstrates the corresponding increase in GET count.
func TestColdReadGranularityTradeoff(t *testing.T) {
	for _, rangeBytes := range []int{8 << 20, 1 << 20} {
		for _, trace := range []string{"sparse-demand", "full-scan"} {
			t.Run(fmt.Sprintf("range-%d/%s", rangeBytes, trace), func(t *testing.T) {
				const logicalBytes = 16 << 20
				logical := make([]byte, logicalBytes)
				// Make each range distinct so checksum deduplication cannot hide
				// physical reads in the geometry comparison.
				for index := range logical {
					logical[index] = byte(index/(1<<20) + 1)
				}
				plain := newBuildTestStore()
				built, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(logical), logicalBytes, plain, BuildOptions{DataRangeBytes: rangeBytes})
				require.NoError(t, err)
				base := &diagnosticCountingStore{Store: objectstore.NewMemoryStore(t.Name())}
				source := objectstore.EncryptingImmutable(base, objectstore.EncryptionConfig{
					Enabled: true, KeyEncryptor: diagnosticKeyWrapper{},
				}, objectstore.EncryptedHeaderCacheConfig{MaxEntries: 16, MaxBytes: 1 << 20})
				for key, payload := range plain.objects {
					require.NoError(t, source.Put(key, bytes.NewReader(payload)))
				}
				reader, err := NewReader(source, built.Descriptor, DefaultReadCacheBytes)
				require.NoError(t, err)
				base.calls, base.bytes = 0, 0
				var logicalRead int
				if trace == "sparse-demand" {
					for _, offset := range []int64{0, 8 << 20} {
						payload := make([]byte, LogicalBlockSize)
						_, err = reader.ReadAt(payload, offset)
						require.NoError(t, err)
						require.Equal(t, logical[offset:offset+LogicalBlockSize], payload)
						logicalRead += len(payload)
					}
				} else {
					payload := make([]byte, logicalBytes)
					_, err = reader.ReadAt(payload, 0)
					require.NoError(t, err)
					require.Equal(t, logical, payload)
					logicalRead = len(payload)
				}
				t.Logf("logical_bytes=%d underlying_gets=%d fetched_bytes=%d amplification=%.2f", logicalRead, base.calls, base.bytes, float64(base.bytes)/float64(logicalRead))
				readRanges := 2
				if trace == "full-scan" {
					readRanges = logicalBytes / rangeBytes
				}
				require.Equal(t, readRanges+1, base.calls, "one combined encryption header probe per immutable pack")
				dataBytes := int64(readRanges * rangeBytes)
				require.GreaterOrEqual(t, base.bytes, dataBytes)
				require.Less(t, base.bytes, dataBytes+4096, "only bounded encryption framing/header overhead is expected")
			})
		}
	}
}

func (s *diagnosticCountingStore) GetContext(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.Get(key, offset, length)
}

// Only wraps ephemeral test keys; data encryption still uses the real codec.
type diagnosticKeyWrapper struct{}

func (diagnosticKeyWrapper) Encrypt(data []byte) ([]byte, error) {
	return append([]byte(nil), data...), nil
}

func (diagnosticKeyWrapper) Decrypt(data []byte) ([]byte, error) {
	return append([]byte(nil), data...), nil
}
