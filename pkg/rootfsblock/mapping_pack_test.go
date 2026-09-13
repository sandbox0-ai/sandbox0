package rootfsblock

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

// This is a metadata locator/codec compatibility fixture, not an XFS image or
// startup benchmark. The unmodified v2 Reader must read selected, nonadjacent
// child pages stored as independent zstd ranges inside one immutable object.
func mappingPackFixture(t *testing.T, encoded bool) (*buildTestStore, Descriptor, []MappingEntry, [][]byte, string) {
	t.Helper()
	store := newBuildTestStore()
	const totalBlocks = uint64(1 << 28)
	starts := []uint64{0, 1 << 20, 2 << 20}
	var data, group []byte
	var expected [][]byte
	var children []MappingEntry
	var leaves [][]byte
	for i, start := range starts {
		payload := bytes.Repeat([]byte{byte(i + 1)}, LogicalBlockSize)
		expected = append(expected, payload)
		dataObject := ObjectRange{Key: "packs/map-compat/data", Offset: int64(len(data)), Length: LogicalBlockSize, Checksum: digest.FromBytes(payload).String()}
		data = append(data, payload...)
		count := uint64(1 << 20)
		if i == len(starts)-1 {
			count = totalBlocks - start
		}
		leaf := MappingPage{Version: CompressedFormatVersion, StartBlock: start, BlockCount: count, Entries: []MappingEntry{{Kind: MappingEntryData, LogicalStart: start, BlockCount: 1, Object: dataObject}}}
		raw, err := EncodeMappingPage(leaf)
		require.NoError(t, err)
		stored := raw
		object := ObjectRange{Length: int64(len(raw)), Checksum: digest.FromBytes(raw).String()}
		if encoded {
			stored, object, err = encodeRangePayload(t.Context(), raw)
			require.NoError(t, err)
			require.Equal(t, RangeEncodingZstd, object.Encoding)
		}
		leaves = append(leaves, stored)
		children = append(children, MappingEntry{Kind: MappingEntryChild, LogicalStart: start, BlockCount: uint32(count), Object: object})
	}
	require.NoError(t, store.PutImmutable(t.Context(), "packs/map-compat/data", data))
	for _, i := range []int{0, 2} {
		children[i].Object.Offset = int64(len(group))
		group = append(group, leaves[i]...)
	}
	pack := "maps/sha256/" + digest.FromBytes(group).Encoded()
	require.NoError(t, store.PutImmutable(t.Context(), pack, group))
	children[0].Object.Key, children[2].Object.Key = pack, pack
	other := "maps/sha256/" + digest.FromBytes(leaves[1]).Encoded()
	require.NoError(t, store.PutImmutable(t.Context(), other, leaves[1]))
	children[1].Object.Key = other
	descriptor := publishMappingPackRoot(t, store, children, totalBlocks)
	return store, descriptor, children, expected, pack
}

func publishMappingPackRoot(t *testing.T, store *buildTestStore, children []MappingEntry, blocks uint64) Descriptor {
	t.Helper()
	page := MappingPage{Version: CompressedFormatVersion, Level: 1, BlockCount: blocks, Entries: children}
	raw, err := EncodeMappingPage(page)
	require.NoError(t, err)
	stored, object, err := encodeRangePayload(t.Context(), raw)
	require.NoError(t, err)
	object.Key = "maps/sha256/" + digest.FromBytes(stored).Encoded()
	require.NoError(t, store.PutImmutable(t.Context(), object.Key, stored))
	descriptor := testReaderDescriptor(object, int64(blocks))
	descriptor.Version, descriptor.MappingRoot.Version = CompressedFormatVersion, CompressedFormatVersion
	require.NoError(t, descriptor.Validate())
	return descriptor
}

func TestMappingPackCompatibilityCurrentReader(t *testing.T) {
	for _, encoded := range []bool{false, true} {
		for _, budget := range []int64{0, DefaultReadCacheBytes} {
			t.Run(fmt.Sprintf("encoded-%t/cache-%d", encoded, budget), func(t *testing.T) {
				store, descriptor, children, expected, pack := mappingPackFixture(t, encoded)
				source := &adaptiveSource{RangeSource: store, pack: pack}
				reader, err := NewReader(source, descriptor, budget)
				require.NoError(t, err)
				for i, child := range children {
					actual := make([]byte, 2*LogicalBlockSize)
					_, err := reader.ReadAt(actual, int64(child.LogicalStart)*LogicalBlockSize)
					require.NoError(t, err)
					require.Equal(t, expected[i], actual[:LogicalBlockSize])
					require.Equal(t, make([]byte, LogicalBlockSize), actual[LogicalBlockSize:])
				}
				var wanted []adaptiveGet
				for _, i := range []int{0, 2} {
					request := adaptiveGet{children[i].Object.Offset, children[i].Object.StoredLength()}
					wanted = append(wanted, request)
					if budget == 0 {
						// Current Reader re-resolves the following hole when the
						// metadata cache is disabled; this probe does not alter it.
						wanted = append(wanted, request)
					}
				}
				if groupCandidate() && budget == DefaultReadCacheBytes {
					wanted = []adaptiveGet{{0, children[0].Object.StoredLength() + children[2].Object.StoredLength()}}
				}
				require.Equal(t, wanted, source.requests())
				require.LessOrEqual(t, reader.cache.bytes, budget)
			})
		}
	}
}

func TestMappingPackCompatibilityNeighborCorruptionAndParentBinding(t *testing.T) {
	for _, corruption := range []string{"neighbor", "parent-binding"} {
		t.Run(corruption, func(t *testing.T) {
			store, descriptor, children, expected, pack := mappingPackFixture(t, true)
			if corruption == "neighbor" {
				store.objects[pack][children[2].Object.Offset] ^= 1
			} else {
				children[2].Object = children[0].Object
				descriptor = publishMappingPackRoot(t, store, children, uint64(descriptor.LogicalSizeBytes/LogicalBlockSize))
			}
			reader, err := NewReader(store, descriptor, DefaultReadCacheBytes)
			require.NoError(t, err)
			actual := make([]byte, LogicalBlockSize)
			_, err = reader.ReadAt(actual, 0)
			require.NoError(t, err)
			require.Equal(t, expected[0], actual)
			actual = bytes.Repeat([]byte{0xee}, LogicalBlockSize)
			n, err := reader.ReadAt(actual, int64(children[2].LogicalStart)*LogicalBlockSize)
			require.Error(t, err)
			require.Zero(t, n)
			require.Equal(t, bytes.Repeat([]byte{0xee}, LogicalBlockSize), actual)
			if corruption == "parent-binding" {
				require.ErrorContains(t, err, "mapping child does not match")
			}
		})
	}
}

func TestMappingPackCompatibilityIncrementalPathCopy(t *testing.T) {
	store, descriptor, children, expected, pack := mappingPackFixture(t, true)
	changed := bytes.Repeat([]byte{0x99}, LogicalBlockSize)
	result, err := BuildIncrementalGeneration(t.Context(), store, descriptor, []BlockUpdate{{Block: 0, Data: changed}}, store, BuildOptions{PageEntries: 2})
	require.NoError(t, err)
	reader, err := NewReader(store, result.Descriptor, DefaultReadCacheBytes)
	require.NoError(t, err)
	expected[0] = changed
	for i, child := range children {
		actual := make([]byte, LogicalBlockSize)
		_, err := reader.ReadAt(actual, int64(child.LogicalStart)*LogicalBlockSize)
		require.NoError(t, err)
		require.Equal(t, expected[i], actual)
	}
	var retained func(MappingPage) bool
	retained = func(page MappingPage) bool {
		for _, child := range page.Entries {
			if child.Kind != MappingEntryChild {
				continue
			}
			if child.Object.Key == pack && child.Object.Offset == children[2].Object.Offset {
				return true
			}
			if page.Level > 1 {
				next, err := reader.readMappingPage(child.Object)
				require.NoError(t, err)
				if retained(next) {
					return true
				}
			}
		}
		return false
	}
	require.True(t, retained(reader.root), "path copying must preserve the unchanged child's packed locator after root rebalancing")
	old, err := NewReader(store, descriptor, DefaultReadCacheBytes)
	require.NoError(t, err)
	actual := make([]byte, LogicalBlockSize)
	_, err = old.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{1}, LogicalBlockSize), actual, "old immutable generation remains readable")
}
