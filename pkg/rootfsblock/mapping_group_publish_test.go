package rootfsblock

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func TestMappingPublicationPolicyValidation(t *testing.T) {
	for _, version := range []int{0, 1, 2} {
		for _, policy := range []string{"", ContiguousMappingV1, "unknown"} {
			_, err := NormalizeBuildOptions(BuildOptions{FormatVersion: version, MappingGroupPolicy: policy})
			if policy == "" || version == 2 && policy == ContiguousMappingV1 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		}
	}
}

func mappingPublicationInventory(t *testing.T, store *buildTestStore, result BuildResult) map[string][]ObjectRange {
	t.Helper()
	r, err := NewReader(store, result.Descriptor, DefaultReadCacheBytes)
	require.NoError(t, err)
	ranges := make(map[string][]ObjectRange)
	reachable := make(map[string]bool)
	var visit func(ObjectRange)
	visit = func(object ObjectRange) {
		ranges[object.Key] = append(ranges[object.Key], object)
		reachable[object.Key] = true
		page, err := r.readMappingPage(object)
		require.NoError(t, err)
		for _, entry := range page.Entries {
			if entry.Kind == MappingEntryData {
				reachable[entry.Object.Key] = true
				continue
			}
			child, err := r.readMappingPage(entry.Object)
			require.NoError(t, err)
			require.Equal(t, page.Level, child.Level+1)
			require.Equal(t, entry.LogicalStart, child.StartBlock)
			require.EqualValues(t, entry.BlockCount, child.BlockCount)
			visit(entry.Object)
		}
	}
	visit(result.Descriptor.MappingRoot.Object)
	refs := make(map[string]ObjectReference)
	for _, ref := range result.References {
		require.NoError(t, ValidateObjectReference(ref))
		require.True(t, reachable[ref.Key], "published orphan: %s", ref.Key)
		data, ok := store.objects[ref.Key]
		require.True(t, ok)
		require.EqualValues(t, len(data), ref.Size)
		require.Equal(t, digest.FromBytes(data).String(), ref.Checksum)
		_, exists := refs[ref.Key]
		require.False(t, exists, "duplicate physical inventory entry")
		refs[ref.Key] = ref
	}
	require.Len(t, refs, len(reachable))
	for key, parts := range ranges {
		sort.Slice(parts, func(i, j int) bool { return parts[i].Offset < parts[j].Offset })
		var end int64
		for _, part := range parts {
			require.Equal(t, end, part.Offset, "unreferenced or overlapping packed mapping bytes")
			end += part.StoredLength()
		}
		require.Equal(t, refs[key].Size, end)
		if len(parts) > 1 {
			require.LessOrEqual(t, len(parts), mappingPublishPages)
			require.LessOrEqual(t, end, int64(mappingPublishStoredBytes))
		}
	}
	return ranges
}

func TestMappingPublicationMaterializedTreeAndInventory(t *testing.T) {
	for _, fanout := range []int{2, 3, 4, 7, 31, 1024} {
		t.Run(fmt.Sprint(fanout), func(t *testing.T) {
			count := fanout*5 + 1
			size := int64(count) * LogicalBlockSize
			options := BuildOptions{FormatVersion: CompressedFormatVersion, DataRangeBytes: LogicalBlockSize, PageEntries: fanout}
			plain, err := BuildMaterializedGeneration(t.Context(), &streamPatternReader{size: size}, size, newBuildTestStore(), options)
			require.NoError(t, err)
			options.MappingGroupPolicy = ContiguousMappingV1
			store := newBuildTestStore()
			result, err := BuildMaterializedGeneration(t.Context(), &streamPatternReader{size: size}, size, store, options)
			require.NoError(t, err)
			require.NotEqual(t, plain.Payload, result.Payload)
			require.Less(t, result.Objects, plain.Objects)
			ranges := mappingPublicationInventory(t, store, result)
			r, err := NewReader(store, result.Descriptor, DefaultReadCacheBytes)
			require.NoError(t, err)
			got, want := make([]byte, size), make([]byte, size)
			_, err = r.ReadAt(got, 0)
			require.NoError(t, err)
			_, err = (&streamPatternReader{size: size}).ReadAt(want, 0)
			require.NoError(t, err)
			require.Equal(t, want, got)
			retry, err := BuildMaterializedGeneration(t.Context(), &streamPatternReader{size: size}, size, store, options)
			require.NoError(t, err)
			require.Equal(t, result, retry)
			t.Logf("fanout=%d logical_bytes=%d mapping_objects=%d total_objects=%d baseline_objects=%d", fanout, size, len(ranges), result.Objects, plain.Objects)
		})
	}
}

func TestMappingPublicationStreamingBounds(t *testing.T) {
	for _, fanout := range []int{2, 7, 31, 1024} {
		t.Run(fmt.Sprint(fanout), func(t *testing.T) {
			options, err := NormalizeBuildOptions(BuildOptions{FormatVersion: 2, PageEntries: fanout, MappingGroupPolicy: ContiguousMappingV1})
			require.NoError(t, err)
			builder := generationBuilder{ctx: t.Context(), publisher: streamDiscardPublisher{}, options: options}
			defer builder.encoder.close()
			count := max(4097, fanout*5+1)
			tree := newMappingStream(&builder, uint64(count*16))
			object := ObjectRange{Key: "packs/bounded-model", Length: CompressedDataRangeBytes, Checksum: digest.FromBytes(bytes.Repeat([]byte{1}, CompressedDataRangeBytes)).String()}
			peakBytes, peakPages := 0, 0
			for i := range count {
				require.NoError(t, tree.add(MappingEntry{Kind: MappingEntryData, LogicalStart: uint64(i * 16), BlockCount: 16, Object: object}))
				var retainedBytes, retainedPages int
				for _, pending := range tree.pending {
					require.LessOrEqual(t, cap(pending), mappingPublishPages)
					require.Less(t, len(pending), min(mappingPublishPages, fanout))
					var stored, decoded int
					for _, page := range pending {
						stored += len(page.stored)
						decoded += len(page.payload)
					}
					require.LessOrEqual(t, stored, mappingPublishStoredBytes)
					require.LessOrEqual(t, decoded, mappingPublishDecodedBytes)
					retainedBytes += stored + decoded
					retainedPages += len(pending)
				}
				for _, pages := range tree.levels {
					require.LessOrEqual(t, cap(pages), fanout)
					for _, page := range pages {
						require.Nil(t, page.payload)
						require.Nil(t, page.stored)
					}
				}
				peakBytes = max(peakBytes, retainedBytes)
				peakPages = max(peakPages, retainedPages)
			}
			_, _, err = tree.finish()
			require.NoError(t, err)
			for _, pending := range tree.pending {
				require.Empty(t, pending)
			}
			t.Logf("fanout=%d generated_extents=%d levels=%d peak_pending_pages=%d peak_pending_bytes=%d", fanout, count, len(tree.levels), peakPages, peakBytes)
		})
	}
}

func TestMappingPublicationFailureAndCancellation(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelled), func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			input := &streamPatternReader{size: 256 * LogicalBlockSize}
			publisher := &streamInspectPublisher{store: newBuildTestStore(), onMap: func() error {
				if cancelled {
					cancel()
					return nil
				}
				return errors.New("publication failure")
			}}
			result, err := BuildMaterializedGeneration(ctx, input, input.size, publisher, BuildOptions{FormatVersion: 2, PageEntries: 2, DataRangeBytes: LogicalBlockSize, PackBytes: 4 * LogicalBlockSize, MappingGroupPolicy: ContiguousMappingV1})
			require.Error(t, err)
			require.Empty(t, result.Payload)
			require.Equal(t, 1, publisher.maps)
			require.Less(t, input.readBytes, input.size)
			if cancelled {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.ErrorContains(t, err, "publication failure")
			}
		})
	}
}

func TestMappingPublicationIncrementalRetainsPhysicalObjects(t *testing.T) {
	store := newBuildTestStore()
	size := int64(64 * LogicalBlockSize)
	base, err := BuildMaterializedGeneration(t.Context(), &streamPatternReader{size: size}, size, store, BuildOptions{FormatVersion: 2, PageEntries: 7, DataRangeBytes: LogicalBlockSize, MappingGroupPolicy: ContiguousMappingV1})
	require.NoError(t, err)
	groups := mappingPublicationInventory(t, store, base)
	require.Greater(t, len(groups), 1)
	changed := bytes.Repeat([]byte{0xf1}, LogicalBlockSize)
	next, err := BuildIncrementalGeneration(t.Context(), store, base.Descriptor, []BlockUpdate{{Block: 0, Data: changed}}, store, BuildOptions{PageEntries: 7})
	require.NoError(t, err)
	for _, tc := range []struct {
		d     Descriptor
		first []byte
	}{{base.Descriptor, bytes.Repeat([]byte{1}, LogicalBlockSize)}, {next.Descriptor, changed}} {
		r, err := NewReader(store, tc.d, DefaultReadCacheBytes)
		require.NoError(t, err)
		got := make([]byte, size)
		_, err = r.ReadAt(got, 0)
		require.NoError(t, err)
		want := make([]byte, size)
		_, err = (&streamPatternReader{size: size}).ReadAt(want, 0)
		require.NoError(t, err)
		copy(want, tc.first)
		require.Equal(t, want, got)
	}
	retained := false
	r, err := NewReader(store, next.Descriptor, DefaultReadCacheBytes)
	require.NoError(t, err)
	var walk func(MappingPage)
	walk = func(p MappingPage) {
		for _, e := range p.Entries {
			if e.Kind == MappingEntryChild {
				if len(groups[e.Object.Key]) > 1 {
					retained = true
				}
				child, err := r.readMappingPage(e.Object)
				require.NoError(t, err)
				walk(child)
			}
		}
	}
	walk(r.root)
	require.True(t, retained)
	for _, ref := range next.References {
		require.NoError(t, ValidateObjectReference(ref))
		require.True(t, strings.Contains(ref.Key, "/maps/") || strings.Contains(ref.Key, "/packs/"))
	}
}
