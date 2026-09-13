package rootfsblock

import (
	"context"
	"encoding/binary"
	"testing"
	"testing/synctest"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func TestMappingGroupInternalGeometry(t *testing.T) {
	store, d, children, _ := groupFixture(t, true, "geometry")
	r, err := NewReader(store, d, DefaultReadCacheBytes)
	require.NoError(t, err)
	plan, ok := r.planMappingGroup(r.root, children[0].Object)
	require.True(t, ok)
	require.Len(t, plan.objects, 4)
	for _, child := range children {
		other, ok := r.planMappingGroup(r.root, child.Object)
		require.True(t, ok)
		require.Equal(t, plan.identity, other.identity)
	}
	for _, kind := range []string{"gap", "overlap", "singleton", "stored-bound", "decoded-bound", "page-bound", "missing-target", "legacy", "cache-disabled", "cache-small"} {
		t.Run(kind, func(t *testing.T) {
			p := r.root
			p.Entries = append([]MappingEntry(nil), children...)
			copyReader := *r
			switch kind {
			case "gap":
				p.Entries[2].Object.Offset++
			case "overlap":
				p.Entries[2].Object.Offset--
			case "singleton":
				p.Entries = p.Entries[:1]
			case "stored-bound":
				for i := range p.Entries {
					p.Entries[i].Object.Encoding = ""
					p.Entries[i].Object.EncodedLength = 0
					p.Entries[i].Object.Length = 300000
					p.Entries[i].Object.Offset = int64(i) * 300000
				}
			case "decoded-bound":
				for i := range p.Entries {
					p.Entries[i].Object.Length = 2 << 20
				}
			case "page-bound":
				for i := range 33 {
					v := children[0]
					v.Object.Offset = int64(i) * v.Object.StoredLength()
					p.Entries = append(p.Entries, v)
				}
			case "missing-target":
				p.Entries = p.Entries[1:]
			case "legacy":
				p.Version = MappingPageVersion
			case "cache-disabled":
				copyReader.cache, err = NewReadCache(0)
			case "cache-small":
				copyReader.cache, err = NewReadCache(1 << 20)
			}
			target := p.Entries[0].Object
			if kind == "missing-target" {
				target = children[0].Object
			}
			_, ok := copyReader.planMappingGroup(p, target)
			require.False(t, ok)
		})
	}
}

func TestMappingGroupInternalInvalidNeighborAndTrim(t *testing.T) {
	for _, kind := range []string{"decode", "entry-count", "parent"} {
		t.Run(kind, func(t *testing.T) {
			store, d, children, expected, pack := mappingPackFixture(t, false)
			if kind == "parent" {
				children[2].Object = children[0].Object
			} else {
				part := store.objects[pack][children[2].Object.Offset:]
				if kind == "decode" {
					part[0] ^= 1
				} else {
					binary.BigEndian.PutUint32(part[12:16], MaxMappingPageEntries)
				}
				children[2].Object.Checksum = digest.FromBytes(part).String()
			}
			d = publishMappingPackRoot(t, store, children, uint64(d.LogicalSizeBytes/LogicalBlockSize))
			r, err := NewReader(store, d, DefaultReadCacheBytes)
			require.NoError(t, err)
			payload := make([]byte, LogicalBlockSize)
			_, err = r.ReadAt(payload, 0)
			require.NoError(t, err)
			require.Equal(t, expected[0], payload)
			_, err = r.ReadAt(payload, int64(children[2].LogicalStart)*LogicalBlockSize)
			require.Error(t, err)
		})
	}
	store, d, children, pack := groupFixture(t, true, "trim")
	source := &groupProbeSource{RangeSource: store, pack: pack}
	r, err := NewReader(source, d, DefaultReadCacheBytes)
	require.NoError(t, err)
	for _, i := range []int{0, 3} {
		_, err := r.readMappingPage(children[i].Object)
		require.NoError(t, err)
	}
	source.reset()
	require.NoError(t, groupRead(r, r.root, children[1].LogicalStart))
	require.Equal(t, []adaptiveGet{{children[1].Object.Offset, children[1].Object.StoredLength() + children[2].Object.StoredLength()}}, source.requests())
}

func TestMappingGroupInternalAdmissionAndIndependentData(t *testing.T) {
	store, d, children, pack := groupFixture(t, true, "admission")
	plain, err := NewReader(store, d, DefaultReadCacheBytes)
	require.NoError(t, err)
	leaf, err := plain.readMappingPage(children[0].Object)
	require.NoError(t, err)
	synctest.Test(t, func(t *testing.T) {
		source := &groupProbeSource{RangeSource: store, pack: pack, finish: make(chan struct{})}
		cache, err := NewReadCache(DefaultReadCacheBytes)
		require.NoError(t, err)
		r, err := NewReaderWithCache(source, d, cache)
		require.NoError(t, err)
		groupResult := make(chan error, 1)
		go func() { groupResult <- groupRead(r, r.root, 0) }()
		synctest.Wait()
		requireReadAdmissionUsage(t, &cache.sourceSlots, 1, 0, 0)
		// A direct, independent exact data read can finish while group I/O waits.
		dataResult := make(chan error, 1)
		go func() { _, err := r.readRange(leaf.Entries[0].Object); dataResult <- err }()
		synctest.Wait()
		require.Len(t, dataResult, 1)
		require.NoError(t, <-dataResult)
		close(source.finish)
		require.NoError(t, <-groupResult)
		requireReadAdmissionUsage(t, &cache.sourceSlots, 0, 0, 0)
		var releases []func()
		for range 8 {
			release, err := r.acquireSourceSlot()
			require.NoError(t, err)
			releases = append(releases, release)
		}
		defer func() {
			for _, release := range releases {
				release()
			}
		}()
		hot := make(chan error, 1)
		go func() { hot <- groupRead(r, r.root, children[0].LogicalStart) }()
		synctest.Wait()
		require.Len(t, hot, 1)
		require.NoError(t, <-hot)
	})
}

func TestMappingGroupInternalQueuedCancellation(t *testing.T) {
	store, d, _, pack := groupFixture(t, true, "queued")
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		source := &groupProbeSource{RangeSource: store, pack: pack}
		cache, err := NewReadCache(DefaultReadCacheBytes)
		require.NoError(t, err)
		r, err := NewReaderWithCacheContext(ctx, source, d, cache)
		require.NoError(t, err)
		var releases []func()
		for range 8 {
			release, err := r.acquireSourceSlot()
			require.NoError(t, err)
			releases = append(releases, release)
		}
		defer func() {
			for _, release := range releases {
				release()
			}
		}()
		result := make(chan error, 1)
		go func() { result <- groupRead(r, r.root, 0) }()
		synctest.Wait()
		require.Empty(t, source.requests())
		requireReadAdmissionUsage(t, &cache.sourceSlots, 8, 1, 1)
		cancel()
		require.ErrorIs(t, <-result, context.Canceled)
		requireReadAdmissionUsage(t, &cache.sourceSlots, 8, 0, 0)
	})
}
