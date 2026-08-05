package rootfsreader

import (
	"bytes"
	"context"
	"testing"

	ctldrootfs "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfs"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfsstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/stretchr/testify/require"
)

func BenchmarkReadFileManifestReuse(b *testing.B) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore(b.Name())
	writer, err := rootfsstore.NewTeamWriter(store, "reader-benchmark-team")
	require.NoError(b, err)
	chunk, err := writer.Put(ctx, rootfshead.ChunkMediaType, []byte{'x'})
	require.NoError(b, err)

	const extentCount = 4096
	extents := make([]rootfshead.FileExtent, 0, extentCount)
	for index := range extentCount {
		extents = append(extents, rootfshead.FileExtent{
			Offset: uint64(4096 + index*2),
			Length: 1,
			Object: chunk,
		})
	}
	encodedManifest := rootfshead.FileManifest{
		Version: rootfshead.Version,
		Size:    uint64(4096 + extentCount*2),
		Extents: extents,
	}
	manifestPayload, err := rootfshead.EncodeFileManifest(encodedManifest)
	require.NoError(b, err)
	manifestObject, err := writer.Put(ctx, rootfshead.FileMediaType, manifestPayload)
	require.NoError(b, err)
	entry := rootfshead.Entry{
		Name: "large-manifest",
		Kind: rootfshead.EntryFile,
		Size: encodedManifest.Size,
		File: &manifestObject,
	}
	reader, err := New(ReaderConfig{
		Store:              store,
		Prefix:             writer.Prefix(),
		MetadataCacheBytes: 8 << 20,
	})
	require.NoError(b, err)
	manifest, err := reader.LoadFileManifest(ctx, entry)
	require.NoError(b, err)
	destination := make([]byte, 4096)

	b.Run("decode-every-read", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, err := reader.ReadFile(ctx, entry, destination, 0)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("reuse-decoded-manifest", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, err := reader.ReadFileManifest(ctx, manifest, destination, 0)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkDirectoryLookupReuse(b *testing.B) {
	fixture := newReaderFixture(b)
	ctx := context.Background()
	view, err := fixture.reader.OpenDirectory(ctx, fixture.root)
	require.NoError(b, err)
	_, err = view.Lookup(ctx, fixture.firstName)
	require.NoError(b, err)

	b.Run("decode-index-and-shard", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, err := fixture.reader.Lookup(ctx, fixture.root, fixture.firstName)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("reuse-directory-view", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, err := view.Lookup(ctx, fixture.firstName)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRandom4KiBReadCacheAmplification(b *testing.B) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore(b.Name())
	writer, err := rootfsstore.NewTeamWriter(store, "reader-cache-benchmark-team")
	require.NoError(b, err)
	chunkPayload := bytes.Repeat([]byte("x"), 1<<20)
	chunk, err := writer.Put(ctx, rootfshead.ChunkMediaType, chunkPayload)
	require.NoError(b, err)
	manifest := rootfshead.FileManifest{
		Version: rootfshead.Version,
		Size:    uint64(len(chunkPayload)),
		Blocks:  uint64(len(chunkPayload) / 512),
		Extents: []rootfshead.FileExtent{{Offset: 0, Length: uint64(len(chunkPayload)), Object: chunk}},
	}
	destination := make([]byte, 4096)

	withoutCache, err := New(ReaderConfig{Store: store, Prefix: writer.Prefix()})
	require.NoError(b, err)
	cache := ctldrootfs.NewObjectCache(ctldrootfs.ObjectCacheConfig{
		Dir: b.TempDir(), MaxBytes: 8 << 20,
	})
	withCache, err := New(ReaderConfig{Store: store, Prefix: writer.Prefix(), ObjectCache: cache})
	require.NoError(b, err)
	_, err = withCache.ReadFileManifest(ctx, manifest, destination, 512<<10)
	require.NoError(b, err)

	for name, reader := range map[string]*Reader{
		"object-store-cold": withoutCache,
		"disk-cas-warm":     withCache,
	} {
		b.Run(name, func(b *testing.B) {
			b.SetBytes(int64(len(destination)))
			b.ReportAllocs()
			for index := range b.N {
				offset := int64((index * len(destination)) % (len(chunkPayload) - len(destination)))
				if _, err := reader.ReadFileManifest(ctx, manifest, destination, offset); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
