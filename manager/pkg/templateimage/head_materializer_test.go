package templateimage

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"sort"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaterializeHeadLayerExportsCurrentOverlayAsOCITar(t *testing.T) {
	reader := &fakeObjectReader{objects: make(map[string][]byte)}
	addObject := func(mediaType string, payload []byte) rootfshead.Object {
		digestValue := digest.FromBytes(payload)
		object := rootfshead.Object{
			Key:       "objects/" + digestValue.Encoded(),
			Digest:    digestValue.String(),
			Size:      int64(len(payload)),
			MediaType: mediaType,
		}
		reader.objects[object.Key] = payload
		return object
	}
	chunk := addObject(rootfshead.ChunkMediaType, []byte("abc"))
	manifestPayload, err := rootfshead.EncodeFileManifest(rootfshead.FileManifest{
		Version: rootfshead.Version,
		Size:    6,
		Extents: []rootfshead.FileExtent{{Offset: 1, Length: 3, Object: chunk}},
	})
	require.NoError(t, err)
	fileObject := addObject(rootfshead.FileMediaType, manifestPayload)
	fileEntry := rootfshead.Entry{
		Inode: "inode-file",
		Kind:  rootfshead.EntryFile,
		Mode:  0o640,
		Nlink: 2,
		Size:  6,
		File:  &fileObject,
	}
	makeDirectory := func(entries []rootfshead.Entry) rootfshead.Object {
		byBucket := make(map[uint8][]rootfshead.Entry)
		for _, entry := range entries {
			byBucket[rootfshead.NameBucket(entry.Name)] = append(byBucket[rootfshead.NameBucket(entry.Name)], entry)
		}
		buckets := make([]int, 0, len(byBucket))
		for bucket := range byBucket {
			buckets = append(buckets, int(bucket))
		}
		sort.Ints(buckets)
		index := rootfshead.DirectoryIndex{Version: rootfshead.Version}
		for _, rawBucket := range buckets {
			bucket := uint8(rawBucket)
			entries := byBucket[bucket]
			sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
			payload, err := rootfshead.EncodeDirectoryShard(rootfshead.DirectoryShard{
				Version: rootfshead.Version,
				Bucket:  bucket,
				Entries: entries,
			})
			require.NoError(t, err)
			index.Shards = append(index.Shards, rootfshead.ShardRef{Bucket: bucket, Object: addObject(rootfshead.DirectoryShardMediaType, payload)})
		}
		payload, err := rootfshead.EncodeDirectoryIndex(index)
		require.NoError(t, err)
		return addObject(rootfshead.DirectoryIndexMediaType, payload)
	}

	nested := fileEntry
	nested.Name = "nested"
	nested.Nlink = 1
	childDirectory := makeDirectory([]rootfshead.Entry{nested})
	directory := rootfshead.Entry{
		Name:      "dir",
		Inode:     "inode-dir",
		Kind:      rootfshead.EntryDirectory,
		Mode:      0o750,
		Nlink:     2,
		XAttrs:    []rootfshead.XAttr{{Name: "trusted.overlay.opaque", Value: []byte("y")}},
		Directory: &childDirectory,
	}
	firstLink := fileEntry
	firstLink.Name = "a"
	secondLink := fileEntry
	secondLink.Name = "b"
	symlink := rootfshead.Entry{Name: "link", Inode: "inode-link", Kind: rootfshead.EntrySymlink, Mode: 0o777, Target: "a"}
	whiteout := rootfshead.Entry{Name: "gone", Inode: "inode-whiteout", Kind: rootfshead.EntryWhiteout}
	rootDirectory := makeDirectory([]rootfshead.Entry{firstLink, secondLink, directory, symlink, whiteout})
	head := rootfshead.Head{
		Version:         rootfshead.Version,
		HeadID:          "head-1",
		BaseImageDigest: digest.FromString("base").String(),
		BaseSnapshotKey: digest.FromString("snapshot").String(),
		Root: rootfshead.Entry{
			Inode:     "root",
			Kind:      rootfshead.EntryDirectory,
			Mode:      0o755,
			Nlink:     2,
			Directory: &rootDirectory,
		},
	}
	headPayload, err := rootfshead.EncodeHead(head)
	require.NoError(t, err)
	headObject := addObject(rootfshead.HeadMediaType, headPayload)

	layer, err := materializeHeadLayer(context.Background(), reader, rootfshead.HeadReference{
		Version:  rootfshead.Version,
		HeadID:   head.HeadID,
		Manifest: headObject,
	})
	require.NoError(t, err)
	defer layer.Close()

	file, err := layer.OpenAt(0)
	require.NoError(t, err)
	payload, err := io.ReadAll(file)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	assert.Equal(t, layer.Size, int64(len(payload)))
	assert.Equal(t, layer.Digest, digest.FromBytes(payload).String())
	assert.Equal(t, layer.Digest, layer.DiffID)

	headers := make(map[string]*tar.Header)
	contents := make(map[string][]byte)
	tarReader := tar.NewReader(bytes.NewReader(payload))
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		copyHeader := *header
		headers[header.Name] = &copyHeader
		content, err := io.ReadAll(tarReader)
		require.NoError(t, err)
		contents[header.Name] = content
	}
	assert.Equal(t, byte(tar.TypeDir), headers["."].Typeflag)
	assert.Equal(t, []byte{0, 'a', 'b', 'c', 0, 0}, contents["a"])
	assert.Equal(t, byte(tar.TypeLink), headers["b"].Typeflag)
	assert.Equal(t, "a", headers["b"].Linkname)
	assert.Equal(t, byte(tar.TypeSymlink), headers["link"].Typeflag)
	assert.Equal(t, "a", headers["link"].Linkname)
	assert.Contains(t, headers, ".wh.gone")
	assert.Contains(t, headers, "dir/.wh..wh..opq")
	assert.Equal(t, []byte{0, 'a', 'b', 'c', 0, 0}, contents["dir/nested"])
}

func TestMaterializeHeadLayerRejectsCorruptManifest(t *testing.T) {
	digestValue := digest.FromString("expected")
	reference := rootfshead.HeadReference{
		Version: rootfshead.Version,
		HeadID:  "head-corrupt",
		Manifest: rootfshead.Object{
			Key:       "objects/head",
			Digest:    digestValue.String(),
			Size:      int64(len("corrupt")),
			MediaType: rootfshead.HeadMediaType,
		},
	}
	reader := &fakeObjectReader{objects: map[string][]byte{"objects/head": []byte("corrupt")}}

	_, err := materializeHeadLayer(context.Background(), reader, reference)

	require.ErrorContains(t, err, "digest")
}
