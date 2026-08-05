package rootfsmaintenance

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
)

func TestInventoryRootFSHeadReadsMetadataOnly(t *testing.T) {
	job, payloads, chunk := rootFSInventoryFixture(t)
	reader := &countingRootFSObjectReader{payloads: payloads, reads: make(map[string]int)}

	objects, err := inventoryRootFSHead(context.Background(), reader, job)
	if err != nil {
		t.Fatalf("inventoryRootFSHead() error = %v", err)
	}
	if reader.reads[chunk.Key] != 0 {
		t.Fatalf("chunk %s was downloaded during metadata inventory", chunk.Key)
	}
	want := map[string]bool{
		job.Reference.Manifest.Key: true,
		job.Image.Marker.Key:       true,
		job.Image.Envelope.Key:     true,
		chunk.Key:                  true,
	}
	for _, object := range objects {
		delete(want, object.Key)
	}
	if len(want) != 0 {
		t.Fatalf("inventory is missing objects: %v", want)
	}
}

func TestInventoryRootFSHeadRejectsCorruptMetadata(t *testing.T) {
	job, payloads, _ := rootFSInventoryFixture(t)
	corrupt := append([]byte(nil), payloads[job.Reference.Manifest.Key]...)
	corrupt[len(corrupt)-1] ^= 0xff
	payloads[job.Reference.Manifest.Key] = corrupt

	_, err := inventoryRootFSHead(context.Background(), &countingRootFSObjectReader{payloads: payloads}, job)
	if err == nil {
		t.Fatal("inventoryRootFSHead() error = nil, want metadata integrity failure")
	}
}

type countingRootFSObjectReader struct {
	payloads map[string][]byte
	reads    map[string]int
}

func (r *countingRootFSObjectReader) Get(key string, offset, length int64) (io.ReadCloser, error) {
	if r.reads != nil {
		r.reads[key]++
	}
	payload, ok := r.payloads[key]
	if !ok {
		return nil, io.EOF
	}
	if offset < 0 || offset > int64(len(payload)) {
		return nil, io.EOF
	}
	end := int64(len(payload))
	if length >= 0 && offset+length < end {
		end = offset + length
	}
	return io.NopCloser(bytes.NewReader(payload[offset:end])), nil
}

func rootFSInventoryFixture(t *testing.T) (sandboxstore.RootFSInventoryJob, map[string][]byte, rootfshead.Object) {
	t.Helper()
	prefix, err := rootfshead.TeamObjectPrefix("team-1")
	if err != nil {
		t.Fatal(err)
	}
	payloads := make(map[string][]byte)
	put := func(mediaType string, payload []byte) rootfshead.Object {
		t.Helper()
		digestValue := digest.FromBytes(payload)
		key, keyErr := rootfshead.ObjectKey(prefix, mediaType, digestValue.String())
		if keyErr != nil {
			t.Fatal(keyErr)
		}
		object := rootfshead.Object{Key: key, Digest: digestValue.String(), Size: int64(len(payload)), MediaType: mediaType}
		payloads[key] = append([]byte(nil), payload...)
		return object
	}

	chunk := put(rootfshead.ChunkMediaType, []byte("hello"))
	filePayload, err := rootfshead.EncodeFileManifest(rootfshead.FileManifest{
		Version: rootfshead.Version,
		Size:    uint64(chunk.Size),
		Extents: []rootfshead.FileExtent{{Offset: 0, Length: uint64(chunk.Size), Object: chunk}},
	})
	if err != nil {
		t.Fatal(err)
	}
	fileObject := put(rootfshead.FileMediaType, filePayload)
	shardPayload, err := rootfshead.EncodeDirectoryShard(rootfshead.DirectoryShard{
		Version: rootfshead.Version,
		Bucket:  rootfshead.NameBucket("data.txt"),
		Entries: []rootfshead.Entry{{
			Name: "data.txt", Inode: "inode-file", Kind: rootfshead.EntryFile,
			Mode: 0o100644, Nlink: 1, Size: uint64(chunk.Size), File: &fileObject,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	shardObject := put(rootfshead.DirectoryShardMediaType, shardPayload)
	indexPayload, err := rootfshead.EncodeDirectoryIndex(rootfshead.DirectoryIndex{
		Version: rootfshead.Version,
		Shards: []rootfshead.ShardRef{{
			Bucket: rootfshead.NameBucket("data.txt"), Object: shardObject,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	indexObject := put(rootfshead.DirectoryIndexMediaType, indexPayload)
	headPayload, err := rootfshead.EncodeHead(rootfshead.Head{
		Version: rootfshead.Version,
		HeadID:  "head-1",
		Base: rootfshead.BaseIdentity{
			ImageReference: "docker.io/library/busybox@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			ManifestDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			ChainID:        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			OS:             "linux",
			Architecture:   "amd64",
		},
		Root: rootfshead.Entry{Inode: "inode-root", Kind: rootfshead.EntryDirectory, Mode: 0o040755, Nlink: 2, Directory: &indexObject},
	})
	if err != nil {
		t.Fatal(err)
	}
	headObject := put(rootfshead.HeadMediaType, headPayload)
	marker := put(rootfshead.MarkerMediaType, []byte("marker"))
	envelope := put(rootfshead.ImageEnvelopeMediaType, []byte("envelope"))
	imageDigest := digest.FromString("image manifest").String()
	return sandboxstore.RootFSInventoryJob{
		HeadID: "head-1",
		TeamID: "team-1",
		Reference: rootfshead.HeadReference{
			Version: rootfshead.Version, HeadID: "head-1", Manifest: headObject,
		},
		Image: rootfshead.ImageReference{
			Name: rootfshead.LocalImageReference(imageDigest), ManifestDigest: imageDigest,
			Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"},
			Marker:   marker, Envelope: envelope,
		},
	}, payloads, chunk
}
