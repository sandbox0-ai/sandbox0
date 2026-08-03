package service

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"path"
	"sort"
	"testing"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollectRootFSObjectInventoryReadsMetadataButNotChunks(t *testing.T) {
	store := objectstore.NewMemoryStore(t.Name())
	candidate, objects, files := rootFSInventoryFixture(t, store, "sandbox-rootfs/cow-v2/teams/team/filesystems/fs", "layer-1", map[string][]byte{
		"keep.txt": []byte("keep"),
	})
	reader := &recordingRootFSObjectReader{Store: store}

	got, err := CollectRootFSObjectInventory(context.Background(), reader, candidate)

	require.NoError(t, err)
	assert.Equal(t, rootFSObjectKeys(objects), rootFSObjectKeys(got))
	assert.NotContains(t, reader.keys, files["keep.txt"].Key, "chunk payloads must remain lazy")
	assert.Contains(t, reader.keys, candidate.Head.Manifest.Key)
}

func TestRootFSInventoryCompactionReclaimsDeletedAndTransientS3Objects(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	database := NewPGSandboxStore(pool)
	objects := objectstore.NewMemoryStore(t.Name())
	prefix := "sandbox-rootfs/cow-v2/teams/team/filesystems/fs"

	parentCandidate, parentObjects, parentFiles := rootFSInventoryFixture(t, objects, prefix, "layer-parent", map[string][]byte{
		"deleted.bin": []byte("persisted then deleted"),
		"keep.bin":    []byte("still live"),
	})
	childCandidate, childObjects, childFiles := rootFSInventoryFixture(t, objects, prefix, "layer-child", map[string][]byte{
		"keep.bin": []byte("still live"),
	})
	transient := rootFSInventoryTestObject(t, objects, prefix, rootfshead.ChunkMediaType, []byte("superseded background capture"))

	require.NoError(t, database.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-inventory", "team-1")))
	parent := rootFSInventoryTestState("sandbox-inventory", "team-1", parentCandidate, "", 1, parentObjects)
	require.NoError(t, database.SaveRootFSState(ctx, parent))

	parentKeys := make(map[string]struct{}, len(parentObjects))
	for _, object := range parentObjects {
		parentKeys[object.Key] = struct{}{}
	}
	conservativeChildObjects := []rootfshead.Object{transient}
	for _, object := range childObjects {
		if _, inherited := parentKeys[object.Key]; !inherited {
			conservativeChildObjects = append(conservativeChildObjects, object)
		}
	}
	child := rootFSInventoryTestState("sandbox-inventory", "team-1", childCandidate, parent.LayerID, 2, conservativeChildObjects)
	require.NoError(t, database.SaveRootFSState(ctx, child))
	allWrittenObjects := append(append([]rootfshead.Object{}, parentObjects...), childObjects...)
	allWrittenObjects = append(allWrittenObjects, transient)
	beforeBytes := rootFSInventoryKnownPhysicalBytes(t, allWrittenObjects, objects)

	controller := NewRootFSMaintenanceController(database, objects, RootFSMaintenanceControllerConfig{
		BatchSize:        100,
		MaxBatchesPerRun: 10,
	}, nil, nil)
	controller.SetObjectReader(objects)
	require.NoError(t, controller.RunOnce(ctx))

	var complete bool
	var parentLayerID *string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT object_inventory_complete, parent_layer_id
		FROM manager.rootfs_layers
		WHERE layer_id = $1
	`, child.LayerID).Scan(&complete, &parentLayerID))
	assert.True(t, complete)
	assert.Nil(t, parentLayerID, "collecting a compacted ancestor must detach the historical FK")

	var parentExists bool
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM manager.rootfs_layers WHERE layer_id = $1)
	`, parent.LayerID).Scan(&parentExists))
	assert.False(t, parentExists)

	var relationCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM manager.rootfs_layer_objects WHERE layer_id = $1
	`, child.LayerID).Scan(&relationCount))
	assert.Equal(t, len(childObjects), relationCount)

	_, err := objects.Head(parentFiles["deleted.bin"].Key)
	assert.True(t, objectstore.IsNotFound(err), "deleted file chunk must be physically removed")
	_, err = objects.Head(parentCandidate.ImageEnvelope.Key)
	assert.True(t, objectstore.IsNotFound(err), "unreferenced parent OCI envelope must be physically removed")
	_, err = objects.Head(transient.Key)
	assert.True(t, objectstore.IsNotFound(err), "transient write-amplification object must be physically removed")
	_, err = objects.Head(childFiles["keep.bin"].Key)
	require.NoError(t, err, "an inherited live chunk must remain")
	_, err = objects.Head(childCandidate.ImageEnvelope.Key)
	require.NoError(t, err, "the live head OCI envelope must remain")
	assert.Equal(t, int64(0), rootFSTestCountRows(t, pool, "rootfs_object_deletions"))

	afterBytes := rootFSInventoryKnownPhysicalBytes(t, childObjects, objects)
	assert.Equal(t, rootFSInventoryObjectBytes(childObjects), afterBytes)
	assert.Less(t, afterBytes, beforeBytes)
}

type recordingRootFSObjectReader struct {
	objectstore.Store
	keys []string
}

func (r *recordingRootFSObjectReader) Get(key string, offset, limit int64) (io.ReadCloser, error) {
	r.keys = append(r.keys, key)
	return r.Store.Get(key, offset, limit)
}

func rootFSInventoryFixture(t *testing.T, store objectstore.Store, prefix, headID string, files map[string][]byte) (RootFSInventoryCandidate, []rootfshead.Object, map[string]rootfshead.Object) {
	t.Helper()
	objects := make(map[string]rootfshead.Object)
	fileObjects := make(map[string]rootfshead.Object, len(files))
	entriesByBucket := make(map[uint8][]rootfshead.Entry)
	for name, payload := range files {
		chunk := rootFSInventoryTestObject(t, store, prefix, rootfshead.ChunkMediaType, payload)
		manifestPayload, err := rootfshead.EncodeFileManifest(rootfshead.FileManifest{
			Version: rootfshead.Version,
			Size:    uint64(len(payload)),
			Extents: []rootfshead.FileExtent{{Offset: 0, Length: uint64(len(payload)), Object: chunk}},
		})
		require.NoError(t, err)
		manifest := rootFSInventoryTestObject(t, store, prefix, rootfshead.FileMediaType, manifestPayload)
		entry := rootfshead.Entry{
			Name:  name,
			Inode: "inode:" + name,
			Kind:  rootfshead.EntryFile,
			Mode:  0o644,
			Nlink: 1,
			Size:  uint64(len(payload)),
			File:  &manifest,
		}
		bucket := rootfshead.NameBucket(name)
		entriesByBucket[bucket] = append(entriesByBucket[bucket], entry)
		objects[chunk.Key] = chunk
		objects[manifest.Key] = manifest
		fileObjects[name] = chunk
	}

	buckets := make([]int, 0, len(entriesByBucket))
	for bucket := range entriesByBucket {
		buckets = append(buckets, int(bucket))
	}
	sort.Ints(buckets)
	index := rootfshead.DirectoryIndex{Version: rootfshead.Version}
	for _, rawBucket := range buckets {
		bucket := uint8(rawBucket)
		entries := entriesByBucket[bucket]
		sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
		payload, err := rootfshead.EncodeDirectoryShard(rootfshead.DirectoryShard{
			Version: rootfshead.Version,
			Bucket:  bucket,
			Entries: entries,
		})
		require.NoError(t, err)
		shard := rootFSInventoryTestObject(t, store, prefix, rootfshead.DirectoryShardMediaType, payload)
		objects[shard.Key] = shard
		index.Shards = append(index.Shards, rootfshead.ShardRef{Bucket: bucket, Object: shard})
	}
	indexPayload, err := rootfshead.EncodeDirectoryIndex(index)
	require.NoError(t, err)
	indexObject := rootFSInventoryTestObject(t, store, prefix, rootfshead.DirectoryIndexMediaType, indexPayload)
	objects[indexObject.Key] = indexObject

	headPayload, err := rootfshead.EncodeHead(rootfshead.Head{
		Version:         rootfshead.Version,
		HeadID:          headID,
		BaseImageDigest: rootFSTestBaseDigest,
		BaseSnapshotKey: rootFSTestBaseSnapshot,
		Root: rootfshead.Entry{
			Inode:     "root",
			Kind:      rootfshead.EntryDirectory,
			Mode:      0o755,
			Nlink:     2,
			Directory: &indexObject,
		},
	})
	require.NoError(t, err)
	headObject := rootFSInventoryTestObject(t, store, prefix, rootfshead.HeadMediaType, headPayload)
	objects[headObject.Key] = headObject
	candidate := RootFSInventoryCandidate{
		LayerID: headID,
		TeamID:  "team-1",
		Head: rootfshead.HeadReference{
			Version:  rootfshead.Version,
			HeadID:   headID,
			Manifest: headObject,
		},
	}
	marker, markerPayload, err := rootfshead.MarkerObject(candidate.Head)
	require.NoError(t, err)
	require.NoError(t, store.Put(marker.Key, bytes.NewReader(markerPayload)))
	objects[marker.Key] = marker
	baseConfig, err := json.Marshal(ocispec.Image{
		Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"},
		RootFS:   ocispec.RootFS{Type: "layers"},
	})
	require.NoError(t, err)
	_, envelope, err := rootfshead.ComposeImage(candidate.Head, baseConfig)
	require.NoError(t, err)
	envelopeObject, envelopePayload, err := rootfshead.ImageEnvelopeObject(envelope)
	require.NoError(t, err)
	require.NoError(t, store.Put(envelopeObject.Key, bytes.NewReader(envelopePayload)))
	objects[envelopeObject.Key] = envelopeObject
	candidate.ImageEnvelope = envelopeObject

	result := make([]rootfshead.Object, 0, len(objects))
	for _, object := range objects {
		result = append(result, object)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return candidate, result, fileObjects
}

func rootFSInventoryTestObject(t *testing.T, store objectstore.Store, prefix, mediaType string, payload []byte) rootfshead.Object {
	t.Helper()
	digestValue := digest.FromBytes(payload)
	kind := "metadata"
	switch mediaType {
	case rootfshead.ChunkMediaType:
		kind = "chunks"
	case rootfshead.FileMediaType:
		kind = "files"
	case rootfshead.DirectoryShardMediaType:
		kind = "directory-shards"
	case rootfshead.DirectoryIndexMediaType:
		kind = "directories"
	case rootfshead.HeadMediaType:
		kind = "heads"
	}
	object := rootfshead.Object{
		Key:       path.Join(prefix, kind, digestValue.Algorithm().String(), digestValue.Encoded()),
		Digest:    digestValue.String(),
		Size:      int64(len(payload)),
		MediaType: mediaType,
	}
	require.NoError(t, store.Put(object.Key, bytes.NewReader(payload)))
	return object
}

func rootFSInventoryTestState(sandboxID, teamID string, candidate RootFSInventoryCandidate, parentLayerID string, generation int64, objects []rootfshead.Object) *SandboxRootFSState {
	return &SandboxRootFSState{
		LayerID:              candidate.LayerID,
		ParentLayerID:        parentLayerID,
		SandboxID:            sandboxID,
		TeamID:               teamID,
		RuntimeGeneration:    generation,
		Runtime:              "runc",
		RuntimeHandler:       "io.containerd.runc.v2",
		BaseImageRef:         "docker.io/library/busybox:1.36",
		BaseImageDigest:      rootFSTestBaseDigest,
		PlatformOS:           "linux",
		PlatformArchitecture: "amd64",
		Snapshotter:          rootfshead.SnapshotterName,
		SnapshotParent:       rootFSTestBaseSnapshot,
		HeadObjectDigest:     candidate.Head.Manifest.Digest,
		HeadObjectMediaType:  candidate.Head.Manifest.MediaType,
		HeadObjectSize:       candidate.Head.Manifest.Size,
		HeadObjectKey:        candidate.Head.Manifest.Key,
		Objects:              append([]rootfshead.Object(nil), objects...),
	}
}

func rootFSObjectKeys(objects []rootfshead.Object) []string {
	keys := make([]string, 0, len(objects))
	for _, object := range objects {
		keys = append(keys, object.Key)
	}
	sort.Strings(keys)
	return keys
}

func rootFSInventoryObjectBytes(objects []rootfshead.Object) int64 {
	seen := make(map[string]struct{}, len(objects))
	var total int64
	for _, object := range objects {
		if _, ok := seen[object.Key]; ok {
			continue
		}
		seen[object.Key] = struct{}{}
		total += object.Size
	}
	return total
}

func rootFSInventoryKnownPhysicalBytes(t *testing.T, objects []rootfshead.Object, store objectstore.Store) int64 {
	t.Helper()
	seen := make(map[string]struct{}, len(objects))
	var total int64
	for _, object := range objects {
		if _, ok := seen[object.Key]; ok {
			continue
		}
		seen[object.Key] = struct{}{}
		info, err := store.Head(object.Key)
		require.NoError(t, err)
		total += info.Size
	}
	return total
}
