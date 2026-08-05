package rootfshead

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHeadEncodingIsDeterministicAndRoundTrips(t *testing.T) {
	prefix, err := TeamObjectPrefix("team-1")
	require.NoError(t, err)
	directory := testObject(t, prefix, DirectoryIndexMediaType, []byte("directory"))
	head := Head{
		Version: Version,
		HeadID:  "head-1",
		Base: BaseIdentity{
			ImageReference: "registry.example/base@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			ManifestDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			ChainID:        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			OS:             "linux",
			Architecture:   "amd64",
		},
		Root: Entry{
			Inode:     "root",
			Kind:      EntryDirectory,
			Mode:      0o040755,
			Nlink:     2,
			ModTime:   NewTimestamp(time.Unix(123, 456)),
			Directory: &directory,
		},
	}

	first, err := EncodeHead(head)
	require.NoError(t, err)
	second, err := EncodeHead(head)
	require.NoError(t, err)
	assert.Equal(t, first, second)

	decoded, err := DecodeHead(bytes.NewReader(first))
	require.NoError(t, err)
	assert.Equal(t, head, decoded)
}

func TestHeadEncodingPoolRemainsDeterministicConcurrently(t *testing.T) {
	prefix, err := TeamObjectPrefix("team-concurrent")
	require.NoError(t, err)
	directory := testObject(t, prefix, DirectoryIndexMediaType, []byte("directory"))
	head := Head{
		Version: Version,
		HeadID:  "head-concurrent",
		Base: BaseIdentity{
			ImageReference: "registry.example/base@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			ManifestDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			ChainID:        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			OS:             "linux",
			Architecture:   "amd64",
		},
		Root: Entry{Inode: "root", Kind: EntryDirectory, Mode: 0o040755, Nlink: 2, Directory: &directory},
	}
	want, err := EncodeHead(head)
	require.NoError(t, err)

	const workers = 32
	errors := make(chan error, workers)
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			for range 50 {
				got, encodeErr := EncodeHead(head)
				if encodeErr != nil {
					errors <- encodeErr
					return
				}
				if !bytes.Equal(got, want) {
					errors <- fmt.Errorf("concurrent rootfs Head encoding is not deterministic")
					return
				}
			}
		}()
	}
	group.Wait()
	close(errors)
	for encodeErr := range errors {
		require.NoError(t, encodeErr)
	}
}

func TestFileManifestPreservesSparseExtentsAndBlocks(t *testing.T) {
	prefix, err := TeamObjectPrefix("team-1")
	require.NoError(t, err)
	chunk := testObject(t, prefix, ChunkMediaType, []byte("payload"))
	manifest := FileManifest{
		Version: Version,
		Size:    1 << 30,
		Blocks:  8,
		Extents: []FileExtent{{Offset: 1 << 29, Length: uint64(chunk.Size), Object: chunk}},
	}
	payload, err := EncodeFileManifest(manifest)
	require.NoError(t, err)
	decoded, err := DecodeFileManifest(bytes.NewReader(payload))
	require.NoError(t, err)
	assert.Equal(t, manifest, decoded)
}

func TestDirectoryShardRejectsWrongBucketAndOrder(t *testing.T) {
	prefix, err := TeamObjectPrefix("team-1")
	require.NoError(t, err)
	file := testObject(t, prefix, FileMediaType, []byte("file"))
	first := Entry{Name: "a", Inode: "1", Kind: EntryFile, Mode: 0o100644, File: &file}
	second := Entry{Name: "b", Inode: "2", Kind: EntryFile, Mode: 0o100644, File: &file}

	wrongBucket := DirectoryShard{Version: Version, Bucket: NameBucket(first.Name) + 1, Entries: []Entry{first}}
	assert.Error(t, wrongBucket.Validate())

	unsorted := DirectoryShard{Version: Version, Bucket: NameBucket(first.Name), Entries: []Entry{second, first}}
	assert.Error(t, unsorted.Validate())
}

func TestEntryRejectsDuplicateXAttrs(t *testing.T) {
	prefix, err := TeamObjectPrefix("team-1")
	require.NoError(t, err)
	file := testObject(t, prefix, FileMediaType, []byte("file"))
	entry := Entry{
		Name:  "file",
		Inode: "inode",
		Kind:  EntryFile,
		Mode:  0o100644,
		File:  &file,
		XAttrs: []XAttr{
			{Name: "user.a", Value: []byte("1")},
			{Name: "user.a", Value: []byte("2")},
		},
	}
	assert.Error(t, entry.Validate(false))
}

func TestEntryOpaqueIsDirectoryOnlyAndRejectsRawOverlayXAttrs(t *testing.T) {
	prefix, err := TeamObjectPrefix("team-1")
	require.NoError(t, err)
	file := testObject(t, prefix, FileMediaType, []byte("file"))
	assert.Error(t, (Entry{
		Name: "file", Inode: "inode", Kind: EntryFile, Mode: 0o100644, File: &file, Opaque: true,
	}).Validate(false))
	directory := testObject(t, prefix, DirectoryIndexMediaType, []byte("directory"))
	assert.Error(t, (Entry{
		Name: "directory", Inode: "inode", Kind: EntryDirectory, Mode: 0o040755, Directory: &directory,
		XAttrs: []XAttr{{Name: "trusted.overlay.opaque", Value: []byte("y")}},
	}).Validate(false))
	assert.NoError(t, (Entry{
		Name: "directory", Inode: "inode", Kind: EntryDirectory, Mode: 0o040755, Directory: &directory, Opaque: true,
	}).Validate(false))
}

func TestHeadAnnotationRoundTrip(t *testing.T) {
	prefix, err := TeamObjectPrefix("team-1")
	require.NoError(t, err)
	reference := HeadReference{Version: Version, HeadID: "head-1", Manifest: testObject(t, prefix, HeadMediaType, []byte("head"))}
	encoded, err := EncodeHeadAnnotation(reference)
	require.NoError(t, err)
	decoded, err := DecodeHeadAnnotation(encoded)
	require.NoError(t, err)
	assert.Equal(t, reference, decoded)
}

func TestHeadAnnotationRejectsOversizedEncodedValue(t *testing.T) {
	_, err := DecodeHeadAnnotation(strings.Repeat("A", maxAnnotationBytes+1))
	assert.ErrorContains(t, err, "exceeds")
}

func TestObjectRejectsOversizedMetadata(t *testing.T) {
	prefix, err := TeamObjectPrefix("team-1")
	require.NoError(t, err)
	digestValue := digest.FromString("oversized metadata")
	key, err := ObjectKey(prefix, HeadMediaType, digestValue.String())
	require.NoError(t, err)
	object := Object{
		Key: key, Digest: digestValue.String(), Size: MaxMetadataObjectBytes + 1, MediaType: HeadMediaType,
	}
	assert.ErrorContains(t, object.Validate(HeadMediaType), "exceeds")
}

func TestMetadataLimitWriterRejectsInputBeyondDecodedLimit(t *testing.T) {
	var destination bytes.Buffer
	writer := &metadataLimitWriter{writer: &destination, remaining: 4}
	written, err := writer.Write([]byte("12345"))
	assert.Zero(t, written)
	assert.ErrorIs(t, err, ErrMetadataObjectTooLarge)
	assert.Empty(t, destination.Bytes())
	written, err = writer.Write([]byte("1234"))
	require.NoError(t, err)
	assert.Equal(t, 4, written)
}

func testObject(t *testing.T, prefix, mediaType string, payload []byte) Object {
	t.Helper()
	digestValue := digest.FromBytes(payload)
	key, err := ObjectKey(prefix, mediaType, digestValue.String())
	require.NoError(t, err)
	object := Object{Key: key, Digest: digestValue.String(), Size: int64(len(payload)), MediaType: mediaType}
	require.NoError(t, object.Validate(mediaType))
	require.NoError(t, ValidateObjectScope(prefix, object))
	return object
}
