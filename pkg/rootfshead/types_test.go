package rootfshead

import (
	"bytes"
	"testing"

	"github.com/opencontainers/go-digest"
)

func TestHeadAndAnnotationRoundTrip(t *testing.T) {
	directoryPayload, err := EncodeDirectoryIndex(DirectoryIndex{Version: Version})
	if err != nil {
		t.Fatal(err)
	}
	directory := testObject("objects/root", DirectoryIndexMediaType, directoryPayload)
	head := Head{
		Version:         Version,
		HeadID:          "head-2",
		BaseImageDigest: digest.FromString("base").String(),
		BaseSnapshotKey: digest.FromString("base-snapshot").String(),
		Root: Entry{
			Inode:     "root",
			Kind:      EntryDirectory,
			Mode:      0o755,
			Directory: &directory,
		},
	}
	payload, err := EncodeHead(head)
	if err != nil {
		t.Fatalf("EncodeHead() error = %v", err)
	}
	decoded, err := DecodeHead(bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("DecodeHead() error = %v", err)
	}
	if decoded.HeadID != head.HeadID || decoded.Root.Directory.Digest != directory.Digest {
		t.Fatalf("DecodeHead() = %+v", decoded)
	}

	reference := HeadReference{
		Version:  Version,
		HeadID:   head.HeadID,
		Manifest: testObject("heads/head-2", HeadMediaType, payload),
	}
	annotation, err := EncodeHeadAnnotation(reference)
	if err != nil {
		t.Fatalf("EncodeHeadAnnotation() error = %v", err)
	}
	decodedReference, err := DecodeHeadAnnotation(annotation)
	if err != nil {
		t.Fatalf("DecodeHeadAnnotation() error = %v", err)
	}
	if decodedReference.Manifest.Digest != reference.Manifest.Digest {
		t.Fatalf("DecodeHeadAnnotation() = %+v", decodedReference)
	}

	marker, err := EncodeMarker(reference)
	if err != nil {
		t.Fatalf("EncodeMarker() error = %v", err)
	}
	markerReference, err := DecodeMarker(bytes.NewReader(marker))
	if err != nil {
		t.Fatalf("DecodeMarker() error = %v", err)
	}
	if markerReference.HeadID != reference.HeadID {
		t.Fatalf("DecodeMarker().HeadID = %q", markerReference.HeadID)
	}
}

func TestDirectoryShardRequiresStableBucketAndOrdering(t *testing.T) {
	name := "alpha"
	filePayload, err := EncodeFileManifest(FileManifest{Version: Version})
	if err != nil {
		t.Fatal(err)
	}
	file := testObject("files/empty", FileMediaType, filePayload)
	valid := DirectoryShard{
		Version: Version,
		Bucket:  NameBucket(name),
		Entries: []Entry{{Name: name, Inode: "inode-alpha", Kind: EntryFile, Mode: 0o644, File: &file}},
	}
	if _, err := EncodeDirectoryShard(valid); err != nil {
		t.Fatalf("EncodeDirectoryShard() error = %v", err)
	}
	invalid := valid
	invalid.Bucket++
	if _, err := EncodeDirectoryShard(invalid); err == nil {
		t.Fatal("EncodeDirectoryShard() accepted an entry in the wrong bucket")
	}
}

func TestFileManifestAllowsSparseNonOverlappingExtents(t *testing.T) {
	chunk := testObject("chunks/a", ChunkMediaType, []byte("payload"))
	manifest := FileManifest{
		Version: Version,
		Size:    4096,
		Extents: []FileExtent{{Offset: 1024, Length: uint64(chunk.Size), Object: chunk}},
	}
	payload, err := EncodeFileManifest(manifest)
	if err != nil {
		t.Fatalf("EncodeFileManifest() error = %v", err)
	}
	decoded, err := DecodeFileManifest(bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("DecodeFileManifest() error = %v", err)
	}
	if decoded.Size != manifest.Size || len(decoded.Extents) != 1 {
		t.Fatalf("DecodeFileManifest() = %+v", decoded)
	}

	manifest.Extents = append(manifest.Extents, FileExtent{Offset: 1025, Length: 1, Object: chunk})
	if _, err := EncodeFileManifest(manifest); err == nil {
		t.Fatal("EncodeFileManifest() accepted overlapping extents")
	}
}

func testObject(key, mediaType string, payload []byte) Object {
	return Object{Key: key, Digest: digest.FromBytes(payload).String(), Size: int64(len(payload)), MediaType: mediaType}
}
