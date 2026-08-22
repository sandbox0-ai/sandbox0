//go:build linux

// Copyright 2026 Sandbox0 Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rootfsimporter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"

	"github.com/sandbox0-ai/sandbox0/pkg/ocirootfs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsartifact"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

func TestBlockBuilderPublishesAttestedArtifactAndCleansLocalStaging(t *testing.T) {
	fixture := newOCIBlockBuildFixture(t)
	first, err := fixture.builder().Build(t.Context(), fixture.request)
	require.NoError(t, err)
	require.Equal(t, fixture.sourceDigest, first.SourceOCIDigest)
	require.Equal(t, fixture.procdDigest, first.ProcdDigest)
	require.Equal(t, digest.FromBytes(first.DescriptorBytes), first.DescriptorDigest)
	require.Equal(t, first.Descriptor.MappingRoot.RootDigest, first.BaseBlockRoot.String())
	require.NotEmpty(t, first.References)
	require.Equal(t, len(fixture.publisher.objects), len(first.References))
	for _, reference := range first.References {
		payload, found := fixture.publisher.objects[reference.Key]
		require.True(t, found)
		require.Equal(t, int64(len(payload)), reference.Size)
		require.Equal(t, digest.FromBytes(payload).String(), reference.Checksum)
	}
	require.NoDirExists(t, fixture.unpacker.lastRoot)
	require.NoFileExists(t, fixture.unpacker.lastRoot+".xfs")

	second, err := fixture.builder().Build(t.Context(), fixture.request)
	require.NoError(t, err)
	require.Equal(t, first, second, "an exact retry must produce the same immutable artifact")
	require.NoDirExists(t, fixture.unpacker.lastRoot)
	require.NoFileExists(t, fixture.unpacker.lastRoot+".xfs")
}

func TestBlockBuilderJournalsEveryObjectBeforeReturningArtifact(t *testing.T) {
	fixture := newOCIBlockBuildFixture(t)
	journal := newRecordingPublicationJournal()
	objects := &recordingImmutablePublisher{objects: make(map[string][]byte), events: &journal.events}
	result, err := (BlockBuilder{
		Unpacker: fixture.unpacker, Filesystem: fixture.filesystem,
		Publisher: JournaledPublisher{
			OperationID: "rootfs-import-block-build", Journal: journal, Publisher: objects,
		},
	}).Build(t.Context(), fixture.request)
	require.NoError(t, err)
	require.Len(t, journal.prepared, len(result.References))
	for _, reference := range result.References {
		require.Equal(t, reference, journal.prepared[reference.Key])
		require.Equal(t, "published", journal.states[reference.Key])
		require.Equal(t, 1, journal.prepareCalls[reference.Key])
	}
}

func TestBlockBuilderRejectsEvidenceMismatchBeforeFilesystemBuild(t *testing.T) {
	fixture := newOCIBlockBuildFixture(t)
	fixture.unpacker.mutate = func(result *ocirootfs.Result) {
		result.ProcdDigest = digest.FromString("different-procd")
	}
	_, err := fixture.builder().Build(t.Context(), fixture.request)
	require.ErrorContains(t, err, "procd digest")
	require.Zero(t, fixture.filesystem.calls)
	require.Empty(t, fixture.publisher.objects)
	require.NoDirExists(t, fixture.unpacker.lastRoot)
}

func TestBlockBuilderRejectsUnsafeObjectPrefixBeforeImport(t *testing.T) {
	fixture := newOCIBlockBuildFixture(t)
	fixture.request.BlockOptions.ObjectPrefix = "../another-tenant"
	_, err := fixture.builder().Build(t.Context(), fixture.request)
	require.ErrorContains(t, err, "object prefix")
	require.Zero(t, fixture.unpacker.calls)
	require.Zero(t, fixture.filesystem.calls)
	require.Empty(t, fixture.publisher.objects)
}

func TestBlockBuilderRejectsInvalidBlockBoundsBeforeImport(t *testing.T) {
	fixture := newOCIBlockBuildFixture(t)
	fixture.request.BlockOptions.DataRangeBytes = 1
	_, err := fixture.builder().Build(t.Context(), fixture.request)
	require.ErrorContains(t, err, "block build options")
	require.Zero(t, fixture.unpacker.calls)
	require.Zero(t, fixture.filesystem.calls)
	require.Empty(t, fixture.publisher.objects)
}

func TestBlockBuilderRejectsUnsafeImageAndCleansStaging(t *testing.T) {
	fixture := newOCIBlockBuildFixture(t)
	fixture.filesystem.mode = 0o644
	_, err := fixture.builder().Build(t.Context(), fixture.request)
	require.ErrorContains(t, err, "metadata")
	require.Empty(t, fixture.publisher.objects)
	require.NoDirExists(t, fixture.unpacker.lastRoot)
	require.NoFileExists(t, fixture.unpacker.lastRoot+".xfs")
}

func TestBlockBuilderCleansStagingAfterPublicationFailure(t *testing.T) {
	fixture := newOCIBlockBuildFixture(t)
	fixture.publisher.err = errors.New("object store unavailable")
	_, err := fixture.builder().Build(t.Context(), fixture.request)
	require.ErrorContains(t, err, "object store unavailable")
	require.NoDirExists(t, fixture.unpacker.lastRoot)
	require.NoFileExists(t, fixture.unpacker.lastRoot+".xfs")
}

func TestBlockBuilderRejectsRootOutsideOperationDirectory(t *testing.T) {
	fixture := newOCIBlockBuildFixture(t)
	outside := filepath.Join(t.TempDir(), "oci-rootfs-outside")
	fixture.unpacker.rootOverride = outside
	_, err := fixture.builder().Build(t.Context(), fixture.request)
	require.ErrorContains(t, err, "outside its operation work directory")
	require.Zero(t, fixture.filesystem.calls)
	require.Empty(t, fixture.publisher.objects)
	require.DirExists(t, outside, "an untrusted path must not be recursively removed")
}

func TestBlockBuilderDoesNotDeletePreexistingImagePath(t *testing.T) {
	fixture := newOCIBlockBuildFixture(t)
	fixture.unpacker.precreateImage = true
	_, err := fixture.builder().Build(t.Context(), fixture.request)
	require.ErrorContains(t, err, "already exists")
	require.NoDirExists(t, fixture.unpacker.lastRoot)
	require.FileExists(t, fixture.unpacker.lastRoot+".xfs")
	require.Zero(t, fixture.filesystem.calls)
	require.Empty(t, fixture.publisher.objects)
}

func TestBlockBuilderRetainsImageWhenFilesystemReportsMounted(t *testing.T) {
	fixture := newOCIBlockBuildFixture(t)
	fixture.filesystem.err = rootfsartifact.ErrXFSImageStillMounted
	_, err := fixture.builder().Build(t.Context(), fixture.request)
	require.ErrorIs(t, err, rootfsartifact.ErrXFSImageStillMounted)
	require.NoDirExists(t, fixture.unpacker.lastRoot)
	require.FileExists(t, fixture.unpacker.lastRoot+".xfs")
	require.Empty(t, fixture.publisher.objects)
}

func TestPrivilegedBlockBuilderPublishesMountableXFSArtifact(t *testing.T) {
	if os.Getenv("SANDBOX0_PRIVILEGED_ROOTFS_IMPORTER") != "1" {
		t.Skip("set SANDBOX0_PRIVILEGED_ROOTFS_IMPORTER=1 on an isolated Linux host")
	}
	if os.Geteuid() != 0 {
		t.Fatal("privileged RootFS importer test requires root")
	}
	for _, command := range []string{"mkfs.xfs", "mount", "cp", "umount", "xfs_repair"} {
		if _, err := exec.LookPath(command); err != nil {
			t.Fatalf("required command %s: %v", command, err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	fixture := newOCIBlockBuildFixture(t)
	const marker = "sandbox0-rootfs-block-roundtrip\n"
	fixture.unpacker.populate = func(root string) error {
		if err := os.Mkdir(filepath.Join(root, "etc"), 0o755); err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(root, "etc", "marker"), []byte(marker), 0o640)
	}
	result, err := (BlockBuilder{
		Unpacker: fixture.unpacker, Filesystem: rootfsartifact.XFSBuilder{}, Publisher: fixture.publisher,
	}).Build(ctx, fixture.request)
	require.NoError(t, err)
	require.NoDirExists(t, fixture.unpacker.lastRoot)
	require.NoFileExists(t, fixture.unpacker.lastRoot+".xfs")

	reader, err := rootfsblock.NewReader(fixture.publisher, result.Descriptor, 64<<20)
	require.NoError(t, err)
	reconstructed := filepath.Join(fixture.request.Image.WorkRoot, "reconstructed.xfs")
	image, err := os.OpenFile(reconstructed, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	require.NoError(t, err)
	require.NoError(t, image.Truncate(result.LogicalSizeBytes))
	const chunkBytes = 8 << 20
	for offset := int64(0); offset < result.LogicalSizeBytes; offset += chunkBytes {
		payload := make([]byte, min(int64(chunkBytes), result.LogicalSizeBytes-offset))
		n, readErr := reader.ReadAt(payload, offset)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			t.Fatalf("read reconstructed image at %d: %v", offset, readErr)
		}
		if n != len(payload) {
			t.Fatalf("read reconstructed image at %d = %d, want %d", offset, n, len(payload))
		}
		if len(bytes.Trim(payload, "\x00")) != 0 {
			written, writeErr := image.WriteAt(payload, offset)
			if writeErr != nil || written != len(payload) {
				t.Fatalf("write reconstructed image at %d = %d, %v", offset, written, writeErr)
			}
		}
	}
	require.NoError(t, image.Sync())
	require.NoError(t, image.Close())
	mountRoot := filepath.Join(fixture.request.Image.WorkRoot, "verify")
	require.NoError(t, os.Mkdir(mountRoot, 0o700))
	require.NoError(t, runRootFSImporterCommand(ctx, "mount", "-t", "xfs", "-o", "loop,ro,nouuid,noatime", reconstructed, mountRoot))
	mounted := true
	defer func() {
		if mounted {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			if err := runRootFSImporterCommand(cleanupCtx, "umount", mountRoot); err != nil {
				t.Errorf("unmount reconstructed RootFS: %v", err)
			}
		}
	}()
	payload, err := os.ReadFile(filepath.Join(mountRoot, "lower", "etc", "marker"))
	require.NoError(t, err)
	require.Equal(t, marker, string(payload))
	require.NoError(t, runRootFSImporterCommand(ctx, "umount", mountRoot))
	mounted = false
}

type ociBlockBuildFixture struct {
	request      BuildRequest
	sourceDigest digest.Digest
	procdDigest  digest.Digest
	unpacker     *fakeOCIUnpacker
	filesystem   *fakeFilesystemImageBuilder
	publisher    *fakeImmutablePublisher
}

func newOCIBlockBuildFixture(t *testing.T) *ociBlockBuildFixture {
	t.Helper()
	workRoot := t.TempDir()
	require.NoError(t, os.Chmod(workRoot, 0o700))
	sourceDigest := digest.FromString("source-image")
	procdDigest := digest.FromString("production-procd")
	request := BuildRequest{
		Image: ocirootfs.Request{
			Reference: "registry.example/sandbox@" + sourceDigest.String(),
			Platform:  ocispec.Platform{OS: "linux", Architecture: "arm64"},
			WorkRoot:  workRoot, ProcdPath: "/opt/sandbox0/procd",
			ExpectedProcdDigest: procdDigest,
		},
		LogicalSizeBytes: rootfsartifact.MinimumLogicalSizeBytes,
		BlockOptions: rootfsblock.BuildOptions{
			ObjectPrefix: "rootfs/import-test",
		},
	}
	return &ociBlockBuildFixture{
		request: request, sourceDigest: sourceDigest, procdDigest: procdDigest,
		unpacker:   &fakeOCIUnpacker{sourceDigest: sourceDigest, procdDigest: procdDigest},
		filesystem: &fakeFilesystemImageBuilder{mode: 0o600},
		publisher:  &fakeImmutablePublisher{objects: make(map[string][]byte)},
	}
}

func (f *ociBlockBuildFixture) builder() BlockBuilder {
	return BlockBuilder{Unpacker: f.unpacker, Filesystem: f.filesystem, Publisher: f.publisher}
}

type fakeOCIUnpacker struct {
	calls          int
	sourceDigest   digest.Digest
	procdDigest    digest.Digest
	lastRoot       string
	rootOverride   string
	precreateImage bool
	populate       func(string) error
	mutate         func(*ocirootfs.Result)
}

func (f *fakeOCIUnpacker) Import(_ context.Context, request ocirootfs.Request) (ocirootfs.Result, error) {
	f.calls++
	root := filepath.Join(request.WorkRoot, "oci-rootfs-test")
	if f.rootOverride != "" {
		root = f.rootOverride
	}
	if err := os.Mkdir(root, 0o700); err != nil {
		return ocirootfs.Result{}, err
	}
	if f.precreateImage {
		if err := os.WriteFile(root+".xfs", []byte("not-owned"), 0o600); err != nil {
			return ocirootfs.Result{}, err
		}
	}
	if f.populate != nil {
		if err := f.populate(root); err != nil {
			return ocirootfs.Result{}, err
		}
	}
	f.lastRoot = root
	result := ocirootfs.Result{
		Reference: request.Reference, SourceDigest: f.sourceDigest,
		ManifestDigest: digest.FromString("manifest"), ConfigDigest: digest.FromString("config"),
		Platform:     request.Platform,
		LayerDigests: []digest.Digest{digest.FromString("layer")},
		DiffIDs:      []digest.Digest{digest.FromString("diff-id")},
		ProcdDigest:  f.procdDigest, RootPath: root,
		UnpackedBytes: 4096, Files: 1,
	}
	if f.mutate != nil {
		f.mutate(&result)
	}
	return result, nil
}

type fakeFilesystemImageBuilder struct {
	calls int
	mode  os.FileMode
	err   error
}

func (f *fakeFilesystemImageBuilder) Build(
	_ context.Context,
	sourceRoot string,
	destination string,
	logicalSize int64,
) error {
	f.calls++
	if _, err := os.Stat(filepath.Join(sourceRoot, ".")); err != nil {
		return err
	}
	file, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_RDWR, f.mode)
	if err != nil {
		return err
	}
	if err := file.Truncate(logicalSize); err != nil {
		_ = file.Close()
		return err
	}
	_, writeErr := file.WriteAt(bytes.Repeat([]byte{0x5a}, rootfsblock.LogicalBlockSize), 0)
	return errors.Join(writeErr, file.Close(), f.err)
}

type fakeImmutablePublisher struct {
	objects map[string][]byte
	err     error
}

func (p *fakeImmutablePublisher) PutImmutable(_ context.Context, key string, payload []byte) error {
	if p.err != nil {
		return p.err
	}
	if current, found := p.objects[key]; found && !bytes.Equal(current, payload) {
		return fmt.Errorf("immutable object conflict")
	}
	p.objects[key] = append([]byte(nil), payload...)
	return nil
}

func (p *fakeImmutablePublisher) Get(key string, offset, length int64) (io.ReadCloser, error) {
	payload, found := p.objects[key]
	if !found || offset < 0 || length < 0 || offset > int64(len(payload))-length {
		return nil, fmt.Errorf("object range not found")
	}
	return io.NopCloser(bytes.NewReader(payload[offset : offset+length])), nil
}

func runRootFSImporterCommand(ctx context.Context, name string, args ...string) error {
	command := exec.CommandContext(ctx, name, args...)
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s: %s: %w", name, strings.TrimSpace(string(output)), err)
	}
	return nil
}
