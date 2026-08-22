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
	"os"
	"path/filepath"
	"testing"

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
