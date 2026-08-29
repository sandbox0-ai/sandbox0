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

package ocirootfs

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/containerd/containerd/v2/core/remotes"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestImporterAppliesSelectedPlatformWhiteoutsAndExactProcd(t *testing.T) {
	first := testLayer(t,
		testTarEntry{name: "removed", body: "old"},
		testTarEntry{name: "keep", body: "first"},
		testTarEntry{name: "procd", typeflag: tar.TypeSymlink, linkname: "bin/another"},
	)
	second := testLayer(t,
		testTarEntry{name: ".wh.removed"},
		testTarEntry{name: "keep", body: "second"},
		testTarEntry{name: "nested", typeflag: tar.TypeDir, mode: 0o755},
		testTarEntry{name: "nested/value", body: "value"},
	)
	fixture := newOCIImportFixture(t, []testLayerBlob{first, second})
	importer := fixture.importer(t, Limits{})

	result, err := importer.Import(t.Context(), fixture.request())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(result.RootPath)) })
	assert.Equal(t, fixture.sourceDescriptor.Digest, result.SourceDigest)
	assert.Equal(t, fixture.manifestDescriptor.Digest, result.ManifestDigest)
	assert.Equal(t, fixture.configDescriptor.Digest, result.ConfigDigest)
	assert.Equal(t, fixture.procdDigest, result.ProcdDigest)
	assert.Equal(t, []digest.Digest{first.descriptor.Digest, second.descriptor.Digest}, result.LayerDigests)
	assert.Equal(t, []digest.Digest{first.diffID, second.diffID}, result.DiffIDs)
	assert.Positive(t, result.UnpackedBytes)
	assert.Equal(t, 7, result.Files)

	_, err = os.Lstat(filepath.Join(result.RootPath, "removed"))
	require.ErrorIs(t, err, os.ErrNotExist)
	keep, err := os.ReadFile(filepath.Join(result.RootPath, "keep"))
	require.NoError(t, err)
	assert.Equal(t, "second", string(keep))
	nested, err := os.ReadFile(filepath.Join(result.RootPath, "nested", "value"))
	require.NoError(t, err)
	assert.Equal(t, "value", string(nested))
	procd, err := os.ReadFile(filepath.Join(result.RootPath, "procd"))
	require.NoError(t, err)
	assert.Equal(t, testProcdPayload, string(procd))
	info, err := os.Lstat(filepath.Join(result.RootPath, "procd"))
	require.NoError(t, err)
	assert.True(t, info.Mode().IsRegular())
	assert.Equal(t, os.FileMode(0o555), info.Mode().Perm())
	require.Len(t, mustReadDir(t, fixture.workRoot), 1)
}

func TestImporterRejectsDiffIDMismatchAndRemovesPartialRoot(t *testing.T) {
	layer := testLayer(t, testTarEntry{name: "value", body: "payload"})
	fixture := newOCIImportFixture(t, []testLayerBlob{layer})
	fixture.config.RootFS.DiffIDs[0] = digest.FromString("another-diff")
	fixture.rebuildMetadata(t)
	importer := fixture.importer(t, Limits{})

	_, err := importer.Import(t.Context(), fixture.request())
	require.ErrorContains(t, err, "diff_id")
	assert.Empty(t, mustReadDir(t, fixture.workRoot))
}

func TestImporterRejectsDeviceEntryBeforeCreationAndRemovesPartialRoot(t *testing.T) {
	layer := testLayer(t, testTarEntry{name: "device", typeflag: tar.TypeChar})
	fixture := newOCIImportFixture(t, []testLayerBlob{layer})
	importer := fixture.importer(t, Limits{})

	_, err := importer.Import(t.Context(), fixture.request())
	require.ErrorContains(t, err, "device and FIFO entries are forbidden")
	assert.Empty(t, mustReadDir(t, fixture.workRoot))
}

func TestImporterPreservesLinuxBackslashFilename(t *testing.T) {
	layer := testLayer(t, testTarEntry{name: `literal\backslash`, body: "payload"})
	fixture := newOCIImportFixture(t, []testLayerBlob{layer})
	importer := fixture.importer(t, Limits{})

	result, err := importer.Import(t.Context(), fixture.request())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(result.RootPath)) })
	payload, err := os.ReadFile(filepath.Join(result.RootPath, `literal\backslash`))
	require.NoError(t, err)
	assert.Equal(t, "payload", string(payload))
}

func TestImporterRejectsCompressedLayerDigestMismatch(t *testing.T) {
	layer := testLayer(t, testTarEntry{name: "value", body: "payload"})
	fixture := newOCIImportFixture(t, []testLayerBlob{layer})
	corrupted := append([]byte(nil), layer.compressed...)
	corrupted[4] ^= 1 // Change gzip mtime without changing the decompressed tar.
	fixture.resolver.blobs[layer.descriptor.Digest] = corrupted
	importer := fixture.importer(t, Limits{})

	_, err := importer.Import(t.Context(), fixture.request())
	require.ErrorContains(t, err, "compressed digest")
	assert.Empty(t, mustReadDir(t, fixture.workRoot))
}

func TestImporterBoundsUnpackedLayerBytes(t *testing.T) {
	layer := testLayer(t, testTarEntry{name: "value", body: strings.Repeat("x", 4096)})
	fixture := newOCIImportFixture(t, []testLayerBlob{layer})
	limits := testLimits()
	limits.MaxLayerUnpacked = 1024
	limits.MaxImageUnpacked = 2048
	limits.MaxFileBytes = 1024
	importer := fixture.importer(t, limits)

	_, err := importer.Import(t.Context(), fixture.request())
	require.ErrorContains(t, err, "file bytes exceed configured bounds")
	assert.Empty(t, mustReadDir(t, fixture.workRoot))
}

func TestImporterRejectsCompressionThatContradictsMediaType(t *testing.T) {
	layer := testLayer(t, testTarEntry{name: "value", body: "payload"})
	layer.descriptor.MediaType = ocispec.MediaTypeImageLayer
	fixture := newOCIImportFixture(t, []testLayerBlob{layer})
	importer := fixture.importer(t, Limits{})

	_, err := importer.Import(t.Context(), fixture.request())
	require.ErrorContains(t, err, `layer compression is "gzip", media type requires "none"`)
	assert.Empty(t, mustReadDir(t, fixture.workRoot))
}

func TestImporterRejectsUnpinnedReferenceBeforeRemoteAccess(t *testing.T) {
	fixture := newOCIImportFixture(t, []testLayerBlob{testLayer(t, testTarEntry{name: "value", body: "payload"})})
	request := fixture.request()
	request.Reference = "registry.example/sandbox/base:latest"
	importer := fixture.importer(t, Limits{})

	_, err := importer.Import(t.Context(), request)
	require.ErrorContains(t, err, "digest-pinned")
	fixture.resolver.mu.Lock()
	defer fixture.resolver.mu.Unlock()
	assert.Zero(t, fixture.resolver.resolveCalls)
	assert.Empty(t, mustReadDir(t, fixture.workRoot))
}

func TestImporterRejectsUnknownManifestField(t *testing.T) {
	layer := testLayer(t, testTarEntry{name: "value", body: "payload"})
	fixture := newOCIImportFixture(t, []testLayerBlob{layer})
	payload := mustJSON(t, map[string]any{
		"schemaVersion": 2,
		"mediaType":     ocispec.MediaTypeImageManifest,
		"config":        fixture.configDescriptor,
		"layers":        []ocispec.Descriptor{layer.descriptor},
		"unknown":       true,
	})
	fixture.manifestDescriptor = testDescriptor(ocispec.MediaTypeImageManifest, payload, &fixture.platform)
	fixture.resolver.blobs[fixture.manifestDescriptor.Digest] = payload
	fixture.rebuildIndex(t)
	importer := fixture.importer(t, Limits{})

	_, err := importer.Import(t.Context(), fixture.request())
	require.ErrorContains(t, err, "unknown field")
	assert.Empty(t, mustReadDir(t, fixture.workRoot))
}

func TestDecodeImageConfigAcceptsBoundedDockerHealthcheck(t *testing.T) {
	payload := mustJSON(t, map[string]any{
		"architecture": "amd64",
		"os":           "linux",
		"config": map[string]any{
			"Entrypoint": []string{"/entrypoint"},
			"Healthcheck": map[string]any{
				"Test":     []string{"CMD-SHELL", "curl -f http://localhost/ || exit 1"},
				"Interval": 30_000_000_000,
				"Timeout":  5_000_000_000,
				"Retries":  3,
			},
		},
		"rootfs": map[string]any{"type": "layers", "diff_ids": []string{digest.FromString("layer").String()}},
	})
	decoded, err := decodeImageConfig(payload)
	require.NoError(t, err)
	assert.Equal(t, []string{"/entrypoint"}, decoded.Config.Entrypoint)

	var document map[string]any
	require.NoError(t, json.Unmarshal(payload, &document))
	document["config"].(map[string]any)["Healthcheck"].(map[string]any)["Unknown"] = true
	_, err = decodeImageConfig(mustJSON(t, document))
	require.ErrorContains(t, err, "unknown field")
}

func TestImporterSelectsRequestedPlatformFromIndex(t *testing.T) {
	layer := testLayer(t, testTarEntry{name: "value", body: "payload"})
	fixture := newOCIImportFixture(t, []testLayerBlob{layer})
	wrongPayload := mustJSON(t, ocispec.Manifest{
		Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: ocispec.MediaTypeImageManifest,
		Config: fixture.configDescriptor, Layers: []ocispec.Descriptor{layer.descriptor},
	})
	wrong := testDescriptor(ocispec.MediaTypeImageManifest, wrongPayload, &ocispec.Platform{OS: "linux", Architecture: "arm64"})
	fixture.resolver.blobs[wrong.Digest] = wrongPayload
	fixture.index.Manifests = append([]ocispec.Descriptor{wrong}, fixture.manifestDescriptor)
	fixture.rebuildIndexPayload(t)
	importer := fixture.importer(t, Limits{})

	result, err := importer.Import(t.Context(), fixture.request())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(result.RootPath)) })
	assert.Equal(t, fixture.manifestDescriptor.Digest, result.ManifestDigest)
}

const testProcdPayload = "production-procd"

type testLayerBlob struct {
	descriptor ocispec.Descriptor
	compressed []byte
	diffID     digest.Digest
}

type testTarEntry struct {
	name     string
	body     string
	typeflag byte
	linkname string
	mode     int64
}

func testLayer(t *testing.T, entries ...testTarEntry) testLayerBlob {
	t.Helper()
	var tarPayload bytes.Buffer
	writer := tar.NewWriter(&tarPayload)
	for _, entry := range entries {
		mode := entry.mode
		if mode == 0 {
			mode = 0o644
		}
		typeflag := entry.typeflag
		if typeflag == 0 {
			typeflag = tar.TypeReg
		}
		header := &tar.Header{
			Name: entry.name, Mode: mode, Typeflag: typeflag, Linkname: entry.linkname,
			Uid: os.Getuid(), Gid: os.Getgid(),
		}
		if typeflag == tar.TypeReg || typeflag == tar.TypeRegA {
			header.Size = int64(len(entry.body))
		}
		require.NoError(t, writer.WriteHeader(header))
		if header.Size > 0 {
			_, err := io.WriteString(writer, entry.body)
			require.NoError(t, err)
		}
	}
	require.NoError(t, writer.Close())
	var compressed bytes.Buffer
	gzipWriter := gzip.NewWriter(&compressed)
	_, err := gzipWriter.Write(tarPayload.Bytes())
	require.NoError(t, err)
	require.NoError(t, gzipWriter.Close())
	payload := compressed.Bytes()
	return testLayerBlob{
		descriptor: testDescriptor(ocispec.MediaTypeImageLayerGzip, payload, nil),
		compressed: append([]byte(nil), payload...), diffID: digest.FromBytes(tarPayload.Bytes()),
	}
}

type ociImportFixture struct {
	workRoot           string
	procdPath          string
	procdDigest        digest.Digest
	platform           ocispec.Platform
	config             ocispec.Image
	configDescriptor   ocispec.Descriptor
	manifest           ocispec.Manifest
	manifestDescriptor ocispec.Descriptor
	index              ocispec.Index
	sourceDescriptor   ocispec.Descriptor
	resolver           *fakeOCIResolver
}

func newOCIImportFixture(t *testing.T, layers []testLayerBlob) *ociImportFixture {
	t.Helper()
	workRoot := filepath.Join(t.TempDir(), "work")
	require.NoError(t, os.Mkdir(workRoot, 0o700))
	procdPath := filepath.Join(t.TempDir(), "procd")
	require.NoError(t, os.WriteFile(procdPath, []byte(testProcdPayload), 0o555))
	fixture := &ociImportFixture{
		workRoot: workRoot, procdPath: procdPath,
		procdDigest: digest.FromString(testProcdPayload),
		platform:    ocispec.Platform{OS: "linux", Architecture: "amd64"},
		resolver:    &fakeOCIResolver{blobs: make(map[digest.Digest][]byte)},
	}
	fixture.config = ocispec.Image{
		Platform: fixture.platform,
		RootFS:   ocispec.RootFS{Type: "layers"},
	}
	for _, layer := range layers {
		fixture.config.RootFS.DiffIDs = append(fixture.config.RootFS.DiffIDs, layer.diffID)
		fixture.manifest.Layers = append(fixture.manifest.Layers, layer.descriptor)
		fixture.resolver.blobs[layer.descriptor.Digest] = append([]byte(nil), layer.compressed...)
	}
	fixture.rebuildMetadata(t)
	return fixture
}

func (f *ociImportFixture) rebuildMetadata(t *testing.T) {
	t.Helper()
	configPayload := mustJSON(t, f.config)
	f.configDescriptor = testDescriptor(ocispec.MediaTypeImageConfig, configPayload, nil)
	f.resolver.blobs[f.configDescriptor.Digest] = configPayload
	f.manifest.Versioned = specs.Versioned{SchemaVersion: 2}
	f.manifest.MediaType = ocispec.MediaTypeImageManifest
	f.manifest.Config = f.configDescriptor
	manifestPayload := mustJSON(t, f.manifest)
	f.manifestDescriptor = testDescriptor(ocispec.MediaTypeImageManifest, manifestPayload, &f.platform)
	f.resolver.blobs[f.manifestDescriptor.Digest] = manifestPayload
	f.rebuildIndex(t)
}

func (f *ociImportFixture) rebuildIndex(t *testing.T) {
	t.Helper()
	f.index = ocispec.Index{
		Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: ocispec.MediaTypeImageIndex,
		Manifests: []ocispec.Descriptor{f.manifestDescriptor},
	}
	f.rebuildIndexPayload(t)
}

func (f *ociImportFixture) rebuildIndexPayload(t *testing.T) {
	t.Helper()
	payload := mustJSON(t, f.index)
	f.sourceDescriptor = testDescriptor(ocispec.MediaTypeImageIndex, payload, nil)
	f.resolver.root = f.sourceDescriptor
	f.resolver.blobs[f.sourceDescriptor.Digest] = payload
}

func (f *ociImportFixture) request() Request {
	return Request{
		Reference: fmt.Sprintf("registry.example/sandbox/base@%s", f.sourceDescriptor.Digest),
		Platform:  f.platform, WorkRoot: f.workRoot, ProcdPath: f.procdPath,
		ExpectedProcdDigest: f.procdDigest,
	}
}

func (f *ociImportFixture) importer(t *testing.T, limits Limits) *Importer {
	t.Helper()
	limits, err := normalizeLimits(limits)
	require.NoError(t, err)
	return &Importer{resolver: f.resolver, limits: limits, allowNonRoot: true}
}

type fakeOCIResolver struct {
	mu           sync.Mutex
	root         ocispec.Descriptor
	blobs        map[digest.Digest][]byte
	resolveCalls int
}

func (r *fakeOCIResolver) Resolve(_ context.Context, ref string) (string, ocispec.Descriptor, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resolveCalls++
	return ref, r.root, nil
}

func (r *fakeOCIResolver) Fetcher(context.Context, string) (remotes.Fetcher, error) {
	return remotes.FetcherFunc(func(_ context.Context, descriptor ocispec.Descriptor) (io.ReadCloser, error) {
		r.mu.Lock()
		defer r.mu.Unlock()
		payload, found := r.blobs[descriptor.Digest]
		if !found {
			return nil, fmt.Errorf("blob %s not found", descriptor.Digest)
		}
		return io.NopCloser(bytes.NewReader(append([]byte(nil), payload...))), nil
	}), nil
}

func (r *fakeOCIResolver) Pusher(context.Context, string) (remotes.Pusher, error) {
	return nil, fmt.Errorf("push is unsupported")
}

func testDescriptor(mediaType string, payload []byte, platform *ocispec.Platform) ocispec.Descriptor {
	return ocispec.Descriptor{
		MediaType: mediaType, Digest: digest.FromBytes(payload), Size: int64(len(payload)), Platform: platform,
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	payload, err := json.Marshal(value)
	require.NoError(t, err)
	return payload
}

func mustReadDir(t *testing.T, path string) []os.DirEntry {
	t.Helper()
	entries, err := os.ReadDir(path)
	require.NoError(t, err)
	return entries
}

func testLimits() Limits {
	limits, err := normalizeLimits(Limits{})
	if err != nil {
		panic(err)
	}
	return limits
}
