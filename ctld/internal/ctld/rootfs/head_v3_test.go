package rootfs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/plugins/content/local"
	"github.com/containerd/errdefs"
	"github.com/containerd/platforms"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/identity"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func TestWaitForCRIImageWaitsForImageServiceVisibility(t *testing.T) {
	imageClient := &sequencedCRIImageClient{visibleAt: 3}
	runtime := NewContainerdRuntime(ContainerdRuntimeConfig{CRIImageClient: imageClient})

	require.NoError(t, runtime.waitForCRIImage(context.Background(), "sandbox0.local/rootfs-heads@sha256:test"))
	assert.Equal(t, 3, imageClient.calls)
}

type sequencedCRIImageClient struct {
	calls     int
	visibleAt int
}

func (c *sequencedCRIImageClient) ImageStatus(
	_ context.Context,
	request *runtimeapi.ImageStatusRequest,
	_ ...grpc.CallOption,
) (*runtimeapi.ImageStatusResponse, error) {
	c.calls++
	if c.calls < c.visibleAt {
		return &runtimeapi.ImageStatusResponse{}, nil
	}
	return &runtimeapi.ImageStatusResponse{Image: &runtimeapi.Image{Id: request.Image.Image}}, nil
}

func TestBaseIdentityAndConfigSelectsNodePlatformFromImageIndex(t *testing.T) {
	client := newBaseIdentityClient(t)
	hostPlatform := platforms.Normalize(platforms.DefaultSpec())
	hostPlatform.OSVersion = ""
	hostPlatform.OSFeatures = nil
	otherPlatform := ocispec.Platform{OS: hostPlatform.OS, Architecture: "amd64"}
	if hostPlatform.Architecture == "amd64" {
		otherPlatform.Architecture = "arm64"
		otherPlatform.Variant = "v8"
	}

	hostConfig := ocispec.Image{
		Platform: hostPlatform,
		RootFS:   ocispec.RootFS{Type: "layers", DiffIDs: []digest.Digest{digest.FromString("host layer")}},
	}
	otherConfig := ocispec.Image{
		Platform: otherPlatform,
		RootFS:   ocispec.RootFS{Type: "layers", DiffIDs: []digest.Digest{digest.FromString("other layer")}},
	}
	hostConfigDescriptor, hostConfigData := writeImageJSON(t, client.contentStore, ocispec.MediaTypeImageConfig, hostConfig)
	otherConfigDescriptor, _ := writeImageJSON(t, client.contentStore, ocispec.MediaTypeImageConfig, otherConfig)
	hostManifestDescriptor, _ := writeImageJSON(t, client.contentStore, ocispec.MediaTypeImageManifest, ocispec.Manifest{Config: hostConfigDescriptor})
	otherManifestDescriptor, _ := writeImageJSON(t, client.contentStore, ocispec.MediaTypeImageManifest, ocispec.Manifest{Config: otherConfigDescriptor})
	hostManifestDescriptor.Platform = &hostPlatform
	otherManifestDescriptor.Platform = &otherPlatform
	indexDescriptor, _ := writeImageJSON(t, client.contentStore, ocispec.MediaTypeImageIndex, ocispec.Index{
		Manifests: []ocispec.Descriptor{otherManifestDescriptor, hostManifestDescriptor},
	})
	imageReference := "docker.io/sandbox0ai/base:latest"
	client.imageStore.records[imageReference] = images.Image{Name: imageReference, Target: indexDescriptor}
	runtime := NewContainerdRuntime(ContainerdRuntimeConfig{ContainerdClient: client})
	hostChainID := identity.ChainID(hostConfig.RootFS.DiffIDs).String()

	base, configData, err := runtime.BaseIdentityAndConfig(context.Background(), ctldapi.RootFSInfo{
		BaseImageRef:   imageReference,
		SnapshotParent: hostChainID,
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, indexDescriptor.Digest.String(), base.ManifestDigest)
	assert.Equal(t, hostChainID, base.ChainID)
	assert.Equal(t, hostPlatform.OS, base.OS)
	assert.Equal(t, hostPlatform.Architecture, base.Architecture)
	assert.Equal(t, hostPlatform.Variant, base.Variant)
	assert.Equal(t, hostConfigData, configData)
}

func TestBaseIdentityAndConfigResolvesFamiliarImageReference(t *testing.T) {
	client := newBaseIdentityClient(t)
	platform := platforms.Normalize(platforms.DefaultSpec())
	platform.OSVersion = ""
	platform.OSFeatures = nil
	config := ocispec.Image{
		Platform: platform,
		RootFS:   ocispec.RootFS{Type: "layers", DiffIDs: []digest.Digest{digest.FromString("base layer")}},
	}
	configDescriptor, _ := writeImageJSON(t, client.contentStore, ocispec.MediaTypeImageConfig, config)
	manifestDescriptor, _ := writeImageJSON(t, client.contentStore, ocispec.MediaTypeImageManifest, ocispec.Manifest{Config: configDescriptor})
	canonical := "docker.io/sandbox0ai/base:latest"
	client.imageStore.records[canonical] = images.Image{Name: canonical, Target: manifestDescriptor}
	runtime := NewContainerdRuntime(ContainerdRuntimeConfig{ContainerdClient: client})

	base, _, err := runtime.BaseIdentityAndConfig(context.Background(), ctldapi.RootFSInfo{BaseImageRef: "sandbox0ai/base:latest"}, nil)
	require.NoError(t, err)
	assert.Equal(t, canonical, base.ImageReference)
	assert.Equal(t, []string{"sandbox0ai/base:latest", canonical}, client.imageStore.gets)
}

func TestBaseIdentityAndConfigUsesPublishedHeadMarkerAcrossLocalImageRecords(t *testing.T) {
	runtime, client, info, expected, markerConfigData := publishedBaseFixture(t, ocispec.Platform{OS: "linux", Architecture: "amd64"}, "")

	base, configData, err := runtime.BaseIdentityAndConfig(context.Background(), info, &expected)
	require.NoError(t, err)
	assert.Equal(t, expected, base)
	assert.Equal(t, markerConfigData, configData)
	assert.Equal(t, []string{info.BaseImageRef}, client.imageStore.gets, "published Head recovery must not reload the mutable base tag")
}

func TestBaseIdentityAndConfigRejectsPublishedHeadBaseMismatch(t *testing.T) {
	runtime, _, info, expected, _ := publishedBaseFixture(t, ocispec.Platform{OS: "linux", Architecture: "amd64"}, digest.FromString("different base").String())

	_, _, err := runtime.BaseIdentityAndConfig(context.Background(), info, &expected)
	assert.ErrorContains(t, err, "marker base")
}

func TestBaseIdentityAndConfigRejectsPublishedHeadPlatformMismatch(t *testing.T) {
	runtime, _, info, expected, _ := publishedBaseFixture(t, ocispec.Platform{OS: "linux", Architecture: "arm64", Variant: "v8"}, "")

	_, _, err := runtime.BaseIdentityAndConfig(context.Background(), info, &expected)
	assert.ErrorContains(t, err, "image platform")
}

func TestEnsureRootFSHeadSnapshotCommitsExplicitMarker(t *testing.T) {
	snapshotter := newMarkerSnapshotter()
	snapshotter.infos["base"] = snapshots.Info{Name: "base", Kind: snapshots.KindCommitted}

	require.NoError(t, ensureRootFSHeadSnapshot(context.Background(), snapshotter, "head", "base", "annotation", "active"))
	info := snapshotter.infos["head"]
	assert.Equal(t, snapshots.KindCommitted, info.Kind)
	assert.Equal(t, "base", info.Parent)
	assert.Equal(t, "annotation", info.Labels[rootfshead.AnnotationHead])
	assert.Equal(t, "base", info.Labels[rootfshead.LabelBaseChainID])
	assert.NotContains(t, snapshotter.infos, "active")
	assert.Equal(t, 1, snapshotter.prepareCalls)
	assert.Equal(t, 1, snapshotter.commitCalls)
}

func TestEnsureRootFSHeadSnapshotIsIdempotent(t *testing.T) {
	snapshotter := newMarkerSnapshotter()
	snapshotter.infos["head"] = snapshots.Info{
		Name:   "head",
		Kind:   snapshots.KindCommitted,
		Parent: "base",
		Labels: map[string]string{
			rootfshead.AnnotationHead:   "annotation",
			rootfshead.LabelBaseChainID: "base",
		},
	}

	require.NoError(t, ensureRootFSHeadSnapshot(context.Background(), snapshotter, "head", "base", "annotation", "active"))
	assert.Zero(t, snapshotter.prepareCalls)
	assert.Zero(t, snapshotter.commitCalls)
}

func TestEnsureRootFSHeadSnapshotRejectsMissingBaseMetadata(t *testing.T) {
	snapshotter := newMarkerSnapshotter()
	snapshotter.infos["head"] = snapshots.Info{
		Name:   "head",
		Kind:   snapshots.KindCommitted,
		Parent: "base",
		Labels: map[string]string{rootfshead.AnnotationHead: "annotation"},
	}

	err := ensureRootFSHeadSnapshot(context.Background(), snapshotter, "head", "base", "annotation", "active")
	assert.ErrorContains(t, err, "conflicting base metadata")
	assert.Zero(t, snapshotter.prepareCalls)
}

func TestEnsureRootFSHeadSnapshotRejectsConflictingExistingSnapshot(t *testing.T) {
	snapshotter := newMarkerSnapshotter()
	snapshotter.infos["head"] = snapshots.Info{Name: "head", Kind: snapshots.KindCommitted}

	err := ensureRootFSHeadSnapshot(context.Background(), snapshotter, "head", "base", "annotation", "active")
	assert.ErrorContains(t, err, "has parent")
	assert.Zero(t, snapshotter.prepareCalls)
}

func TestEnsureRootFSHeadSnapshotHandlesConcurrentCommit(t *testing.T) {
	snapshotter := newMarkerSnapshotter()
	snapshotter.commitRace = &snapshots.Info{
		Name:   "head",
		Kind:   snapshots.KindCommitted,
		Parent: "base",
		Labels: map[string]string{
			rootfshead.AnnotationHead:   "annotation",
			rootfshead.LabelBaseChainID: "base",
		},
	}

	require.NoError(t, ensureRootFSHeadSnapshot(context.Background(), snapshotter, "head", "base", "annotation", "active"))
	assert.NotContains(t, snapshotter.infos, "active")
	assert.Equal(t, 1, snapshotter.removeCalls)
}

func TestEnsureCanonicalBaseSnapshotUnpacksVerifiedBaseOnCarrierNode(t *testing.T) {
	client := newBaseIdentityClient(t)
	platform := platforms.Normalize(platforms.DefaultSpec())
	platform.OSVersion = ""
	platform.OSFeatures = nil
	diffID := digest.FromString("carrier base layer")
	config := ocispec.Image{
		Platform: platform,
		RootFS:   ocispec.RootFS{Type: "layers", DiffIDs: []digest.Digest{diffID}},
	}
	configDescriptor, _ := writeImageJSON(t, client.contentStore, ocispec.MediaTypeImageConfig, config)
	manifestDescriptor, _ := writeImageJSON(t, client.contentStore, ocispec.MediaTypeImageManifest, ocispec.Manifest{Config: configDescriptor})
	imageReference := "docker.io/sandbox0ai/infra:carrier-base-v1"
	record := images.Image{Name: imageReference, Target: manifestDescriptor}
	client.imageStore.records[imageReference] = record
	snapshotter := newMarkerSnapshotter()
	client.snapshotter = snapshotter
	base := rootfshead.BaseIdentity{
		ImageReference: imageReference,
		ManifestDigest: manifestDescriptor.Digest.String(),
		ChainID:        identity.ChainID(config.RootFS.DiffIDs).String(),
		OS:             platform.OS,
		Architecture:   platform.Architecture,
		Variant:        platform.Variant,
	}
	image := &unpackingImage{record: record, store: client.contentStore, snapshotter: snapshotter, chainID: base.ChainID}
	client.images[imageReference] = image

	require.NoError(t, ensureCanonicalBaseSnapshot(context.Background(), client, snapshotter, base))
	assert.Equal(t, 1, image.unpackCalls)
	info, err := snapshotter.Stat(context.Background(), base.ChainID)
	require.NoError(t, err)
	assert.Equal(t, snapshots.KindCommitted, info.Kind)

	require.NoError(t, ensureCanonicalBaseSnapshot(context.Background(), client, snapshotter, base))
	assert.Equal(t, 1, image.unpackCalls, "an existing committed base must be reused")
}

func TestEnsureCanonicalBaseSnapshotRejectsMutableTagDrift(t *testing.T) {
	client := newBaseIdentityClient(t)
	platform := platforms.Normalize(platforms.DefaultSpec())
	platform.OSVersion = ""
	platform.OSFeatures = nil
	config := ocispec.Image{
		Platform: platform,
		RootFS:   ocispec.RootFS{Type: "layers", DiffIDs: []digest.Digest{digest.FromString("replacement layer")}},
	}
	configDescriptor, _ := writeImageJSON(t, client.contentStore, ocispec.MediaTypeImageConfig, config)
	manifestDescriptor, _ := writeImageJSON(t, client.contentStore, ocispec.MediaTypeImageManifest, ocispec.Manifest{Config: configDescriptor})
	imageReference := "docker.io/sandbox0ai/infra:carrier-base-v1"
	client.imageStore.records[imageReference] = images.Image{Name: imageReference, Target: manifestDescriptor}
	snapshotter := newMarkerSnapshotter()
	client.snapshotter = snapshotter
	base := rootfshead.BaseIdentity{
		ImageReference: imageReference,
		ManifestDigest: digest.FromString("original manifest").String(),
		ChainID:        digest.FromString("original chain").String(),
		OS:             platform.OS,
		Architecture:   platform.Architecture,
		Variant:        platform.Variant,
	}

	err := ensureCanonicalBaseSnapshot(context.Background(), client, snapshotter, base)
	assert.ErrorContains(t, err, "does not match ImageFS Head")
}

type markerSnapshotter struct {
	snapshots.Snapshotter
	infos        map[string]snapshots.Info
	commitRace   *snapshots.Info
	prepareCalls int
	commitCalls  int
	removeCalls  int
}

func newMarkerSnapshotter() *markerSnapshotter {
	return &markerSnapshotter{infos: make(map[string]snapshots.Info)}
}

func (s *markerSnapshotter) Stat(_ context.Context, key string) (snapshots.Info, error) {
	info, ok := s.infos[key]
	if !ok {
		return snapshots.Info{}, fmt.Errorf("snapshot %s: %w", key, errdefs.ErrNotFound)
	}
	return info, nil
}

func (s *markerSnapshotter) Prepare(_ context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	s.prepareCalls++
	if _, ok := s.infos[key]; ok {
		return nil, errdefs.ErrAlreadyExists
	}
	info := snapshots.Info{Name: key, Kind: snapshots.KindActive, Parent: parent}
	for _, opt := range opts {
		if err := opt(&info); err != nil {
			return nil, err
		}
	}
	s.infos[key] = info
	return nil, nil
}

func (s *markerSnapshotter) Commit(_ context.Context, name, key string, opts ...snapshots.Opt) error {
	s.commitCalls++
	if s.commitRace != nil {
		s.infos[name] = *s.commitRace
		s.commitRace = nil
		return errdefs.ErrAlreadyExists
	}
	active, ok := s.infos[key]
	if !ok {
		return errdefs.ErrNotFound
	}
	if _, ok := s.infos[name]; ok {
		return errdefs.ErrAlreadyExists
	}
	active.Name = name
	active.Kind = snapshots.KindCommitted
	for _, opt := range opts {
		if err := opt(&active); err != nil {
			return err
		}
	}
	delete(s.infos, key)
	s.infos[name] = active
	return nil
}

func (s *markerSnapshotter) Remove(_ context.Context, key string) error {
	s.removeCalls++
	if _, ok := s.infos[key]; !ok {
		return errdefs.ErrNotFound
	}
	delete(s.infos, key)
	return nil
}

type baseIdentityClient struct {
	containerdClient
	contentStore content.Store
	imageStore   *baseIdentityImageStore
	snapshotter  snapshots.Snapshotter
	images       map[string]containerd.Image
}

func newBaseIdentityClient(t *testing.T) *baseIdentityClient {
	t.Helper()
	store, err := local.NewStore(t.TempDir())
	require.NoError(t, err)
	return &baseIdentityClient{
		contentStore: store,
		imageStore:   &baseIdentityImageStore{records: make(map[string]images.Image)},
		images:       make(map[string]containerd.Image),
	}
}

func (c *baseIdentityClient) ContentStore() content.Store {
	return c.contentStore
}

func (c *baseIdentityClient) ImageService() images.Store {
	return c.imageStore
}

func (c *baseIdentityClient) SnapshotService(string) snapshots.Snapshotter {
	return c.snapshotter
}

func (c *baseIdentityClient) Close() error {
	return nil
}

func (c *baseIdentityClient) GetImage(_ context.Context, name string) (containerd.Image, error) {
	image, ok := c.images[name]
	if !ok {
		return nil, fmt.Errorf("image %s: %w", name, errdefs.ErrNotFound)
	}
	return image, nil
}

type unpackingImage struct {
	record      images.Image
	store       content.Store
	snapshotter *markerSnapshotter
	chainID     string
	unpackCalls int
}

func (i *unpackingImage) Name() string               { return i.record.Name }
func (i *unpackingImage) Target() ocispec.Descriptor { return i.record.Target }
func (i *unpackingImage) Labels() map[string]string  { return i.record.Labels }
func (i *unpackingImage) Unpack(_ context.Context, _ string, _ ...containerd.UnpackOpt) error {
	i.unpackCalls++
	i.snapshotter.infos[i.chainID] = snapshots.Info{Name: i.chainID, Kind: snapshots.KindCommitted}
	return nil
}
func (i *unpackingImage) RootFS(context.Context) ([]digest.Digest, error)              { return nil, nil }
func (i *unpackingImage) Size(context.Context) (int64, error)                          { return 0, nil }
func (i *unpackingImage) Usage(context.Context, ...containerd.UsageOpt) (int64, error) { return 0, nil }
func (i *unpackingImage) Config(context.Context) (ocispec.Descriptor, error) {
	return ocispec.Descriptor{}, nil
}
func (i *unpackingImage) IsUnpacked(context.Context, string) (bool, error) {
	return i.unpackCalls > 0, nil
}
func (i *unpackingImage) ContentStore() content.Store                 { return i.store }
func (i *unpackingImage) Metadata() images.Image                      { return i.record }
func (i *unpackingImage) Platform() platforms.MatchComparer           { return nil }
func (i *unpackingImage) Spec(context.Context) (ocispec.Image, error) { return ocispec.Image{}, nil }

type baseIdentityImageStore struct {
	images.Store
	records map[string]images.Image
	gets    []string
}

func (s *baseIdentityImageStore) Get(_ context.Context, name string) (images.Image, error) {
	s.gets = append(s.gets, name)
	record, ok := s.records[name]
	if !ok {
		return images.Image{}, fmt.Errorf("image %s: %w", name, errdefs.ErrNotFound)
	}
	return record, nil
}

func publishedBaseFixture(
	t *testing.T,
	markerPlatform ocispec.Platform,
	declaredBase string,
) (*ContainerdRuntime, *baseIdentityClient, ctldapi.RootFSInfo, rootfshead.BaseIdentity, []byte) {
	t.Helper()
	client := newBaseIdentityClient(t)
	expected := rootfshead.BaseIdentity{
		ImageReference: "docker.io/sandbox0ai/base:latest",
		ManifestDigest: digest.FromString("registry image index").String(),
		ChainID:        digest.FromString("canonical base chain").String(),
		OS:             "linux",
		Architecture:   "amd64",
	}
	if declaredBase == "" {
		declaredBase = expected.ChainID
	}
	markerConfig := ocispec.Image{
		Platform: markerPlatform,
		Config:   ocispec.ImageConfig{Env: []string{"PATH=/bin"}},
		RootFS:   ocispec.RootFS{Type: "layers", DiffIDs: []digest.Digest{digest.FromString("Head marker")}},
	}
	configDescriptor, configData := writeImageJSON(t, client.contentStore, ocispec.MediaTypeImageConfig, markerConfig)
	manifestDescriptor, _ := writeImageJSON(t, client.contentStore, ocispec.MediaTypeImageManifest, ocispec.Manifest{Config: configDescriptor})
	markerReference := rootfshead.LocalImageReference(manifestDescriptor.Digest.String())
	client.imageStore.records[markerReference] = images.Image{
		Name:   markerReference,
		Target: manifestDescriptor,
		Labels: map[string]string{rootFSHeadImageLabel: "head-1"},
	}
	snapshotter := newMarkerSnapshotter()
	snapshotter.infos["head-marker-chain"] = snapshots.Info{
		Name: "head-marker-chain", Kind: snapshots.KindCommitted,
		Labels: map[string]string{rootfshead.LabelBaseChainID: declaredBase},
	}
	client.snapshotter = snapshotter
	info := ctldapi.RootFSInfo{
		BaseImageRef:   markerReference,
		Snapshotter:    rootfshead.SnapshotterName,
		SnapshotParent: "head-marker-chain",
	}
	return NewContainerdRuntime(ContainerdRuntimeConfig{ContainerdClient: client}), client, info, expected, configData
}

func writeImageJSON(t *testing.T, store content.Store, mediaType string, value any) (ocispec.Descriptor, []byte) {
	t.Helper()
	payload, err := json.Marshal(value)
	require.NoError(t, err)
	descriptor := ocispec.Descriptor{
		MediaType: mediaType,
		Digest:    digest.FromBytes(payload),
		Size:      int64(len(payload)),
	}
	require.NoError(t, content.WriteBlob(context.Background(), store, "test-"+descriptor.Digest.Encoded(), bytes.NewReader(payload), descriptor))
	return descriptor, payload
}
