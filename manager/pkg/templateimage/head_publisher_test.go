package templateimage

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/containerd/containerd/v2/core/remotes"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	managerconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	managerregistry "github.com/sandbox0-ai/sandbox0/manager/pkg/registry"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPublishHeadPushesOnlyMetadataMarkerWithoutBaseOrRootFSPayload(t *testing.T) {
	t.Parallel()
	baseLayer := []byte("base-layer")
	baseLayerDesc := descriptorFromBytes(ocispec.MediaTypeImageLayer, baseLayer)
	baseDiffID := digest.FromString("base-diff")
	baseConfig := ocispec.Image{
		Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"},
		RootFS:   ocispec.RootFS{Type: "layers", DiffIDs: []digest.Digest{baseDiffID}},
		Config:   ocispec.ImageConfig{WorkingDir: "/workspace"},
	}
	baseConfigBytes := mustJSON(t, baseConfig)
	baseConfigDesc := descriptorFromBytes(ocispec.MediaTypeImageConfig, baseConfigBytes)
	baseManifest := ocispec.Manifest{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: ocispec.MediaTypeImageManifest,
		Config:    baseConfigDesc,
		Layers:    []ocispec.Descriptor{baseLayerDesc},
	}
	baseManifestBytes := mustJSON(t, baseManifest)
	baseManifestDesc := descriptorFromBytes(ocispec.MediaTypeImageManifest, baseManifestBytes)
	source := &fakeResolver{
		root: baseManifestDesc,
		fetcher: &fakeFetcher{blobs: map[digest.Digest][]byte{
			baseManifestDesc.Digest: baseManifestBytes,
			baseConfigDesc.Digest:   baseConfigBytes,
			baseLayerDesc.Digest:    baseLayer,
		}},
	}
	targetPusher := newFakePusher()
	target := &fakeResolver{pusher: targetPusher}
	credentials := &fakeCredentialProvider{credential: &managerregistry.Credential{
		Provider:     "builtin",
		PushRegistry: "registry.example/team",
		PullRegistry: "registry.example/team",
		Username:     "user",
		Password:     "password",
	}}
	publisher, err := NewPublisher(&fakeObjectReader{objects: map[string][]byte{}}, credentials, managerconfig.RegistryConfig{
		Provider:         "builtin",
		InternalRegistry: "registry.internal:5000",
	})
	require.NoError(t, err)
	publisher.newResolver = func(options resolverOptions) remotes.Resolver {
		if options.purpose == resolverPurposeTarget {
			return target
		}
		return source
	}
	headDigest := digest.FromString("persisted-rootfs-head")
	reference := rootfshead.HeadReference{
		Version: rootfshead.Version,
		HeadID:  "layer-1",
		Manifest: rootfshead.Object{
			Key:       "rootfs/head.json.gz",
			Digest:    headDigest.String(),
			Size:      512,
			MediaType: rootfshead.HeadMediaType,
		},
	}

	result, err := publisher.PublishHead(context.Background(), HeadRequest{
		TeamID:          "team-1",
		SandboxID:       "sandbox-1",
		BaseImageRef:    "base:latest",
		BaseImageDigest: baseManifestDesc.Digest.String(),
		Platform:        ocispec.Platform{OS: "linux", Architecture: "amd64"},
		Reference:       reference,
		CreatedAt:       time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC),
	})

	require.NoError(t, err)
	assert.Contains(t, targetPusher.reference, "/rootfs-heads:head-")
	assert.Contains(t, result.PullReference, "/rootfs-heads@sha256:")
	manifestPayload := targetPusher.blobs[result.ManifestDigest]
	var manifest ocispec.Manifest
	require.NoError(t, json.Unmarshal(manifestPayload, &manifest))
	require.Len(t, manifest.Layers, 1)
	marker := manifest.Layers[0]
	markerPayload := targetPusher.blobs[marker.Digest]
	assert.Equal(t, int64(len(markerPayload)), marker.Size)
	assert.Equal(t, digest.FromBytes(markerPayload), marker.Digest)
	decoded, err := rootfshead.DecodeHeadAnnotation(marker.Annotations[rootfshead.AnnotationHead])
	require.NoError(t, err)
	assert.Equal(t, reference, decoded)
	markerReference, err := rootfshead.DecodeMarker(bytes.NewReader(markerPayload))
	require.NoError(t, err)
	assert.Equal(t, reference, markerReference)
	assert.NotEqual(t, digest.FromBytes(make([]byte, 1024)), marker.Digest)
	assert.NotContains(t, targetPusher.blobs, baseLayerDesc.Digest, "base payload must be reused from the warm node snapshot")
	assert.NotContains(t, targetPusher.blobs, headDigest, "Merkle head payload must stay in object storage")

	var publishedConfig ocispec.Image
	require.NoError(t, json.Unmarshal(targetPusher.blobs[manifest.Config.Digest], &publishedConfig))
	assert.Equal(t, []digest.Digest{marker.Digest}, publishedConfig.RootFS.DiffIDs)
	assert.Equal(t, "/workspace", publishedConfig.Config.WorkingDir)
	require.Len(t, credentials.requests, 1)
	assert.Equal(t, "rootfs-heads", credentials.requests[0].TargetImage[:len("rootfs-heads")])
}
