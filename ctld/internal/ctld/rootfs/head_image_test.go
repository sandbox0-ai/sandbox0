package rootfs

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/containerd/containerd/v2/core/content"
	localcontent "github.com/containerd/containerd/v2/plugins/content/local"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/stretchr/testify/require"
)

func TestProtectRootFSHeadImageContentKeepsConfigAndMarkerReachable(t *testing.T) {
	ctx := context.Background()
	store, err := localcontent.NewLabeledStore(t.TempDir(), newTestLabelStore())
	require.NoError(t, err)

	head := rootfshead.HeadReference{
		Version: rootfshead.Version,
		HeadID:  "head-1",
		Manifest: rootfshead.Object{
			Key:       "sandbox-rootfs/filesystems/fs/heads/sha256/head",
			Digest:    digest.FromString("head").String(),
			Size:      128,
			MediaType: rootfshead.HeadMediaType,
		},
	}
	baseConfig, err := json.Marshal(ocispec.Image{
		Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"},
		RootFS:   ocispec.RootFS{Type: "layers"},
	})
	require.NoError(t, err)
	_, envelope, err := rootfshead.ComposeImage(head, baseConfig)
	require.NoError(t, err)
	marker, err := rootfshead.EncodeMarker(head)
	require.NoError(t, err)

	var manifest ocispec.Manifest
	require.NoError(t, json.Unmarshal(envelope.ManifestData, &manifest))
	blobs := []struct {
		descriptor ocispec.Descriptor
		payload    []byte
	}{
		{descriptor: manifest.Layers[0], payload: marker},
		{descriptor: envelope.Config, payload: envelope.ConfigData},
		{descriptor: envelope.Manifest, payload: envelope.ManifestData},
	}
	for _, blob := range blobs {
		require.NoError(t, content.WriteBlob(ctx, store, blob.descriptor.Digest.String(), bytes.NewReader(blob.payload), blob.descriptor))
	}

	require.NoError(t, protectRootFSHeadImageContent(ctx, store, envelope.Manifest))
	info, err := store.Info(ctx, envelope.Manifest.Digest)
	require.NoError(t, err)
	require.Equal(t, envelope.Config.Digest.String(), info.Labels["containerd.io/gc.ref.content.config"])
	require.Equal(t, manifest.Layers[0].Digest.String(), info.Labels["containerd.io/gc.ref.content.l.0"])
}

type testLabelStore struct {
	mu     sync.Mutex
	labels map[digest.Digest]map[string]string
}

func newTestLabelStore() *testLabelStore {
	return &testLabelStore{labels: make(map[digest.Digest]map[string]string)}
}

func (s *testLabelStore) Get(value digest.Digest) (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneTestLabels(s.labels[value]), nil
}

func (s *testLabelStore) Set(value digest.Digest, labels map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.labels[value] = cloneTestLabels(labels)
	return nil
}

func (s *testLabelStore) Update(value digest.Digest, updates map[string]string) (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	labels := cloneTestLabels(s.labels[value])
	if labels == nil {
		labels = make(map[string]string)
	}
	for key, update := range updates {
		if update == "" {
			delete(labels, key)
			continue
		}
		labels[key] = update
	}
	s.labels[value] = labels
	return cloneTestLabels(labels), nil
}

func cloneTestLabels(labels map[string]string) map[string]string {
	if labels == nil {
		return nil
	}
	clone := make(map[string]string, len(labels))
	for key, value := range labels {
		clone[key] = value
	}
	return clone
}
