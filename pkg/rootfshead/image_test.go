package rootfshead

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComposeImageIsDeterministicAndTeamScoped(t *testing.T) {
	prefix, err := TeamObjectPrefix("team-1")
	require.NoError(t, err)
	reference := HeadReference{Version: Version, HeadID: "head-1", Manifest: testObject(t, prefix, HeadMediaType, []byte("head"))}
	baseConfig, err := json.Marshal(ocispec.Image{
		Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"},
		Config:   ocispec.ImageConfig{Env: []string{"PATH=/bin"}, Entrypoint: []string{"/procd"}},
		RootFS:   ocispec.RootFS{Type: "layers", DiffIDs: []digest.Digest{digest.FromString("base")}},
	})
	require.NoError(t, err)

	first, err := ComposeImage(prefix, reference, baseConfig)
	require.NoError(t, err)
	second, err := ComposeImage(prefix, reference, baseConfig)
	require.NoError(t, err)
	assert.Equal(t, first, second)
	assert.NoError(t, ValidateObjectScope(prefix, first.Reference.Marker))
	assert.NoError(t, ValidateObjectScope(prefix, first.Reference.Envelope))
	assert.Equal(t, LocalImageReference(first.Reference.ManifestDigest), first.Reference.Name)
}

func TestMarkerRoundTripAndRejectsFilesystemEntries(t *testing.T) {
	prefix, err := TeamObjectPrefix("team-1")
	require.NoError(t, err)
	reference := HeadReference{Version: Version, HeadID: "head-1", Manifest: testObject(t, prefix, HeadMediaType, []byte("head"))}
	payload, err := EncodeMarker(reference)
	require.NoError(t, err)
	decoded, err := DecodeMarker(bytes.NewReader(payload))
	require.NoError(t, err)
	assert.Equal(t, reference, decoded)

	_, err = DecodeMarker(bytes.NewReader([]byte("not a tar")))
	assert.Error(t, err)
}

func TestComposeImageRejectsCrossTeamReference(t *testing.T) {
	sourcePrefix, err := TeamObjectPrefix("team-1")
	require.NoError(t, err)
	targetPrefix, err := TeamObjectPrefix("team-2")
	require.NoError(t, err)
	reference := HeadReference{Version: Version, HeadID: "head-1", Manifest: testObject(t, sourcePrefix, HeadMediaType, []byte("head"))}
	baseConfig, err := json.Marshal(ocispec.Image{Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"}})
	require.NoError(t, err)
	_, err = ComposeImage(targetPrefix, reference, baseConfig)
	assert.Error(t, err)
}
