package rootfshead

import (
	"encoding/json"
	"testing"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComposeImageBuildsNodeLocalMarkerEnvelope(t *testing.T) {
	reference := testImageHeadReference()
	base := ocispec.Image{
		Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"},
		Config: ocispec.ImageConfig{
			User:       "1000:1000",
			WorkingDir: "/workspace",
			Env:        []string{"FROM_BASE=true"},
		},
		RootFS: ocispec.RootFS{Type: "layers", DiffIDs: []digest.Digest{digest.FromString("base")}},
	}
	baseData, err := json.Marshal(base)
	require.NoError(t, err)

	image, envelope, err := ComposeImage(reference, baseData)
	require.NoError(t, err)
	require.NoError(t, ValidateImage(reference, image, envelope))
	assert.Equal(t, LocalImageReference(image.ManifestDigest), image.Name)
	assert.Equal(t, ocispec.Platform{OS: "linux", Architecture: "amd64"}, image.Platform)

	var config ocispec.Image
	require.NoError(t, json.Unmarshal(envelope.ConfigData, &config))
	assert.Equal(t, "1000:1000", config.Config.User)
	assert.Equal(t, "/workspace", config.Config.WorkingDir)
	assert.Equal(t, []string{"FROM_BASE=true"}, config.Config.Env)
	require.Len(t, config.RootFS.DiffIDs, 1)

	object, payload, err := ImageEnvelopeObject(envelope)
	require.NoError(t, err)
	assert.Equal(t, ImageEnvelopeMediaType, object.MediaType)
	assert.Equal(t, digest.FromBytes(payload).String(), object.Digest)
	assert.Equal(t, int64(len(payload)), object.Size)
	wantKey, err := ImageEnvelopeObjectKey(image.ManifestDigest)
	require.NoError(t, err)
	assert.Equal(t, wantKey, object.Key)
}

func TestValidateImageRejectsMarkerForAnotherHead(t *testing.T) {
	reference := testImageHeadReference()
	baseData, err := json.Marshal(ocispec.Image{
		Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"},
		RootFS:   ocispec.RootFS{Type: "layers"},
	})
	require.NoError(t, err)
	image, envelope, err := ComposeImage(reference, baseData)
	require.NoError(t, err)

	other := reference
	other.HeadID = "other-head"
	err = ValidateImage(other, image, envelope)
	require.Error(t, err)
}

func testImageHeadReference() HeadReference {
	return HeadReference{
		Version: Version,
		HeadID:  "head-1",
		Manifest: Object{
			Key:       "sandbox-rootfs/filesystems/fs/heads/sha256/head",
			Digest:    digest.FromString("head").String(),
			Size:      128,
			MediaType: HeadMediaType,
		},
	}
}
