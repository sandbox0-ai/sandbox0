package rootfshead

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path"
	"strings"

	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

const (
	imageEnvelopeVersion = 1
	localImageRepository = "sandbox0.local/rootfs-heads"

	// ImageEnvelopeMediaType identifies the small OCI config and manifest bundle
	// used to reconstruct a rootfs head directly in a node's containerd.
	ImageEnvelopeMediaType = "application/vnd.sandbox0.rootfs.head-image.v1+json"
)

// ImageReference is the durable identity of one metadata-only OCI head image.
// Name is always a digest-pinned local reference; it is never pushed to or
// pulled from a registry.
type ImageReference struct {
	Name           string           `json:"name"`
	ManifestDigest string           `json:"manifest_digest"`
	Platform       ocispec.Platform `json:"platform"`
}

func (r ImageReference) Validate() error {
	digestValue, err := digest.Parse(strings.TrimSpace(r.ManifestDigest))
	if err != nil {
		return fmt.Errorf("invalid rootfs head image digest: %w", err)
	}
	if strings.TrimSpace(r.Name) != LocalImageReference(digestValue.String()) {
		return fmt.Errorf("rootfs head image reference %q is not node-local and digest-pinned", r.Name)
	}
	if strings.TrimSpace(r.Platform.OS) == "" || strings.TrimSpace(r.Platform.Architecture) == "" {
		return fmt.Errorf("rootfs head image platform is required")
	}
	return nil
}

// ImageEnvelope contains only the OCI config and manifest. The marker bytes
// are reconstructed from the immutable HeadReference and verified against the
// manifest before they are written to containerd.
type ImageEnvelope struct {
	Version      int                `json:"version"`
	Config       ocispec.Descriptor `json:"config"`
	ConfigData   []byte             `json:"config_data"`
	Manifest     ocispec.Descriptor `json:"manifest"`
	ManifestData []byte             `json:"manifest_data"`
}

// ComposeImage builds a deterministic, filesystem-empty OCI image around one
// persistent rootfs head while preserving the base image's runtime config.
func ComposeImage(reference HeadReference, baseConfig []byte) (ImageReference, ImageEnvelope, error) {
	if err := reference.Validate(); err != nil {
		return ImageReference{}, ImageEnvelope{}, err
	}
	var config ocispec.Image
	if len(baseConfig) == 0 {
		return ImageReference{}, ImageEnvelope{}, fmt.Errorf("rootfs base image config is required")
	}
	if err := json.Unmarshal(baseConfig, &config); err != nil {
		return ImageReference{}, ImageEnvelope{}, fmt.Errorf("decode rootfs base image config: %w", err)
	}
	if strings.TrimSpace(config.OS) == "" || strings.TrimSpace(config.Architecture) == "" {
		return ImageReference{}, ImageEnvelope{}, fmt.Errorf("rootfs base image platform is required")
	}

	annotation, err := EncodeHeadAnnotation(reference)
	if err != nil {
		return ImageReference{}, ImageEnvelope{}, err
	}
	markerData, err := EncodeMarker(reference)
	if err != nil {
		return ImageReference{}, ImageEnvelope{}, err
	}
	marker := descriptorFromBytes(ocispec.MediaTypeImageLayer, markerData)
	marker.Annotations = map[string]string{AnnotationHead: annotation}

	config.RootFS.Type = "layers"
	config.RootFS.DiffIDs = []digest.Digest{marker.Digest}
	config.History = []ocispec.History{{
		CreatedBy: "sandbox0 metadata-only persistent rootfs head",
		Comment:   "immutable head " + strings.TrimSpace(reference.HeadID),
	}}
	configData, err := json.Marshal(config)
	if err != nil {
		return ImageReference{}, ImageEnvelope{}, fmt.Errorf("encode rootfs head image config: %w", err)
	}
	configDescriptor := descriptorFromBytes(ocispec.MediaTypeImageConfig, configData)
	manifest := ocispec.Manifest{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: ocispec.MediaTypeImageManifest,
		Config:    configDescriptor,
		Layers:    []ocispec.Descriptor{marker},
	}
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		return ImageReference{}, ImageEnvelope{}, fmt.Errorf("encode rootfs head image manifest: %w", err)
	}
	manifestDescriptor := descriptorFromBytes(ocispec.MediaTypeImageManifest, manifestData)
	image := ImageReference{
		Name:           LocalImageReference(manifestDescriptor.Digest.String()),
		ManifestDigest: manifestDescriptor.Digest.String(),
		Platform:       config.Platform,
	}
	envelope := ImageEnvelope{
		Version:      imageEnvelopeVersion,
		Config:       configDescriptor,
		ConfigData:   configData,
		Manifest:     manifestDescriptor,
		ManifestData: manifestData,
	}
	if err := ValidateImage(reference, image, envelope); err != nil {
		return ImageReference{}, ImageEnvelope{}, err
	}
	return image, envelope, nil
}

// ImageEnvelopeObject returns the content-addressed S3 object for envelope.
// The key is derived from the OCI manifest digest so a selected node can find
// it from the digest-pinned local image reference alone.
func ImageEnvelopeObject(envelope ImageEnvelope) (Object, []byte, error) {
	payload, err := json.Marshal(envelope)
	if err != nil {
		return Object{}, nil, fmt.Errorf("encode rootfs head image envelope: %w", err)
	}
	manifestDigest, err := digest.Parse(strings.TrimSpace(envelope.Manifest.Digest.String()))
	if err != nil {
		return Object{}, nil, fmt.Errorf("invalid rootfs head manifest digest: %w", err)
	}
	objectDigest := digest.FromBytes(payload)
	object := Object{
		Key:       imageEnvelopeObjectKey(manifestDigest),
		Digest:    objectDigest.String(),
		Size:      int64(len(payload)),
		MediaType: ImageEnvelopeMediaType,
	}
	if err := object.Validate(ImageEnvelopeMediaType); err != nil {
		return Object{}, nil, err
	}
	return object, payload, nil
}

// DecodeImageEnvelope validates the encoded envelope's own descriptors.
func DecodeImageEnvelope(payload []byte) (ImageEnvelope, error) {
	var envelope ImageEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return ImageEnvelope{}, fmt.Errorf("decode rootfs head image envelope: %w", err)
	}
	if envelope.Version != imageEnvelopeVersion {
		return ImageEnvelope{}, fmt.Errorf("unsupported rootfs head image envelope version %d", envelope.Version)
	}
	if err := validateDescriptorBytes(envelope.Config, envelope.ConfigData, ocispec.MediaTypeImageConfig); err != nil {
		return ImageEnvelope{}, fmt.Errorf("rootfs head image config: %w", err)
	}
	if err := validateDescriptorBytes(envelope.Manifest, envelope.ManifestData, ocispec.MediaTypeImageManifest); err != nil {
		return ImageEnvelope{}, fmt.Errorf("rootfs head image manifest: %w", err)
	}
	return envelope, nil
}

// ValidateImage binds the head reference, local image identity, marker layer,
// config, and manifest into one tamper-evident unit.
func ValidateImage(reference HeadReference, image ImageReference, envelope ImageEnvelope) error {
	if err := reference.Validate(); err != nil {
		return err
	}
	decoded, err := DecodeImageEnvelope(mustMarshalEnvelope(envelope))
	if err != nil {
		return err
	}
	if err := image.Validate(); err != nil {
		return err
	}
	manifestDigest, _ := digest.Parse(strings.TrimSpace(image.ManifestDigest))
	if decoded.Manifest.Digest != manifestDigest {
		return fmt.Errorf("rootfs head image manifest digest %s does not match %s", decoded.Manifest.Digest, manifestDigest)
	}
	if strings.TrimSpace(image.Name) != LocalImageReference(manifestDigest.String()) {
		return fmt.Errorf("rootfs head image reference %q is not the expected node-local reference", image.Name)
	}
	var manifest ocispec.Manifest
	if err := json.Unmarshal(decoded.ManifestData, &manifest); err != nil {
		return fmt.Errorf("decode rootfs head OCI manifest: %w", err)
	}
	if manifest.Config.Digest != decoded.Config.Digest || manifest.Config.Size != decoded.Config.Size || manifest.Config.MediaType != decoded.Config.MediaType {
		return fmt.Errorf("rootfs head OCI manifest has a mismatched config descriptor")
	}
	if len(manifest.Layers) != 1 {
		return fmt.Errorf("rootfs head OCI manifest has %d layers, expected 1", len(manifest.Layers))
	}
	markerData, err := EncodeMarker(reference)
	if err != nil {
		return err
	}
	marker := manifest.Layers[0]
	if err := validateDescriptorBytes(marker, markerData, ocispec.MediaTypeImageLayer); err != nil {
		return fmt.Errorf("rootfs head OCI marker: %w", err)
	}
	annotation, err := EncodeHeadAnnotation(reference)
	if err != nil {
		return err
	}
	if marker.Annotations[AnnotationHead] != annotation {
		return fmt.Errorf("rootfs head OCI marker annotation does not match head reference")
	}
	var config ocispec.Image
	if err := json.Unmarshal(decoded.ConfigData, &config); err != nil {
		return fmt.Errorf("decode rootfs head OCI config: %w", err)
	}
	if len(config.RootFS.DiffIDs) != 1 || config.RootFS.DiffIDs[0] != marker.Digest {
		return fmt.Errorf("rootfs head OCI config has an invalid marker diff id")
	}
	if config.OS != image.Platform.OS || config.Architecture != image.Platform.Architecture || config.Variant != image.Platform.Variant {
		return fmt.Errorf("rootfs head OCI config platform does not match image reference")
	}
	return nil
}

// LocalImageReference returns the name installed into a selected node's
// containerd image service.
func LocalImageReference(manifestDigest string) string {
	value, err := digest.Parse(strings.TrimSpace(manifestDigest))
	if err != nil {
		return ""
	}
	return localImageRepository + "@" + value.String()
}

// ImageEnvelopeObjectKey resolves the S3 envelope key from an OCI manifest.
func ImageEnvelopeObjectKey(manifestDigest string) (string, error) {
	value, err := digest.Parse(strings.TrimSpace(manifestDigest))
	if err != nil {
		return "", fmt.Errorf("invalid rootfs head image digest %q: %w", manifestDigest, err)
	}
	return imageEnvelopeObjectKey(value), nil
}

func imageEnvelopeObjectKey(value digest.Digest) string {
	return path.Join("sandbox-rootfs", "images", value.Algorithm().String(), value.Encoded()+".json")
}

func descriptorFromBytes(mediaType string, payload []byte) ocispec.Descriptor {
	return ocispec.Descriptor{MediaType: mediaType, Digest: digest.FromBytes(payload), Size: int64(len(payload))}
}

func validateDescriptorBytes(descriptor ocispec.Descriptor, payload []byte, mediaType string) error {
	if descriptor.MediaType != mediaType {
		return fmt.Errorf("media type %q does not match %q", descriptor.MediaType, mediaType)
	}
	if descriptor.Size != int64(len(payload)) {
		return fmt.Errorf("size %d does not match payload size %d", descriptor.Size, len(payload))
	}
	if descriptor.Digest != digest.FromBytes(payload) {
		return fmt.Errorf("digest %s does not match payload", descriptor.Digest)
	}
	return nil
}

func mustMarshalEnvelope(envelope ImageEnvelope) []byte {
	payload, _ := json.Marshal(envelope)
	return bytes.Clone(payload)
}
