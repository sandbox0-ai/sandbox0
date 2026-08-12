package rootfshead

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

const (
	imageEnvelopeVersion = 3
	localImageRepository = "sandbox0.local/rootfs-heads"

	ImageEnvelopeMediaType = "application/vnd.sandbox0.rootfs.head-image.v3+json"
)

type ImageReference struct {
	Name           string           `json:"name"`
	ManifestDigest string           `json:"manifest_digest"`
	Platform       ocispec.Platform `json:"platform"`
	Marker         Object           `json:"marker"`
	Envelope       Object           `json:"envelope"`
}

func (r ImageReference) Validate() error {
	manifestDigest, err := digest.Parse(strings.TrimSpace(r.ManifestDigest))
	if err != nil || manifestDigest.Algorithm() != digest.Canonical {
		return fmt.Errorf("invalid rootfs head image digest %q", r.ManifestDigest)
	}
	if strings.TrimSpace(r.Name) != LocalImageReference(manifestDigest.String()) {
		return fmt.Errorf("rootfs head image reference %q is not node-local and digest-pinned", r.Name)
	}
	if strings.TrimSpace(r.Platform.OS) == "" || strings.TrimSpace(r.Platform.Architecture) == "" {
		return fmt.Errorf("rootfs head image platform is required")
	}
	if err := r.Marker.Validate(MarkerMediaType); err != nil {
		return err
	}
	return r.Envelope.Validate(ImageEnvelopeMediaType)
}

type ImageEnvelope struct {
	Version      int                `json:"version"`
	Config       ocispec.Descriptor `json:"config"`
	ConfigData   []byte             `json:"config_data"`
	Manifest     ocispec.Descriptor `json:"manifest"`
	ManifestData []byte             `json:"manifest_data"`
}

type ComposedImage struct {
	Reference       ImageReference
	Envelope        ImageEnvelope
	MarkerPayload   []byte
	EnvelopePayload []byte
}

func ComposeImage(prefix string, reference HeadReference, baseConfig []byte) (ComposedImage, error) {
	if err := reference.Validate(); err != nil {
		return ComposedImage{}, err
	}
	if err := ValidateObjectScope(prefix, reference.Manifest); err != nil {
		return ComposedImage{}, err
	}
	var config ocispec.Image
	if len(baseConfig) == 0 {
		return ComposedImage{}, fmt.Errorf("rootfs base image config is required")
	}
	if err := json.Unmarshal(baseConfig, &config); err != nil {
		return ComposedImage{}, fmt.Errorf("decode rootfs base image config: %w", err)
	}
	if strings.TrimSpace(config.OS) == "" || strings.TrimSpace(config.Architecture) == "" {
		return ComposedImage{}, fmt.Errorf("rootfs base image platform is required")
	}

	markerObject, markerPayload, err := MarkerObject(prefix, reference)
	if err != nil {
		return ComposedImage{}, err
	}
	annotation, err := EncodeHeadAnnotation(reference)
	if err != nil {
		return ComposedImage{}, err
	}
	markerDescriptor := descriptorFromBytes(ocispec.MediaTypeImageLayer, markerPayload)
	markerDescriptor.Annotations = map[string]string{AnnotationHead: annotation}

	config.RootFS.Type = "layers"
	config.RootFS.DiffIDs = []digest.Digest{markerDescriptor.Digest}
	config.History = []ocispec.History{{
		CreatedBy: "sandbox0 immutable persistent rootfs head v3",
		Comment:   "rootfs head " + strings.TrimSpace(reference.HeadID),
	}}
	configData, err := json.Marshal(config)
	if err != nil {
		return ComposedImage{}, fmt.Errorf("encode rootfs head image config: %w", err)
	}
	configDescriptor := descriptorFromBytes(ocispec.MediaTypeImageConfig, configData)
	manifest := ocispec.Manifest{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: ocispec.MediaTypeImageManifest,
		Config:    configDescriptor,
		Layers:    []ocispec.Descriptor{markerDescriptor},
	}
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		return ComposedImage{}, fmt.Errorf("encode rootfs head image manifest: %w", err)
	}
	manifestDescriptor := descriptorFromBytes(ocispec.MediaTypeImageManifest, manifestData)
	envelope := ImageEnvelope{
		Version:      imageEnvelopeVersion,
		Config:       configDescriptor,
		ConfigData:   configData,
		Manifest:     manifestDescriptor,
		ManifestData: manifestData,
	}
	envelopePayload, err := json.Marshal(envelope)
	if err != nil {
		return ComposedImage{}, fmt.Errorf("encode rootfs head image envelope: %w", err)
	}
	envelopeDigest := digest.FromBytes(envelopePayload)
	envelopeKey, err := ObjectKey(prefix, ImageEnvelopeMediaType, envelopeDigest.String())
	if err != nil {
		return ComposedImage{}, err
	}
	envelopeObject := Object{
		Key:       envelopeKey,
		Digest:    envelopeDigest.String(),
		Size:      int64(len(envelopePayload)),
		MediaType: ImageEnvelopeMediaType,
	}
	image := ImageReference{
		Name:           LocalImageReference(manifestDescriptor.Digest.String()),
		ManifestDigest: manifestDescriptor.Digest.String(),
		Platform:       config.Platform,
		Marker:         markerObject,
		Envelope:       envelopeObject,
	}
	composed := ComposedImage{
		Reference:       image,
		Envelope:        envelope,
		MarkerPayload:   markerPayload,
		EnvelopePayload: envelopePayload,
	}
	if err := ValidateComposedImage(prefix, reference, composed); err != nil {
		return ComposedImage{}, err
	}
	return composed, nil
}

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

func ValidateComposedImage(prefix string, reference HeadReference, composed ComposedImage) error {
	if err := reference.Validate(); err != nil {
		return err
	}
	if err := composed.Reference.Validate(); err != nil {
		return err
	}
	if err := ValidateObjectScope(prefix, composed.Reference.Marker); err != nil {
		return err
	}
	if err := ValidateObjectScope(prefix, composed.Reference.Envelope); err != nil {
		return err
	}
	markerDigest := digest.FromBytes(composed.MarkerPayload)
	if markerDigest.String() != composed.Reference.Marker.Digest || int64(len(composed.MarkerPayload)) != composed.Reference.Marker.Size {
		return fmt.Errorf("rootfs head marker payload does not match descriptor")
	}
	envelopeDigest := digest.FromBytes(composed.EnvelopePayload)
	if envelopeDigest.String() != composed.Reference.Envelope.Digest || int64(len(composed.EnvelopePayload)) != composed.Reference.Envelope.Size {
		return fmt.Errorf("rootfs head image envelope payload does not match descriptor")
	}
	decoded, err := DecodeImageEnvelope(composed.EnvelopePayload)
	if err != nil {
		return err
	}
	if decoded.Manifest.Digest.String() != composed.Reference.ManifestDigest {
		return fmt.Errorf("rootfs head image manifest digest does not match image reference")
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
	marker := manifest.Layers[0]
	if err := validateDescriptorBytes(marker, composed.MarkerPayload, ocispec.MediaTypeImageLayer); err != nil {
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
	if config.OS != composed.Reference.Platform.OS || config.Architecture != composed.Reference.Platform.Architecture || config.Variant != composed.Reference.Platform.Variant {
		return fmt.Errorf("rootfs head OCI config platform does not match image reference")
	}
	return nil
}

func LocalImageReference(manifestDigest string) string {
	value, err := digest.Parse(strings.TrimSpace(manifestDigest))
	if err != nil || value.Algorithm() != digest.Canonical {
		return ""
	}
	return localImageRepository + "@" + value.String()
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
