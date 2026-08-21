package templatebuild

import (
	"errors"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/templateimage"
	templatepkg "github.com/sandbox0-ai/sandbox0/pkg/template"
)

const (
	// CaptureMetadataVersion is the legacy OCI-layer handoff format version.
	CaptureMetadataVersion = templatepkg.TemplateBuildCaptureVersionOCI
	// BlockCaptureMetadataVersion is the runtime-native block-COW handoff
	// format. It must never be interpreted as an OCI layer chain.
	BlockCaptureMetadataVersion = templatepkg.TemplateBuildCaptureVersionBlockCOW
)

// ErrCaptureInvalid marks capture state that cannot be published safely.
var ErrCaptureInvalid = errors.New("invalid template build capture")

// CaptureMetadata is the durable handoff between checkpointing and publishing.
// It pins both the immutable rootfs head and the source runtime platform.
type CaptureMetadata struct {
	Version            int                   `json:"version"`
	SnapshotID         string                `json:"snapshot_id"`
	HeadLayerID        string                `json:"head_layer_id"`
	BaseImageRef       string                `json:"base_image_ref"`
	BaseImageDigest    string                `json:"base_image_digest"`
	Platform           ocispec.Platform      `json:"platform"`
	Layers             []templateimage.Layer `json:"layers"`
	CapturedAt         time.Time             `json:"captured_at"`
	StorageFormat      string                `json:"storage_format,omitempty"`
	HeadGenerationID   string                `json:"head_generation_id,omitempty"`
	SourceOCIDigest    string                `json:"source_oci_digest,omitempty"`
	BaseArtifactDigest string                `json:"base_artifact_digest,omitempty"`
	FormatGeneration   int                   `json:"format_generation,omitempty"`
}

type TemplateBuildCaptureMetadata = CaptureMetadata

const templateBuildCaptureMetadataVersion = CaptureMetadataVersion

var errTemplateBuildCaptureInvalid = ErrCaptureInvalid
