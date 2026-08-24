package templatebuild

import (
	"errors"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	templatepkg "github.com/sandbox0-ai/sandbox0/pkg/template"
)

// CaptureMetadataVersion is the runtime-native block-COW handoff version.
const CaptureMetadataVersion = templatepkg.TemplateBuildCaptureVersion

// ErrCaptureInvalid marks capture state that cannot be published safely.
var ErrCaptureInvalid = errors.New("invalid template build capture")

// CaptureMetadata is the durable handoff between checkpointing and publishing.
// It pins both the immutable rootfs head and the source runtime platform.
type CaptureMetadata struct {
	Version            int              `json:"version"`
	SnapshotID         string           `json:"snapshot_id"`
	Platform           ocispec.Platform `json:"platform"`
	CapturedAt         time.Time        `json:"captured_at"`
	StorageFormat      string           `json:"storage_format"`
	HeadGenerationID   string           `json:"head_generation_id"`
	SourceOCIDigest    string           `json:"source_oci_digest"`
	BaseArtifactDigest string           `json:"base_artifact_digest"`
	FormatGeneration   int              `json:"format_generation"`
}

type TemplateBuildCaptureMetadata = CaptureMetadata

var errTemplateBuildCaptureInvalid = ErrCaptureInvalid
