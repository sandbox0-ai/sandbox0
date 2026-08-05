package templatebuild

import (
	"errors"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/templateimage"
)

// CaptureMetadataVersion is the durable template build handoff format version.
const CaptureMetadataVersion = 2

// ErrCaptureInvalid marks capture state that cannot be published safely.
var ErrCaptureInvalid = errors.New("invalid template build capture")

// CaptureMetadata is the durable handoff between checkpointing and publishing.
// It pins both the immutable rootfs head and the source runtime platform.
type CaptureMetadata struct {
	Version         int                   `json:"version"`
	SnapshotID      string                `json:"snapshot_id"`
	HeadID          string                `json:"head_id"`
	BaseImageRef    string                `json:"base_image_ref"`
	BaseImageDigest string                `json:"base_image_digest"`
	Platform        ocispec.Platform      `json:"platform"`
	Layers          []templateimage.Layer `json:"layers"`
	CapturedAt      time.Time             `json:"captured_at"`
}

type TemplateBuildCaptureMetadata = CaptureMetadata

const templateBuildCaptureMetadataVersion = CaptureMetadataVersion

var errTemplateBuildCaptureInvalid = ErrCaptureInvalid
