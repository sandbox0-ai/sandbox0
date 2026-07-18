package templateimage

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	managerregistry "github.com/sandbox0-ai/sandbox0/manager/pkg/registry"
)

// ObjectReader opens immutable rootfs layer objects.
type ObjectReader interface {
	Get(key string, off, limit int64) (io.ReadCloser, error)
}

// CredentialProvider returns registry credentials scoped to a team and target
// repository.
type CredentialProvider interface {
	GetPushCredentials(ctx context.Context, req managerregistry.PushCredentialsRequest) (*managerregistry.Credential, error)
}

// Layer describes one immutable rootfs diff stored by manager.
type Layer struct {
	ID        string
	ObjectKey string
	MediaType string
	Digest    string
	DiffID    string
	Size      int64
}

// BuildRequest contains all immutable inputs required to publish one template
// image. Platform must be captured from the source sandbox; manager's own
// platform must never be used as an implicit fallback.
type BuildRequest struct {
	BuildID         string
	TeamID          string
	TemplateID      string
	SourceSandboxID string
	BaseImageRef    string
	BaseImageDigest string
	Platform        ocispec.Platform
	Layers          []Layer
	CreatedAt       time.Time
}

// Result identifies the published single-platform image.
type Result struct {
	PushReference  string
	PullReference  string
	ManifestDigest digest.Digest
	Platform       ocispec.Platform
}

func (r BuildRequest) validate(allowLegacyDiffID bool) error {
	switch {
	case strings.TrimSpace(r.BuildID) == "":
		return fmt.Errorf("build_id is required")
	case strings.TrimSpace(r.TeamID) == "":
		return fmt.Errorf("team_id is required")
	case strings.TrimSpace(r.TemplateID) == "":
		return fmt.Errorf("template_id is required")
	case strings.TrimSpace(r.BaseImageRef) == "":
		return fmt.Errorf("base image reference is required")
	case strings.TrimSpace(r.BaseImageDigest) == "":
		return fmt.Errorf("base image digest is required")
	case strings.TrimSpace(r.Platform.OS) == "":
		return fmt.Errorf("source platform os is required")
	case strings.TrimSpace(r.Platform.Architecture) == "":
		return fmt.Errorf("source platform architecture is required")
	case len(r.Layers) == 0:
		return fmt.Errorf("rootfs layer chain is empty")
	}
	if _, err := digest.Parse(strings.TrimSpace(r.BaseImageDigest)); err != nil {
		return fmt.Errorf("parse base image digest: %w", err)
	}
	for i, layer := range r.Layers {
		if strings.TrimSpace(layer.ObjectKey) == "" {
			return fmt.Errorf("rootfs layer %d object key is required", i)
		}
		if strings.TrimSpace(layer.MediaType) == "" {
			return fmt.Errorf("rootfs layer %d media type is required", i)
		}
		if layer.Size < 0 {
			return fmt.Errorf("rootfs layer %d size must not be negative", i)
		}
		if _, err := digest.Parse(strings.TrimSpace(layer.Digest)); err != nil {
			return fmt.Errorf("parse rootfs layer %d digest: %w", i, err)
		}
		if strings.TrimSpace(layer.DiffID) == "" && allowLegacyDiffID {
			continue
		}
		if _, err := digest.Parse(strings.TrimSpace(layer.DiffID)); err != nil {
			return fmt.Errorf("parse rootfs layer %d diff_id: %w", i, err)
		}
	}
	return nil
}
