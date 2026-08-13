package template

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	TemplateImageRevisionStateResolving = "resolving"
	TemplateImageRevisionStateImporting = "importing"
	TemplateImageRevisionStateReady     = "ready"
	TemplateImageRevisionStateFailed    = "failed"
	PublicImageFSStorageScope           = rootfshead.PublicImageFSTeamID
)

// TemplateImageRevision is one immutable OCI resolution and S0FS ImageFS import.
type TemplateImageRevision struct {
	RevisionID           string
	TemplateID           string
	Scope                string
	TeamID               string
	SourceImage          string
	SpecHash             string
	ResolvedDigest       string
	PlatformOS           string
	PlatformArchitecture string
	PlatformVariant      string
	ImageFSHeadID        string
	OCIConfig            json.RawMessage
	State                string
	AttemptCount         int
	NextAttemptAt        time.Time
	LeaseOwner           string
	LeaseExpiresAt       time.Time
	Reason               string
	Message              string
	StartedAt            time.Time
	CompletedAt          time.Time
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

// ImageFSStorageScope returns the object-store isolation key for a revision.
func (r *TemplateImageRevision) ImageFSStorageScope() string {
	if r == nil || strings.TrimSpace(r.Scope) == "public" {
		return PublicImageFSStorageScope
	}
	return strings.TrimSpace(r.TeamID)
}

// NewTemplateImageRevision derives a stable identity from the complete spec.
// Moving tags remain pinned by the resolved digest; an explicit refresh API can
// introduce a new revision generation without mutating this immutable record.
func NewTemplateImageRevision(tpl *Template) (*TemplateImageRevision, error) {
	if tpl == nil {
		return nil, fmt.Errorf("template is required")
	}
	sourceImage := strings.TrimSpace(tpl.Spec.MainContainer.Image)
	if sourceImage == "" {
		return nil, fmt.Errorf("template image is required")
	}
	payload, err := json.Marshal(tpl.Spec)
	if err != nil {
		return nil, fmt.Errorf("marshal template image revision spec: %w", err)
	}
	hash := sha256.Sum256(payload)
	specHash := hex.EncodeToString(hash[:])
	identity := strings.Join([]string{tpl.Scope, tpl.TeamID, tpl.TemplateID, specHash}, "\x00")
	revisionHash := sha256.Sum256([]byte(identity))
	return &TemplateImageRevision{
		RevisionID:  "tir-" + hex.EncodeToString(revisionHash[:16]),
		TemplateID:  tpl.TemplateID,
		Scope:       tpl.Scope,
		TeamID:      tpl.TeamID,
		SourceImage: sourceImage,
		SpecHash:    specHash,
		State:       TemplateImageRevisionStateResolving,
	}, nil
}

// Status returns the public immutable revision projection.
func (r *TemplateImageRevision) Status() *v1alpha1.TemplateImageRevisionStatus {
	if r == nil {
		return nil
	}
	return &v1alpha1.TemplateImageRevisionStatus{
		RevisionID:     r.RevisionID,
		SourceImage:    r.SourceImage,
		ResolvedDigest: r.ResolvedDigest,
		Platform:       revisionPlatform(r),
		ImageFSHeadID:  r.ImageFSHeadID,
		State:          v1alpha1.TemplateImageRevisionState(r.State),
		Reason:         r.Reason,
		Message:        r.Message,
		StartedAt:      metaTimePointer(r.StartedAt),
		CompletedAt:    metaTimePointer(r.CompletedAt),
	}
}

func revisionPlatform(r *TemplateImageRevision) string {
	if r == nil || strings.TrimSpace(r.PlatformOS) == "" || strings.TrimSpace(r.PlatformArchitecture) == "" {
		return ""
	}
	platform := r.PlatformOS + "/" + r.PlatformArchitecture
	if strings.TrimSpace(r.PlatformVariant) != "" {
		platform += "/" + r.PlatformVariant
	}
	return platform
}

func metaTimePointer(value time.Time) *metav1.Time {
	if value.IsZero() {
		return nil
	}
	result := metav1.NewTime(value.UTC())
	return &result
}
