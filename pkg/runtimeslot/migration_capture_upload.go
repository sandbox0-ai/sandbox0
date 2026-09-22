package runtimeslot

import (
	"fmt"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
)

// MigrationCaptureUpload is the optional immutable regional object reservation
// inside the existing staging command. The comparable zero value preserves old
// command digests through omitzero. Staging alone never authorizes a producer;
// the exact source must additionally own its durable capture intent.
type MigrationCaptureUpload struct {
	Version             int    `json:"version"`
	TeamID              string `json:"team_id"`
	CompatibilityDigest string `json:"compatibility_digest"`
	CPUFeaturesDigest   string `json:"cpu_features_digest"`
	MaxBytes            int64  `json:"max_bytes"`
	ScopeDigest         string `json:"scope_digest"`
}

func NewMigrationCaptureUpload(source MigrationCaptureRequest, teamID, compatibilityDigest, cpuFeaturesDigest string, imageBytes int64) (MigrationCaptureUpload, error) {
	bytes, err := MigrationCaptureUploadBytes(imageBytes)
	if err != nil {
		return MigrationCaptureUpload{}, err
	}
	g := MigrationCaptureUpload{Version: 1, TeamID: teamID, CompatibilityDigest: compatibilityDigest, CPUFeaturesDigest: cpuFeaturesDigest, MaxBytes: bytes}
	scope, err := g.sourceScope(source)
	if err != nil {
		return MigrationCaptureUpload{}, err
	}
	g.ScopeDigest, err = scope.Digest()
	return g, err
}

// MigrationCaptureUploadBytes bounds one pass over tentative full ranges plus
// all final ranges and short-file tails. Each distinct object costs a whole
// chunk slot, so even discarded speculative pages remain charged.
func MigrationCaptureUploadBytes(imageBytes int64) (int64, error) {
	if imageBytes <= 0 || imageBytes > runtimecheckpoint.MaxImageBytes {
		return 0, fmt.Errorf("capture upload image budget exceeds limit")
	}
	chunk := int64(runtimecheckpoint.ChunkBytes)
	bytes := (2*((imageBytes+chunk-1)/chunk) + runtimecheckpoint.MaxFiles) * chunk
	if bytes > runtimecheckpoint.MaxImageBytes {
		return 0, fmt.Errorf("capture upload object budget exceeds limit")
	}
	return bytes, nil
}

// ReservedBytes additionally charges bounded encryption and metadata overhead.
// This amount remains regionally reserved until exact scope GC has completed.
func (g MigrationCaptureUpload) ReservedBytes() int64 {
	return g.MaxBytes + g.MaxBytes/32 + 2*runtimecheckpoint.MaxManifestBytes
}

func (g MigrationCaptureUpload) Scope(source MigrationCaptureRequest) (runtimecheckpoint.CaptureScope, error) {
	scope, err := g.sourceScope(source)
	if err != nil {
		return runtimecheckpoint.CaptureScope{}, err
	}
	want, err := scope.Digest()
	if err != nil || want != g.ScopeDigest {
		return runtimecheckpoint.CaptureScope{}, fmt.Errorf("capture upload changed source scope")
	}
	return scope, nil
}

func (g MigrationCaptureUpload) sourceScope(source MigrationCaptureRequest) (runtimecheckpoint.CaptureScope, error) {
	if err := source.Validate(); err != nil {
		return runtimecheckpoint.CaptureScope{}, err
	}
	if g.Version != 1 || g.MaxBytes < runtimecheckpoint.ChunkBytes || g.MaxBytes > runtimecheckpoint.MaxImageBytes || g.MaxBytes%runtimecheckpoint.ChunkBytes != 0 {
		return runtimecheckpoint.CaptureScope{}, fmt.Errorf("invalid regional capture upload grant")
	}
	return runtimecheckpoint.NewCaptureScope(runtimecheckpoint.Binding{
		OperationID: source.OperationID, SandboxID: source.SandboxID, TeamID: g.TeamID,
		SourceBindingDigest: "sha256:" + source.BindingDigest, AssignmentRevision: "sha256:" + source.AssignmentRevision,
		RuntimeCompatibilityDigest: g.CompatibilityDigest, CPUFeaturesDigest: g.CPUFeaturesDigest,
	})
}

func (g MigrationCaptureUpload) ValidateFor(source MigrationCaptureRequest, imageBytes int64) error {
	bytes, err := MigrationCaptureUploadBytes(imageBytes)
	if err != nil {
		return err
	}
	if g.MaxBytes != bytes {
		return fmt.Errorf("capture upload changed its admitted image budget")
	}
	_, err = g.Scope(source)
	return err
}
