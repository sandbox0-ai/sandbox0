package main

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/legacyackmigration"
)

type validationSummary struct {
	Valid                        bool   `json:"valid"`
	Error                        string `json:"error,omitempty"`
	SandboxCount                 int    `json:"sandbox_count,omitempty"`
	LayerChainCount              int    `json:"layer_chain_count,omitempty"`
	PinnedBaseImageCount         int    `json:"pinned_base_image_count,omitempty"`
	InferredPlatformCount        int    `json:"inferred_platform_count,omitempty"`
	AdjustedSandboxCount         int    `json:"adjusted_sandbox_count,omitempty"`
	CompatibilityAdjustmentCount int    `json:"compatibility_adjustment_count,omitempty"`
}

type report struct {
	FormatVersion       int                                         `json:"format_version"`
	CapturedAt          time.Time                                   `json:"captured_at"`
	Mode                string                                      `json:"mode"`
	SessionID           string                                      `json:"session_id,omitempty"`
	SourceCatalogDigest string                                      `json:"source_catalog_digest,omitempty"`
	TargetClusterID     string                                      `json:"target_cluster_id"`
	Platform            ocispec.Platform                            `json:"platform"`
	Inventory           legacyackmigration.Inventory                `json:"inventory"`
	Validation          validationSummary                           `json:"validation"`
	Capture             *captureSummary                             `json:"capture,omitempty"`
	Preparation         *legacyackmigration.TargetPreparationResult `json:"preparation,omitempty"`
	Build               *buildSummary                               `json:"build,omitempty"`
	Commit              *legacyackmigration.TargetCommitResult      `json:"commit,omitempty"`
}

type captureSummary struct {
	CapturedAt time.Time  `json:"captured_at"`
	RetiredAt  *time.Time `json:"retired_at,omitempty"`
}

type buildSummary struct {
	Builds  int   `json:"builds"`
	Ready   int   `json:"ready"`
	Objects int   `json:"objects"`
	Bytes   int64 `json:"bytes"`
}

func catalogReport(
	opts options,
	platform ocispec.Platform,
	normalizeOptions legacyackmigration.NormalizeOptions,
	catalog *legacyackmigration.Catalog,
) (report, *legacyackmigration.NormalizedCatalog, error) {
	result := report{
		FormatVersion: 1, CapturedAt: time.Now().UTC(), Mode: opts.mode, SessionID: opts.sessionID,
		TargetClusterID: opts.targetClusterID, Platform: platform, Inventory: catalog.BuildInventory(),
	}
	digestValue, err := catalog.Digest()
	if err != nil {
		return result, nil, err
	}
	result.SourceCatalogDigest = digestValue
	normalized, validationErr := catalog.Normalize(normalizeOptions)
	if validationErr != nil {
		result.Validation.Error = validationErr.Error()
		return result, nil, validationErr
	}
	var adjustedSandboxes, adjustments int
	for _, sandbox := range normalized.Sandboxes {
		if len(sandbox.CompatibilityAdjustments) != 0 {
			adjustedSandboxes++
			adjustments += len(sandbox.CompatibilityAdjustments)
		}
	}
	result.Validation = validationSummary{
		Valid: true, SandboxCount: len(normalized.Sandboxes),
		LayerChainCount: len(normalized.LayerChains), PinnedBaseImageCount: len(normalized.PinnedImageRefs),
		InferredPlatformCount: len(normalized.InferredLayers), AdjustedSandboxCount: adjustedSandboxes,
		CompatibilityAdjustmentCount: adjustments,
	}
	return result, normalized, nil
}

func writeReport(path string, result report, stdout io.Writer) error {
	payload, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("encode migration report: %w", err)
	}
	payload = append(payload, '\n')
	if path != "" {
		return writeAtomicOwnerOnly(path, payload)
	}
	if _, err := stdout.Write(payload); err != nil {
		return fmt.Errorf("write migration report: %w", err)
	}
	return nil
}
