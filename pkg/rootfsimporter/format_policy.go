package rootfsimporter

import (
	"fmt"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

// ImageImportFormat resolves the policy for new image imports only. Existing
// generations keep their committed format, including when this policy changes.
func ImageImportFormat(configured int) (int, error) {
	if configured == 0 {
		return rootfsblock.DescriptorVersion, nil
	}
	if configured != rootfsblock.DescriptorVersion && configured != rootfsblock.CompressedFormatVersion {
		return 0, fmt.Errorf("unsupported image import format_generation %d: want 1 or 2", configured)
	}
	return configured, nil
}

// NormalizeBlockOptions binds builder defaults to the durable format generation.
// Keep the legacy v1 zero encoding intact: it participates in import identities.
func NormalizeBlockOptions(formatGeneration int, options rootfsblock.BuildOptions) (rootfsblock.BuildOptions, error) {
	if formatGeneration <= 0 {
		return rootfsblock.BuildOptions{}, fmt.Errorf("RootFS format generation must be positive")
	}
	if formatGeneration == rootfsblock.CompressedFormatVersion && options.FormatVersion == 0 {
		options.FormatVersion = rootfsblock.CompressedFormatVersion
	}
	normalized, err := rootfsblock.NormalizeBuildOptions(options)
	if err != nil {
		return rootfsblock.BuildOptions{}, fmt.Errorf("RootFS block build options: %w", err)
	}
	version := normalized.FormatVersion
	if version == 0 {
		version = rootfsblock.DescriptorVersion
	}
	if err := rootfsblock.ValidateFormatBinding(formatGeneration, version); err != nil {
		return rootfsblock.BuildOptions{}, err
	}
	return normalized, nil
}
