package template

import (
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/quantity"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsartifact"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
)

// ResolveRootFSLogicalSize returns the exact immutable block-device size for
// an image-based template. An omitted public value uses the platform default.
func ResolveRootFSLogicalSize(spec sandboxspec.TemplateSpec) (int64, error) {
	raw := strings.TrimSpace(spec.MainContainer.Resources.EphemeralStorage)
	if raw == "" {
		raw = sandboxspec.DefaultSandboxEphemeralStorage
	}
	parsed, err := quantity.Parse(raw)
	if err != nil {
		return 0, fmt.Errorf("spec.mainContainer.resources.ephemeralStorage is invalid: %w", err)
	}
	logicalSize := parsed.Value()
	if logicalSize <= 0 || quantity.New(logicalSize).Cmp(parsed) != 0 {
		return 0, fmt.Errorf("spec.mainContainer.resources.ephemeralStorage must be an exact byte quantity")
	}
	if logicalSize < rootfsartifact.MinimumLogicalSizeBytes ||
		logicalSize > rootfsartifact.MaximumLogicalSizeBytes ||
		logicalSize%rootfsblock.LogicalBlockSize != 0 {
		return 0, fmt.Errorf(
			"spec.mainContainer.resources.ephemeralStorage must be between %d and %d bytes and aligned to %d bytes",
			rootfsartifact.MinimumLogicalSizeBytes,
			rootfsartifact.MaximumLogicalSizeBytes,
			rootfsblock.LogicalBlockSize,
		)
	}
	return logicalSize, nil
}
