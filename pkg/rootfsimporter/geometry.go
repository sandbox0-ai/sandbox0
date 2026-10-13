package rootfsimporter

import (
	"fmt"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsartifact"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

// ValidateGeometry binds an initial filesystem to its immutable sparse device.
// A zero initial size is the legacy fixed-size contract; it stays omitted from
// canonical operation and attestation bytes. New sparse imports explicitly bind
// both sizes, even when they happen to be equal.
func ValidateGeometry(logicalSize, initialFilesystemSize int64) error {
	initial := initialFilesystemSize
	maximum := int64(rootfsartifact.MaximumSparseDeviceSizeBytes)
	if initial == 0 {
		initial = logicalSize
		maximum = rootfsartifact.MaximumLogicalSizeBytes
	}
	if initial < rootfsartifact.MinimumLogicalSizeBytes || initial > rootfsartifact.MaximumLogicalSizeBytes ||
		initial%rootfsblock.LogicalBlockSize != 0 {
		return fmt.Errorf("initial filesystem size must be between %d and %d bytes and aligned to %d bytes",
			rootfsartifact.MinimumLogicalSizeBytes, rootfsartifact.MaximumLogicalSizeBytes, rootfsblock.LogicalBlockSize)
	}
	if logicalSize < initial || logicalSize > maximum || logicalSize%rootfsblock.LogicalBlockSize != 0 {
		return fmt.Errorf("logical size must be at least the initial filesystem size, at most %d bytes, and aligned to %d bytes", maximum, rootfsblock.LogicalBlockSize)
	}
	return nil
}

// ValidateSparseDeviceSize validates the regional import setting. Zero keeps
// fixed-size import compatibility during the coordinated rollout. A configured
// ceiling accommodates every supported initial-size template.
func ValidateSparseDeviceSize(size int64) error {
	if size == 0 {
		return nil
	}
	if size < rootfsartifact.MaximumLogicalSizeBytes || size > rootfsartifact.MaximumSparseDeviceSizeBytes || size%rootfsblock.LogicalBlockSize != 0 {
		return fmt.Errorf("sparse device size must be zero or an aligned size between 1 TiB and 64 TiB")
	}
	return nil
}

// ResolveImportGeometry turns a template's initial capacity into the exact
// regional import requirement. The zero setting preserves legacy identities.
func ResolveImportGeometry(initial, ceiling int64) (logicalSize, initialFilesystemSize int64, err error) {
	if err = ValidateSparseDeviceSize(ceiling); err != nil {
		return 0, 0, err
	}
	if ceiling == 0 {
		return initial, 0, ValidateGeometry(initial, 0)
	}
	return ceiling, initial, ValidateGeometry(ceiling, initial)
}
