package rootfsimporter

import (
	"fmt"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsartifact"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

// XFSFileRangesV1 is an import policy, not a runtime compatibility class. It
// prefers complete file-relative 64KiB ranges. If bounded discovery exhausts its
// metadata budget, the entire image uses the legacy grid and attests that fact.
// Empty policy preserves the original operation/attestation identity exactly.
const XFSFileRangesV1 = "xfs-file-ranges-v1"

// ValidateDataLayoutPolicy accepts only explicitly supported import policies and
// their geometry. Unknown policies must fail before unpacking or publication.
func ValidateDataLayoutPolicy(policy string, formatGeneration int, options rootfsblock.BuildOptions) error {
	if policy == "" {
		return nil
	}
	if policy != XFSFileRangesV1 {
		return fmt.Errorf("unsupported RootFS data_layout_policy %q", policy)
	}
	normalized, err := NormalizeBlockOptions(formatGeneration, options)
	if err != nil {
		return err
	}
	if formatGeneration != rootfsblock.CompressedFormatVersion || normalized.DataRangeBytes != rootfsblock.CompressedDataRangeBytes {
		return fmt.Errorf("%s requires format 2 and 64KiB data ranges", policy)
	}
	return nil
}

func validateDataLayoutEvidence(policy, fallback string, formatGeneration, rangeBytes int) error {
	if policy == "" {
		if fallback != "" || rangeBytes != 0 {
			return fmt.Errorf("legacy layout must not carry new layout evidence")
		}
		return nil
	}
	if rangeBytes != rootfsblock.CompressedDataRangeBytes {
		return fmt.Errorf("file-range layout evidence must bind 64KiB geometry")
	}
	if err := ValidateDataLayoutPolicy(policy, formatGeneration, rootfsblock.BuildOptions{DataRangeBytes: rangeBytes}); err != nil {
		return err
	}
	if fallback != "" && fallback != rootfsartifact.XFSDataRangeBudgetFallback {
		return fmt.Errorf("unknown data layout fallback %q", fallback)
	}
	return nil
}
