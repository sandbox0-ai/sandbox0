package rootfsartifact

import (
	"errors"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

// ErrXFSDataRangeLimit is distinguishable from unsafe or malformed metadata.
// Only this error permits an explicit whole-image segmentation fallback.
var ErrXFSDataRangeLimit = errors.New("XFS data range discovery budget exhausted")

const XFSDataRangeBudgetFallback = "scan-budget-exceeded"

// XFSDataRangePlan describes preferred file-relative ranges in an immutable XFS
// image. It is not a readiness attestation. Publication callers must bind the
// chosen layout policy into the durable import operation before publishing ready.
type XFSDataRangePlan struct {
	Layout *rootfsblock.DataRangeLayout
	Stats  XFSDataRangeStats
	// Fallback is nonempty only for BuildWithBoundedDataRanges. It means the
	// preferred plan was discarded entirely, not that a partial scan succeeded.
	Fallback string
}

// XFSDataRangeStats reports bounded metadata discovery, not startup working-set
// size or cache hits. PreferredSpans is counted before adjacent spans coalesce.
type XFSDataRangeStats struct {
	Entries          int64
	RegularPaths     int64
	FilesScanned     int64
	HardlinksSkipped int64
	Extents          int64
	FlaggedExtents   int64
	SharedExtents    int64
	PreferredSpans   int64
	PreferredBytes   int64
}
