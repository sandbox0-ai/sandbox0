package rootfsrebase

import (
	"encoding/hex"
	"fmt"
	"strings"
)

const ApplyResultVersion = 1

// ApplyRequest describes an offline three-way filesystem merge. OldRoot and
// SourceRoot must remain immutable for the call. TargetRoot is an unpublished
// writable filesystem initialized from the new base.
type ApplyRequest struct {
	OldRoot    string
	SourceRoot string
	TargetRoot string
	Old        Manifest
	Source     Manifest
	Diff       DiffResult
}

type ApplyIOStats struct {
	OldReadBytes    uint64 `json:"old_read_bytes"`
	SourceReadBytes uint64 `json:"source_read_bytes"`
	TargetReadBytes uint64 `json:"target_read_bytes"`
	WrittenBytes    uint64 `json:"written_bytes"`
	PunchedBytes    uint64 `json:"punched_bytes"`
}

// ApplyResult is suitable for inclusion in the health proof passed to the
// regional paused-rebase publication transaction.
type ApplyResult struct {
	Version              int          `json:"version"`
	AppliedChanges       int          `json:"applied_changes"`
	ConvergedChanges     int          `json:"converged_changes"`
	TargetNodeCount      int          `json:"target_node_count"`
	OldManifestDigest    string       `json:"old_manifest_digest"`
	SourceManifestDigest string       `json:"source_manifest_digest"`
	DiffDigest           string       `json:"diff_digest"`
	TargetManifestDigest string       `json:"target_manifest_digest"`
	HealthProof          string       `json:"health_proof"`
	IO                   ApplyIOStats `json:"io"`
}

// HealthProofBytes returns the exact 32-byte value accepted by the regional
// paused-rebase publication transaction.
func (r ApplyResult) HealthProofBytes() ([]byte, error) {
	value, err := hex.DecodeString(r.HealthProof)
	if err != nil || len(value) != 32 {
		return nil, fmt.Errorf("health proof must be a 32-byte SHA-256 digest")
	}
	return value, nil
}

type Conflict struct {
	Path   string `json:"path"`
	Aspect string `json:"aspect"`
	Reason string `json:"reason"`
}

// ConflictError reports bounded, deterministic merge conflicts. The target is
// not mutated when Apply returns this error.
type ConflictError struct {
	Conflicts []Conflict `json:"conflicts"`
	Omitted   int        `json:"omitted,omitempty"`
}

func (e *ConflictError) Error() string {
	if e == nil || len(e.Conflicts) == 0 {
		return "RootFS rebase conflict"
	}
	parts := make([]string, 0, min(len(e.Conflicts), 3))
	for _, conflict := range e.Conflicts[:min(len(e.Conflicts), 3)] {
		parts = append(parts, fmt.Sprintf("%s (%s): %s", conflict.Path, conflict.Aspect, conflict.Reason))
	}
	if len(e.Conflicts) > 3 || e.Omitted > 0 {
		parts = append(parts, fmt.Sprintf("and %d more", len(e.Conflicts)-3+e.Omitted))
	}
	return "RootFS rebase conflicts: " + strings.Join(parts, "; ")
}

const maxReportedConflicts = 128

type conflictCollector struct {
	values  []Conflict
	omitted int
}

func (c *conflictCollector) add(path, aspect, reason string) {
	if len(c.values) < maxReportedConflicts {
		c.values = append(c.values, Conflict{Path: path, Aspect: aspect, Reason: reason})
		return
	}
	c.omitted++
}

func (c *conflictCollector) err() error {
	if len(c.values) == 0 {
		return nil
	}
	return &ConflictError{Conflicts: c.values, Omitted: c.omitted}
}
