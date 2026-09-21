package runtimeslot

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/opencontainers/go-digest"
)

const MigrationCPUProfileVersion = 1

// MigrationCPUObservation records the exact CPU set over which a homogeneous
// profile was measured. CPU numbers scope the observation on one node; they
// must not be compared as cross-node compatibility or capacity requirements.
// Node boot, runtime launch, and freshness are bound by the caller separately.
type MigrationCPUObservation struct {
	Profile MigrationCPUProfile `json:"profile"`
	CPUSet  string              `json:"cpu_set"`
}

func (o MigrationCPUObservation) Validate() error {
	if err := o.Profile.Validate(); err != nil {
		return err
	}
	if _, err := ValidateCPUSet(o.CPUSet); err != nil {
		return fmt.Errorf("migration CPU observation coverage: %w", err)
	}
	return nil
}

func (o MigrationCPUObservation) Digest() (string, error) {
	if err := o.Validate(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(o)
	if err != nil {
		return "", err
	}
	return digest.FromBytes(payload).String(), nil
}

// Covers rejects reuse of a narrow observation after a runtime's eligible CPU
// set widens. It says nothing about whether the observation is still fresh.
func (o MigrationCPUObservation) Covers(cpuSet string) error {
	if err := o.Validate(); err != nil {
		return err
	}
	contains, err := CPUSetContains(o.CPUSet, cpuSet)
	if err != nil {
		return err
	}
	if !contains {
		return fmt.Errorf("migration CPU observation does not cover runtime CPU set")
	}
	return nil
}

// MigrationCPUProfile describes measured host capability, not migration
// authorization or the historical feature set of an already-running guest.
// The source launch observation, node boot and current CPU coverage must be
// bound separately before this profile may authorize a capture or restore.
// Resource quotas and CPU counts do not belong in this compatibility evidence.
type MigrationCPUProfile struct {
	Version            int      `json:"version"`
	Architecture       string   `json:"architecture"`
	RunscVersion       string   `json:"runsc_version"`
	Features           []string `json:"features"`
	CacheLineBytes     uint32   `json:"cache_line_bytes,omitempty"`
	XStateLayoutDigest string   `json:"xstate_layout_digest,omitempty"`
}

func (p MigrationCPUProfile) Validate() error {
	if p.Version != MigrationCPUProfileVersion || p.RunscVersion == "" || strings.TrimSpace(p.RunscVersion) != p.RunscVersion || len(p.RunscVersion) > 512 || strings.ContainsAny(p.RunscVersion, "\x00\r\n") {
		return fmt.Errorf("migration CPU profile lacks canonical runtime identity")
	}
	switch p.Architecture {
	case "amd64":
		if p.CacheLineBytes < 8 || p.CacheLineBytes > 2048 || p.CacheLineBytes&(p.CacheLineBytes-1) != 0 {
			return fmt.Errorf("migration CPU profile lacks a measured cache line size")
		}
		if err := digest.Digest(p.XStateLayoutDigest).Validate(); err != nil || !strings.HasPrefix(p.XStateLayoutDigest, "sha256:") {
			return fmt.Errorf("migration CPU profile lacks an xstate layout digest")
		}
	case "arm64":
		if p.CacheLineBytes != 0 || p.XStateLayoutDigest != "" {
			return fmt.Errorf("ARM64 CPU profile cannot carry x86 state")
		}
	default:
		return fmt.Errorf("unsupported migration CPU architecture")
	}
	if len(p.Features) == 0 || len(p.Features) > 512 {
		return fmt.Errorf("migration CPU feature count is invalid")
	}
	for i, feature := range p.Features {
		if !validMigrationCPUFeature(feature) || i > 0 && p.Features[i-1] >= feature {
			return fmt.Errorf("migration CPU features must be sorted, unique canonical names")
		}
	}
	return nil
}

func validMigrationCPUFeature(feature string) bool {
	if len(feature) == 0 || len(feature) > 64 {
		return false
	}
	for _, c := range feature {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '.' || c == '-') {
			return false
		}
	}
	return true
}

// ParseMigrationCPUFeatures accepts the bounded comma-separated output of the
// stock runsc cpu-features command, including its optional final newline. It
// never drops unrecognized text, empty entries or duplicates to make a match.
func ParseMigrationCPUFeatures(output []byte) ([]string, error) {
	if len(output) == 0 || len(output) > 64<<10 {
		return nil, fmt.Errorf("runsc CPU feature output size is invalid")
	}
	raw := strings.TrimSuffix(string(output), "\n")
	features := strings.Split(raw, ",")
	if len(features) > 512 {
		return nil, fmt.Errorf("runsc CPU feature count exceeds limit")
	}
	for _, f := range features {
		if !validMigrationCPUFeature(f) {
			return nil, fmt.Errorf("runsc returned an invalid CPU feature name")
		}
	}
	slices.Sort(features)
	for i := 1; i < len(features); i++ {
		if features[i] == features[i-1] {
			return nil, fmt.Errorf("runsc returned duplicate CPU features")
		}
	}
	return features, nil
}

func (p MigrationCPUProfile) Digest() (string, error) {
	if err := p.Validate(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	return digest.FromBytes(payload).String(), nil
}

// CheckMigrationCPUProfiles is a necessary host-capability check. It cannot
// replace the source's launch-time binding, node attestation, or stock runsc's
// restore validation. Require identical x86 state layouts rather than assuming
// that a feature-name superset can safely interpret every saved FP buffer.
func CheckMigrationCPUProfiles(source, target MigrationCPUProfile) error {
	if err := source.Validate(); err != nil {
		return err
	}
	if err := target.Validate(); err != nil {
		return err
	}
	if source.Architecture != target.Architecture || source.RunscVersion != target.RunscVersion || source.CacheLineBytes != target.CacheLineBytes || source.XStateLayoutDigest != target.XStateLayoutDigest {
		return fmt.Errorf("migration CPU runtime or state layout differs")
	}
	for _, feature := range source.Features {
		if _, ok := slices.BinarySearch(target.Features, feature); !ok {
			return fmt.Errorf("migration target lacks CPU feature %q", feature)
		}
	}
	return nil
}
