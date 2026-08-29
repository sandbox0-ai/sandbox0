package template

import (
	"fmt"
	"path"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/quantity"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
)

const (
	minEphemeralMountBytes int64 = 1 << 20
	maxEphemeralMountBytes int64 = 1 << 40
)

// ResolveEphemeralMounts validates runtime-neutral ephemeral mounts and
// returns exact byte limits for the claim assignment.
func ResolveEphemeralMounts(spec sandboxspec.TemplateSpec) ([]ResolvedEphemeralMount, error) {
	result := make([]ResolvedEphemeralMount, 0, len(spec.EphemeralMounts))
	for index, mount := range spec.EphemeralMounts {
		mountPath := strings.TrimSpace(mount.MountPath)
		if mountPath != mount.MountPath || !strings.HasPrefix(mountPath, "/") || path.Clean(mountPath) != mountPath {
			return nil, fmt.Errorf("spec.ephemeralMounts[%d].mountPath must be a canonical absolute path", index)
		}
		if reservedEphemeralMountPath(mountPath) {
			return nil, fmt.Errorf("spec.ephemeralMounts[%d].mountPath overlaps a reserved runtime path", index)
		}
		limit, err := quantity.Parse(strings.TrimSpace(mount.SizeLimit))
		if err != nil {
			return nil, fmt.Errorf("spec.ephemeralMounts[%d].sizeLimit is invalid: %w", index, err)
		}
		bytes := limit.Value()
		if bytes < minEphemeralMountBytes || bytes > maxEphemeralMountBytes || quantity.New(bytes).Cmp(limit) != 0 {
			return nil, fmt.Errorf(
				"spec.ephemeralMounts[%d].sizeLimit must be an exact byte quantity between 1Mi and 1Ti", index,
			)
		}
		for _, existing := range result {
			if pathOverlaps(existing.MountPath, mountPath) {
				return nil, fmt.Errorf("spec.ephemeralMounts[%d].mountPath overlaps %s", index, existing.MountPath)
			}
		}
		result = append(result, ResolvedEphemeralMount{MountPath: mountPath, SizeBytes: bytes})
	}
	return result, nil
}

// ResolvedEphemeralMount is the exact claim-time mount shape.
type ResolvedEphemeralMount struct {
	MountPath string
	SizeBytes int64
}

func reservedEphemeralMountPath(value string) bool {
	if value == "/" || value == "/dev" || value == "/proc" || value == "/sys" || value == "/config" || value == "/procd" {
		return true
	}
	if strings.HasPrefix(value, "/proc/") || strings.HasPrefix(value, "/sys/") ||
		strings.HasPrefix(value, "/config/") || strings.HasPrefix(value, "/procd/") {
		return true
	}
	return strings.HasPrefix(value, "/dev/") && value != "/dev/shm"
}

func pathOverlaps(left, right string) bool {
	return left == right || strings.HasPrefix(left, right+"/") || strings.HasPrefix(right, left+"/")
}
