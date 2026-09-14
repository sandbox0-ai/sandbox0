package nomadinventory

import (
	"fmt"
	"strings"
)

const (
	// WarmJobShardCount bounds the 512 standard and 64 privileged carriers.
	WarmJobShardCount = 18
	// WarmJobMaxGroups bounds the full job embedded in every Nomad allocation.
	WarmJobMaxGroups = 32
)

// WarmJobID identifies one exact member of a configured carrier job family.
// Shard zero retains the original ID, including the default eight carriers.
func WarmJobID(base string, shard int) (string, error) {
	if strings.TrimSpace(base) == "" || base != strings.TrimSpace(base) || shard < 0 || shard >= WarmJobShardCount {
		return "", fmt.Errorf("invalid warm job family or shard")
	}
	if shard == 0 {
		return base, nil
	}
	return fmt.Sprintf("%s-shard-%02d", base, shard), nil
}

// IsWarmJob accepts only the bounded canonical IDs, never an arbitrary prefix.
func IsWarmJob(base, candidate string) bool {
	for shard := range WarmJobShardCount {
		id, err := WarmJobID(base, shard)
		if err == nil && id == candidate {
			return true
		}
	}
	return false
}
