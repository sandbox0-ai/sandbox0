package http

import (
	"fmt"
	"strings"

	sharednaming "github.com/sandbox0-ai/sandbox0/pkg/naming"
)

const maxClusterNameLength = 255

// validateClusterName enforces the scheduler's durable cluster record policy.
func validateClusterName(name string) error {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return fmt.Errorf("cluster_name is required")
	}
	if len(trimmed) > maxClusterNameLength {
		return fmt.Errorf("cluster_name is too long (%d > %d)", len(trimmed), maxClusterNameLength)
	}
	if strings.Contains(trimmed, "/") {
		return fmt.Errorf("cluster_name cannot contain '/'")
	}
	return nil
}

// clusterIDFromName derives the scheduler-owned stable routing key for a
// durable cluster name.
func clusterIDFromName(name string) (string, error) {
	if err := validateClusterName(name); err != nil {
		return "", err
	}
	return sharednaming.DNSLabelWithHash(name, sharednaming.ClusterIDMaxLen)
}
