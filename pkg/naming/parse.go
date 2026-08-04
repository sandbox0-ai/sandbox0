package naming

import (
	"fmt"
	"strconv"
	"strings"
)

// ParsedSandboxName provides decoded fields from a sandbox name.
type ParsedSandboxName struct {
	ClusterID  string
	ClusterKey string
}

// ParseSandboxName extracts the cluster ID from a sandbox (pod) name.
// Accepts names generated from ReplicaSet-based sandbox names.
func ParseSandboxName(name string) (*ParsedSandboxName, error) {
	clusterKey, err := extractClusterKey(name, sandboxNamePrefix)
	if err != nil {
		return nil, err
	}
	clusterID, err := decodeClusterKey(clusterKey)
	if err != nil {
		return nil, err
	}
	return &ParsedSandboxName{
		ClusterID:  clusterID,
		ClusterKey: clusterKey,
	}, nil
}

func extractClusterKey(name, prefix string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("name is empty")
	}
	expected := prefix + "-"
	if !strings.HasPrefix(name, expected) {
		return "", fmt.Errorf("name '%s' does not start with '%s'", name, expected)
	}
	rest := strings.TrimPrefix(name, expected)
	parts := strings.SplitN(rest, "-", 2)
	if len(parts) < 2 || parts[0] == "" {
		return "", fmt.Errorf("name '%s' does not include cluster key", name)
	}
	return parts[0], nil
}

// BuildExposureHostLabel builds the host label used for sandbox public exposure.
// Format: <sandboxName>--p<port>.
func BuildExposureHostLabel(sandboxName string, port int) (string, error) {
	if sandboxName == "" {
		return "", fmt.Errorf("sandboxName is empty")
	}
	if err := validateDNSLabel(sandboxName); err != nil {
		return "", fmt.Errorf("invalid sandboxName: %w", err)
	}
	if port <= 0 || port > 65535 {
		return "", fmt.Errorf("invalid port: %d", port)
	}
	label := fmt.Sprintf("%s%s%d", sandboxName, exposurePortDelimiter, port)
	if len(label) > dnsLabelMaxLen {
		return "", fmt.Errorf("exposure label too long (%d > %d)", len(label), dnsLabelMaxLen)
	}
	if err := validateDNSLabel(label); err != nil {
		return "", err
	}
	return label, nil
}

// ParseExposureHostLabel parses a host label created by BuildExposureHostLabel.
func ParseExposureHostLabel(label string) (sandboxName string, port int, err error) {
	if label == "" {
		return "", 0, fmt.Errorf("label is empty")
	}
	idx := strings.LastIndex(label, exposurePortDelimiter)
	if idx <= 0 {
		return "", 0, fmt.Errorf("invalid exposure label: missing '%s'", exposurePortDelimiter)
	}
	sandboxName = label[:idx]
	if err := validateDNSLabel(sandboxName); err != nil {
		return "", 0, fmt.Errorf("invalid sandbox name: %w", err)
	}
	portStr := label[idx+len(exposurePortDelimiter):]
	if portStr == "" {
		return "", 0, fmt.Errorf("invalid exposure label: empty port")
	}
	port, err = strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return "", 0, fmt.Errorf("invalid port in exposure label: %s", portStr)
	}
	return sandboxName, port, nil
}
