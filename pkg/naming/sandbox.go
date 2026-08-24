package naming

import (
	"crypto/sha256"
	"fmt"
	"strings"
)

const operationSandboxSuffixLength = 16

// SandboxNameForOperation derives a retry-stable, routeable sandbox identity
// from one trusted regional operation.
func SandboxNameForOperation(clusterID, templateName, operationID string) (string, error) {
	if operationID == "" || operationID != strings.TrimSpace(operationID) {
		return "", fmt.Errorf("operationID must be non-empty and canonical")
	}
	clusterKey, err := ClusterKey(clusterID)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(clusterID + "\x00" + templateName + "\x00" + operationID))
	suffix := strings.ToLower(base32NoPadding.EncodeToString(sum[:]))[:operationSandboxSuffixLength]
	prefix := fmt.Sprintf("%s-%s-", sandboxNamePrefix, clusterKey)
	remaining := sandboxNameMaxLen - len(prefix) - 1 - len(suffix)
	if remaining <= 0 {
		return "", fmt.Errorf("cluster key is too long to build operation sandbox name")
	}
	templateKey, err := slugWithHash(templateName, remaining)
	if err != nil {
		return "", err
	}
	name := prefix + templateKey + "-" + suffix
	if err := validateDNSLabel(name); err != nil {
		return "", err
	}
	if len(name) > sandboxNameMaxLen {
		return "", fmt.Errorf("sandbox name too long for exposure routing (%d > %d)", len(name), sandboxNameMaxLen)
	}
	return name, nil
}
