package naming

import (
	"fmt"
	"strings"
)

// This file centralizes naming conventions for:
// - S3/object-store key prefixes (used by S0FS chunk/object storage)
// - S0FS internal filesystem paths (used by S0FS meta operations)
//
// These are different namespaces and do not need to be identical, but they must
// be consistent within their own layers to avoid data loss or cross-tenant leaks.

func validatePathID(kind, id string) error {
	if id == "" {
		return fmt.Errorf("%s is empty", kind)
	}
	// Disallow path separators to avoid path traversal / prefix injection.
	if strings.Contains(id, "/") {
		return fmt.Errorf("%s contains invalid '/'", kind)
	}
	return nil
}

// S3VolumePrefix returns the object-store prefix used for a team's volume data.
// Example: sandboxvolumes/<teamID>/<volumeID>
func S3VolumePrefix(teamID, volumeID string) (string, error) {
	if err := validatePathID("teamID", teamID); err != nil {
		return "", err
	}
	if err := validatePathID("volumeID", volumeID); err != nil {
		return "", err
	}
	return fmt.Sprintf("sandboxvolumes/%s/%s", teamID, volumeID), nil
}
