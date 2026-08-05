package rootfshead

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"strings"

	"github.com/opencontainers/go-digest"
)

const objectRoot = "sandbox-rootfs/cow-v3"

func ObjectRootPrefix() string { return objectRoot }

// TeamObjectPrefix returns the tenant-isolated CAS prefix shared by every
// filesystem owned by a team.
func TeamObjectPrefix(teamID string) (string, error) {
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return "", fmt.Errorf("rootfs team id is required")
	}
	sum := sha256.Sum256([]byte(teamID))
	return path.Join(objectRoot, "teams", "sha256", fmt.Sprintf("%x", sum[:])), nil
}

// TeamPrefixFromObjectKey returns the opaque team CAS prefix encoded in a v3
// object key. It does not recover or expose the original team identifier.
func TeamPrefixFromObjectKey(key string) (string, error) {
	key = strings.Trim(strings.TrimSpace(key), "/")
	if key == "" || path.Clean(key) != key {
		return "", fmt.Errorf("invalid rootfs object key %q", key)
	}
	parts := strings.Split(key, "/")
	rootParts := strings.Split(objectRoot, "/")
	if len(parts) < len(rootParts)+5 || parts[0] != rootParts[0] || parts[1] != rootParts[1] || parts[2] != "teams" || parts[3] != "sha256" {
		return "", fmt.Errorf("object key %q is outside the rootfs v3 team namespace", key)
	}
	teamHash := parts[4]
	decoded, err := hex.DecodeString(teamHash)
	if err != nil || len(decoded) != sha256.Size {
		return "", fmt.Errorf("object key %q has an invalid team hash", key)
	}
	return strings.Join(parts[:5], "/"), nil
}

func ObjectKey(prefix, mediaType, digestString string) (string, error) {
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	if prefix == "" || path.Clean(prefix) != prefix {
		return "", fmt.Errorf("invalid rootfs object prefix %q", prefix)
	}
	value, err := digest.Parse(strings.TrimSpace(digestString))
	if err != nil || value.Algorithm() != digest.Canonical {
		return "", fmt.Errorf("invalid rootfs object digest %q", digestString)
	}
	kind, err := objectKind(mediaType)
	if err != nil {
		return "", err
	}
	return path.Join(prefix, kind, value.Algorithm().String(), value.Encoded()), nil
}

func ValidateObjectScope(prefix string, object Object) error {
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	if prefix == "" || path.Clean(prefix) != prefix {
		return fmt.Errorf("invalid rootfs object prefix %q", prefix)
	}
	if err := object.Validate(""); err != nil {
		return err
	}
	if !strings.HasPrefix(object.Key, prefix+"/") {
		return fmt.Errorf("rootfs object %s escapes team prefix %s", object.Key, prefix)
	}
	expected, err := ObjectKey(prefix, object.MediaType, object.Digest)
	if err != nil {
		return err
	}
	if object.Key != expected {
		return fmt.Errorf("rootfs object key %s does not match canonical key %s", object.Key, expected)
	}
	return nil
}

func objectKind(mediaType string) (string, error) {
	switch mediaType {
	case ChunkMediaType:
		return "chunks", nil
	case FileMediaType:
		return "files", nil
	case DirectoryShardMediaType:
		return "directory-shards", nil
	case DirectoryIndexMediaType:
		return "directory-indexes", nil
	case HeadMediaType:
		return "heads", nil
	case MarkerMediaType:
		return "markers", nil
	case ImageEnvelopeMediaType:
		return "image-envelopes", nil
	case ExportLayerMediaType:
		return "exports", nil
	default:
		return "", fmt.Errorf("unsupported rootfs object media type %q", mediaType)
	}
}
