package volume

import (
	"encoding/json"
	"strings"
)

// AccessMode describes how a volume can be mounted across storage-proxy instances.
type AccessMode string

const (
	// AccessModeRWO allows read-write mounts on a single storage-proxy instance.
	AccessModeRWO AccessMode = "RWO"
	// AccessModeROX allows read-only mounts across multiple instances.
	AccessModeROX AccessMode = "ROX"
	// AccessModeRWX allows read-write mounts across multiple instances.
	AccessModeRWX AccessMode = "RWX"
)

const (
	OwnerKindStorageProxy = "storage-proxy"
	OwnerKindCtld         = "ctld"
)

// MountOptions describes how a volume is mounted by an instance.
type MountOptions struct {
	AccessMode   AccessMode `json:"access_mode,omitempty"`
	OwnerKind    string     `json:"owner_kind,omitempty"`
	OwnerPort    int        `json:"owner_port,omitempty"`
	NodeName     string     `json:"node_name,omitempty"`
	PodNamespace string     `json:"pod_namespace,omitempty"`
}

// NormalizeAccessMode normalizes the input and applies the default mode (RWO).
func NormalizeAccessMode(value string) AccessMode {
	if parsed, ok := ParseAccessMode(value); ok {
		return parsed
	}
	return AccessModeRWO
}

// IsValidAccessMode returns true if the access mode is supported.
func IsValidAccessMode(value AccessMode) bool {
	switch value {
	case AccessModeRWO, AccessModeROX, AccessModeRWX:
		return true
	default:
		return false
	}
}

// ParseAccessMode parses and validates the access mode string.
func ParseAccessMode(value string) (AccessMode, bool) {
	normalized := AccessMode(strings.ToUpper(strings.TrimSpace(value)))
	switch normalized {
	case AccessModeRWO, AccessModeROX, AccessModeRWX:
		return normalized, true
	default:
		return "", false
	}
}

func NormalizeOwnerKind(value string) string {
	switch strings.TrimSpace(value) {
	case OwnerKindCtld:
		return OwnerKindCtld
	case OwnerKindStorageProxy:
		return OwnerKindStorageProxy
	default:
		return ""
	}
}

func DecodeMountOptions(raw *json.RawMessage) MountOptions {
	if raw == nil || len(*raw) == 0 {
		return MountOptions{}
	}
	var opts MountOptions
	if err := json.Unmarshal(*raw, &opts); err != nil {
		return MountOptions{}
	}
	opts.AccessMode = NormalizeAccessMode(string(opts.AccessMode))
	opts.OwnerKind = NormalizeOwnerKind(opts.OwnerKind)
	return opts
}
