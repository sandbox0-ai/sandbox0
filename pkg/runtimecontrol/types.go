// Package runtimecontrol defines the immutable runtime assignment shared by
// manager, the Nomad driver, and procd.
package runtimecontrol

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
)

const (
	EnvSandboxID        = "SANDBOX0_SANDBOX_ID"
	EnvAppDomain        = "SANDBOX0_APP_DOMAIN"
	EnvControlMode      = "SANDBOX0_RUNTIME_CONTROL_MODE"
	EnvStaticAssignment = "SANDBOX0_RUNTIME_ASSIGNMENT"
	ControlModeStatic   = "static"
)

// WebhookConfig configures sandbox-scoped event delivery from procd.
type WebhookConfig struct {
	URL      string `json:"url"`
	Secret   string `json:"secret,omitempty"`
	WatchDir string `json:"watch_dir,omitempty"`
}

// EphemeralMount is a claim-lifetime tmpfs excluded from durable RootFS state.
type EphemeralMount struct {
	MountPath string `json:"mount_path"`
	SizeBytes int64  `json:"size_bytes"`
}

// Assignment is the complete input that the Nomad driver passes to a fresh
// procd process. The assignment cannot change during the process lifetime.
type Assignment struct {
	SandboxID               string            `json:"sandbox_id"`
	TeamID                  string            `json:"team_id,omitempty"`
	RuntimeGeneration       int64             `json:"runtime_generation"`
	SecurityClass           string            `json:"security_class"`
	EphemeralMounts         []EphemeralMount  `json:"ephemeral_mounts,omitempty"`
	EnvVars                 map[string]string `json:"env_vars,omitempty"`
	Webhook                 *WebhookConfig    `json:"webhook,omitempty"`
	ResetCopiedSessionState bool              `json:"reset_copied_session_state,omitempty"`
}

// Revision returns a deterministic assignment digest.
func (a Assignment) Revision() (string, error) {
	if err := a.Validate(); err != nil {
		return "", err
	}
	data, err := json.Marshal(a)
	if err != nil {
		return "", fmt.Errorf("marshal runtime assignment: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// Validate checks the immutable runtime identity.
func (a Assignment) Validate() error {
	if strings.TrimSpace(a.SandboxID) == "" {
		return errors.New("sandbox id is required")
	}
	if a.RuntimeGeneration <= 0 {
		return errors.New("runtime generation must be positive")
	}
	securityClass, ok := sandboxspec.EffectiveSandboxSecurityClass(sandboxspec.SandboxSecurityClass(a.SecurityClass))
	if !ok || string(securityClass) != a.SecurityClass {
		return errors.New("security class must be canonical")
	}
	for index, mount := range a.EphemeralMounts {
		if mount.MountPath == "" || strings.TrimSpace(mount.MountPath) != mount.MountPath ||
			!strings.HasPrefix(mount.MountPath, "/") || path.Clean(mount.MountPath) != mount.MountPath ||
			mount.SizeBytes < 1<<20 || mount.SizeBytes > 1<<40 || reservedEphemeralPath(mount.MountPath) {
			return fmt.Errorf("ephemeral mount %d is invalid", index)
		}
		for previous := 0; previous < index; previous++ {
			other := a.EphemeralMounts[previous].MountPath
			if mount.MountPath == other || strings.HasPrefix(mount.MountPath, other+"/") ||
				strings.HasPrefix(other, mount.MountPath+"/") {
				return fmt.Errorf("ephemeral mount %d overlaps another mount", index)
			}
		}
	}
	return nil
}

func reservedEphemeralPath(value string) bool {
	if value == "/" || value == "/dev" || value == "/proc" || value == "/sys" || value == "/config" || value == "/procd" {
		return true
	}
	if strings.HasPrefix(value, "/proc/") || strings.HasPrefix(value, "/sys/") ||
		strings.HasPrefix(value, "/config/") || strings.HasPrefix(value, "/procd/") {
		return true
	}
	return strings.HasPrefix(value, "/dev/") && value != "/dev/shm"
}
