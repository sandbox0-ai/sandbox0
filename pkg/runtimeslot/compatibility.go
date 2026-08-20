package runtimeslot

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
)

const RuntimeCompatibilityVersion = 1

// RuntimeCompatibility is the exact immutable warm-slot execution shape. Both
// the node registration and regional claim profile must derive their digest
// from this shared wire type rather than maintaining compatible-looking hash
// implementations.
type RuntimeCompatibility struct {
	Version          int    `json:"version"`
	Architecture     string `json:"architecture"`
	DriverVersion    string `json:"driver_version"`
	RunscVersion     string `json:"runsc_version"`
	Platform         string `json:"platform"`
	Overlay2         string `json:"overlay2"`
	FileAccess       string `json:"file_access"`
	DirectFS         bool   `json:"directfs"`
	Command          string `json:"command"`
	ProcdPort        int    `json:"procd_port"`
	RuntimeMode      string `json:"runtime_mode"`
	CPUPeriod        int64  `json:"cpu_period"`
	CPUQuota         int64  `json:"cpu_quota"`
	CPUShares        int64  `json:"cpu_shares"`
	MemoryLimitBytes int64  `json:"memory_limit_bytes"`
}

// Validate rejects ambiguous profiles before they become scheduling keys.
func (c RuntimeCompatibility) Validate() error {
	if c.Version != RuntimeCompatibilityVersion {
		return fmt.Errorf("runtime compatibility version must be %d", RuntimeCompatibilityVersion)
	}
	for name, value := range map[string]string{
		"architecture": c.Architecture, "driver_version": c.DriverVersion,
		"runsc_version": c.RunscVersion, "platform": c.Platform,
		"overlay2": c.Overlay2, "file_access": c.FileAccess,
	} {
		if value == "" || value != strings.TrimSpace(value) || len(value) > 512 {
			return fmt.Errorf("%s must be non-empty, canonical, and at most 512 bytes", name)
		}
	}
	if c.Command != "/procd" {
		return fmt.Errorf("runtime compatibility command must be /procd")
	}
	if c.ProcdPort != NomadProcdPort {
		return fmt.Errorf("runtime compatibility procd port must be %d", NomadProcdPort)
	}
	if c.RuntimeMode != runtimecontrol.ControlModeStatic {
		return fmt.Errorf("runtime compatibility mode must be %s", runtimecontrol.ControlModeStatic)
	}
	return nil
}

// Digest returns the canonical slot scheduling key.
func (c RuntimeCompatibility) Digest() (string, error) {
	if err := c.Validate(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("encode runtime compatibility: %w", err)
	}
	return digest.FromBytes(payload).String(), nil
}
