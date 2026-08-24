// Package runtimecontrol defines the immutable runtime assignment shared by
// manager, the Nomad driver, and procd.
package runtimecontrol

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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

// Assignment is the complete input that the Nomad driver passes to a fresh
// procd process. The assignment cannot change during the process lifetime.
type Assignment struct {
	SandboxID               string            `json:"sandbox_id"`
	TeamID                  string            `json:"team_id,omitempty"`
	RuntimeGeneration       int64             `json:"runtime_generation"`
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
	return nil
}
