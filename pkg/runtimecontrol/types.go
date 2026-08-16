// Package runtimecontrol defines the event-driven assignment contract shared by
// manager, ctld, and procd.
package runtimecontrol

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxpod"
	corev1 "k8s.io/api/core/v1"
)

const (
	AnnotationSandboxID          = sandboxpod.AnnotationSandboxID
	AnnotationTeamID             = sandboxpod.AnnotationTeamID
	AnnotationRuntimeGeneration  = sandboxpod.AnnotationRuntimeGeneration
	AnnotationConfig             = sandboxpod.AnnotationConfig
	AnnotationAppDomain          = sandboxpod.AnnotationAppDomain
	AnnotationResetCopiedState   = sandboxpod.AnnotationResetCopiedState
	AnnotationAssignmentRevision = sandboxpod.AnnotationAssignmentRevision
	AnnotationAssignmentReady    = sandboxpod.AnnotationAssignmentReady
	AnnotationObservedRevision   = sandboxpod.AnnotationObservedRevision
	AnnotationObservedGeneration = sandboxpod.AnnotationObservedGeneration
	AnnotationObservedState      = sandboxpod.AnnotationObservedState
)

const (
	WatchPath               = "/api/v1/runtime/watch"
	DefaultCtldWatchPort    = 8096
	EnvSandboxID            = "SANDBOX0_SANDBOX_ID"
	EnvAppDomain            = "SANDBOX0_APP_DOMAIN"
	EnvPodName              = "SANDBOX0_POD_NAME"
	EnvPodNamespace         = "SANDBOX0_POD_NAMESPACE"
	EnvPodUID               = "SANDBOX0_POD_UID"
	EnvNodeHostIP           = "SANDBOX0_NODE_HOST_IP"
	EnvCtldRuntimeWatchPort = "SANDBOX0_CTLD_RUNTIME_WATCH_PORT"
	ProcdContainerName      = "procd"
)

type DesiredState string

const (
	DesiredStandby DesiredState = "standby"
	// Keep the wire value stable while predecessor ctld/procd Pods drain.
	DesiredWaitingRootFS DesiredState = "waiting_storage"
	DesiredActive        DesiredState = "active"
	DesiredRevoked       DesiredState = "revoked"
)

type ObservedState string

const (
	ObservedStandby ObservedState = "standby"
	// Keep the wire value stable while predecessor ctld/procd Pods drain.
	ObservedWaitingRootFS ObservedState = "waiting_storage"
	ObservedLoading       ObservedState = "loading"
	ObservedRecovering    ObservedState = "recovering"
	ObservedReady         ObservedState = "ready"
	ObservedFailed        ObservedState = "failed"
	ObservedDisconnected  ObservedState = "disconnected"
)

// WebhookConfig configures sandbox-scoped event delivery from procd.
type WebhookConfig struct {
	URL      string `json:"url"`
	Secret   string `json:"secret,omitempty"`
	WatchDir string `json:"watch_dir,omitempty"`
}

// Assignment is the complete manager-owned input needed by a fresh procd
// process. CTLD derives it from the existing Pod manifest.
type Assignment struct {
	SandboxID               string            `json:"sandbox_id"`
	TeamID                  string            `json:"team_id,omitempty"`
	RuntimeGeneration       int64             `json:"runtime_generation"`
	EnvVars                 map[string]string `json:"env_vars,omitempty"`
	Webhook                 *WebhookConfig    `json:"webhook,omitempty"`
	ResetCopiedSessionState bool              `json:"reset_copied_session_state,omitempty"`
}

// Revision returns a deterministic digest for activation and observation
// matching.
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

func (a Assignment) Validate() error {
	if strings.TrimSpace(a.SandboxID) == "" {
		return errors.New("sandbox id is required")
	}
	if a.RuntimeGeneration <= 0 {
		return errors.New("runtime generation must be positive")
	}
	return nil
}

// Snapshot is a level-triggered CTLD view. Every event carries the complete
// current state so reconnects and coalesced events cannot lose correctness.
type Snapshot struct {
	Sequence   uint64       `json:"sequence"`
	State      DesiredState `json:"state"`
	Revision   string       `json:"revision,omitempty"`
	Assignment *Assignment  `json:"assignment,omitempty"`
	Reason     string       `json:"reason,omitempty"`
}

// Observation is procd's acknowledgement of the latest applied snapshot.
type Observation struct {
	State             ObservedState `json:"state"`
	Revision          string        `json:"revision,omitempty"`
	RuntimeGeneration int64         `json:"runtime_generation,omitempty"`
	Reason            string        `json:"reason,omitempty"`
}

type sandboxConfig struct {
	EnvVars map[string]string `json:"env_vars,omitempty"`
	Webhook *WebhookConfig    `json:"webhook,omitempty"`
}

// AssignmentFromPod derives the complete procd input from the existing
// manager-owned Pod manifest. The assignment itself is not stored separately.
func AssignmentFromPod(pod *corev1.Pod) (*Assignment, string, error) {
	if pod == nil {
		return nil, "", nil
	}
	annotations := pod.GetAnnotations()
	sandboxID := strings.TrimSpace(annotations[AnnotationSandboxID])
	if sandboxID == "" {
		return nil, "", nil
	}
	generation, err := strconv.ParseInt(strings.TrimSpace(annotations[AnnotationRuntimeGeneration]), 10, 64)
	if err != nil || generation <= 0 {
		return nil, "", errors.New("runtime generation must be positive")
	}

	var cfg sandboxConfig
	if raw := strings.TrimSpace(annotations[AnnotationConfig]); raw != "" {
		if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
			return nil, "", fmt.Errorf("decode sandbox config: %w", err)
		}
	}
	envVars := cloneStringMap(cfg.EnvVars)
	if envVars == nil {
		envVars = make(map[string]string, 2)
	}
	envVars[EnvSandboxID] = sandboxID
	if appDomain := strings.Trim(strings.TrimSpace(annotations[AnnotationAppDomain]), "."); appDomain != "" {
		envVars[EnvAppDomain] = appDomain
	}

	assignment := Assignment{
		SandboxID:               sandboxID,
		TeamID:                  strings.TrimSpace(annotations[AnnotationTeamID]),
		RuntimeGeneration:       generation,
		EnvVars:                 envVars,
		Webhook:                 cfg.Webhook,
		ResetCopiedSessionState: strings.EqualFold(strings.TrimSpace(annotations[AnnotationResetCopiedState]), "true"),
	}
	revision, err := assignment.Revision()
	if err != nil {
		return nil, "", err
	}
	return &assignment, revision, nil
}

func cloneStringMap(source map[string]string) map[string]string {
	if len(source) == 0 {
		return nil
	}
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
