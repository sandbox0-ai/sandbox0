// Package session supervises durable, process-backed execution sessions.
package session

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	defaultEventMaxBytes       int64 = 64 << 20
	defaultEventMaxAgeSeconds  int64 = 24 * 60 * 60
	defaultStopGraceSeconds    int32 = 10
	defaultRestartWindow       int32 = 60
	defaultRestartMaxAttempts  int32 = 5
	defaultInitialBackoffMS    int32 = 250
	defaultMaximumBackoffMS    int32 = 5000
	defaultSubscriptionBacklog       = 256
)

var (
	ErrSessionNotFound      = errors.New("session not found")
	ErrSessionExists        = errors.New("session already exists")
	ErrSessionLimitExceeded = errors.New("sandbox session limit exceeded")
	ErrSessionNotRunning    = errors.New("session is not running")
	ErrAttemptMismatch      = errors.New("session attempt mismatch")
	ErrInputAlreadyExists   = errors.New("input id was already used with different content")
	ErrCursorExpired        = errors.New("event cursor expired")
)

type IOMode string

const (
	IOModePipes IOMode = "pipes"
	IOModePTY   IOMode = "pty"
)

type DesiredState string

const (
	DesiredStateRunning DesiredState = "running"
	DesiredStateStopped DesiredState = "stopped"
)

type Phase string

const (
	PhasePending   Phase = "pending"
	PhaseStarting  Phase = "starting"
	PhaseRunning   Phase = "running"
	PhasePaused    Phase = "paused"
	PhaseStopping  Phase = "stopping"
	PhaseStopped   Phase = "stopped"
	PhaseExited    Phase = "exited"
	PhaseBackoff   Phase = "backoff"
	PhaseFailed    Phase = "failed"
	PhaseSuspended Phase = "suspended"
)

type RestartPolicy string

const (
	RestartPolicyNever     RestartPolicy = "never"
	RestartPolicyOnFailure RestartPolicy = "on_failure"
	RestartPolicyAlways    RestartPolicy = "always"
)

type RuntimeRecoveryPolicy string

const (
	RuntimeRecoveryRestart RuntimeRecoveryPolicy = "restart"
	RuntimeRecoveryStop    RuntimeRecoveryPolicy = "stop"
)

type ReadinessType string

const (
	ReadinessProcess ReadinessType = "process"
	ReadinessDelay   ReadinessType = "delay"
	ReadinessOutput  ReadinessType = "output"
)

type IOSpec struct {
	Mode     IOMode        `json:"mode"`
	Terminal *TerminalSpec `json:"terminal,omitempty"`
}

type TerminalSpec struct {
	Rows uint16 `json:"rows"`
	Cols uint16 `json:"cols"`
	Term string `json:"term,omitempty"`
}

type RestartSpec struct {
	Policy           RestartPolicy `json:"policy"`
	MaxRestarts      int32         `json:"max_restarts,omitempty"`
	WindowSeconds    int32         `json:"window_seconds,omitempty"`
	InitialBackoffMS int32         `json:"initial_backoff_ms,omitempty"`
	MaxBackoffMS     int32         `json:"max_backoff_ms,omitempty"`
}

type LifecycleSpec struct {
	DesiredState           DesiredState          `json:"desired_state"`
	Restart                RestartSpec           `json:"restart"`
	RuntimeRecovery        RuntimeRecoveryPolicy `json:"runtime_recovery"`
	IdleTimeoutSeconds     int64                 `json:"idle_timeout_seconds,omitempty"`
	MaxLifetimeSeconds     int64                 `json:"max_lifetime_seconds,omitempty"`
	StopGracePeriodSeconds int32                 `json:"stop_grace_period_seconds,omitempty"`
}

type ReadinessSpec struct {
	Type      ReadinessType `json:"type"`
	DelayMS   int32         `json:"delay_ms,omitempty"`
	Output    string        `json:"output,omitempty"`
	TimeoutMS int32         `json:"timeout_ms,omitempty"`
}

type EventRetentionSpec struct {
	MaxBytes      int64 `json:"max_bytes,omitempty"`
	MaxAgeSeconds int64 `json:"max_age_seconds,omitempty"`
}

type SessionSpec struct {
	Name           string             `json:"name,omitempty"`
	Command        []string           `json:"command"`
	CWD            string             `json:"cwd,omitempty"`
	Env            map[string]string  `json:"env,omitempty"`
	IO             IOSpec             `json:"io"`
	Lifecycle      LifecycleSpec      `json:"lifecycle"`
	Readiness      ReadinessSpec      `json:"readiness"`
	EventRetention EventRetentionSpec `json:"event_retention"`
}

type Attempt struct {
	ID                string     `json:"id"`
	Number            int64      `json:"number"`
	RuntimeGeneration int64      `json:"runtime_generation"`
	PID               int        `json:"pid,omitempty"`
	StartedAt         *time.Time `json:"started_at,omitempty"`
	FinishedAt        *time.Time `json:"finished_at,omitempty"`
	ExitCode          *int       `json:"exit_code,omitempty"`
	Reason            string     `json:"reason,omitempty"`
}

type EventCursor struct {
	Earliest int64 `json:"earliest"`
	Latest   int64 `json:"latest"`
}

type Session struct {
	ID                string         `json:"id"`
	Spec              SessionSpec    `json:"spec"`
	SpecVersion       int64          `json:"spec_version"`
	Phase             Phase          `json:"phase"`
	RuntimeGeneration int64          `json:"runtime_generation"`
	Attempt           *Attempt       `json:"attempt,omitempty"`
	RestartCount      int32          `json:"restart_count"`
	Cursor            EventCursor    `json:"cursor"`
	CreatedAt         time.Time      `json:"created_at"`
	UpdatedAt         time.Time      `json:"updated_at"`
	LastActivityAt    time.Time      `json:"last_activity_at"`
	InputReceipts     []InputReceipt `json:"-"`
	CreationKey       string         `json:"-"`
}

type InputRequest struct {
	InputID           string `json:"input_id"`
	ExpectedAttemptID string `json:"expected_attempt_id,omitempty"`
	DataBase64        string `json:"data_base64,omitempty"`
	EOF               bool   `json:"eof,omitempty"`
}

type InputReceipt struct {
	InputID    string    `json:"input_id"`
	AttemptID  string    `json:"attempt_id"`
	Digest     string    `json:"digest"`
	AcceptedAt time.Time `json:"accepted_at"`
}

type InputResponse struct {
	InputID   string `json:"input_id"`
	AttemptID string `json:"attempt_id"`
	Accepted  bool   `json:"accepted"`
	Duplicate bool   `json:"duplicate"`
}

type DesiredStateRequest struct {
	State DesiredState `json:"state"`
}

type CreateAttemptRequest struct {
	ReplaceCurrent bool `json:"replace_current,omitempty"`
}

type SignalRequest struct {
	Signal            string `json:"signal"`
	ExpectedAttemptID string `json:"expected_attempt_id,omitempty"`
}

type TerminalResizeRequest struct {
	Rows              uint16 `json:"rows"`
	Cols              uint16 `json:"cols"`
	ExpectedAttemptID string `json:"expected_attempt_id,omitempty"`
}

type Event struct {
	Seq               int64     `json:"seq"`
	SessionID         string    `json:"session_id"`
	RuntimeGeneration int64     `json:"runtime_generation"`
	AttemptID         string    `json:"attempt_id,omitempty"`
	Type              string    `json:"type"`
	Stream            string    `json:"stream,omitempty"`
	DataBase64        string    `json:"data_base64,omitempty"`
	ExitCode          *int      `json:"exit_code,omitempty"`
	Reason            string    `json:"reason,omitempty"`
	OccurredAt        time.Time `json:"occurred_at"`
}

type EventPage struct {
	Events []Event     `json:"events"`
	Cursor EventCursor `json:"cursor"`
}

type CursorExpiredError struct {
	Earliest int64
}

func (e *CursorExpiredError) Error() string {
	return fmt.Sprintf("%s; earliest available sequence is %d", ErrCursorExpired, e.Earliest)
}

func (e *CursorExpiredError) Unwrap() error {
	return ErrCursorExpired
}

func normalizeSpec(spec SessionSpec) SessionSpec {
	spec.Name = strings.TrimSpace(spec.Name)
	spec.Command = append([]string(nil), spec.Command...)
	if spec.Env == nil {
		spec.Env = map[string]string{}
	} else {
		env := make(map[string]string, len(spec.Env))
		for key, value := range spec.Env {
			env[key] = value
		}
		spec.Env = env
	}
	if spec.IO.Mode == "" {
		spec.IO.Mode = IOModePipes
	}
	if spec.IO.Mode == IOModePTY {
		if spec.IO.Terminal == nil {
			spec.IO.Terminal = &TerminalSpec{Rows: 24, Cols: 80, Term: "xterm-256color"}
		} else {
			terminal := *spec.IO.Terminal
			if terminal.Rows == 0 {
				terminal.Rows = 24
			}
			if terminal.Cols == 0 {
				terminal.Cols = 80
			}
			if terminal.Term == "" {
				terminal.Term = "xterm-256color"
			}
			spec.IO.Terminal = &terminal
		}
	} else {
		spec.IO.Terminal = nil
	}
	if spec.Lifecycle.DesiredState == "" {
		spec.Lifecycle.DesiredState = DesiredStateRunning
	}
	if spec.Lifecycle.Restart.Policy == "" {
		spec.Lifecycle.Restart.Policy = RestartPolicyNever
	}
	if spec.Lifecycle.RuntimeRecovery == "" {
		spec.Lifecycle.RuntimeRecovery = RuntimeRecoveryRestart
	}
	if spec.Lifecycle.StopGracePeriodSeconds == 0 {
		spec.Lifecycle.StopGracePeriodSeconds = defaultStopGraceSeconds
	}
	if spec.Lifecycle.Restart.MaxRestarts == 0 {
		spec.Lifecycle.Restart.MaxRestarts = defaultRestartMaxAttempts
	}
	if spec.Lifecycle.Restart.WindowSeconds == 0 {
		spec.Lifecycle.Restart.WindowSeconds = defaultRestartWindow
	}
	if spec.Lifecycle.Restart.InitialBackoffMS == 0 {
		spec.Lifecycle.Restart.InitialBackoffMS = defaultInitialBackoffMS
	}
	if spec.Lifecycle.Restart.MaxBackoffMS == 0 {
		spec.Lifecycle.Restart.MaxBackoffMS = defaultMaximumBackoffMS
	}
	if spec.Readiness.Type == "" {
		spec.Readiness.Type = ReadinessProcess
	}
	if spec.Readiness.TimeoutMS == 0 {
		spec.Readiness.TimeoutMS = 30_000
	}
	if spec.EventRetention.MaxBytes == 0 {
		spec.EventRetention.MaxBytes = defaultEventMaxBytes
	}
	if spec.EventRetention.MaxAgeSeconds == 0 {
		spec.EventRetention.MaxAgeSeconds = defaultEventMaxAgeSeconds
	}
	return spec
}

func validateSpec(spec SessionSpec) error {
	if len(spec.Command) == 0 || strings.TrimSpace(spec.Command[0]) == "" {
		return errors.New("command is required")
	}
	switch spec.IO.Mode {
	case IOModePipes, IOModePTY:
	default:
		return fmt.Errorf("io.mode must be %q or %q", IOModePipes, IOModePTY)
	}
	switch spec.Lifecycle.DesiredState {
	case DesiredStateRunning, DesiredStateStopped:
	default:
		return fmt.Errorf("lifecycle.desired_state must be %q or %q", DesiredStateRunning, DesiredStateStopped)
	}
	switch spec.Lifecycle.Restart.Policy {
	case RestartPolicyNever, RestartPolicyOnFailure, RestartPolicyAlways:
	default:
		return errors.New("lifecycle.restart.policy is invalid")
	}
	switch spec.Lifecycle.RuntimeRecovery {
	case RuntimeRecoveryRestart, RuntimeRecoveryStop:
	default:
		return errors.New("lifecycle.runtime_recovery is invalid")
	}
	if spec.Lifecycle.IdleTimeoutSeconds < 0 || spec.Lifecycle.MaxLifetimeSeconds < 0 || spec.Lifecycle.StopGracePeriodSeconds < 0 {
		return errors.New("lifecycle timeouts must be non-negative")
	}
	if spec.Lifecycle.Restart.MaxRestarts < 0 || spec.Lifecycle.Restart.WindowSeconds < 0 || spec.Lifecycle.Restart.InitialBackoffMS < 0 || spec.Lifecycle.Restart.MaxBackoffMS < 0 {
		return errors.New("restart limits and backoff must be non-negative")
	}
	if spec.Lifecycle.Restart.MaxBackoffMS < spec.Lifecycle.Restart.InitialBackoffMS {
		return errors.New("restart max_backoff_ms must be greater than or equal to initial_backoff_ms")
	}
	switch spec.Readiness.Type {
	case ReadinessProcess, ReadinessDelay, ReadinessOutput:
	default:
		return errors.New("readiness.type is invalid")
	}
	if spec.Readiness.Type == ReadinessOutput && spec.Readiness.Output == "" {
		return errors.New("readiness.output is required for output readiness")
	}
	if spec.Readiness.DelayMS < 0 || spec.Readiness.TimeoutMS < 0 {
		return errors.New("readiness durations must be non-negative")
	}
	if spec.EventRetention.MaxBytes < 0 || spec.EventRetention.MaxAgeSeconds < 0 {
		return errors.New("event retention limits must be non-negative")
	}
	return nil
}

func cloneSession(value Session) Session {
	value.Spec = normalizeSpec(value.Spec)
	if value.Attempt != nil {
		attempt := *value.Attempt
		value.Attempt = &attempt
	}
	value.InputReceipts = append([]InputReceipt(nil), value.InputReceipts...)
	return value
}
