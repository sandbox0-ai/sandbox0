package runtimecontroller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	ctxpkg "github.com/sandbox0-ai/sandbox0/manager/procd/pkg/context"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/file"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/session"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/webhook"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
	"go.uber.org/zap"
)

// Phase is the local activation state of an immutable procd assignment.
type Phase string

const (
	PhasePending    Phase = "pending"
	PhaseActivating Phase = "activating"
	PhaseReady      Phase = "ready"
	PhaseFailed     Phase = "failed"
)

// State is procd's local activation state.
type State struct {
	Phase             Phase
	Revision          string
	RuntimeGeneration int64
	Reason            string
}

// Controller activates the immutable assignment of one procd process.
type Controller struct {
	contextManager    *ctxpkg.Manager
	sessionSupervisor *session.Supervisor
	fileManager       *file.Manager
	dispatcher        *webhook.Dispatcher
	httpPort          int
	logger            *zap.Logger

	mu      sync.RWMutex
	state   State
	applyMu sync.Mutex
	watchMu sync.Mutex
	watch   struct {
		path        string
		unsubscribe func() error
	}
	readyMu      sync.Mutex
	readySentKey string
}

func New(
	contextManager *ctxpkg.Manager,
	sessionSupervisor *session.Supervisor,
	fileManager *file.Manager,
	dispatcher *webhook.Dispatcher,
	httpPort int,
	logger *zap.Logger,
) *Controller {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Controller{
		contextManager:    contextManager,
		sessionSupervisor: sessionSupervisor,
		fileManager:       fileManager,
		dispatcher:        dispatcher,
		httpPort:          httpPort,
		logger:            logger,
		state: State{
			Phase:  PhasePending,
			Reason: "runtime assignment has not been activated",
		},
	}
}

func (c *Controller) State() State {
	if c == nil {
		return State{Phase: PhaseFailed, Reason: "runtime controller is not configured"}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

// CanServe reports whether authenticated sandbox APIs may execute.
func (c *Controller) CanServe() (bool, string) {
	state := c.State()
	if state.Phase == PhaseReady {
		return true, ""
	}
	if state.Reason != "" {
		return false, state.Reason
	}
	return false, "sandbox runtime is not ready"
}

func (c *Controller) Probe(kind sandboxprobe.Kind) sandboxprobe.Response {
	state := c.State()
	if kind == sandboxprobe.KindLiveness {
		return sandboxprobe.Passed(kind, "ProcdLive", "procd is live", nil)
	}
	switch state.Phase {
	case PhaseReady:
		return sandboxprobe.Passed(kind, "RuntimeReady", "runtime assignment is ready", nil)
	case PhaseFailed:
		return sandboxprobe.Failed(kind, "RuntimeFailed", state.Reason, nil)
	case PhaseActivating:
		return sandboxprobe.Suspended(kind, "RuntimeActivating", "runtime assignment is activating", nil)
	default:
		return sandboxprobe.Suspended(kind, "RuntimePending", "runtime assignment has not been activated", nil)
	}
}

// Activate applies the process's immutable assignment exactly once.
func (c *Controller) Activate(ctx context.Context, assignment runtimecontrol.Assignment) error {
	if c == nil {
		return errors.New("runtime controller is not configured")
	}
	if err := assignment.Validate(); err != nil {
		return err
	}
	revision, err := assignment.Revision()
	if err != nil {
		return err
	}

	c.applyMu.Lock()
	defer c.applyMu.Unlock()
	current := c.State()
	if current.Phase == PhaseReady {
		if current.Revision == revision && current.RuntimeGeneration == assignment.RuntimeGeneration {
			return nil
		}
		return errors.New("runtime assignment is immutable after activation")
	}
	if current.Revision != "" && current.Revision != revision {
		return errors.New("runtime assignment changed during activation")
	}
	c.setState(PhaseActivating, revision, assignment.RuntimeGeneration, "runtime assignment is activating")

	if err := ctx.Err(); err != nil {
		return c.failActivation(revision, assignment.RuntimeGeneration, err)
	}
	if err := c.configureAssignment(assignment); err != nil {
		return c.failActivation(revision, assignment.RuntimeGeneration, err)
	}
	if c.sessionSupervisor != nil {
		err := c.sessionSupervisor.Activate(session.Activation{
			SandboxID:               assignment.SandboxID,
			RuntimeGeneration:       assignment.RuntimeGeneration,
			SandboxEnv:              assignment.EnvVars,
			ResetCopiedSessionState: assignment.ResetCopiedSessionState,
		})
		if err != nil {
			return c.failActivation(revision, assignment.RuntimeGeneration, err)
		}
	}
	if err := c.enqueueSandboxReady(assignment); err != nil {
		return c.failActivation(revision, assignment.RuntimeGeneration, err)
	}
	c.setState(PhaseReady, revision, assignment.RuntimeGeneration, "")
	return nil
}

func (c *Controller) failActivation(revision string, generation int64, cause error) error {
	reason := "runtime activation failed"
	if cause != nil && strings.TrimSpace(cause.Error()) != "" {
		reason = cause.Error()
	}
	c.setState(PhaseFailed, revision, generation, reason)
	c.logger.Error("Runtime activation failed", zap.Error(cause))
	return fmt.Errorf("activate runtime assignment: %w", cause)
}

func (c *Controller) setState(phase Phase, revision string, generation int64, reason string) {
	c.mu.Lock()
	c.state = State{
		Phase:             phase,
		Revision:          revision,
		RuntimeGeneration: generation,
		Reason:            strings.TrimSpace(reason),
	}
	c.mu.Unlock()
}

func (c *Controller) configureAssignment(assignment runtimecontrol.Assignment) error {
	if c.contextManager != nil {
		c.contextManager.SetSandboxEnvVars(assignment.EnvVars)
	}
	if c.dispatcher == nil {
		if assignment.Webhook != nil && strings.TrimSpace(assignment.Webhook.URL) != "" {
			return errors.New("webhook dispatcher is not configured")
		}
		return nil
	}

	webhookURL := ""
	webhookSecret := ""
	watchDir := ""
	if assignment.Webhook != nil {
		webhookURL = strings.TrimSpace(assignment.Webhook.URL)
		webhookSecret = assignment.Webhook.Secret
		watchDir = strings.TrimSpace(assignment.Webhook.WatchDir)
	}
	c.dispatcher.SetConfig(webhookURL, webhookSecret)
	c.dispatcher.SetIdentity(assignment.SandboxID, assignment.TeamID)
	return c.configureWebhookWatch(webhookURL, watchDir)
}

func (c *Controller) configureWebhookWatch(webhookURL, watchDir string) error {
	if c.fileManager == nil {
		if webhookURL != "" && watchDir != "" {
			return errors.New("file manager is not configured")
		}
		return nil
	}
	c.watchMu.Lock()
	defer c.watchMu.Unlock()

	if webhookURL == "" || watchDir == "" {
		if c.watch.unsubscribe != nil {
			_ = c.watch.unsubscribe()
		}
		c.watch.path = ""
		c.watch.unsubscribe = nil
		return nil
	}
	if c.watch.path == watchDir && c.watch.unsubscribe != nil {
		return nil
	}
	if c.watch.unsubscribe != nil {
		_ = c.watch.unsubscribe()
		c.watch.path = ""
		c.watch.unsubscribe = nil
	}
	_, unsubscribe, err := c.fileManager.SubscribeWatch(watchDir, true, func(event file.WatchEvent) {
		if event.Type == file.EventInvalidate {
			return
		}
		payload := map[string]any{
			"event_type": event.Type,
			"path":       event.Path,
		}
		if event.OldPath != "" {
			payload["old_path"] = event.OldPath
		}
		if _, err := c.dispatcher.Enqueue(webhook.Event{
			EventType: webhook.EventTypeFileModified,
			Payload:   payload,
		}); err != nil {
			c.logger.Warn("Failed to enqueue file webhook", zap.Error(err))
		}
	})
	if err != nil {
		return fmt.Errorf("watch webhook directory: %w", err)
	}
	c.watch.path = watchDir
	c.watch.unsubscribe = unsubscribe
	return nil
}

func (c *Controller) enqueueSandboxReady(assignment runtimecontrol.Assignment) error {
	if c.dispatcher == nil || assignment.Webhook == nil || strings.TrimSpace(assignment.Webhook.URL) == "" {
		return nil
	}
	c.readyMu.Lock()
	defer c.readyMu.Unlock()
	key := assignment.SandboxID + "\x00" + strings.TrimSpace(assignment.Webhook.URL)
	if c.readySentKey == key {
		return nil
	}
	if _, err := c.dispatcher.Enqueue(webhook.Event{
		EventType: webhook.EventTypeSandboxReady,
		Payload: map[string]any{
			"http_port":  c.httpPort,
			"sandbox_id": assignment.SandboxID,
		},
	}); err != nil {
		return fmt.Errorf("enqueue sandbox ready webhook: %w", err)
	}
	c.readySentKey = key
	return nil
}

func (c *Controller) Close() {
	if c == nil {
		return
	}
	c.watchMu.Lock()
	defer c.watchMu.Unlock()
	if c.watch.unsubscribe != nil {
		_ = c.watch.unsubscribe()
	}
	c.watch.path = ""
	c.watch.unsubscribe = nil
}
