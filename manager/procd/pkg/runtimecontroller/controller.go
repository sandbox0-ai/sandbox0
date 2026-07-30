package runtimecontroller

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

type Reporter func(runtimecontrol.Observation) error

type State struct {
	Desired           runtimecontrol.DesiredState
	Observed          runtimecontrol.ObservedState
	Revision          string
	RuntimeGeneration int64
	Reason            string
}

// Controller applies level-triggered CTLD snapshots to one procd process.
type Controller struct {
	contextManager    *ctxpkg.Manager
	sessionSupervisor *session.Supervisor
	fileManager       *file.Manager
	dispatcher        *webhook.Dispatcher
	httpPort          int
	logger            *zap.Logger

	mu    sync.RWMutex
	state State

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
			Observed: runtimecontrol.ObservedDisconnected,
			Reason:   "runtime control stream is disconnected",
		},
	}
}

func (c *Controller) State() State {
	if c == nil {
		return State{Observed: runtimecontrol.ObservedDisconnected}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

func (c *Controller) MarkDisconnected(reason string) {
	if c == nil {
		return
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "runtime control stream is disconnected"
	}
	c.mu.Lock()
	c.state.Observed = runtimecontrol.ObservedDisconnected
	c.state.Reason = reason
	c.mu.Unlock()
}

// CanServe reports whether authenticated sandbox APIs may execute.
func (c *Controller) CanServe() (bool, string) {
	state := c.State()
	if state.Observed == runtimecontrol.ObservedReady {
		return true, ""
	}
	if state.Reason != "" {
		return false, state.Reason
	}
	return false, "sandbox runtime is not ready"
}

func (c *Controller) Probe(kind sandboxprobe.Kind) sandboxprobe.Response {
	state := c.State()
	switch state.Observed {
	case runtimecontrol.ObservedStandby:
		return sandboxprobe.Passed(kind, "RuntimeStandby", "runtime is ready for assignment", nil)
	case runtimecontrol.ObservedWaiting:
		if kind == sandboxprobe.KindReadiness {
			return sandboxprobe.Suspended(kind, "RuntimeWaitingStorage", "runtime assignment is waiting for storage", nil)
		}
		return sandboxprobe.Passed(kind, "RuntimeControlConnected", "runtime control stream is connected", nil)
	case runtimecontrol.ObservedLoading:
		if kind == sandboxprobe.KindReadiness {
			return sandboxprobe.Suspended(kind, "RuntimeLoading", "runtime state is loading", nil)
		}
		return sandboxprobe.Passed(kind, "RuntimeControlConnected", "runtime control stream is connected", nil)
	case runtimecontrol.ObservedRecovering:
		if kind == sandboxprobe.KindReadiness {
			return sandboxprobe.Suspended(kind, "RuntimeRecovering", "runtime processes are recovering", nil)
		}
		return sandboxprobe.Passed(kind, "RuntimeControlConnected", "runtime control stream is connected", nil)
	case runtimecontrol.ObservedReady:
		return sandboxprobe.Passed(kind, "RuntimeReady", "runtime assignment is ready", nil)
	case runtimecontrol.ObservedFailed:
		if kind != sandboxprobe.KindReadiness {
			return sandboxprobe.Passed(kind, "ProcdLive", "procd is live but runtime activation failed", nil)
		}
		return sandboxprobe.Failed(kind, "RuntimeFailed", state.Reason, nil)
	default:
		if kind == sandboxprobe.KindLiveness {
			return sandboxprobe.Passed(kind, "ProcdLive", "procd is live while runtime control reconnects", nil)
		}
		return sandboxprobe.Failed(kind, "RuntimeControlDisconnected", "runtime control stream is disconnected", nil)
	}
}

// HandleSnapshot applies a complete desired snapshot. Deterministic activation
// errors are reported as failed and remain level-triggered until the manifest
// changes or the process restarts.
func (c *Controller) HandleSnapshot(ctx context.Context, snapshot runtimecontrol.Snapshot, report Reporter) error {
	if c == nil {
		return errors.New("runtime controller is not configured")
	}
	if report == nil {
		return errors.New("runtime observation reporter is required")
	}
	c.applyMu.Lock()
	defer c.applyMu.Unlock()

	switch snapshot.State {
	case runtimecontrol.DesiredStandby:
		return c.observe(report, snapshot, runtimecontrol.ObservedStandby, "")
	case runtimecontrol.DesiredWaitingStorage:
		if snapshot.Assignment == nil {
			return c.failActiveSnapshot(report, snapshot, errors.New("runtime assignment is missing"))
		}
		return c.observe(report, snapshot, runtimecontrol.ObservedWaiting, "runtime assignment is waiting for storage")
	case runtimecontrol.DesiredActive:
		return c.activate(ctx, snapshot, report)
	case runtimecontrol.DesiredRevoked:
		c.setState(snapshot, runtimecontrol.ObservedDisconnected, snapshot.Reason)
		return nil
	default:
		return fmt.Errorf("unsupported desired runtime state %q", snapshot.State)
	}
}

func (c *Controller) activate(ctx context.Context, snapshot runtimecontrol.Snapshot, report Reporter) error {
	assignment := snapshot.Assignment
	if assignment == nil {
		return c.failActiveSnapshot(report, snapshot, errors.New("runtime assignment is missing"))
	}
	if err := assignment.Validate(); err != nil {
		return c.failActiveSnapshot(report, snapshot, err)
	}
	revision, err := assignment.Revision()
	if err != nil {
		return c.failActiveSnapshot(report, snapshot, err)
	}
	if revision != snapshot.Revision {
		return c.failActiveSnapshot(report, snapshot, errors.New("runtime assignment revision does not match snapshot"))
	}
	current := c.State()
	if current.Observed == runtimecontrol.ObservedReady &&
		current.Revision == snapshot.Revision &&
		current.RuntimeGeneration == assignment.RuntimeGeneration {
		return c.observe(report, snapshot, runtimecontrol.ObservedReady, "")
	}

	if err := c.observe(report, snapshot, runtimecontrol.ObservedLoading, "runtime assignment is loading"); err != nil {
		return err
	}
	if err := ensureMountDirs(assignment.MountDirs); err != nil {
		return c.failActiveSnapshot(report, snapshot, err)
	}
	if err := c.configureAssignment(*assignment); err != nil {
		return c.failActiveSnapshot(report, snapshot, err)
	}
	if err := c.observe(report, snapshot, runtimecontrol.ObservedRecovering, "runtime processes are recovering"); err != nil {
		return err
	}
	if c.sessionSupervisor != nil {
		err := c.sessionSupervisor.Activate(session.Activation{
			SandboxID:               assignment.SandboxID,
			RuntimeGeneration:       assignment.RuntimeGeneration,
			SandboxEnv:              assignment.EnvVars,
			ResetCopiedSessionState: assignment.ResetCopiedSessionState,
		})
		if err != nil {
			return c.failActiveSnapshot(report, snapshot, err)
		}
	}
	if err := c.enqueueSandboxReady(ctx, *assignment); err != nil {
		return c.failActiveSnapshot(report, snapshot, err)
	}
	return c.observe(report, snapshot, runtimecontrol.ObservedReady, "")
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

func (c *Controller) enqueueSandboxReady(_ context.Context, assignment runtimecontrol.Assignment) error {
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

func (c *Controller) observe(
	report Reporter,
	snapshot runtimecontrol.Snapshot,
	state runtimecontrol.ObservedState,
	reason string,
) error {
	c.setState(snapshot, state, reason)
	observation := runtimecontrol.Observation{State: state, Reason: reason}
	if snapshot.Assignment != nil {
		observation.Revision = snapshot.Revision
		observation.RuntimeGeneration = snapshot.Assignment.RuntimeGeneration
	}
	return report(observation)
}

func (c *Controller) failActiveSnapshot(report Reporter, snapshot runtimecontrol.Snapshot, cause error) error {
	reason := "runtime activation failed"
	if cause != nil && strings.TrimSpace(cause.Error()) != "" {
		reason = cause.Error()
	}
	if err := c.observe(report, snapshot, runtimecontrol.ObservedFailed, reason); err != nil {
		return err
	}
	c.logger.Error("Runtime activation failed", zap.Error(cause))
	return nil
}

func (c *Controller) setState(snapshot runtimecontrol.Snapshot, observed runtimecontrol.ObservedState, reason string) {
	generation := int64(0)
	if snapshot.Assignment != nil {
		generation = snapshot.Assignment.RuntimeGeneration
	}
	c.mu.Lock()
	c.state = State{
		Desired:           snapshot.State,
		Observed:          observed,
		Revision:          snapshot.Revision,
		RuntimeGeneration: generation,
		Reason:            strings.TrimSpace(reason),
	}
	c.mu.Unlock()
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

func ensureMountDirs(dirs []string) error {
	for i := range dirs {
		dir := filepath.Clean(strings.TrimSpace(dirs[i]))
		if dir == "." || dir == string(filepath.Separator) || !filepath.IsAbs(dir) {
			return fmt.Errorf("mount_dirs[%d] must be an absolute non-root path", i)
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create mount dir %q: %w", dir, err)
		}
	}
	return nil
}
