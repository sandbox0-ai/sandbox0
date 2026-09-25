package cacheprewarm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/carrierpool"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	"go.uber.org/zap"
)

const teamID = "sandbox0-cache-prewarm"

type Store interface {
	ListRuntimeCarrierNodes(context.Context, string) ([]sandboxstore.RuntimeCarrierNode, error)
	ListElasticRuntimeCarrierNodeUIDs(context.Context, string) (map[string]bool, error)
	BeginRuntimeNodeCachePrewarm(context.Context, sandboxstore.RuntimeNodeCachePrewarmKey) (int, int, bool, error)
	FinishRuntimeNodeCachePrewarm(context.Context, sandboxstore.RuntimeNodeCachePrewarmKey, int, bool, string) error
	RuntimeNodeCachePrewarmRestored(context.Context, string, sandboxstore.RuntimeCarrierNode, string, int) (bool, error)
	GetSandbox(context.Context, string) (*sandboxstore.SandboxRecord, error)
}

type Templates interface {
	GetTemplateForTeam(context.Context, string, string) (*template.Template, error)
}

type Commander interface {
	CreateCommand(context.Context, string, string, []string) (*procdapi.ContextResponse, error)
}

type Tokens interface {
	GenerateToken(string, string, string) (string, error)
}

type Windows interface {
	ActivePrewarmWindows(time.Time) []carrierpool.PrewarmWindow
}

type Worker struct {
	store                    Store
	templates                Templates
	claimer                  service.SandboxClaimer
	terminator               service.SandboxTerminator
	commander                Commander
	tokens                   Tokens
	carriers                 Windows
	clusterID, compatibility string
	logger                   *zap.Logger
}

func New(store Store, templates Templates, claimer service.SandboxClaimer, terminator service.SandboxTerminator,
	commander Commander, tokens Tokens, carriers Windows, clusterID, compatibility string, logger *zap.Logger) (*Worker, error) {
	if store == nil || templates == nil || claimer == nil || terminator == nil || commander == nil || tokens == nil ||
		carriers == nil || clusterID == "" || compatibility == "" {
		return nil, errors.New("cache prewarm dependencies are required")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Worker{store: store, templates: templates, claimer: claimer, terminator: terminator,
		commander: commander, tokens: tokens, carriers: carriers, clusterID: clusterID,
		compatibility: compatibility, logger: logger}, nil
}

func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()
	for {
		if err := w.Reconcile(ctx); err != nil && ctx.Err() == nil {
			w.logger.Warn("Runtime node cache prewarm failed", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Reconcile warms at most one newly admitted elastic node per pass. It never
// occupies the carrier controller's reconciliation goroutine.
func (w *Worker) Reconcile(ctx context.Context) error {
	windows := w.carriers.ActivePrewarmWindows(time.Now().UTC())
	if len(windows) == 0 {
		return nil
	}
	tpl, err := w.templates.GetTemplateForTeam(ctx, teamID, "default")
	if err != nil {
		return err
	}
	if tpl == nil || !tpl.ReadyForClaim() {
		return errors.New("default prewarm template is not ready")
	}
	artifact, err := json.Marshal(struct {
		Spec          any
		RootFS        any
		Compatibility string
	}{tpl.Spec, tpl.RootFS, w.compatibility})
	if err != nil {
		return err
	}
	hash := sha256.Sum256(artifact)
	templateDigest := "sha256:" + hex.EncodeToString(hash[:])
	nodes, err := w.store.ListRuntimeCarrierNodes(ctx, w.clusterID)
	if err != nil {
		return err
	}
	elastic, err := w.store.ListElasticRuntimeCarrierNodeUIDs(ctx, w.clusterID)
	if err != nil {
		return err
	}
	for _, window := range windows {
		if window.SecurityClass != "privileged" {
			continue
		}
		for _, node := range nodes {
			readyBefore := node.ReadyByCompatibility[w.compatibility]
			if !elastic[node.NodeUID] || node.Retiring || node.Pending || node.StaleIdentity || readyBefore < 1 {
				continue
			}
			key := sandboxstore.RuntimeNodeCachePrewarmKey{WindowName: window.Name,
				ClusterID: w.clusterID, NodeUID: node.NodeUID, NodeBootID: node.NodeBootID, TemplateDigest: templateDigest}
			attempt, previous, acquired, err := w.store.BeginRuntimeNodeCachePrewarm(ctx, key)
			if err != nil {
				return err
			}
			if !acquired {
				continue
			}
			if err := w.warm(ctx, key, node, readyBefore, attempt, previous); err != nil {
				return err
			}
			return nil
		}
	}
	return nil
}

func operationID(key sandboxstore.RuntimeNodeCachePrewarmKey, attempt int) string {
	sum := sha256.Sum256([]byte(key.WindowName + "\x00" + key.NodeUID + "\x00" + key.NodeBootID + "\x00" + key.TemplateDigest))
	return "cache-prewarm-" + hex.EncodeToString(sum[:12]) + "-" + strconv.Itoa(attempt)
}

func (w *Worker) cleanup(ctx context.Context, sandboxID string) error {
	record, err := w.store.GetSandbox(ctx, sandboxID)
	if err != nil {
		return err
	}
	if record == nil || !record.DeletedAt.IsZero() {
		return nil
	}
	return w.terminator.TerminateSandbox(ctx, sandboxID)
}

func (w *Worker) warm(parent context.Context, key sandboxstore.RuntimeNodeCachePrewarmKey,
	node sandboxstore.RuntimeCarrierNode, readyBefore, attempt, previous int) (resultErr error) {
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	op := operationID(key, attempt)
	sandboxID, err := naming.SandboxNameForOperation(w.clusterID, "default", op)
	if err != nil {
		return err
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := w.cleanup(cleanupCtx, sandboxID); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("cleanup prewarm sandbox: %w", err))
		}
		finishCtx, finishCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer finishCancel()
		if err := w.store.FinishRuntimeNodeCachePrewarm(finishCtx, key, attempt, resultErr == nil, fmt.Sprint(resultErr)); err != nil {
			resultErr = errors.Join(resultErr, err)
		}
	}()
	if previous > 0 {
		oldID, err := naming.SandboxNameForOperation(w.clusterID, "default", operationID(key, previous))
		if err != nil {
			return err
		}
		if err := w.cleanup(ctx, oldID); err != nil {
			return fmt.Errorf("clean previous prewarm sandbox: %w", err)
		}
		for {
			old, err := w.store.GetSandbox(ctx, oldID)
			if err != nil {
				return err
			}
			if old == nil || !old.DeletedAt.IsZero() {
				break
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("previous prewarm cleanup timed out: %w", ctx.Err())
			case <-time.After(time.Second):
			}
		}
	}
	hardTTL := int32(300)
	ttl := int32(300)
	response, err := w.claimer.ClaimSandbox(ctx, &service.ClaimRequest{TeamID: teamID, Template: "default", OperationID: op,
		TargetNode: &service.ClaimNodeTarget{NodeID: node.NodeID, NodeUID: node.NodeUID, NodeBootID: node.NodeBootID},
		Config:     &sandboxstore.SandboxConfig{TTL: &ttl, HardTTL: &hardTTL}})
	if err != nil {
		return fmt.Errorf("claim node %s: %w", node.NodeID, err)
	}
	if response == nil || response.SandboxID != sandboxID || response.ProcdAddress == "" {
		return errors.New("prewarm claim returned unexpected binding")
	}
	token, err := w.tokens.GenerateToken(teamID, "", sandboxID)
	if err != nil {
		return err
	}
	command, err := w.commander.CreateCommand(ctx, response.ProcdAddress, token, []string{"node", "-v"})
	if err != nil {
		return fmt.Errorf("run node -v: %w", err)
	}
	if command == nil || command.Stdout == nil || *command.Stdout == "" {
		return errors.New("node -v produced no stdout")
	}
	if err := w.cleanup(ctx, sandboxID); err != nil {
		return err
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		restored, err := w.store.RuntimeNodeCachePrewarmRestored(ctx, sandboxID, node, w.compatibility, readyBefore)
		if err != nil {
			return err
		}
		if restored {
			w.logger.Info("Runtime node cache prewarm completed", zap.String("node_id", node.NodeID), zap.String("node_uid", node.NodeUID),
				zap.String("window", key.WindowName), zap.Int("ready_carriers", readyBefore))
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("prewarm cleanup and carrier restoration timed out: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}
