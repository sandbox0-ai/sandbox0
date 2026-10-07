package cacheprewarm

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/carrierpool"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type warmFixture struct {
	records    map[string]*sandboxstore.SandboxRecord
	deletedIDs []string
	nodes      []sandboxstore.RuntimeCarrierNode
	templates  map[string]*template.Template
	windows    []carrierpool.PrewarmWindow
	seen       map[sandboxstore.RuntimeNodeCachePrewarmKey]bool
	lookups    []string

	record     *sandboxstore.SandboxRecord
	claim      *service.ClaimRequest
	command    []string
	commandErr error
	deleted    bool
	finished   bool
	succeeded  bool
}

func (f *warmFixture) ListRuntimeCarrierNodes(context.Context, string) ([]sandboxstore.RuntimeCarrierNode, error) {
	return f.nodes, nil
}
func (f *warmFixture) ListElasticRuntimeCarrierNodeUIDs(context.Context, string) (map[string]bool, error) {
	elastic := map[string]bool{}
	for _, n := range f.nodes {
		elastic[n.NodeUID] = true
	}
	return elastic, nil
}
func (f *warmFixture) BeginRuntimeNodeCachePrewarm(_ context.Context, key sandboxstore.RuntimeNodeCachePrewarmKey) (int, int, bool, error) {
	if f.seen == nil || f.seen[key] {
		return 0, 0, false, nil
	}
	f.seen[key] = true
	return 1, 0, true, nil
}
func (f *warmFixture) FinishRuntimeNodeCachePrewarm(_ context.Context, _ sandboxstore.RuntimeNodeCachePrewarmKey, _ int, succeeded bool, _ string) error {
	f.finished, f.succeeded = true, succeeded
	return nil
}
func (f *warmFixture) RuntimeNodeCachePrewarmRestored(_ context.Context, _ string, _ sandboxstore.RuntimeCarrierNode, _ string, ready int) (bool, error) {
	return f.deleted && ready == 2, nil
}
func (f *warmFixture) GetSandbox(_ context.Context, id string) (*sandboxstore.SandboxRecord, error) {
	if f.records != nil {
		return f.records[id], nil
	}
	return f.record, nil
}
func (f *warmFixture) ClaimSandbox(_ context.Context, request *service.ClaimRequest) (*service.ClaimResponse, error) {
	f.claim = request
	f.deleted = false
	id, err := naming.SandboxNameForOperation("ali-ue1", request.Template, request.OperationID)
	if err != nil {
		return nil, err
	}
	f.record = &sandboxstore.SandboxRecord{ID: id}
	if f.records != nil {
		f.records[id] = f.record
	}
	return &service.ClaimResponse{SandboxID: id, ProcdAddress: "http://procd"}, nil
}
func (f *warmFixture) TerminateSandbox(_ context.Context, id string) error {
	f.deletedIDs = append(f.deletedIDs, id)
	if f.records != nil {
		f.records[id].DeletedAt = time.Now()
		f.deleted = true
		return nil
	}
	f.deleted = true
	f.record.DeletedAt = time.Now()
	return nil
}
func (f *warmFixture) CreateCommand(_ context.Context, _, _ string, command []string) (*procdapi.ContextResponse, error) {
	f.command = command
	if f.commandErr != nil {
		return nil, f.commandErr
	}
	stdout := "v22.0.0\n"
	zero := 0
	return &procdapi.ContextResponse{Stdout: &stdout, ExitCode: &zero}, nil
}
func (*warmFixture) GenerateToken(string, string, string) (string, error) { return "token", nil }

func TestWarmUsesExactNodeAndCleansSandboxBeforeSuccess(t *testing.T) {
	f := &warmFixture{}
	w := &Worker{store: f, claimer: f, terminator: f, commander: f, tokens: f,
		clusterID: "ali-ue1", compatibility: "priv", logger: zap.NewNop()}
	key := sandboxstore.RuntimeNodeCachePrewarmKey{WindowName: "benchmark", ClusterID: "ali-ue1", NodeUID: "uid", NodeBootID: "boot", TemplateDigest: "sha256:abc"}
	node := sandboxstore.RuntimeCarrierNode{ClusterID: "ali-ue1", NodeID: "node", NodeUID: "uid", NodeBootID: "boot"}
	require.NoError(t, w.warm(t.Context(), key, "default", node, 2, 1, 0))
	require.Equal(t, &service.ClaimNodeTarget{NodeID: "node", NodeUID: "uid", NodeBootID: "boot"}, f.claim.TargetNode)
	require.Equal(t, []string{"node", "-v"}, f.command)
	require.True(t, f.deleted)
	require.True(t, f.finished && f.succeeded)
	require.EqualValues(t, 300, *f.claim.Config.TTL)
	require.EqualValues(t, 300, *f.claim.Config.HardTTL)
}

func TestWarmCommandFailureStillCleansSandbox(t *testing.T) {
	f := &warmFixture{commandErr: errors.New("process failed")}
	w := &Worker{store: f, claimer: f, terminator: f, commander: f, tokens: f,
		clusterID: "ali-ue1", compatibility: "priv", logger: zap.NewNop()}
	key := sandboxstore.RuntimeNodeCachePrewarmKey{WindowName: "benchmark", ClusterID: "ali-ue1", NodeUID: "uid", NodeBootID: "boot", TemplateDigest: "sha256:abc"}
	node := sandboxstore.RuntimeCarrierNode{ClusterID: "ali-ue1", NodeID: "node", NodeUID: "uid", NodeBootID: "boot"}
	require.Error(t, w.warm(t.Context(), key, "default", node, 2, 1, 0))
	require.True(t, f.deleted)
	require.True(t, f.finished)
	require.False(t, f.succeeded)
}

func (f *warmFixture) GetTemplateForTeam(_ context.Context, _, id string) (*template.Template, error) {
	f.lookups = append(f.lookups, id)
	return f.templates[id], nil
}
func (f *warmFixture) ActivePrewarmWindows(time.Time) []carrierpool.PrewarmWindow { return f.windows }

func TestReconcileWarmsConfiguredTemplateAndRewarmsChangedArtifact(t *testing.T) {
	tpl := &template.Template{TemplateID: "coding-agent"}
	f := &warmFixture{
		nodes:     []sandboxstore.RuntimeCarrierNode{{NodeID: "node", NodeUID: "uid", NodeBootID: "boot", ReadyByCompatibility: map[string]int{"priv": 2}}},
		templates: map[string]*template.Template{"coding-agent": tpl},
		windows:   []carrierpool.PrewarmWindow{{Name: "batch", SecurityClass: "privileged", CacheTemplate: "coding-agent"}},
		seen:      map[sandboxstore.RuntimeNodeCachePrewarmKey]bool{},
	}
	w := &Worker{store: f, templates: f, carriers: f, claimer: f, terminator: f, commander: f, tokens: f, clusterID: "ali-ue1", compatibility: "priv", logger: zap.NewNop()}
	require.NoError(t, w.Reconcile(t.Context()))
	require.Equal(t, "coding-agent", f.claim.Template)
	require.True(t, f.deleted && f.succeeded)
	require.Len(t, f.seen, 1)
	require.NoError(t, w.Reconcile(t.Context()))
	require.Len(t, f.seen, 1)
	tpl.Spec.Description = "changed immutable template"
	require.NoError(t, w.Reconcile(t.Context()))
	require.Len(t, f.seen, 2)
	f.nodes[0].NodeBootID = "next-boot"
	require.NoError(t, w.Reconcile(t.Context()))
	require.Len(t, f.seen, 3)
}

func TestReconcileDefaultsToDefaultTemplate(t *testing.T) {
	f := &warmFixture{templates: map[string]*template.Template{"default": {}}, windows: []carrierpool.PrewarmWindow{{Name: "batch", SecurityClass: "privileged"}}}
	w := &Worker{store: f, templates: f, carriers: f, clusterID: "ali-ue1", compatibility: "priv"}
	require.NoError(t, w.Reconcile(t.Context()))
	require.Equal(t, []string{"default"}, f.lookups)
}

func TestConfiguredTemplateRetryCleansExactPreviousSandbox(t *testing.T) {
	key := sandboxstore.RuntimeNodeCachePrewarmKey{WindowName: "batch", ClusterID: "ali-ue1", NodeUID: "uid", NodeBootID: "boot", TemplateDigest: "sha256:abc"}
	previous, err := naming.SandboxNameForOperation("ali-ue1", "coding-agent", operationID(key, 1))
	require.NoError(t, err)
	current, err := naming.SandboxNameForOperation("ali-ue1", "coding-agent", operationID(key, 2))
	require.NoError(t, err)
	f := &warmFixture{records: map[string]*sandboxstore.SandboxRecord{previous: {ID: previous}}}
	w := &Worker{store: f, claimer: f, terminator: f, commander: f, tokens: f, clusterID: "ali-ue1", compatibility: "priv", logger: zap.NewNop()}
	require.NoError(t, w.warm(t.Context(), key, "coding-agent", sandboxstore.RuntimeCarrierNode{NodeID: "node", NodeUID: "uid", NodeBootID: "boot"}, 2, 2, 1))
	require.Equal(t, []string{previous, current}, f.deletedIDs)
	require.True(t, f.succeeded)
}
