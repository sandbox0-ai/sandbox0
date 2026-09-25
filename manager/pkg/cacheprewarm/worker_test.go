package cacheprewarm

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type warmFixture struct {
	record     *sandboxstore.SandboxRecord
	claim      *service.ClaimRequest
	command    []string
	commandErr error
	deleted    bool
	finished   bool
	succeeded  bool
}

func (*warmFixture) ListRuntimeCarrierNodes(context.Context, string) ([]sandboxstore.RuntimeCarrierNode, error) {
	return nil, nil
}
func (*warmFixture) ListElasticRuntimeCarrierNodeUIDs(context.Context, string) (map[string]bool, error) {
	return nil, nil
}
func (*warmFixture) BeginRuntimeNodeCachePrewarm(context.Context, sandboxstore.RuntimeNodeCachePrewarmKey) (int, int, bool, error) {
	return 0, 0, false, nil
}
func (f *warmFixture) FinishRuntimeNodeCachePrewarm(_ context.Context, _ sandboxstore.RuntimeNodeCachePrewarmKey, _ int, succeeded bool, _ string) error {
	f.finished, f.succeeded = true, succeeded
	return nil
}
func (f *warmFixture) RuntimeNodeCachePrewarmRestored(_ context.Context, _ string, _ sandboxstore.RuntimeCarrierNode, _ string, ready int) (bool, error) {
	return f.deleted && ready == 2, nil
}
func (f *warmFixture) GetSandbox(context.Context, string) (*sandboxstore.SandboxRecord, error) {
	return f.record, nil
}
func (f *warmFixture) ClaimSandbox(_ context.Context, request *service.ClaimRequest) (*service.ClaimResponse, error) {
	f.claim = request
	id, err := naming.SandboxNameForOperation("ali-ue1", "default", request.OperationID)
	if err != nil {
		return nil, err
	}
	f.record = &sandboxstore.SandboxRecord{ID: id}
	return &service.ClaimResponse{SandboxID: id, ProcdAddress: "http://procd"}, nil
}
func (f *warmFixture) TerminateSandbox(_ context.Context, _ string) error {
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
	require.NoError(t, w.warm(t.Context(), key, node, 2, 1, 0))
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
	require.Error(t, w.warm(t.Context(), key, node, 2, 1, 0))
	require.True(t, f.deleted)
	require.True(t, f.finished)
	require.False(t, f.succeeded)
}
