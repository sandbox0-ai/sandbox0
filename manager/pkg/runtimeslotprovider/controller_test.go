package runtimeslotprovider

import (
	"bytes"
	"context"
	"errors"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotreconciler"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/providerinstance"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type storeStub struct {
	grant       *sandboxstore.RootFSWriterGrant
	proof       *sandboxstore.RuntimeProviderAbsenceProof
	request     *sandboxstore.CompleteRuntimeSlotProviderAbandonRequest
	recordCalls int
}

func (s *storeStub) GetRootFSWriterGrant(context.Context, string) (*sandboxstore.RootFSWriterGrant, error) {
	return s.grant, nil
}
func (s *storeStub) RecordRuntimeProviderInstanceObservation(context.Context, sandboxstore.RuntimeProviderTarget, providerinstance.Observation) (*sandboxstore.RuntimeProviderAbsenceProof, error) {
	s.recordCalls++
	return s.proof, nil
}
func (s *storeStub) CompleteRuntimeSlotProviderAbandon(_ context.Context, r *sandboxstore.CompleteRuntimeSlotProviderAbandonRequest) (*sandboxstore.RuntimeSlot, error) {
	s.request = r
	return &sandboxstore.RuntimeSlot{ID: r.SlotID, State: sandboxstore.RuntimeSlotStateTerminal}, nil
}

type writerStub struct{ fenced int }

func (w *writerStub) Fence(context.Context, runtimeslotreconciler.WriterFenceRequest) (runtimeslotreconciler.WriterFenceProof, error) {
	w.fenced++
	return runtimeslotreconciler.WriterFenceProof{}, nil
}
func (*writerStub) Complete(context.Context, runtimeslotreconciler.WriterCompleteRequest) (runtimeslotreconciler.WriterFinalizeProof, error) {
	panic("provider recovery must complete atomically in store")
}

type nomadStub struct {
	calls int
	err   error
}

func (n *nomadStub) RetireProviderAbsent(context.Context, runtimeslotreconciler.AllocationPurgeRequest, []byte) ([]byte, error) {
	n.calls++
	return bytes.Repeat([]byte{1}, 32), n.err
}

type observerFunc func(context.Context, providerinstance.Scope) (providerinstance.Observation, error)

func (f observerFunc) Observe(ctx context.Context, s providerinstance.Scope) (providerinstance.Observation, error) {
	return f(ctx, s)
}

func TestProviderControllerNeedsExactProofFenceAndServerAck(t *testing.T) {
	ctx := context.Background()
	slot := sandboxstore.RuntimeSlot{ID: "slot", State: sandboxstore.RuntimeSlotStateQuiescing, ClusterID: "cluster", NodeID: "node", NodeUID: "ecs/us-east-1/i-source", NodeBootID: "original-boot", AllocationID: "allocation", AllocationNamespace: "sandbox0", SandboxID: "sandbox", ClaimID: "claim", FilesystemID: "filesystem", SourceGenerationID: "initial", WriterGrantID: "grant"}
	scope := providerinstance.Scope{Provider: "aliyun", AccountID: "1234567890123456", RegionID: "us-east-1", InstanceID: "i-source"}
	observation := providerinstance.Observation{Scope: scope, RequestID: "ecs-last", IdentityRequestID: "sts-last"}
	store := &storeStub{grant: &sandboxstore.RootFSWriterGrant{ID: slot.WriterGrantID, SlotID: slot.ID, NodeUID: slot.NodeUID, NodeBootID: slot.NodeBootID, SandboxID: slot.SandboxID, ClaimID: slot.ClaimID, FilesystemID: slot.FilesystemID, InitialGenerationID: slot.SourceGenerationID, RuntimeNamespace: slot.AllocationNamespace, RuntimeIncarnationID: slot.AllocationID, GateParent: "parent", WriterEpoch: 1, BindingVersion: 1}}
	writer := &writerStub{}
	nomad := &nomadStub{}
	controller, err := New(store, writer, nomad, observerFunc(func(context.Context, providerinstance.Scope) (providerinstance.Observation, error) {
		return observation, nil
	}), scope.AccountID, scope.RegionID)
	require.NoError(t, err)
	completed, err := controller.Recover(ctx, slot)
	require.NoError(t, err)
	require.False(t, completed)
	require.Zero(t, writer.fenced)
	require.Zero(t, nomad.calls)
	first := observation
	first.RequestID, first.IdentityRequestID = "ecs-first", "sts-first"
	store.proof = &sandboxstore.RuntimeProviderAbsenceProof{Version: 1, Scope: scope, Target: sandboxstore.RuntimeProviderTarget{ClusterID: slot.ClusterID, NodeID: slot.NodeID, NodeUID: slot.NodeUID, NodeBootID: slot.NodeBootID}, First: first, Last: observation, FirstObservedAt: time.Unix(1, 0), LastObservedAt: time.Unix(32, 0)}
	nomad.err = errors.New("server ack lost")
	completed, err = controller.Recover(ctx, slot)
	require.Error(t, err)
	require.False(t, completed)
	require.Nil(t, store.request)
	nomad.err = nil
	completed, err = controller.Recover(ctx, slot)
	require.NoError(t, err)
	require.True(t, completed)
	require.Equal(t, sandboxstore.RootFSWriterProviderAbandonProofVersion, store.request.Writer.ProofVersion)
	require.Equal(t, slot.NodeBootID, store.request.Writer.NodeBootID)
	require.Equal(t, nomad.calls, 2)
	store.request = nil
	store.proof.Target.NodeBootID = "replacement-boot"
	completed, err = controller.Recover(ctx, slot)
	require.Error(t, err)
	require.False(t, completed)
	require.Nil(t, store.request)
	observation.Scope.AccountID = "other-account"
	completed, err = controller.Recover(ctx, slot)
	require.Error(t, err)
	require.False(t, completed)
	require.Nil(t, store.request)
}
