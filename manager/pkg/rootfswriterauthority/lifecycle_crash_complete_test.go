package rootfswriterauthority

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
	"github.com/stretchr/testify/require"
)

type crashCompletionTx struct {
	sandboxstore.SandboxStoreTx
	request *sandboxstore.CompleteRootFSWriterCrashAbandonRequest
	err     error
}

func (tx *crashCompletionTx) CompleteRootFSWriterCrashAbandon(_ context.Context, request *sandboxstore.CompleteRootFSWriterCrashAbandonRequest) (*sandboxstore.RootFSWriterGrant, error) {
	tx.request = request
	return nil, tx.err
}

type crashCompletionStore struct {
	LifecycleStore
	grant *sandboxstore.RootFSWriterGrant
	head  string
	tx    *crashCompletionTx
}

func (s *crashCompletionStore) GetRootFSWriterGrant(context.Context, string) (*sandboxstore.RootFSWriterGrant, error) {
	return s.grant, nil
}

func (s *crashCompletionStore) GetRootFSGeneration(context.Context, string) (*sandboxstore.RootFSGeneration, error) {
	return &sandboxstore.RootFSGeneration{CurrentBlockHead: s.head}, nil
}

func (s *crashCompletionStore) GetSandbox(context.Context, string) (*sandboxstore.SandboxRecord, error) {
	return &sandboxstore.SandboxRecord{ID: s.grant.SandboxID}, nil
}

func (s *crashCompletionStore) WithSandboxLock(ctx context.Context, id string, fn func(context.Context, sandboxstore.SandboxStoreTx, *sandboxstore.SandboxRecord) error) error {
	return fn(ctx, s.tx, &sandboxstore.SandboxRecord{ID: id})
}

func TestCrashCompletionUsesWriterAllocationBeforeInitialClaimCommit(t *testing.T) {
	for _, tc := range []struct {
		name       string
		allocation string
		generation string
		storeErr   error
		status     int
		completed  bool
	}{
		{"uncommitted initial claim", "allocation-1", "1", nil, http.StatusNoContent, true},
		{"wrong allocation", "allocation-other", "1", nil, http.StatusConflict, false},
		{"wrong runtime generation", "allocation-1", "2", nil, http.StatusConflict, false},
		{"locked runtime changed", "allocation-1", "1", errors.New("runtime no longer matches cleanup fence"), http.StatusConflict, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			binding, err := hex.DecodeString(strings.Repeat("ab", 32))
			require.NoError(t, err)
			grant := &sandboxstore.RootFSWriterGrant{
				ID: "grant-1", SandboxID: "sandbox-1", ClaimID: "claim-1", GateParent: "parent-1",
				WriterEpoch: 1, BindingVersion: rootfshandoff.WriterBindingVersion, BindingDigest: binding,
				NodeUID: "node-1", NodeBootID: "boot-1", InitialGenerationID: "generation-1",
				RuntimeIncarnationID: "allocation-1", RuntimeGeneration: "1",
				State: sandboxstore.RootFSWriterGrantStateRetiring,
			}
			proof := rootfshandoff.CrashFenceProof{
				Version: rootfshandoff.CrashFenceProofVersion, OperationID: "cleanup-1",
				Parent: grant.GateParent, ClaimID: grant.ClaimID, WriterGrantID: grant.ID,
				WriterEpoch: grant.WriterEpoch, BindingVersion: grant.BindingVersion,
				BindingDigest: hex.EncodeToString(binding), RootFSID: "rootfs-1",
				InitialGeneration: grant.InitialGenerationID, InitialBlockHead: "sha256:" + strings.Repeat("cd", 32),
				HeadAction: rootfshandoff.CrashFenceHeadKeepInitial, NodeUID: grant.NodeUID, BootID: grant.NodeBootID,
				RuntimeGeneration: tc.generation, AllocationID: tc.allocation,
				HostMountNamespaceID: "mnt-1", NetworkIncarnationID: "network-1", TaskName: "task-1",
				SlotNonce: "nonce-1", ActiveKey: "active-1", ContainerAbsent: true, TaskAbsent: true,
				FrontendSnapshotAbsent: true, StableMountAbsent: true, RootFSState: rootfshandoff.StateTombstoned,
				ObservedAt: "2026-10-08T07:00:00Z",
			}
			proof.Session = rootfshandoff.CrashFenceSessionObservation{
				Parent: proof.Parent, RootFSID: proof.RootFSID, WriterEpoch: proof.WriterEpoch,
				OperationID: proof.OperationID, BindingDigest: proof.BindingDigest,
				SessionState: rootfshandoff.StateTombstoned, BranchPath: "/fixture/branch",
				NBDPoolAbsent: true, LiveSessionAbsent: true, MergedMountAbsent: true, XFSMountAbsent: true, ObservedAt: proof.ObservedAt,
			}
			require.NoError(t, proof.Validate())
			tx := &crashCompletionTx{err: tc.storeErr}
			store := &crashCompletionStore{grant: grant, head: proof.InitialBlockHead, tx: tx}
			handler, err := NewLifecycleHandler(&fakeCallerVerifier{identity: CallerIdentity{NodeUID: grant.NodeUID}}, store, http.NotFoundHandler())
			require.NoError(t, err)
			body, err := json.Marshal(CrashAbandonCompleteRequest{CrashAbandonBeginRequest: CrashAbandonBeginRequest{
				WriterEpoch: grant.WriterEpoch, BindingVersion: grant.BindingVersion, BindingDigest: proof.BindingDigest,
				OperationID: proof.OperationID, ExpectedOldGenerationID: grant.InitialGenerationID,
			}, Proof: proof})
			require.NoError(t, err)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodPut, protocol.ConsumePath(grant.ID)+crashAbandonCompletePathSuffix, bytes.NewReader(body)))
			require.Equal(t, tc.status, response.Code, response.Body.String())
			if tc.completed {
				require.NotNil(t, tx.request)
				digest, err := proof.Digest()
				require.NoError(t, err)
				require.Equal(t, digest[:], tx.request.ProofDigest)
			} else {
				require.Nil(t, tx.request)
			}
		})
	}
}
