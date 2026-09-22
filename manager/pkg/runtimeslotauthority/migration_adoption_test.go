package runtimeslotauthority

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauth"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type adoptionReportStore struct {
	*fakeStore
	calls int
	err   error
}

func (s *adoptionReportStore) CommitNomadSandboxMigrationAdoption(context.Context, protocol.MigrationAdoptionRequest, protocol.MigrationAdoptionProof) error {
	s.calls++
	return s.err
}

func adoptionReportReceipt(t *testing.T) protocol.MigrationAdoptionReceipt {
	t.Helper()
	s := testSlot()
	r := protocol.MigrationAdoptionRequest{Target: protocol.NodeChannelTarget{SlotID: s.ID, ClusterID: s.ClusterID, NodeID: s.NodeID, NodeUID: s.NodeUID, NodeBootID: s.NodeBootID, AllocationID: s.AllocationID, ControlEndpoint: "unix:///target.sock"}, OperationID: "migration", ClaimID: "claim", SandboxID: "sandbox", RuntimeGeneration: 2, ProcdInstanceID: "procd", RestoreDigest: strings.Repeat("a", 64), CommandReadyDigest: strings.Repeat("b", 64)}
	digest, err := r.Digest()
	require.NoError(t, err)
	return protocol.MigrationAdoptionReceipt{Request: r, Proof: protocol.MigrationAdoptionProof{RequestDigest: digest, ImageAbsent: true}}
}

func TestMigrationAdoptionReportAuthenticatesHistoricalNodeCustody(t *testing.T) {
	for _, failure := range []string{"valid", "cluster", "node", "uid", "slot", "boot", "allocation", "token", "method", "proof", "database"} {
		t.Run(failure, func(t *testing.T) {
			s := &adoptionReportStore{fakeStore: &fakeStore{slot: testSlot()}}
			v := &fakeVerifier{identity: nodeauth.Identity{ClusterID: "cluster", NodeID: "nomad-node", NodeUID: "node-uid"}}
			receipt := adoptionReportReceipt(t)
			method, token := http.MethodPut, "token"
			switch failure {
			case "cluster":
				v.identity.ClusterID = "foreign"
			case "node":
				v.identity.NodeID = "foreign"
			case "uid":
				v.identity.NodeUID = "foreign"
			case "slot":
				receipt.Request.Target.SlotID = "foreign"
			case "boot":
				receipt.Request.Target.NodeBootID = "foreign"
			case "allocation":
				receipt.Request.Target.AllocationID = "foreign"
			case "token":
				token = ""
			case "method":
				method = http.MethodPost
			case "proof":
				receipt.Proof.ImageAbsent = false
			case "database":
				s.err = errdefs.ErrUnavailable
			}
			receipt.Proof.RequestDigest, _ = receipt.Request.Digest()
			response := doJSON(t, testHandler(t, v, s), method, protocol.MigrationAdoptionReceiptPath("slot"), receipt, token)
			if failure == "valid" {
				require.Equal(t, 200, response.Code)
				require.Equal(t, 1, s.calls)
			} else {
				require.GreaterOrEqual(t, response.Code, 400)
				if failure != "database" {
					require.Zero(t, s.calls)
				}
			}
			require.Nil(t, s.heartbeat)
			require.Nil(t, s.commandReady)
			require.Nil(t, s.starting)
			require.Nil(t, s.register)
		})
	}
}

func TestMigrationAdoptionReportClientRejectsUncertainAcknowledgement(t *testing.T) {
	receipt := adoptionReportReceipt(t)
	for _, body := range []string{`{}`, `{"request_digest":"wrong"}`, `{"request_digest":"` + receipt.Proof.RequestDigest + `","execute":true}`, `{"request_digest":"` + receipt.Proof.RequestDigest + `"}{}`, strings.Repeat("x", maxResponseBytes+1)} {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) }))
		_, err := testClient(t, server).ReportMigrationAdoption(t.Context(), receipt)
		require.ErrorIs(t, err, errdefs.ErrUnavailable)
		server.Close()
	}
	s := &adoptionReportStore{fakeStore: &fakeStore{slot: testSlot()}}
	server := httptest.NewTLSServer(testHandler(t, &fakeVerifier{identity: nodeauth.Identity{ClusterID: "cluster", NodeID: "nomad-node", NodeUID: "node-uid"}}, s))
	defer server.Close()
	ack, err := testClient(t, server).ReportMigrationAdoption(t.Context(), receipt)
	require.NoError(t, err)
	require.NoError(t, ack.ValidateFor(receipt))
}
