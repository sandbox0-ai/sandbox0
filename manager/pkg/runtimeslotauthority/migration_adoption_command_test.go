package runtimeslotauthority

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauth"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type adoptionCommandStore struct {
	*fakeStore
	command *protocol.MigrationAdoptionRequest
	calls   int
	err     error
}

func (s *adoptionCommandStore) GetNomadSandboxMigrationAdoptionForSlot(context.Context, string) (*protocol.MigrationAdoptionRequest, error) {
	s.calls++
	return s.command, s.err
}

func TestMigrationAdoptionCommandAuthenticatesExactHistoricalTarget(t *testing.T) {
	for _, failure := range []string{"valid", "pending", "cluster", "node", "uid", "slot", "stored-slot", "boot", "allocation", "token", "method", "invalid", "database", "unsupported"} {
		t.Run(failure, func(t *testing.T) {
			command := adoptionReportReceipt(t).Request
			s := &adoptionCommandStore{fakeStore: &fakeStore{slot: testSlot()}, command: &command}
			v := &fakeVerifier{identity: nodeauth.Identity{ClusterID: "cluster", NodeID: "nomad-node", NodeUID: "node-uid"}}
			method, token := http.MethodGet, "token"
			var store Store = s
			switch failure {
			case "pending":
				s.command = nil
			case "cluster":
				v.identity.ClusterID = "foreign"
			case "node":
				v.identity.NodeID = "foreign"
			case "uid":
				v.identity.NodeUID = "foreign"
			case "slot":
				command.Target.SlotID = "foreign"
			case "stored-slot":
				s.slot.ID = "foreign"
			case "boot":
				command.Target.NodeBootID = "foreign"
			case "allocation":
				command.Target.AllocationID = "foreign"
			case "token":
				token = ""
			case "method":
				method = http.MethodPut
			case "invalid":
				command.RestoreDigest = "bad"
			case "database":
				s.err = errdefs.ErrUnavailable
			case "unsupported":
				store = s.fakeStore
			}
			response := doJSON(t, testHandler(t, v, store), method, protocol.MigrationAdoptionCommandPath("slot"), nil, token)
			if failure == "valid" || failure == "pending" {
				require.Equal(t, http.StatusOK, response.Code)
				var result protocol.MigrationAdoptionCommandResponse
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
				require.NoError(t, result.ValidateFor("slot"))
				require.Equal(t, s.command, result.Command)
			} else {
				require.GreaterOrEqual(t, response.Code, 400)
			}
			switch failure {
			case "cluster", "node", "uid", "stored-slot", "token", "method", "unsupported":
				require.Zero(t, s.calls)
			default:
				require.Equal(t, 1, s.calls)
			}
			require.Nil(t, s.heartbeat)
			require.Nil(t, s.commandReady)
			require.Nil(t, s.starting)
			require.Nil(t, s.register)
		})
	}
}

func TestMigrationAdoptionCommandClientRequiresBoundedExactResponse(t *testing.T) {
	command := adoptionReportReceipt(t).Request
	payload, err := json.Marshal(protocol.MigrationAdoptionCommandResponse{SlotID: "slot", Command: &command})
	require.NoError(t, err)
	for _, body := range []string{`{}`, `null`, `{"slot_id":"foreign","command":null}`, `{"slot_id":"slot","command":{}}`, `{"slot_id":"slot","command":null,"execute":true}`, string(payload) + `{}`, strings.Repeat("x", maxResponseBytes+1)} {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) }))
		_, err := testClient(t, server).GetMigrationAdoptionCommand(t.Context(), "slot")
		require.ErrorIs(t, err, errdefs.ErrUnavailable)
		server.Close()
	}
	for _, pending := range []bool{true, false} {
		s := &adoptionCommandStore{fakeStore: &fakeStore{slot: testSlot()}}
		if !pending {
			s.command = &command
		}
		server := httptest.NewTLSServer(testHandler(t, &fakeVerifier{identity: nodeauth.Identity{ClusterID: "cluster", NodeID: "nomad-node", NodeUID: "node-uid"}}, s))
		actual, err := testClient(t, server).GetMigrationAdoptionCommand(t.Context(), "slot")
		require.NoError(t, err)
		require.Equal(t, s.command, actual)
		server.Close()
	}
}
