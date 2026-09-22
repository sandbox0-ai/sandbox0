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

type fakeMigrationFinalizationStore struct {
	*fakeStore
	reads   int
	receipt *protocol.MigrationSourceFinalizationReceipt
}

func (s *fakeMigrationFinalizationStore) GetNomadSandboxMigrationSourceFinalizationForSlot(context.Context, string) (*protocol.MigrationSourceFinalizationReceipt, error) {
	s.reads++
	return s.receipt, nil
}

func TestMigrationSourceFinalizationReadRequiresAuthenticatedSourceNode(t *testing.T) {
	for _, failure := range []string{"cluster", "node", "uid", "token", "method", "malformed-proof", "valid"} {
		t.Run(failure, func(t *testing.T) {
			s := &fakeMigrationFinalizationStore{fakeStore: &fakeStore{slot: testSlot()}}
			v := &fakeVerifier{identity: nodeauth.Identity{ClusterID: "cluster", NodeID: "nomad-node", NodeUID: "node-uid"}}
			method, token := http.MethodGet, "token"
			switch failure {
			case "cluster":
				v.identity.ClusterID = "foreign"
			case "node":
				v.identity.NodeID = "foreign"
			case "uid":
				v.identity.NodeUID = "foreign"
			case "token":
				token = ""
			case "method":
				method = http.MethodPut
			case "malformed-proof":
				s.receipt = &protocol.MigrationSourceFinalizationReceipt{}
			}
			r := doJSON(t, testHandler(t, v, s), method, protocol.MigrationSourceFinalizationPath("slot"), nil, token)
			if failure == "valid" {
				require.Equal(t, http.StatusOK, r.Code)
				require.Equal(t, 1, s.reads)
			} else {
				require.GreaterOrEqual(t, r.Code, 400)
				if failure != "malformed-proof" {
					require.Zero(t, s.reads)
				}
			}
			require.Nil(t, s.register)
			require.Nil(t, s.heartbeat)
			require.Nil(t, s.starting)
			require.Nil(t, s.commandReady)
		})
	}
}

func TestMigrationSourceFinalizationClientRejectsAmbiguousResponse(t *testing.T) {
	for _, body := range []string{`{}`, `{"slot_id":"other","receipt":null}`, `{"slot_id":"slot","receipt":{}}`, `{"slot_id":"slot","receipt":null,"execute":true}`, `{"slot_id":"slot","receipt":null}{}`, strings.Repeat("x", protocol.NodeChannelMaxBytes+1)} {
		t.Run(body[:min(len(body), 50)], func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.EscapedPath() != protocol.MigrationSourceFinalizationPath("slot") {
					w.WriteHeader(400)
					return
				}
				_, _ = w.Write([]byte(body))
			}))
			defer server.Close()
			_, err := testClient(t, server).GetMigrationSourceFinalization(t.Context(), "slot")
			require.ErrorIs(t, err, errdefs.ErrUnavailable)
		})
	}
	s := &fakeMigrationFinalizationStore{fakeStore: &fakeStore{slot: testSlot()}}
	server := httptest.NewTLSServer(testHandler(t, &fakeVerifier{identity: nodeauth.Identity{ClusterID: "cluster", NodeID: "nomad-node", NodeUID: "node-uid"}}, s))
	defer server.Close()
	receipt, err := testClient(t, server).GetMigrationSourceFinalization(t.Context(), "slot")
	require.NoError(t, err)
	require.Nil(t, receipt)
}
