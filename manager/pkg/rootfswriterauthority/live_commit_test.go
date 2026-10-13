package rootfswriterauthority

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
	"github.com/stretchr/testify/require"
)

type fakeLiveCommitStore struct {
	fakeGrantStore
	publish  *sandboxstore.PublishRootFSLiveCommitRequest
	install  *sandboxstore.InstallRootFSLiveCommitRequest
	progress sandboxstore.RootFSLiveProgress
	err      error
}

func (f *fakeLiveCommitStore) PublishRootFSLiveCommit(_ context.Context, r *sandboxstore.PublishRootFSLiveCommitRequest) (*sandboxstore.RootFSLiveProgress, error) {
	f.publish = r
	return &f.progress, f.err
}
func (f *fakeLiveCommitStore) InstallRootFSLiveCommit(_ context.Context, r *sandboxstore.InstallRootFSLiveCommitRequest) (*sandboxstore.RootFSLiveProgress, error) {
	f.install = r
	return &f.progress, f.err
}

func liveAuthorityFixture(t *testing.T) (rootfshandoff.StageRequest, *fakeLiveCommitStore, *fakeCallerVerifier, http.Handler) {
	t.Helper()
	stage := crashAbandonClientTestStage().WithoutWriterGrantToken()
	binding, err := stage.BindingDigest()
	require.NoError(t, err)
	store := &fakeLiveCommitStore{fakeGrantStore: fakeGrantStore{grant: &sandboxstore.RootFSWriterGrant{
		ID: stage.Identity.WriterGrantID, WriterEpoch: stage.Identity.WriterEpoch, NodeUID: stage.Identity.NodeUID,
		BindingVersion: stage.BindingVersion, BindingDigest: binding[:], State: sandboxstore.RootFSWriterGrantStateConsumed,
	}}, progress: sandboxstore.RootFSLiveProgress{OperationID: protocol.LiveCommitOperationID(stage.Identity.WriterGrantID, 7), CommittedGenerationID: "live-1", CommittedSequence: 7, InstalledGenerationID: stage.InitialGeneration}}
	verifier := &fakeCallerVerifier{identity: CallerIdentity{NodeUID: stage.Identity.NodeUID, AgentUID: "new-ctld-instance"}}
	handler, err := NewHandler(HandlerConfig{Store: store, Verifier: verifier, LeaseTTL: time.Minute})
	require.NoError(t, err)
	return stage, store, verifier, handler
}

func TestLiveCommitTLSClientUsesDurableBindingAndValidatesCutAcknowledgement(t *testing.T) {
	stage, store, verifier, handler := liveAuthorityFixture(t)
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	root := t.TempDir()
	caPath, tokenPath := filepath.Join(root, "ca.pem"), filepath.Join(root, "token")
	require.NoError(t, os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600))
	require.NoError(t, os.WriteFile(tokenPath, []byte("projected-token"), 0600))
	client, err := NewManagerClient(ManagerClientConfig{BaseURL: server.URL, CAFile: caPath, TokenFile: tokenPath})
	require.NoError(t, err)
	request := PublishLiveCommitRequest{OperationID: protocol.LiveCommitOperationID(stage.Identity.WriterGrantID, 7), CutSequence: 7, BaseGenerationID: stage.InitialGeneration,
		ExpectedCommittedGenerationID: stage.InitialGeneration, Generation: sandboxstore.RootFSGeneration{ID: "live-1", Descriptor: []byte(`{}`)}}
	p, err := client.PublishLiveCommit(t.Context(), stage, request)
	require.NoError(t, err)
	require.Equal(t, uint64(7), p.CommittedSequence)
	require.Equal(t, stage.Identity.NodeUID, store.publish.NodeUID)
	require.Equal(t, stage.Identity.WriterEpoch, store.publish.WriterEpoch)
	require.Equal(t, "projected-token", verifier.token)
	store.progress.InstalledGenerationID = "live-1"
	store.progress.InstalledSequence = 7
	_, err = client.InstallLiveCommit(t.Context(), stage, InstallLiveCommitRequest{OperationID: request.OperationID, GenerationID: "live-1", CutSequence: 7})
	require.NoError(t, err)
	require.Equal(t, stage.Identity.NodeUID, store.install.NodeUID)
	store.progress.CommittedSequence = 6
	_, err = client.PublishLiveCommit(t.Context(), stage, request)
	require.True(t, errdefs.IsUnavailable(err), "a success response for a different cut cannot release the WAL")
}

func TestLiveCommitHandlerRejectsForgedOwnersAndRequestFields(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*PublishLiveCommitRequest, *fakeCallerVerifier)
		path   string
		status int
	}{
		{name: "wrong node", mutate: func(_ *PublishLiveCommitRequest, v *fakeCallerVerifier) { v.identity.NodeUID = "other-node" }, status: http.StatusForbidden},
		{name: "wrong epoch", mutate: func(r *PublishLiveCommitRequest, _ *fakeCallerVerifier) { r.WriterEpoch++ }, status: http.StatusPreconditionFailed},
		{name: "wrong binding", mutate: func(r *PublishLiveCommitRequest, _ *fakeCallerVerifier) {
			r.BindingDigest = hex.EncodeToString(make([]byte, 32))
		}, status: http.StatusForbidden},
		{name: "zero sequence", mutate: func(r *PublishLiveCommitRequest, _ *fakeCallerVerifier) { r.CutSequence = 0 }, status: http.StatusBadRequest},
		{name: "encoded grant slash", path: protocol.ConsumePathPrefix + "grant%2F1" + liveCommitSuffix, status: http.StatusBadRequest},
		{name: "unknown caller field", status: http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			stage, store, verifier, handler := liveAuthorityFixture(t)
			binding, _ := stage.BindingDigest()
			body := PublishLiveCommitRequest{TerminalRequest: protocol.TerminalRequest{WriterEpoch: stage.Identity.WriterEpoch, BindingVersion: stage.BindingVersion, BindingDigest: hex.EncodeToString(binding[:])},
				OperationID: protocol.LiveCommitOperationID(stage.Identity.WriterGrantID, 7), CutSequence: 7, BaseGenerationID: stage.InitialGeneration, ExpectedCommittedGenerationID: stage.InitialGeneration,
				Generation: sandboxstore.RootFSGeneration{ID: "live-1", Descriptor: []byte(`{}`)}}
			if test.mutate != nil {
				test.mutate(&body, verifier)
			}
			payload, err := json.Marshal(body)
			require.NoError(t, err)
			if test.name == "unknown caller field" {
				payload = append(payload[:len(payload)-1], []byte(`,"node_uid":"node-1"}`)...)
			}
			path := test.path
			if path == "" {
				path = protocol.ConsumePath(stage.Identity.WriterGrantID) + liveCommitSuffix
			}
			request := httptest.NewRequest(http.MethodPut, path, bytes.NewReader(payload))
			request.Header.Set("Authorization", "Bearer projected-token")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			require.Equal(t, test.status, response.Code)
			require.Nil(t, store.publish)
		})
	}
}
