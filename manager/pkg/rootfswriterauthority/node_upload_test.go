package rootfswriterauthority

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsrebase"
	"github.com/stretchr/testify/require"
)

type uploadGrantStore struct {
	fakeGrantStore
	requests []*sandboxstore.RootFSNodeUploadRequest
}

func (s *uploadGrantStore) RecordRootFSNodeUpload(_ context.Context, r *sandboxstore.RootFSNodeUploadRequest) error {
	if r.NodeUID != "authenticated-node" {
		return errdefs.ErrPermissionDenied
	}
	copy := *r
	s.requests = append(s.requests, &copy)
	return nil
}

func TestNodeUploadClientAndHandlerUseAuthenticatedDurableBinding(t *testing.T) {
	stage := crashAbandonClientTestStage().WithoutWriterGrantToken()
	store := &uploadGrantStore{}
	verifier := &fakeCallerVerifier{identity: CallerIdentity{NodeUID: "authenticated-node"}}
	handler, err := NewHandler(HandlerConfig{Verifier: verifier, Store: store, LeaseTTL: time.Minute})
	require.NoError(t, err)
	server := httptest.NewServer(handler)
	defer server.Close()
	baseURL, err := url.Parse(server.URL)
	require.NoError(t, err)
	tokenFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("projected-token"), 0600))
	client := &ManagerClient{baseURL: baseURL, tokenFile: tokenFile, http: server.Client()}
	payload := []byte("immutable-object")
	ref := rootfsblock.ObjectReference{Key: "rootfs/v2/packs/sha256/" + digest.FromBytes(payload).Encoded(), Kind: rootfsblock.ObjectKindDataPack, Size: int64(len(payload)), Checksum: digest.FromBytes(payload).String()}
	require.NoError(t, client.RecordRootFSNodeUpload(t.Context(), stage, "operation", ref, false))
	require.NoError(t, client.RecordRootFSNodeUpload(t.Context(), stage, "operation", ref, true))
	require.Equal(t, "projected-token", verifier.token)
	require.Len(t, store.requests, 2)
	require.False(t, store.requests[0].Uploaded)
	require.True(t, store.requests[1].Uploaded)
	require.Equal(t, stage.Identity.WriterGrantID, store.requests[0].GrantID)
	require.Equal(t, "authenticated-node", store.requests[0].NodeUID)
	binding, err := stage.BindingDigest()
	require.NoError(t, err)
	require.Equal(t, binding[:], store.requests[0].BindingDigest)
	require.NoError(t, client.RecordRootFSRebaseNodeUpload(t.Context(), "offline-operation", ref, false))
	require.Len(t, store.requests, 3)
	require.Equal(t, "offline-operation", store.requests[2].RebaseOperationID)
	require.Equal(t, rootfsrebase.UploadOwnerID("offline-operation"), store.requests[2].GrantID)
	require.Zero(t, store.requests[2].WriterEpoch)
	// The body cannot spoof identity, and a different TokenReview owner fails.
	verifier.identity.NodeUID = "another-node"
	require.Error(t, client.RecordRootFSNodeUpload(t.Context(), stage, "operation", ref, false))
	require.Len(t, store.requests, 3)
	body, _ := json.Marshal(map[string]any{"writer_epoch": 1, "node_uid": "authenticated-node"})
	request := httptest.NewRequest(http.MethodPut, "/internal/v1/rootfs-writer-grants/grant-1/objects", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer projected-token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	require.Equal(t, http.StatusBadRequest, response.Code)
}
