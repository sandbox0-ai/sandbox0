package rootfswriterauthority

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
	"github.com/stretchr/testify/require"
)

type fakeCallerVerifier struct {
	identity CallerIdentity
	token    string
	err      error
	calls    int
}

func (f *fakeCallerVerifier) Verify(_ context.Context, token string) (CallerIdentity, error) {
	f.token = token
	f.calls++
	return f.identity, f.err
}

type fakeGrantStore struct {
	request       *sandboxstore.ConsumeRootFSWriterGrantRequest
	renewRequest  *sandboxstore.RenewRootFSWriterGrantRequest
	renewRequests []*sandboxstore.RenewRootFSWriterGrantRequest
	renewalPolicy sandboxstore.RootFSWriterLeaseRenewalPolicy
	renewErr      error
	batchCalls    int
	grant         *sandboxstore.RootFSWriterGrant
	getErr        error
	getGrantIDs   []string
	cancelRequest *sandboxstore.CancelRootFSWriterGrantRequest
	cancelErr     error
	forkRequest   *sandboxstore.ForkRunningRootFSFilesystemRequest
	forkErr       error
}

func (f *fakeGrantStore) RenewRootFSWriterGrants(
	_ context.Context,
	requests []*sandboxstore.RenewRootFSWriterGrantRequest,
	policy sandboxstore.RootFSWriterLeaseRenewalPolicy,
) ([]sandboxstore.RenewRootFSWriterGrantResult, error) {
	f.batchCalls++
	f.renewRequests = append(f.renewRequests, requests...)
	f.renewalPolicy = policy
	results := make([]sandboxstore.RenewRootFSWriterGrantResult, len(requests))
	for index, request := range requests {
		if f.renewErr != nil {
			results[index].Err = f.renewErr
		} else {
			results[index].Grant = testLeasedGrant(request.GrantID)
		}
	}
	return results, nil
}

func (f *fakeGrantStore) ConsumeRootFSWriterGrant(_ context.Context, request *sandboxstore.ConsumeRootFSWriterGrantRequest) (*sandboxstore.RootFSWriterGrant, error) {
	f.request = request
	return testLeasedGrant(request.GrantID), nil
}

func (f *fakeGrantStore) CancelRootFSWriterGrant(
	_ context.Context,
	request *sandboxstore.CancelRootFSWriterGrantRequest,
) (*sandboxstore.RootFSWriterGrant, error) {
	f.cancelRequest = request
	return f.grant, f.cancelErr
}

func (f *fakeGrantStore) RenewRootFSWriterGrant(
	_ context.Context,
	request *sandboxstore.RenewRootFSWriterGrantRequest,
	policy sandboxstore.RootFSWriterLeaseRenewalPolicy,
) (*sandboxstore.RootFSWriterGrant, error) {
	f.renewRequest = request
	f.renewRequests = append(f.renewRequests, request)
	f.renewalPolicy = policy
	if f.renewErr != nil {
		return nil, f.renewErr
	}
	return testLeasedGrant(request.GrantID), nil
}

func TestBatchRenewAuthenticatesNodeOnceAndReturnsPerGrantLeases(t *testing.T) {
	verifier := &fakeCallerVerifier{identity: CallerIdentity{NodeUID: "node-uid", PodUID: "ctld-pod-uid"}}
	store := &fakeGrantStore{}
	handler, err := NewHandler(HandlerConfig{Verifier: verifier, Store: store, LeaseTTL: 5 * time.Minute})
	require.NoError(t, err)
	requestBody := protocol.BatchRenewRequest{Items: []protocol.BatchRenewItem{
		{GrantID: "grant-1", RenewRequest: protocol.RenewRequest{WriterEpoch: 1, BindingVersion: 1, BindingDigest: strings.Repeat("ab", 32)}},
		{GrantID: "grant-2", RenewRequest: protocol.RenewRequest{WriterEpoch: 2, BindingVersion: 1, BindingDigest: strings.Repeat("cd", 32)}},
	}}
	payload, err := json.Marshal(requestBody)
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodPut, protocol.BatchRenewPath, bytes.NewReader(payload))
	request.Header.Set("Authorization", "Bearer projected-token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)
	var result protocol.BatchRenewResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
	require.NoError(t, result.Validate(2))
	require.Equal(t, 1, verifier.calls)
	require.Equal(t, 1, store.batchCalls)
	require.Len(t, store.renewRequests, 2)
	require.Equal(t, "node-uid", store.renewRequests[0].ConsumerNodeUID)
}

func (f *fakeGrantStore) GetRootFSWriterGrant(_ context.Context, grantID string) (*sandboxstore.RootFSWriterGrant, error) {
	f.getGrantIDs = append(f.getGrantIDs, grantID)
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.grant, nil
}

func (f *fakeGrantStore) ForkRunningRootFSFilesystem(
	_ context.Context,
	request *sandboxstore.ForkRunningRootFSFilesystemRequest,
) (*sandboxstore.RootFSFilesystem, error) {
	f.forkRequest = request
	if f.forkErr != nil {
		return nil, f.forkErr
	}
	return &sandboxstore.RootFSFilesystem{ID: request.TargetSandboxID}, nil
}

func TestRunningForkAuthenticatesLiveWriterAndPublishesExactCheckpoint(t *testing.T) {
	stage := crashAbandonClientTestStage()
	fork := rootfshandoff.RunningForkCheckpointRequest{
		OperationID: "running-fork", SourceSandboxID: "source-sandbox",
		TargetSandboxID: "target-sandbox", TargetGenerationID: "target-generation",
	}
	checkpoint := runningForkClientTestCheckpoint(t, stage, fork)
	binding, err := stage.BindingDigest()
	require.NoError(t, err)
	store := &fakeGrantStore{grant: &sandboxstore.RootFSWriterGrant{
		ID: stage.Identity.WriterGrantID, SandboxID: fork.SourceSandboxID, FilesystemID: stage.Identity.RootFSID,
		InitialGenerationID: stage.InitialGeneration, WriterEpoch: stage.Identity.WriterEpoch,
		BindingVersion: stage.BindingVersion, BindingDigest: binding[:], NodeUID: stage.Identity.NodeUID,
		State: sandboxstore.RootFSWriterGrantStateConsumed,
	}}
	handler, err := NewHandler(HandlerConfig{
		Verifier: &fakeCallerVerifier{identity: CallerIdentity{NodeUID: stage.Identity.NodeUID}},
		Store:    store, LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	body, err := json.Marshal(PublishRunningForkRequest{
		WriterEpoch: stage.Identity.WriterEpoch, BindingVersion: stage.BindingVersion,
		BindingDigest: checkpoint.Proof.BindingDigest, Checkpoint: checkpoint,
	})
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodPut, protocol.RunningForkPath(stage.Identity.WriterGrantID), bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer projected-token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	require.Equal(t, http.StatusNoContent, response.Code, response.Body.String())
	require.NotNil(t, store.forkRequest)
	require.Equal(t, fork.OperationID, store.forkRequest.OperationID)
	require.Equal(t, fork.TargetSandboxID, store.forkRequest.Generation.FilesystemID)
	require.Equal(t, stage.InitialGeneration, store.forkRequest.Generation.ParentGenerationID)
	require.Equal(t, checkpoint.Proof, store.forkRequest.CheckpointProof)
	require.Equal(t, binding[:], store.forkRequest.BindingDigest)
}

func TestHandlerDerivesConsumerAndLeasePolicy(t *testing.T) {
	verifier := &fakeCallerVerifier{identity: CallerIdentity{NodeUID: "node-uid", PodUID: "ctld-pod-uid"}}
	store := &fakeGrantStore{}
	handler, err := NewHandler(HandlerConfig{Verifier: verifier, Store: store, LeaseTTL: 5 * time.Minute})
	require.NoError(t, err)
	body, err := json.Marshal(protocol.ConsumeRequest{
		WriterEpoch: 7, BindingVersion: 1,
		BindingDigest: strings.Repeat("ab", 32), WriterToken: "writer-token",
	})
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodPut, protocol.ConsumePath("grant-1"), bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer projected-token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)
	var observation protocol.LeaseObservation
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &observation))
	require.NoError(t, observation.Validate())
	require.Equal(t, "projected-token", verifier.token)
	require.Equal(t, "node-uid", store.request.ConsumerNodeUID)
	require.Equal(t, "ctld-pod-uid", store.request.ConsumerCtldPodUID)
	require.Equal(t, 5*time.Minute, store.request.LeaseTTL)
	require.Equal(t, 1, store.request.BindingVersion)
}

func TestHandlerRejectsMissingBearerBeforeConsuming(t *testing.T) {
	store := &fakeGrantStore{}
	handler, err := NewHandler(HandlerConfig{
		Verifier: &fakeCallerVerifier{}, Store: store, LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodPut, protocol.ConsumePath("grant-1"), strings.NewReader(`{}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	require.Equal(t, http.StatusUnauthorized, response.Code)
	require.Nil(t, store.request)
}

func TestRenewHTTPClientDerivesLiveNodeAndUsesServerPolicy(t *testing.T) {
	verifier := &fakeCallerVerifier{identity: CallerIdentity{NodeUID: "node-uid", PodUID: "ctld-pod-uid"}}
	store := &fakeGrantStore{}
	policy := sandboxstore.RootFSWriterLeaseRenewalPolicy{LeaseTTL: 90 * time.Second, GracePeriod: time.Second}
	handler, err := NewHandler(HandlerConfig{
		Verifier: verifier, Store: store, LeaseTTL: 5 * time.Minute, RenewalPolicy: policy,
	})
	require.NoError(t, err)
	server := httptest.NewServer(handler)
	defer server.Close()
	body, err := json.Marshal(protocol.RenewRequest{
		WriterEpoch: 7, BindingVersion: sandboxstore.RootFSWriterBindingVersion,
		BindingDigest: strings.Repeat("ab", 32),
	})
	require.NoError(t, err)
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPut,
		server.URL+protocol.RenewPath("grant-1"), bytes.NewReader(body))
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer projected-token")
	request.Header.Set("Content-Type", "application/json")
	response, err := server.Client().Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
	var observation protocol.LeaseObservation
	require.NoError(t, json.NewDecoder(response.Body).Decode(&observation))
	require.NoError(t, observation.Validate())
	require.Equal(t, "projected-token", verifier.token)
	require.Equal(t, "grant-1", store.renewRequest.GrantID)
	require.Equal(t, int64(7), store.renewRequest.WriterEpoch)
	require.Equal(t, "node-uid", store.renewRequest.ConsumerNodeUID)
	require.Equal(t, policy, store.renewalPolicy)
}

func TestRenewHandlerRejectsClientLeasePolicy(t *testing.T) {
	store := &fakeGrantStore{}
	handler, err := NewHandler(HandlerConfig{
		Verifier: &fakeCallerVerifier{identity: CallerIdentity{NodeUID: "node-uid", PodUID: "ctld-pod-uid"}},
		Store:    store, LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodPut, protocol.RenewPath("grant-1"), strings.NewReader(`{
		"writer_epoch":1,
		"binding_version":1,
		"binding_digest":"`+strings.Repeat("ab", 32)+`",
		"writer_grant_token":"`+strings.Repeat("t", 32)+`",
		"lease_ttl":"24h"
	}`))
	request.Header.Set("Authorization", "Bearer projected-token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	require.Equal(t, http.StatusBadRequest, response.Code)
	require.Nil(t, store.renewRequest)
}

func TestHandlerDoesNotTreatGrantIDOrEncodedSlashAsRenewAction(t *testing.T) {
	store := &fakeGrantStore{}
	handler, err := NewHandler(HandlerConfig{
		Verifier: &fakeCallerVerifier{identity: CallerIdentity{NodeUID: "node-uid", PodUID: "ctld-pod-uid"}},
		Store:    store, LeaseTTL: time.Minute,
	})
	require.NoError(t, err)

	consumeBody, err := json.Marshal(protocol.ConsumeRequest{
		WriterEpoch: 1, BindingVersion: sandboxstore.RootFSWriterBindingVersion,
		BindingDigest: strings.Repeat("ab", 32), WriterToken: "writer-token",
	})
	require.NoError(t, err)
	consume := httptest.NewRequest(http.MethodPut, protocol.ConsumePath("renew"), bytes.NewReader(consumeBody))
	consume.Header.Set("Authorization", "Bearer projected-token")
	consumeResponse := httptest.NewRecorder()
	handler.ServeHTTP(consumeResponse, consume)
	require.Equal(t, http.StatusOK, consumeResponse.Code)
	require.Equal(t, "renew", store.request.GrantID)
	require.Nil(t, store.renewRequest)

	encodedSlash := httptest.NewRequest(http.MethodPut,
		protocol.ConsumePathPrefix+"grant%2Frenew", bytes.NewReader(consumeBody))
	encodedSlash.Header.Set("Authorization", "Bearer projected-token")
	encodedSlashResponse := httptest.NewRecorder()
	handler.ServeHTTP(encodedSlashResponse, encodedSlash)
	require.Equal(t, http.StatusBadRequest, encodedSlashResponse.Code)
	require.Nil(t, store.renewRequest)
}

func testLeasedGrant(id string) *sandboxstore.RootFSWriterGrant {
	now := time.Unix(1_700_000_000, 0).UTC()
	return &sandboxstore.RootFSWriterGrant{
		ID: id, AuthorityObservedAt: now, LeaseExpiresAt: now.Add(2 * time.Minute),
	}
}

func TestRenewHandlerPreservesRetryableErrorClassification(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		status int
	}{
		{name: "lease expired", err: sandboxstore.ErrRootFSWriterLeaseExpired, status: http.StatusPreconditionFailed},
		{name: "store unavailable", err: errdefs.ErrUnavailable, status: http.StatusServiceUnavailable},
		{name: "deadline", err: context.DeadlineExceeded, status: http.StatusGatewayTimeout},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &fakeGrantStore{renewErr: test.err}
			handler, err := NewHandler(HandlerConfig{
				Verifier: &fakeCallerVerifier{identity: CallerIdentity{NodeUID: "node-uid", PodUID: "ctld-pod-uid"}},
				Store:    store, LeaseTTL: time.Minute,
			})
			require.NoError(t, err)
			body, err := json.Marshal(protocol.RenewRequest{
				WriterEpoch: 1, BindingVersion: sandboxstore.RootFSWriterBindingVersion,
				BindingDigest: strings.Repeat("ab", 32),
			})
			require.NoError(t, err)
			request := httptest.NewRequest(http.MethodPut, protocol.RenewPath("grant-1"), bytes.NewReader(body))
			request.Header.Set("Authorization", "Bearer projected-token")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			require.Equal(t, test.status, response.Code)
		})
	}
}

func TestTerminalHandlerAcceptsExactTerminalRetryWithEncodedGrantID(t *testing.T) {
	for _, state := range []string{
		sandboxstore.RootFSWriterGrantStateRetired,
		sandboxstore.RootFSWriterGrantStateCanceled,
	} {
		t.Run(state, func(t *testing.T) {
			grantID := "grant id+percent%"
			verifier := &fakeCallerVerifier{identity: CallerIdentity{NodeUID: "issued-node", PodUID: "ctld-pod"}}
			store := &fakeGrantStore{grant: terminalTestGrant(grantID, state)}
			handler, err := NewHandler(HandlerConfig{
				Verifier: verifier, Store: store, LeaseTTL: time.Minute,
			})
			require.NoError(t, err)

			for range 2 {
				request := httptest.NewRequest(http.MethodPut, protocol.TerminalPath(grantID),
					strings.NewReader(terminalTestRequestBody(strings.Repeat("ab", 32), 7)))
				request.Header.Set("Authorization", "Bearer projected-token")
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				require.Equal(t, http.StatusNoContent, response.Code)
				require.Empty(t, response.Body.Bytes())
			}
			require.Equal(t, []string{grantID, grantID}, store.getGrantIDs)
			require.Equal(t, "projected-token", verifier.token)
		})
	}
}

func TestPreconsumeAbortCancelsOnlyAnExactIssuedGrant(t *testing.T) {
	grant := terminalTestGrant("grant id", sandboxstore.RootFSWriterGrantStateIssued)
	grant.IssueOperationID = "issue-operation"
	store := &fakeGrantStore{grant: grant}
	handler, err := NewHandler(HandlerConfig{
		Store: store, Verifier: &fakeCallerVerifier{identity: CallerIdentity{NodeUID: "issued-node"}},
		LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodPut, protocol.PreconsumeAbortPath(grant.ID),
		strings.NewReader(terminalTestRequestBody(strings.Repeat("ab", 32), 7)))
	request.Header.Set("Authorization", "Bearer token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	require.Equal(t, http.StatusNoContent, response.Code)
	require.NotNil(t, store.cancelRequest)
	require.Equal(t, grant.ID, store.cancelRequest.GrantID)
	require.Equal(t, grant.IssueOperationID, store.cancelRequest.OperationID)
	require.Equal(t, grant.BindingDigest, store.cancelRequest.BindingDigest)
}

func TestPreconsumeAbortRejectsAConsumedGrant(t *testing.T) {
	store := &fakeGrantStore{grant: terminalTestGrant("grant-1", sandboxstore.RootFSWriterGrantStateConsumed)}
	handler, err := NewHandler(HandlerConfig{
		Store: store, Verifier: &fakeCallerVerifier{identity: CallerIdentity{NodeUID: "issued-node"}},
		LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodPut, protocol.PreconsumeAbortPath("grant-1"),
		strings.NewReader(terminalTestRequestBody(strings.Repeat("ab", 32), 7)))
	request.Header.Set("Authorization", "Bearer token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	require.Equal(t, http.StatusPreconditionFailed, response.Code)
	require.Nil(t, store.cancelRequest)
}

func TestTerminalHandlerRejectsWrongOwnerBindingAndState(t *testing.T) {
	wrongVersionGrant := terminalTestGrant("grant-1", sandboxstore.RootFSWriterGrantStateRetired)
	wrongVersionGrant.BindingVersion++
	tests := []struct {
		name        string
		identity    CallerIdentity
		grant       *sandboxstore.RootFSWriterGrant
		digest      string
		writerEpoch int64
		getErr      error
		wantStatus  int
	}{
		{
			name: "wrong issued node", identity: CallerIdentity{NodeUID: "other-node"},
			grant:  terminalTestGrant("grant-1", sandboxstore.RootFSWriterGrantStateRetired),
			digest: strings.Repeat("ab", 32), writerEpoch: 7, wantStatus: http.StatusForbidden,
		},
		{
			name: "wrong grant record", identity: CallerIdentity{NodeUID: "issued-node"},
			grant:  terminalTestGrant("other-grant", sandboxstore.RootFSWriterGrantStateRetired),
			digest: strings.Repeat("ab", 32), writerEpoch: 7, wantStatus: http.StatusForbidden,
		},
		{
			name: "wrong binding digest", identity: CallerIdentity{NodeUID: "issued-node"},
			grant:  terminalTestGrant("grant-1", sandboxstore.RootFSWriterGrantStateRetired),
			digest: strings.Repeat("cd", 32), writerEpoch: 7, wantStatus: http.StatusForbidden,
		},
		{
			name: "wrong binding version", identity: CallerIdentity{NodeUID: "issued-node"},
			grant:  wrongVersionGrant,
			digest: strings.Repeat("ab", 32), writerEpoch: 7, wantStatus: http.StatusForbidden,
		},
		{
			name: "wrong writer epoch", identity: CallerIdentity{NodeUID: "issued-node"},
			grant:  terminalTestGrant("grant-1", sandboxstore.RootFSWriterGrantStateRetired),
			digest: strings.Repeat("ab", 32), writerEpoch: 8, wantStatus: http.StatusPreconditionFailed,
		},
		{
			name: "non terminal state", identity: CallerIdentity{NodeUID: "issued-node"},
			grant:  terminalTestGrant("grant-1", sandboxstore.RootFSWriterGrantStateRetiring),
			digest: strings.Repeat("ab", 32), writerEpoch: 7, wantStatus: http.StatusPreconditionFailed,
		},
		{
			name: "store unavailable", identity: CallerIdentity{NodeUID: "issued-node"},
			digest: strings.Repeat("ab", 32), writerEpoch: 7,
			getErr: errdefs.ErrUnavailable, wantStatus: http.StatusServiceUnavailable,
		},
		{
			name: "store not found", identity: CallerIdentity{NodeUID: "issued-node"},
			digest: strings.Repeat("ab", 32), writerEpoch: 7,
			getErr: sandboxstore.ErrRootFSWriterGrantNotFound, wantStatus: http.StatusForbidden,
		},
		{
			name: "unclassified store failure", identity: CallerIdentity{NodeUID: "issued-node"},
			digest: strings.Repeat("ab", 32), writerEpoch: 7,
			getErr: errors.New("database connection reset"), wantStatus: http.StatusServiceUnavailable,
		},
		{
			name: "empty store result", identity: CallerIdentity{NodeUID: "issued-node"},
			digest: strings.Repeat("ab", 32), writerEpoch: 7, wantStatus: http.StatusServiceUnavailable,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &fakeGrantStore{grant: test.grant, getErr: test.getErr}
			handler, err := NewHandler(HandlerConfig{
				Verifier: &fakeCallerVerifier{identity: test.identity},
				Store:    store, LeaseTTL: time.Minute,
			})
			require.NoError(t, err)
			request := httptest.NewRequest(http.MethodPut, protocol.TerminalPath("grant-1"),
				strings.NewReader(terminalTestRequestBody(test.digest, test.writerEpoch)))
			request.Header.Set("Authorization", "Bearer projected-token")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			require.Equal(t, test.wantStatus, response.Code)
			require.Equal(t, []string{"grant-1"}, store.getGrantIDs)
			require.NotContains(t, response.Body.String(), "database connection reset")
		})
	}
}

func TestTerminalHandlerRequiresBearerBeforeGrantLookup(t *testing.T) {
	store := &fakeGrantStore{grant: terminalTestGrant("grant-1", sandboxstore.RootFSWriterGrantStateRetired)}
	handler, err := NewHandler(HandlerConfig{
		Verifier: &fakeCallerVerifier{identity: CallerIdentity{NodeUID: "issued-node"}},
		Store:    store, LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodPut, protocol.TerminalPath("grant-1"),
		strings.NewReader(terminalTestRequestBody(strings.Repeat("ab", 32), 7)))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	require.Equal(t, http.StatusUnauthorized, response.Code)
	require.Empty(t, store.getGrantIDs)
}

func TestTerminalHandlerRejectsClientStateAndEncodedSlash(t *testing.T) {
	store := &fakeGrantStore{grant: terminalTestGrant("grant-1", sandboxstore.RootFSWriterGrantStateRetired)}
	handler, err := NewHandler(HandlerConfig{
		Verifier: &fakeCallerVerifier{identity: CallerIdentity{NodeUID: "issued-node"}},
		Store:    store, LeaseTTL: time.Minute,
	})
	require.NoError(t, err)

	clientState := httptest.NewRequest(http.MethodPut, protocol.TerminalPath("grant-1"), strings.NewReader(`{
		"writer_epoch":7,
		"binding_version":1,
		"binding_digest":"`+strings.Repeat("ab", 32)+`",
		"state":"retired"
	}`))
	clientState.Header.Set("Authorization", "Bearer projected-token")
	clientStateResponse := httptest.NewRecorder()
	handler.ServeHTTP(clientStateResponse, clientState)
	require.Equal(t, http.StatusBadRequest, clientStateResponse.Code)
	require.Empty(t, store.getGrantIDs)

	encodedSlash := httptest.NewRequest(http.MethodPut,
		protocol.ConsumePathPrefix+"grant%2Fid/terminal",
		strings.NewReader(terminalTestRequestBody(strings.Repeat("ab", 32), 7)))
	encodedSlash.Header.Set("Authorization", "Bearer projected-token")
	encodedSlashResponse := httptest.NewRecorder()
	handler.ServeHTTP(encodedSlashResponse, encodedSlash)
	require.Equal(t, http.StatusBadRequest, encodedSlashResponse.Code)
	require.Empty(t, store.getGrantIDs)
}

func terminalTestGrant(grantID, state string) *sandboxstore.RootFSWriterGrant {
	return &sandboxstore.RootFSWriterGrant{
		ID: grantID, WriterEpoch: 7, BindingVersion: sandboxstore.RootFSWriterBindingVersion,
		BindingDigest: bytes.Repeat([]byte{0xab}, 32), NodeUID: "issued-node", State: state,
	}
}

func terminalTestRequestBody(digest string, writerEpoch int64) string {
	payload, _ := json.Marshal(protocol.TerminalRequest{
		WriterEpoch: writerEpoch, BindingVersion: sandboxstore.RootFSWriterBindingVersion, BindingDigest: digest,
	})
	return string(payload)
}

func TestNewHandlerRejectsInvalidRenewalPolicy(t *testing.T) {
	_, err := NewHandler(HandlerConfig{
		Verifier: &fakeCallerVerifier{}, Store: &fakeGrantStore{}, LeaseTTL: time.Minute,
		RenewalPolicy: sandboxstore.RootFSWriterLeaseRenewalPolicy{
			LeaseTTL: time.Minute, GracePeriod: sandboxstore.RootFSWriterMaxRenewGrace + time.Millisecond,
		},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "renewal grace")
}
