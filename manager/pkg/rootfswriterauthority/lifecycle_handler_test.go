package rootfswriterauthority

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
	"github.com/stretchr/testify/require"
)

type plannedPublishLifecycleTx struct {
	sandboxstore.SandboxStoreTx
	active       *sandboxstore.SandboxLifecycleTxn
	begin        *sandboxstore.SandboxLifecycleTxn
	updatedPhase string
}

func (t *plannedPublishLifecycleTx) GetActiveLifecycleTxn(context.Context, string) (*sandboxstore.SandboxLifecycleTxn, error) {
	return t.active, nil
}

func (t *plannedPublishLifecycleTx) BeginLifecycleTxn(_ context.Context, txn *sandboxstore.SandboxLifecycleTxn) error {
	t.begin = txn
	return nil
}

func (t *plannedPublishLifecycleTx) UpdateLifecycleTxnPhase(_ context.Context, _ string, phase string) error {
	t.updatedPhase = phase
	return nil
}

type fakeLifecycleStore struct {
	LifecycleStore
}

func TestNewLifecycleHandlerRequiresDependencies(t *testing.T) {
	verifier := &fakeCallerVerifier{}
	store := &fakeLifecycleStore{}
	next := http.NotFoundHandler()

	for name, create := range map[string]func() (http.Handler, error){
		"verifier": func() (http.Handler, error) { return NewLifecycleHandler(nil, store, next) },
		"store":    func() (http.Handler, error) { return NewLifecycleHandler(verifier, nil, next) },
		"next":     func() (http.Handler, error) { return NewLifecycleHandler(verifier, store, nil) },
	} {
		t.Run(name, func(t *testing.T) {
			handler, err := create()
			require.Error(t, err)
			require.Nil(t, handler)
		})
	}
}

func TestLifecycleHandlerDelegatesNonLifecycleRoutes(t *testing.T) {
	verifier := &fakeCallerVerifier{}
	nextCalls := 0
	handler, err := NewLifecycleHandler(verifier, &fakeLifecycleStore{}, http.HandlerFunc(func(
		writer http.ResponseWriter,
		_ *http.Request,
	) {
		nextCalls++
		writer.WriteHeader(http.StatusAccepted)
	}))
	require.NoError(t, err)

	request := httptest.NewRequest(http.MethodPut, protocol.RenewPath("grant-1"), strings.NewReader(`{}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	require.Equal(t, http.StatusAccepted, response.Code)
	require.Equal(t, 1, nextCalls)
	require.Zero(t, verifier.calls)
}

func TestLifecycleHandlerRejectsInvalidGrantPathBeforeAuthentication(t *testing.T) {
	verifier := &fakeCallerVerifier{}
	nextCalls := 0
	handler, err := NewLifecycleHandler(verifier, &fakeLifecycleStore{}, http.HandlerFunc(func(
		http.ResponseWriter,
		*http.Request,
	) {
		nextCalls++
	}))
	require.NoError(t, err)

	request := httptest.NewRequest(
		http.MethodPut,
		protocol.ConsumePathPrefix+"grant%2Fescaped"+publishPathSuffix,
		strings.NewReader(`{}`),
	)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	require.Equal(t, http.StatusBadRequest, response.Code)
	require.Zero(t, nextCalls)
	require.Zero(t, verifier.calls)
}

func TestLifecycleHandlerAuthenticatesLifecycleRoutesBeforeStorage(t *testing.T) {
	verifier := &fakeCallerVerifier{err: errors.New("rejected")}
	handler, err := NewLifecycleHandler(verifier, &fakeLifecycleStore{}, http.NotFoundHandler())
	require.NoError(t, err)

	request := httptest.NewRequest(
		http.MethodPut,
		protocol.ConsumePath("grant-1")+publishPathSuffix,
		strings.NewReader(`{}`),
	)
	request.Header.Set("Authorization", "Bearer projected-token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	require.Equal(t, http.StatusUnauthorized, response.Code)
	require.Equal(t, 1, verifier.calls)
	require.Equal(t, "Bearer projected-token", verifier.token)
}

func TestLifecycleHandlerRejectsUnknownRequestFields(t *testing.T) {
	verifier := &fakeCallerVerifier{}
	handler, err := NewLifecycleHandler(verifier, &fakeLifecycleStore{}, http.NotFoundHandler())
	require.NoError(t, err)

	request := httptest.NewRequest(
		http.MethodPut,
		protocol.ConsumePath("grant-1")+crashAbandonBeginPathSuffix,
		strings.NewReader(`{"unknown":true}`),
	)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	require.Equal(t, http.StatusBadRequest, response.Code)
	require.Equal(t, 1, verifier.calls)
}

func TestPreparePlannedPublishLifecycleAdvancesPrecreatedIntent(t *testing.T) {
	record := &sandboxstore.SandboxRecord{
		ID: "sandbox-1", CurrentPodNamespace: "nomad", CurrentPodName: "allocation-1",
	}
	grant := &sandboxstore.RootFSWriterGrant{ID: "grant-1", SandboxID: record.ID}
	tx := &plannedPublishLifecycleTx{active: &sandboxstore.SandboxLifecycleTxn{
		ID: "retire-1", SandboxID: record.ID, Kind: sandboxstore.SandboxLifecycleKindPause,
		Phase: sandboxstore.SandboxLifecyclePhasePreparing, Source: sandboxstore.SandboxLifecycleSourceAuto,
		FromGeneration: 7, FromPodNamespace: record.CurrentPodNamespace,
		FromPodName: record.CurrentPodName, ExpectedHeadLayerID: "generation-1",
	}}

	err := preparePlannedPublishLifecycle(
		t.Context(), tx, record, grant, "retire-1", "generation-1", 7,
	)
	require.NoError(t, err)
	require.Equal(t, sandboxstore.SandboxLifecyclePhasePublishing, tx.updatedPhase)
	require.Nil(t, tx.begin)
}

func TestPreparePlannedPublishLifecycleRejectsAnotherRuntime(t *testing.T) {
	record := &sandboxstore.SandboxRecord{
		ID: "sandbox-1", CurrentPodNamespace: "nomad", CurrentPodName: "allocation-1",
	}
	grant := &sandboxstore.RootFSWriterGrant{ID: "grant-1", SandboxID: record.ID}
	tx := &plannedPublishLifecycleTx{active: &sandboxstore.SandboxLifecycleTxn{
		ID: "retire-1", SandboxID: record.ID, Kind: sandboxstore.SandboxLifecycleKindPause,
		Phase: sandboxstore.SandboxLifecyclePhasePreparing, Source: sandboxstore.SandboxLifecycleSourceManual,
		FromGeneration: 7, FromPodNamespace: record.CurrentPodNamespace,
		FromPodName: "another-allocation", ExpectedHeadLayerID: "generation-1",
	}}

	err := preparePlannedPublishLifecycle(
		t.Context(), tx, record, grant, "retire-1", "generation-1", 7,
	)
	require.Error(t, err)
	require.Empty(t, tx.updatedPhase)
	require.Nil(t, tx.begin)
}
