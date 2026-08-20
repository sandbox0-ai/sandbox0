package rootfswriterauthority

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
	"github.com/stretchr/testify/require"
)

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
