package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

type objectLimitCredentialSourceStore struct {
	putCalls int
}

func (*objectLimitCredentialSourceStore) ListSourceMetadata(
	context.Context,
	string,
) ([]egressauth.CredentialSourceMetadata, error) {
	return nil, nil
}

func (*objectLimitCredentialSourceStore) GetSourceMetadata(
	context.Context,
	string,
	string,
) (*egressauth.CredentialSourceMetadata, error) {
	return nil, nil
}

func (s *objectLimitCredentialSourceStore) PutSource(
	_ context.Context,
	_ string,
	record *egressauth.CredentialSourceWriteRequest,
) (*egressauth.CredentialSourceMetadata, error) {
	s.putCalls++
	return &egressauth.CredentialSourceMetadata{
		Name:         record.Name,
		ResolverKind: record.ResolverKind,
	}, nil
}

func (*objectLimitCredentialSourceStore) DeleteSource(context.Context, string, string) error {
	return nil
}

func TestCreateCredentialSourceRejectsOversizedBodyBeforeStore(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &objectLimitCredentialSourceStore{}
	server := &Server{
		credentialSourceService: service.NewCredentialSourceService(store, zap.NewNop()),
		logger:                  zap.NewNop(),
	}
	body := `{"name":"source","resolverKind":"static_headers","spec":{},"padding":"` +
		strings.Repeat("x", int(egressauth.MaxCredentialSourceRequestBytes)) + `"}`
	ctx, recorder := credentialSourceTestContext(t, body)

	server.createCredentialSource(ctx)

	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf(
			"status = %d, want %d body=%s",
			recorder.Code,
			http.StatusRequestEntityTooLarge,
			recorder.Body.String(),
		)
	}
	if store.putCalls != 0 {
		t.Fatalf("store put calls = %d, want 0", store.putCalls)
	}
}

func TestCreateCredentialSourceRejectsOversizedSecretWithoutLeakingIt(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &objectLimitCredentialSourceStore{}
	server := &Server{
		credentialSourceService: service.NewCredentialSourceService(store, zap.NewNop()),
		logger:                  zap.NewNop(),
	}
	secretMarker := "do-not-leak-handler-secret"
	body := `{"name":"source","resolverKind":"static_username_password","spec":` +
		`{"staticUsernamePassword":{"username":"alice","password":"` +
		strings.Repeat("p", int(egressauth.MaxCredentialSecretBytes)) +
		secretMarker + `"}}}`
	ctx, recorder := credentialSourceTestContext(t, body)

	server.createCredentialSource(ctx)

	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf(
			"status = %d, want %d body=%s",
			recorder.Code,
			http.StatusRequestEntityTooLarge,
			recorder.Body.String(),
		)
	}
	if store.putCalls != 0 {
		t.Fatalf("store put calls = %d, want 0", store.putCalls)
	}
	if strings.Contains(recorder.Body.String(), secretMarker) {
		t.Fatalf("response leaked secret material: %s", recorder.Body.String())
	}
}

func credentialSourceTestContext(
	t *testing.T,
	body string,
) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/credential-sources",
		strings.NewReader(body),
	)
	request.Header.Set("Content-Type", "application/json")
	request = request.WithContext(internalauth.WithClaims(
		request.Context(),
		&internalauth.Claims{TeamID: "team-1", UserID: "user-1"},
	))
	ctx.Request = request
	return ctx, recorder
}
