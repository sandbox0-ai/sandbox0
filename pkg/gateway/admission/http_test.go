package admission

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"go.uber.org/zap"
)

const testTeamID = "11111111-1111-4111-8111-111111111111"

type fakeStore struct {
	record    Record
	found     bool
	getErr    error
	putResult PutResult
	putErr    error
	putTeamID string
	putUpdate Update
}

func (s *fakeStore) Get(context.Context, string) (Record, bool, error) {
	return s.record, s.found, s.getErr
}

func (s *fakeStore) Put(_ context.Context, teamID string, update Update) (PutResult, error) {
	s.putTeamID = teamID
	s.putUpdate = update
	return s.putResult, s.putErr
}

func TestHandlerPutValidation(t *testing.T) {
	tests := []struct {
		name   string
		teamID string
		body   string
	}{
		{name: "invalid team id", teamID: "bad", body: `{}`},
		{name: "malformed body", teamID: testTeamID, body: `{`},
		{name: "unknown field", teamID: testTeamID, body: `{"version":1,"state":"allowed","source":"test","unknown":true}`},
		{name: "multiple values", teamID: testTeamID, body: `{"version":1,"state":"allowed","source":"test"} {}`},
		{name: "invalid trailing value", teamID: testTeamID, body: `{"version":1,"state":"allowed","source":"test"} x`},
		{name: "invalid update", teamID: testTeamID, body: `{"version":-1,"state":"allowed","source":"test"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := servePut(t, NewHandler(&fakeStore{}, nil), tt.teamID, tt.body)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
			}
		})
	}
}

func TestHandlerPutResults(t *testing.T) {
	body := `{"version":4,"state":"restricted","source":" control ","reason":" reason "}`

	t.Run("store unavailable", func(t *testing.T) {
		recorder := servePut(t, NewHandler(nil, zap.NewNop()), testTeamID, body)
		if recorder.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
		}
	})

	t.Run("version conflict", func(t *testing.T) {
		store := &fakeStore{putErr: ErrVersionConflict}
		recorder := servePut(t, NewHandler(store, zap.NewNop()), testTeamID, body)
		if recorder.Code != http.StatusConflict {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusConflict)
		}
	})

	t.Run("store error", func(t *testing.T) {
		store := &fakeStore{putErr: errors.New("failure")}
		recorder := servePut(t, NewHandler(store, zap.NewNop()), testTeamID, body)
		if recorder.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusInternalServerError)
		}
	})

	t.Run("success", func(t *testing.T) {
		want := PutResult{
			Record:  Record{TeamID: testTeamID, Version: 4, State: StateRestricted},
			Applied: true,
		}
		store := &fakeStore{putResult: want}
		recorder := servePut(t, NewHandler(store, zap.NewNop()), testTeamID, body)
		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusOK, recorder.Body.String())
		}
		if store.putTeamID != testTeamID || store.putUpdate.Source != "control" || store.putUpdate.Reason != "reason" {
			t.Fatalf("Put() team = %q, update = %#v", store.putTeamID, store.putUpdate)
		}
		var response spec.Response
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil || !response.Success {
			t.Fatalf("response = %#v, error = %v", response, err)
		}
	})
}

func TestNilHandlerReturnsUnavailable(t *testing.T) {
	var handler *Handler
	recorder := servePut(t, handler, testTeamID, `{"version":1,"state":"allowed","source":"test"}`)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
}

func servePut(t *testing.T, handler *Handler, teamID, body string) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.PUT("/internal/v1/teams/:team_id/admission-state", handler.Put)
	request := httptest.NewRequest(
		http.MethodPut,
		"/internal/v1/teams/"+teamID+"/admission-state",
		bytes.NewBufferString(body),
	)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	return recorder
}

func TestUsageMiddleware(t *testing.T) {
	t.Run("non usage route", func(t *testing.T) {
		recorder := serveUsage(t, http.MethodGet, "/api/v1/sandboxes", "/api/v1/sandboxes", nil, nil)
		if recorder.Code != http.StatusNoContent {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
		}
	})

	t.Run("missing auth context", func(t *testing.T) {
		recorder := serveUsage(t, http.MethodPost, "/api/v1/sandboxes", "/api/v1/sandboxes", nil, &fakeStore{})
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
		}
	})

	t.Run("missing team context", func(t *testing.T) {
		authCtx := &authn.AuthContext{}
		recorder := serveUsage(t, http.MethodPost, "/api/v1/sandboxes", "/api/v1/sandboxes", authCtx, &fakeStore{})
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
		}
	})

	t.Run("self hosted reader unavailable", func(t *testing.T) {
		authCtx := &authn.AuthContext{TeamID: testTeamID}
		recorder := serveUsage(t, http.MethodPost, "/api/v1/sandboxes", "/api/v1/sandboxes", authCtx, nil)
		if recorder.Code != http.StatusNoContent {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
		}
	})

	t.Run("required reader unavailable", func(t *testing.T) {
		authCtx := &authn.AuthContext{TeamID: testTeamID}
		recorder := serveUsageWithRequirement(
			t,
			http.MethodPost,
			"/api/v1/sandboxes",
			"/api/v1/sandboxes",
			authCtx,
			nil,
			true,
		)
		if recorder.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
		}
	})

	t.Run("read error", func(t *testing.T) {
		authCtx := &authn.AuthContext{TeamID: testTeamID}
		store := &fakeStore{getErr: errors.New("failure")}
		recorder := serveUsage(t, http.MethodPost, "/api/v1/sandboxes", "/api/v1/sandboxes", authCtx, store)
		if recorder.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
		}
	})

	t.Run("default allowed", func(t *testing.T) {
		authCtx := &authn.AuthContext{TeamID: testTeamID}
		recorder := serveUsage(t, http.MethodPost, "/api/v1/sandboxes", "/api/v1/sandboxes", authCtx, &fakeStore{})
		if recorder.Code != http.StatusNoContent {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
		}
	})

	t.Run("required state missing", func(t *testing.T) {
		authCtx := &authn.AuthContext{TeamID: testTeamID}
		recorder := serveUsageWithRequirement(
			t,
			http.MethodPost,
			"/api/v1/sandboxes",
			"/api/v1/sandboxes",
			authCtx,
			&fakeStore{},
			true,
		)
		if recorder.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
		}
	})

	t.Run("explicitly allowed", func(t *testing.T) {
		authCtx := &authn.AuthContext{TeamID: testTeamID}
		store := &fakeStore{found: true, record: Record{State: StateAllowed}}
		recorder := serveUsage(t, http.MethodPost, "/api/v1/sandboxes", "/api/v1/sandboxes", authCtx, store)
		if recorder.Code != http.StatusNoContent {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
		}
	})

	t.Run("restricted through wildcard route", func(t *testing.T) {
		gin.SetMode(gin.TestMode)
		router := gin.New()
		router.Use(func(c *gin.Context) {
			c.Set("auth_context", &authn.AuthContext{TeamID: testTeamID})
			c.Next()
		})
		store := &fakeStore{
			found:  true,
			record: Record{Version: 9, State: StateRestricted, Reason: "policy"},
		}
		router.Use(NewUsageMiddleware(store, nil, false))
		router.POST("/api/v1/templates/*path", func(c *gin.Context) {
			c.Status(http.StatusNoContent)
		})
		request := httptest.NewRequest(http.MethodPost, "/api/v1/templates/from-sandbox", nil)
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusForbidden {
			t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusForbidden, recorder.Body.String())
		}
		if !strings.Contains(recorder.Body.String(), spec.CodeAdmissionRestricted) ||
			!strings.Contains(recorder.Body.String(), `"version":9`) {
			t.Fatalf("response = %s", recorder.Body.String())
		}
	})
}

func serveUsage(
	t *testing.T,
	method string,
	route string,
	requestPath string,
	authCtx *authn.AuthContext,
	reader Reader,
) *httptest.ResponseRecorder {
	return serveUsageWithRequirement(t, method, route, requestPath, authCtx, reader, false)
}

func serveUsageWithRequirement(
	t *testing.T,
	method string,
	route string,
	requestPath string,
	authCtx *authn.AuthContext,
	reader Reader,
	requireState bool,
) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		if authCtx != nil {
			c.Set("auth_context", authCtx)
		}
		c.Next()
	})
	router.Use(NewUsageMiddleware(reader, zap.NewNop(), requireState))
	router.Handle(method, route, func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})
	request := httptest.NewRequest(method, requestPath, nil)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	return recorder
}

func TestStartsUsage(t *testing.T) {
	tests := []struct {
		method string
		path   string
		want   bool
	}{
		{method: http.MethodPut, path: "/api/v1/sandboxes/sb-1", want: true},
		{method: http.MethodPut, path: "/api/v1/sandboxes/sb-1/network", want: false},
		{method: http.MethodPut, path: "/api/v1/templates/template-1", want: true},
		{method: http.MethodPut, path: "/api/v1/templates/template-1/status", want: false},
		{method: http.MethodPost, path: "/api/v1/sandboxes", want: true},
		{method: http.MethodPost, path: "/api/v1/templates/", want: true},
		{method: http.MethodPost, path: "/api/v1/templates/from-sandbox", want: true},
		{method: http.MethodPost, path: "/api/v1/sandboxes/sb-1/resume", want: true},
		{method: http.MethodPost, path: "/api/v1/sandboxes/sb-1/fork", want: true},
		{method: http.MethodPost, path: "/api/v1/sandboxes/sb-1/snapshots", want: true},
		{method: http.MethodPost, path: "/api/v1/sandboxes//resume", want: false},
		{method: http.MethodPost, path: "/api/v1/sandboxes/sb-1/pause", want: false},
		{method: http.MethodDelete, path: "/api/v1/sandboxes/sb-1", want: false},
	}
	for _, tt := range tests {
		if got := StartsUsage(tt.method, tt.path); got != tt.want {
			t.Errorf("StartsUsage(%q, %q) = %v, want %v", tt.method, tt.path, got, tt.want)
		}
	}
}
