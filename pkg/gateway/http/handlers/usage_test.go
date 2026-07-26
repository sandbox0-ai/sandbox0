package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/metering"
	"go.uber.org/zap"
)

type fakeUsageWindowReader struct {
	*fakeMeteringReader
	windows          []*metering.Window
	err              error
	nextCursor       string
	gotTeamID        string
	gotWindowType    string
	gotCursor        string
	gotLimit         int
	listWindowsCalls int
}

func (f *fakeUsageWindowReader) ListTeamWindows(_ context.Context, teamID string, windowType string, cursor string, limit int) ([]*metering.Window, string, error) {
	f.listWindowsCalls++
	f.gotTeamID = teamID
	f.gotWindowType = windowType
	f.gotCursor = cursor
	f.gotLimit = limit
	if f.err != nil {
		return nil, "", f.err
	}
	return f.windows, f.nextCursor, nil
}

type usageWindowsResponse struct {
	Windows    []UsageWindow `json:"windows"`
	NextCursor string        `json:"next_cursor"`
}

func TestMeteringHandlerListUsageWindowsScopesQueryToAuthenticatedTeam(t *testing.T) {
	gin.SetMode(gin.TestMode)
	windowStart := time.Date(2026, 7, 26, 1, 0, 0, 0, time.UTC)
	windowEnd := windowStart.Add(time.Hour)
	repo := &fakeUsageWindowReader{
		fakeMeteringReader: &fakeMeteringReader{},
		nextCursor:         "next-page",
		windows: []*metering.Window{{
			WindowID:    "window-1",
			RegionID:    "region-1",
			ClusterID:   "cluster-1",
			WindowType:  metering.WindowTypeSandboxRuntimeMiBMilliseconds,
			SubjectType: metering.SubjectTypeSandbox,
			SubjectID:   "sandbox-1",
			SandboxID:   "sandbox-1",
			TeamID:      "team-1",
			UserID:      "sandbox0-user",
			WindowStart: windowStart,
			WindowEnd:   windowEnd,
			Value:       3_686_400_000,
			Unit:        metering.WindowUnitMiBMilliseconds,
			RecordedAt:  windowEnd.Add(time.Second),
			Data:        json.RawMessage(`{"internal":"not-public"}`),
		}},
	}
	handler := NewMeteringHandler(repo, "", zap.NewNop())

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/usage/windows?cursor=page-1&limit=5000&window_type=sandbox.runtime_mib_milliseconds", nil)
	request = request.WithContext(authn.WithAuthContext(request.Context(), &authn.AuthContext{TeamID: "team-1"}))
	ctx.Request = request

	handler.ListUsageWindows(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if repo.gotTeamID != "team-1" {
		t.Fatalf("team_id = %q, want team-1", repo.gotTeamID)
	}
	if repo.gotWindowType != metering.WindowTypeSandboxRuntimeMiBMilliseconds {
		t.Fatalf("window_type = %q", repo.gotWindowType)
	}
	if repo.gotCursor != "page-1" || repo.gotLimit != 1000 {
		t.Fatalf("pagination = (%q, %d), want (page-1, 1000)", repo.gotCursor, repo.gotLimit)
	}

	response, apiErr, err := spec.DecodeResponse[usageWindowsResponse](recorder.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected api error: %+v", apiErr)
	}
	if len(response.Windows) != 1 || response.Windows[0].SandboxID != "sandbox-1" {
		t.Fatalf("windows = %#v", response.Windows)
	}
	if response.NextCursor != "next-page" {
		t.Fatalf("next_cursor = %q, want next-page", response.NextCursor)
	}
	body := recorder.Body.String()
	for _, privateField := range []string{"team_id", "user_id", "internal"} {
		if strings.Contains(body, privateField) {
			t.Fatalf("public response leaked %q: %s", privateField, body)
		}
	}
}

func TestMeteringHandlerListUsageWindowsRejectsMissingTeam(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repo := &fakeUsageWindowReader{fakeMeteringReader: &fakeMeteringReader{}}
	handler := NewMeteringHandler(repo, "", zap.NewNop())

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/usage/windows", nil)
	request = request.WithContext(authn.WithAuthContext(request.Context(), &authn.AuthContext{}))
	ctx.Request = request

	handler.ListUsageWindows(ctx)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
	if repo.listWindowsCalls != 0 {
		t.Fatal("repository was queried without a team")
	}
}

func TestMeteringHandlerListUsageWindowsHandlesInvalidCursor(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repo := &fakeUsageWindowReader{
		fakeMeteringReader: &fakeMeteringReader{},
		err:                errors.New("invalid cursor payload"),
	}
	handler := NewMeteringHandler(repo, "", zap.NewNop())

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/usage/windows?cursor=bad", nil)
	request = request.WithContext(authn.WithAuthContext(request.Context(), &authn.AuthContext{TeamID: "team-1"}))
	ctx.Request = request

	handler.ListUsageWindows(ctx)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
}

func TestMeteringHandlerListUsageWindowsReportsUnavailableQueryBackend(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repo := &fakeUsageWindowReader{
		fakeMeteringReader: &fakeMeteringReader{},
		err:                errors.New("clickhouse unavailable"),
	}
	handler := NewMeteringHandler(repo, "", zap.NewNop())

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/usage/windows", nil)
	request = request.WithContext(authn.WithAuthContext(request.Context(), &authn.AuthContext{TeamID: "team-1"}))
	ctx.Request = request

	handler.ListUsageWindows(ctx)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
}
