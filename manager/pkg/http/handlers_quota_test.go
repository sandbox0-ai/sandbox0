package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"go.uber.org/zap"
)

type quotaHandlerFakeDB struct {
	limit     *quota.Limit
	current   int64
	putLimit  *quota.Limit
	deletedID string
}

func (db *quotaHandlerFakeDB) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	switch {
	case strings.Contains(sql, "INSERT INTO quota.team_quota_limits"):
		db.putLimit = &quota.Limit{
			TeamID:     args[0].(string),
			Dimension:  quota.Dimension(args[1].(string)),
			LimitValue: args[2].(int64),
		}
	case strings.Contains(sql, "DELETE FROM quota.team_quota_limits"):
		db.deletedID = args[0].(string)
	default:
		return pgconn.CommandTag{}, errors.New("unexpected exec")
	}
	return pgconn.CommandTag{}, nil
}

func (db *quotaHandlerFakeDB) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	switch {
	case strings.Contains(sql, "quota.team_quota_limits"):
		if db.limit == nil {
			return quotaHandlerFakeRow{err: pgx.ErrNoRows}
		}
		return quotaHandlerFakeRow{values: []any{db.limit.TeamID, db.limit.Dimension, db.limit.LimitValue}}
	case strings.Contains(sql, "manager_sandbox_projection_state"):
		return quotaHandlerFakeRow{values: []any{db.current}}
	default:
		return quotaHandlerFakeRow{err: errors.New("unexpected query")}
	}
}

type quotaHandlerFakeRow struct {
	values []any
	err    error
}

func (r quotaHandlerFakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	for i := range dest {
		switch target := dest[i].(type) {
		case *string:
			*target = r.values[i].(string)
		case *quota.Dimension:
			*target = r.values[i].(quota.Dimension)
		case *int64:
			*target = r.values[i].(int64)
		default:
			return errors.New("unsupported scan target")
		}
	}
	return nil
}

func TestGetTeamQuotaReturnsUsageStatus(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server := &Server{
		quotaRepo: quota.NewRepositoryWithDB(&quotaHandlerFakeDB{
			limit:   &quota.Limit{TeamID: "team-1", Dimension: quota.DimensionActiveSandboxes, LimitValue: 5},
			current: 2,
		}),
		logger: zap.NewNop(),
	}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/quotas/active_sandboxes", nil)
	request = request.WithContext(internalauth.WithClaims(request.Context(), &internalauth.Claims{TeamID: "team-1"}))
	ctx.Request = request
	ctx.Params = gin.Params{{Key: "dimension", Value: string(quota.DimensionActiveSandboxes)}}

	server.getTeamQuota(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	resp, apiErr, err := spec.DecodeResponse[quota.Status](strings.NewReader(recorder.Body.String()))
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("api error = %+v, want nil", apiErr)
	}
	if resp == nil || resp.LimitValue == nil || *resp.LimitValue != 5 || resp.Current != 2 || resp.Remaining == nil || *resp.Remaining != 3 {
		t.Fatalf("quota status = %+v, want limit=5 current=2 remaining=3", resp)
	}
	if resp.Unlimited || resp.Unit != "count" {
		t.Fatalf("quota status = %+v, want limited count quota", resp)
	}
}

func TestGetTeamQuotaReturnsUnlimitedWhenLimitIsUnset(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server := &Server{
		quotaRepo: quota.NewRepositoryWithDB(&quotaHandlerFakeDB{current: 7}),
		logger:    zap.NewNop(),
	}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/quotas/active_sandboxes", nil)
	request = request.WithContext(internalauth.WithClaims(request.Context(), &internalauth.Claims{TeamID: "team-1"}))
	ctx.Request = request
	ctx.Params = gin.Params{{Key: "dimension", Value: string(quota.DimensionActiveSandboxes)}}

	server.getTeamQuota(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	resp, apiErr, err := spec.DecodeResponse[quota.Status](strings.NewReader(recorder.Body.String()))
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("api error = %+v, want nil", apiErr)
	}
	if resp == nil || !resp.Unlimited || resp.LimitValue != nil || resp.Remaining != nil || resp.Current != 7 {
		t.Fatalf("quota status = %+v, want unlimited with current=7", resp)
	}
}

func TestPutTeamQuotaInternalRejectsMissingLimitValue(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name string
		body string
	}{
		{name: "missing", body: `{}`},
		{name: "null", body: `{"limit_value":null}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := &Server{quotaRepo: &quota.Repository{}, logger: zap.NewNop()}
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			request := httptest.NewRequest(http.MethodPut, "/internal/v1/teams/team-1/quotas/active_sandboxes", strings.NewReader(tt.body))
			request.Header.Set("Content-Type", "application/json")
			request = request.WithContext(internalauth.WithClaims(request.Context(), &internalauth.Claims{IsSystem: true}))
			ctx.Request = request
			ctx.Params = gin.Params{
				{Key: "team_id", Value: "team-1"},
				{Key: "dimension", Value: string(quota.DimensionActiveSandboxes)},
			}

			server.putTeamQuotaInternal(ctx)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d body=%s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
			}
			var response spec.Response
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
				t.Fatalf("unmarshal response: %v", err)
			}
			if response.Success || response.Error == nil || response.Error.Code != spec.CodeBadRequest {
				t.Fatalf("response = %+v, want bad_request error", response)
			}
		})
	}
}

func TestPutTeamQuotaInternalRequiresSystemToken(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server := &Server{quotaRepo: quota.NewRepositoryWithDB(&quotaHandlerFakeDB{}), logger: zap.NewNop()}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(http.MethodPut, "/internal/v1/teams/team-1/quotas/active_sandboxes", strings.NewReader(`{"limit_value":1}`))
	request.Header.Set("Content-Type", "application/json")
	request = request.WithContext(internalauth.WithClaims(request.Context(), &internalauth.Claims{TeamID: "team-1"}))
	ctx.Request = request
	ctx.Params = gin.Params{
		{Key: "team_id", Value: "team-1"},
		{Key: "dimension", Value: string(quota.DimensionActiveSandboxes)},
	}

	server.putTeamQuotaInternal(ctx)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d body=%s", recorder.Code, http.StatusForbidden, recorder.Body.String())
	}
}
