package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

type errorSandboxStore struct {
	sandboxstore.SandboxStore
	err error
}

func (s *errorSandboxStore) GetSandbox(context.Context, string) (*sandboxstore.SandboxRecord, error) {
	return nil, s.err
}

func TestGetOwnedSandboxDistinguishesUnavailableStoreFromNotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name       string
		storeErr   error
		wantStatus int
	}{
		{name: "database unavailable", storeErr: errors.New("database unavailable"), wantStatus: http.StatusServiceUnavailable},
		{name: "record not found", storeErr: sandboxstore.ErrSandboxRecordNotFound, wantStatus: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sandboxService := service.NewSandboxServiceWithDependencies(service.SandboxServiceDependencies{
				SandboxStore: &errorSandboxStore{err: tt.storeErr},
				Logger:       zap.NewNop(),
			})
			server := &Server{sandboxService: sandboxService, logger: zap.NewNop()}
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = httptest.NewRequest(http.MethodGet, "/api/v1/sandboxes/sandbox-1", nil)

			if _, ok := server.getOwnedSandbox(ctx, "sandbox-1", &internalauth.Claims{TeamID: "team-1"}, ""); ok {
				t.Fatal("getOwnedSandbox returned ok")
			}
			if recorder.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", recorder.Code, tt.wantStatus)
			}
		})
	}
}

func TestSandboxHTTPMapsWrappedCtldUnavailableErrorsTo503(t *testing.T) {
	gin.SetMode(gin.TestMode)
	err := fmt.Errorf("checkpoint: %w", &ctldapi.RequestError{
		StatusCode: http.StatusServiceUnavailable,
		Message:    "object store unavailable",
	})

	tests := []struct {
		name  string
		write func(*Server, *gin.Context, error)
	}{
		{
			name: "lifecycle",
			write: func(server *Server, ctx *gin.Context, err error) {
				server.writeSandboxLifecycleTransitionError(ctx, "pause", "sandbox-1", err)
			},
		},
		{
			name: "rootfs product",
			write: func(server *Server, ctx *gin.Context, err error) {
				server.writeSandboxRootFSError(ctx, "create rootfs snapshot", "sandbox-1", err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			server := &Server{logger: zap.NewNop()}

			tt.write(server, ctx, err)

			if recorder.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
			}
		})
	}
}
