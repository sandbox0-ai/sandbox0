package http

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/apierror"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/operationid"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type recordingMemoryLifecycle struct {
	recordingSandboxPauser
	recordingSandboxResumer
	pauses, resumes int
	err             error
}

func (r *recordingMemoryLifecycle) PauseMemorySandboxAndWait(_ context.Context, id string) (*service.PauseSandboxResponse, error) {
	r.pauses++
	return &service.PauseSandboxResponse{SandboxID: id, Paused: true}, r.err
}
func (r *recordingMemoryLifecycle) ResumeMemorySandboxAndWait(_ context.Context, id string) (*managerapi.ResumeSandboxResponse, error) {
	r.resumes++
	return &managerapi.ResumeSandboxResponse{SandboxID: id, Resumed: true}, r.err
}

func TestLifecycleExecutionModeIsExplicitAndNeverFallsBack(t *testing.T) {
	for _, action := range []string{"pause", "resume"} {
		for _, tc := range []struct {
			name, body                string
			memory, unsupported, fail bool
			status                    int
		}{
			{name: "legacy", status: 200},
			{name: "empty", body: `{}`, status: 200},
			{name: "filesystem", body: `{"memory":false}`, status: 200},
			{name: "memory", body: `{"memory":true}`, memory: true, status: 200},
			{name: "unsupported", body: `{"memory":true}`, unsupported: true, status: 503},
			{name: "failed", body: `{"memory":true}`, memory: true, fail: true, status: 503},
			{name: "typo", body: `{"memroy":true}`, status: 400},
			{name: "null", body: `{"memory":null}`, status: 400},
			{name: "null-body", body: `null`, status: 400},
			{name: "string", body: `{"memory":"true"}`, status: 400},
			{name: "trailing", body: `{"memory":true}{}`, status: 400},
			{name: "duplicate", body: `{"memory":true,"memory":false}`, status: 400},
			{name: "case", body: `{"Memory":null}`, status: 400},
			{name: "oversized", body: `{"memory":true,"padding":"` + strings.Repeat("x", 64<<10) + `"}`, status: 400},
		} {
			t.Run(action+"/"+tc.name, func(t *testing.T) {
				backend := &recordingMemoryLifecycle{}
				if tc.fail {
					backend.err = service.ErrSandboxLifecycleUnavailable
				}
				var pauser service.SandboxPauser = backend
				var resumer service.SandboxResumer = backend
				if tc.unsupported {
					pauser = &backend.recordingSandboxPauser
					resumer = &backend.recordingSandboxResumer
				}
				server, ctx, recorder := newSandboxLifecycleHandlerFixture(t, pauser, resumer, http.MethodPost)
				ctx.Request.Body = io.NopCloser(strings.NewReader(tc.body))
				ctx.Request.ContentLength = int64(len(tc.body))
				if action == "pause" {
					server.pauseSandbox(ctx)
				} else {
					server.resumeSandbox(ctx)
				}
				require.Equal(t, tc.status, recorder.Code, recorder.Body.String())
				cold := len(backend.recordingSandboxPauser.sandboxIDs) + len(backend.recordingSandboxResumer.sandboxIDs)
				memory := backend.pauses + backend.resumes
				if tc.memory {
					require.Equal(t, 1, memory)
					require.Zero(t, cold)
				} else if tc.status == 200 {
					require.Equal(t, 1, cold)
					require.Zero(t, memory)
				} else {
					require.Zero(t, cold)
					require.Zero(t, memory)
				}
			})
		}
	}
}

func TestMemoryResumeWithoutRetainedCheckpointExplainsConflict(t *testing.T) {
	backend := &recordingMemoryLifecycle{err: apierror.NewConflict("sandbox", "sandbox-1", sandboxstore.ErrNomadCheckpointNotRetained)}
	server, ctx, recorder := newSandboxLifecycleHandlerFixture(t, backend, backend, http.MethodPost)
	body := `{"memory":true}`
	ctx.Request.Body = io.NopCloser(strings.NewReader(body))
	ctx.Request.ContentLength = int64(len(body))
	server.resumeSandbox(ctx)
	require.Equal(t, http.StatusConflict, recorder.Code)
	require.Contains(t, recorder.Body.String(), "no retained memory checkpoint")
	require.Equal(t, 1, backend.resumes)
	require.Empty(t, backend.recordingSandboxResumer.sandboxIDs)
}

type recordingMemoryForker struct {
	recordingSandboxForker
	memoryRequest *service.ForkSandboxRequest
	err           error
}

func (r *recordingMemoryForker) ForkMemorySandbox(_ context.Context, source, team, user string, request *service.ForkSandboxRequest) (*service.ForkSandboxResponse, error) {
	copied := *request
	r.memoryRequest = &copied
	return &service.ForkSandboxResponse{SourceSandboxID: source}, r.err
}

func TestMemoryForkRequiresSignedStableOperation(t *testing.T) {
	for _, tc := range []struct {
		name, key, operation, body string
		unsupported, pending       bool
		status                     int
	}{
		{name: "legacy", body: `{}`, status: 201},
		{name: "missing-key", body: `{"memory":true}`, status: 400},
		{name: "mismatched-operation", key: "fork-one", operation: "spoofed", body: `{"memory":true}`, status: 409},
		{name: "unsupported", key: "fork-one", body: `{"memory":true}`, unsupported: true, status: 503},
		{name: "memory", key: "fork-one", body: `{"memory":true,"config":{"ttl":30}}`, status: 201},
		{name: "pending", key: "fork-one", body: `{"memory":true}`, pending: true, status: 503},
		{name: "null", key: "fork-one", body: `{"memory":null}`, status: 400},
		{name: "spoofed-body", key: "fork-one", body: `{"memory":true,"operation_id":"spoofed"}`, status: 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			backend := &recordingMemoryForker{}
			if tc.pending {
				backend.err = service.ErrSandboxLifecycleUnavailable
			}
			server := &Server{sandboxForker: backend, logger: zap.NewNop()}
			if tc.unsupported {
				server.sandboxForker = &backend.recordingSandboxForker
			}
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Params = gin.Params{{Key: "id", Value: "sandbox-source"}}
			request := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/sandbox-source/fork", strings.NewReader(tc.body))
			request.Header.Set("Idempotency-Key", tc.key)
			operation := tc.operation
			if operation == "" {
				operation = operationid.FromIdempotencyKey("sandbox.fork", "team-1", "user-1", "sandbox-source", tc.key)
			}
			ctx.Request = request.WithContext(internalauth.WithClaims(request.Context(), &internalauth.Claims{TeamID: "team-1", UserID: "user-1", Audit: &internalauth.AuditContext{OperationID: operation}}))
			server.forkSandbox(ctx)
			require.Equal(t, tc.status, recorder.Code, recorder.Body.String())
			if tc.name == "legacy" {
				require.NotNil(t, backend.request)
				require.Nil(t, backend.memoryRequest)
			} else {
				require.Nil(t, backend.request, "memory requests cannot dispatch filesystem fork")
				if tc.name == "memory" || tc.pending {
					require.NotNil(t, backend.memoryRequest)
					require.True(t, backend.memoryRequest.Memory)
					require.Equal(t, operation, backend.memoryRequest.OperationID)
				} else {
					require.Nil(t, backend.memoryRequest)
				}
			}
		})
	}
}
