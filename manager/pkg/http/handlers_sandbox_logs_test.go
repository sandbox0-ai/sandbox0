package http

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestGetSandboxLogsReturnsOK(t *testing.T) {
	gin.SetMode(gin.TestMode)

	pod := newHTTPLogsTestPod("sandbox-1", "team-1")
	sandboxService := service.NewSandboxService(
		fake.NewSimpleClientset(pod),
		newHTTPTestPodLister(t, pod),
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		service.SandboxServiceConfig{},
		zap.NewNop(),
		nil,
	)
	server := &Server{sandboxService: sandboxService, logger: zap.NewNop()}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sandboxes/sandbox-1/logs?tail_lines=10&limit_bytes=1024&previous=true&timestamps=true&since_seconds=30", nil)
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-1", UserID: "user-1"}))
	ctx.Request = req
	ctx.Params = gin.Params{{Key: "id", Value: "sandbox-1"}}

	server.getSandboxLogs(ctx)

	assert.Equal(t, http.StatusOK, recorder.Code)
	payload, apiErr, err := spec.DecodeResponse[service.SandboxLogsResponse](bytes.NewReader(recorder.Body.Bytes()))
	require.NoError(t, err)
	require.Nil(t, apiErr)
	require.NotNil(t, payload)
	assert.Equal(t, "sandbox-1", payload.SandboxID)
	assert.Equal(t, "procd", payload.Container)
	assert.True(t, payload.Previous)
	assert.Equal(t, "fake logs", payload.Logs)
}

func TestGetSandboxLogsStreamsWhenFollowTrue(t *testing.T) {
	gin.SetMode(gin.TestMode)

	pod := newHTTPLogsTestPod("sandbox-1", "team-1")
	sandboxService := service.NewSandboxService(
		fake.NewSimpleClientset(pod),
		newHTTPTestPodLister(t, pod),
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		service.SandboxServiceConfig{},
		zap.NewNop(),
		nil,
	)
	server := &Server{sandboxService: sandboxService, logger: zap.NewNop()}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sandboxes/sandbox-1/logs?follow=true&tail_lines=10", nil)
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-1", UserID: "user-1"}))
	ctx.Request = req
	ctx.Params = gin.Params{{Key: "id", Value: "sandbox-1"}}

	server.getSandboxLogs(ctx)

	assert.Equal(t, http.StatusOK, recorder.Code)
	assert.Contains(t, recorder.Header().Get("Content-Type"), "text/plain")
	assert.Equal(t, "sandbox-1", recorder.Header().Get("X-Sandbox-ID"))
	assert.Equal(t, "procd", recorder.Header().Get("X-Sandbox-Log-Container"))
	assert.Equal(t, "fake logs", recorder.Body.String())
}

func TestGetSandboxLogsRejectsInvalidTailLines(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server := &Server{logger: zap.NewNop()}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sandboxes/sandbox-1/logs?tail_lines=5001", nil)
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-1", UserID: "user-1"}))
	ctx.Request = req
	ctx.Params = gin.Params{{Key: "id", Value: "sandbox-1"}}

	server.getSandboxLogs(ctx)

	assert.Equal(t, http.StatusBadRequest, recorder.Code)
	_, apiErr, err := spec.DecodeResponse[service.SandboxLogsResponse](bytes.NewReader(recorder.Body.Bytes()))
	require.NoError(t, err)
	require.NotNil(t, apiErr)
	assert.Equal(t, spec.CodeBadRequest, apiErr.Code)
}

func newHTTPLogsTestPod(sandboxID, teamID string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxID,
			Namespace: "default",
			Labels: map[string]string{
				controller.LabelSandboxID: sandboxID,
			},
			Annotations: map[string]string{
				controller.AnnotationTeamID: teamID,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: service.DefaultSandboxLogContainer}},
		},
	}
}
