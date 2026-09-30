package http

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type releaseProbeAuthorityStub struct {
	valid     bool
	operation string
}

func (s *releaseProbeAuthorityStub) ValidateRuntimeReleaseProbe(_ context.Context, operation, node, uid, boot string) (bool, error) {
	s.operation = operation
	return s.valid && node == "node" && uid == "uid" && boot == "boot", nil
}
func TestRuntimeReleaseProbeRequiresScopedSignedAuthority(t *testing.T) {
	gin.SetMode(gin.TestMode)
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	for _, tc := range []struct {
		name, team, caller     string
		permission, authorized bool
		want                   int
	}{
		{"ordinary user", "team", internalauth.ServiceClusterGateway, false, true, http.StatusForbidden},
		{"wrong team", "team", internalauth.ServiceClusterGateway, true, true, http.StatusForbidden},
		{"wrong caller", "sandbox0-runtime-release-probe", internalauth.ServiceCtld, true, true, http.StatusForbidden},
		{"ungranted target", "sandbox0-runtime-release-probe", internalauth.ServiceClusterGateway, true, false, http.StatusConflict},
		{"qualified probe", "sandbox0-runtime-release-probe", internalauth.ServiceClusterGateway, true, true, http.StatusCreated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			authority := &releaseProbeAuthorityStub{valid: tc.authorized}
			claimer := &recordingSandboxClaimer{}
			server := &Server{runtimeReleaseProbes: authority, sandboxClaimer: claimer, logger: zap.NewNop(), authValidator: internalauth.NewValidator(internalauth.ValidatorConfig{Target: internalauth.ServiceManager, PublicKey: pub})}
			options := internalauth.GenerateOptions{Audit: &internalauth.AuditContext{OperationID: "signed-probe"}}
			if tc.permission {
				options.Permissions = []string{"runtime:release-probe"}
			}
			token, err := internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: tc.caller, PrivateKey: key, TTL: time.Minute}).Generate(internalauth.ServiceManager, tc.team, "", options)
			require.NoError(t, err)
			router := gin.New()
			router.POST("/internal/v1/runtime-release/probe", server.authMiddleware(), server.probeRuntimeRelease)
			response := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/internal/v1/runtime-release/probe", strings.NewReader(`{"template":"default","operation_id":"forged","target":{"node_id":"node","node_uid":"uid","node_boot_id":"boot"}}`))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set(internalauth.DefaultTokenHeader, token)
			router.ServeHTTP(response, request)
			require.Equal(t, tc.want, response.Code, response.Body.String())
			if tc.want == http.StatusCreated {
				require.Equal(t, "signed-probe", claimer.request.OperationID)
				require.Equal(t, "uid", claimer.request.TargetNode.NodeUID)
			} else {
				require.Nil(t, claimer.request)
			}
		})
	}
}
