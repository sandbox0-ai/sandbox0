package cacheprewarm

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/stretchr/testify/require"
)

func TestCommandClientRunsSynchronousProcdContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, procdapi.ContextsPath, r.URL.Path)
		require.Equal(t, "prewarm-token", r.Header.Get("X-Internal-Token"))
		var request procdapi.CreateContextRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		require.Equal(t, procdapi.ProcessTypeCMD, request.Type)
		require.Equal(t, []string{"node", "-v"}, request.Cmd.Command)
		require.True(t, request.WaitUntilDone)
		zero, stdout := 0, "v22.0.0\n"
		require.NoError(t, spec.WriteSuccess(w, http.StatusCreated, procdapi.ContextResponse{ExitCode: &zero, Stdout: &stdout}))
	}))
	defer server.Close()
	response, err := NewCommandClient(nil).CreateCommand(t.Context(), server.URL, "prewarm-token", []string{"node", "-v"})
	require.NoError(t, err)
	require.Equal(t, "v22.0.0\n", *response.Stdout)
}
