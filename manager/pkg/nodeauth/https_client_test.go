package nodeauth

import (
	"context"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHTTPSClientPreservesEscapedPathAndReloadsToken(t *testing.T) {
	requests := make(chan [2]string, 2)
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests <- [2]string{request.URL.EscapedPath(), request.Header.Get("Authorization")}
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	directory := t.TempDir()
	caFile := filepath.Join(directory, "ca.pem")
	tokenFile := filepath.Join(directory, "token")
	certificate := server.Certificate()
	require.NoError(t, os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw}), 0o600))
	require.NoError(t, os.WriteFile(tokenFile, []byte("first-token\n"), 0o600))
	client, err := NewHTTPSClient(HTTPSClientConfig{
		Authority: "test authority", BaseURL: server.URL + "/base", CAFile: caFile,
		TokenFile: tokenFile, Timeout: time.Second,
	})
	require.NoError(t, err)

	for index, token := range []string{"first-token", "second-token"} {
		if index > 0 {
			require.NoError(t, os.WriteFile(tokenFile, []byte(token), 0o600))
		}
		request, err := client.NewRequest(context.Background(), http.MethodPut, "/v1/slot%2Fopaque", nil)
		require.NoError(t, err)
		response, err := client.Do(request)
		require.NoError(t, err)
		_, _ = io.Copy(io.Discard, response.Body)
		require.NoError(t, response.Body.Close())
		received := <-requests
		require.Equal(t, "/base/v1/slot%2Fopaque", received[0])
		require.Equal(t, "Bearer "+token, received[1])
	}
}

func TestHTTPSClientRejectsIncompleteSecurityConfiguration(t *testing.T) {
	_, err := NewHTTPSClient(HTTPSClientConfig{Authority: "runtime slot", BaseURL: "http://region.test"})
	require.ErrorContains(t, err, "HTTPS")
	_, err = NewHTTPSClient(HTTPSClientConfig{Authority: "runtime slot", BaseURL: "https://region.test"})
	require.ErrorContains(t, err, "CA and bearer token")
}
