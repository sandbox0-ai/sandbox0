package runtimecheckpoint

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPeerClientPinsSourceAndProvesDestinationKey(t *testing.T) {
	source, err := NewPeerIdentity()
	require.NoError(t, err)
	target, err := NewPeerIdentity()
	require.NoError(t, err)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(r.TLS.PeerCertificates) != 1 || string(r.TLS.PeerCertificates[0].Raw) != string(target.Certificate[0]) {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{source}, ClientAuth: tls.RequireAnyClientCert}
	server.StartTLS()
	defer server.Close()
	endpoint, err := NewPeerEndpoint(strings.TrimPrefix(server.URL, "https://"), source)
	require.NoError(t, err)
	client, err := NewPeerClient(endpoint, target)
	require.NoError(t, err)
	defer client.CloseIdleConnections()
	response, err := client.Get(server.URL)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, response.StatusCode)
	require.NoError(t, response.Body.Close())
	other, err := NewPeerIdentity()
	require.NoError(t, err)
	wrongEndpoint, err := NewPeerEndpoint(strings.TrimPrefix(server.URL, "https://"), other)
	require.NoError(t, err)
	wrongSource, err := NewPeerClient(wrongEndpoint, target)
	require.NoError(t, err)
	defer wrongSource.CloseIdleConnections()
	_, err = wrongSource.Get(server.URL)
	require.Error(t, err, "an HTTPS server with a different key must fail TLS")
	wrongTarget, err := NewPeerClient(endpoint, other)
	require.NoError(t, err)
	defer wrongTarget.CloseIdleConnections()
	response, err = wrongTarget.Get(server.URL)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, response.StatusCode)
	require.NoError(t, response.Body.Close())
}

func TestPeerEndpointRejectsRedirectAndUnboundedAddresses(t *testing.T) {
	for _, address := range []string{"0.0.0.0:9443", "[::]:9443", "example.com:9443", "169.254.169.254:80", "10.0.0.1:0", "10.0.0.1:99999", "10.0.0.1:09443", "8.8.8.8:443"} {
		require.Error(t, ValidatePeerAddress(address), address)
	}
	identity, err := NewPeerIdentity()
	require.NoError(t, err)
	endpoint, err := NewPeerEndpoint("10.0.0.1:9443", identity)
	require.NoError(t, err)
	for _, address := range []string{"http://10.0.0.1:9443", "https://user@10.0.0.1:9443", "https://10.0.0.1:9443/", "https://10.0.0.1:9443?", "https://10.0.0.1:9443?q=1", "https://10.0.0.1:9443#fragment"} {
		bad := endpoint
		bad.Address = address
		require.Error(t, bad.Validate(), address)
	}
	client, err := NewPeerClient(endpoint, identity)
	require.NoError(t, err)
	defer client.CloseIdleConnections()
	require.Error(t, client.CheckRedirect(&http.Request{}, nil))
	require.Nil(t, client.Transport.(*http.Transport).Proxy)
}
