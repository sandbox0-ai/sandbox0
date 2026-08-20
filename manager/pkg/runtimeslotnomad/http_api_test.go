package runtimeslotnomad

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotreconciler"
	"github.com/stretchr/testify/require"
)

const (
	testServerURI = "spiffe://sandbox0.test/nomad/cluster/cluster-1/server"
	testClientURI = "spiffe://sandbox0.test/nomad/cluster/cluster-1/node/node-1"
)

type staticResolver struct {
	server Endpoint
	client Endpoint
}

func (r staticResolver) ServerEndpoint(context.Context, string) (Endpoint, error) {
	return r.server, nil
}

func (r staticResolver) ClientEndpoint(context.Context, string, string) (Endpoint, error) {
	return r.client, nil
}

type nomadTestServerState struct {
	mu              sync.Mutex
	token           string
	desiredStatus   string
	serverPresent   bool
	clientPresent   bool
	gcNotEligible   bool
	stopCalls       int
	gcCalls         int
	lastIdempotency string
}

func TestHTTPAPICallsServerAndExactClientOverMTLS(t *testing.T) {
	state := &nomadTestServerState{
		token: "first-token", desiredStatus: "run", serverPresent: true, clientPresent: true,
	}
	server, resolver, tokenFile := newNomadMTLSTestServer(t, state)
	defer server.Close()
	api, err := NewHTTPAPI(resolver)
	require.NoError(t, err)
	target := testTarget()

	allocation, err := api.ServerAllocation(t.Context(), target)
	require.NoError(t, err)
	require.Equal(t, testAllocation(), allocation)
	present, err := api.ClientAllocationPresent(t.Context(), target)
	require.NoError(t, err)
	require.True(t, present)

	require.NoError(t, api.StopAllocation(t.Context(), target, "purge-operation"))
	require.NoError(t, api.GarbageCollectAllocation(t.Context(), target))
	state.mu.Lock()
	require.Equal(t, 1, state.stopCalls)
	require.Equal(t, 1, state.gcCalls)
	require.Equal(t, "purge-operation", state.lastIdempotency)
	state.token = "second-token"
	state.mu.Unlock()
	require.NoError(t, os.WriteFile(tokenFile, []byte("second-token\n"), 0o600))
	require.NoError(t, api.StopAllocation(t.Context(), target, "purge-operation"))
}

func TestHTTPAPIControllerConvergesStopClientGCAndAbsence(t *testing.T) {
	state := &nomadTestServerState{
		token: "token", desiredStatus: "run", serverPresent: true, clientPresent: true,
	}
	server, resolver, _ := newNomadMTLSTestServer(t, state)
	defer server.Close()
	api, err := NewHTTPAPI(resolver)
	require.NoError(t, err)
	controller, err := New(api)
	require.NoError(t, err)

	before, err := controller.Observe(t.Context(), testTarget())
	require.NoError(t, err)
	require.True(t, before.PhysicalPresent)
	require.NoError(t, controller.Purge(t.Context(), runtimeslotreconciler.AllocationPurgeRequest{
		OperationID: "purge-operation", Target: testTarget(),
	}))
	after, err := controller.Observe(t.Context(), testTarget())
	require.NoError(t, err)
	require.False(t, after.PhysicalPresent)
	require.Len(t, after.ProofDigest, 32)
}

func TestHTTPAPITreatsDirectClientNotFoundAsPhysicalAbsence(t *testing.T) {
	state := &nomadTestServerState{
		token: "token", desiredStatus: "stop", serverPresent: false, clientPresent: false,
	}
	server, resolver, _ := newNomadMTLSTestServer(t, state)
	defer server.Close()
	api, err := NewHTTPAPI(resolver)
	require.NoError(t, err)

	allocation, err := api.ServerAllocation(t.Context(), testTarget())
	require.NoError(t, err)
	require.Nil(t, allocation)
	present, err := api.ClientAllocationPresent(t.Context(), testTarget())
	require.NoError(t, err)
	require.False(t, present)
	require.NoError(t, api.GarbageCollectAllocation(t.Context(), testTarget()))
}

func TestHTTPAPIReturnsClientGCEligibilityFence(t *testing.T) {
	state := &nomadTestServerState{
		token: "token", desiredStatus: "stop", serverPresent: true,
		clientPresent: true, gcNotEligible: true,
	}
	server, resolver, _ := newNomadMTLSTestServer(t, state)
	defer server.Close()
	api, err := NewHTTPAPI(resolver)
	require.NoError(t, err)

	err = api.GarbageCollectAllocation(t.Context(), testTarget())
	require.ErrorIs(t, err, runtimeslotreconciler.ErrAllocationStillPresent)
}

func TestHTTPAPIRejectsWrongPeerAndResolverTarget(t *testing.T) {
	state := &nomadTestServerState{
		token: "token", desiredStatus: "run", serverPresent: true, clientPresent: true,
	}
	server, resolver, _ := newNomadMTLSTestServer(t, state)
	defer server.Close()

	wrongPeer := resolver
	wrongPeer.client.PeerURISAN = "spiffe://sandbox0.test/nomad/cluster/cluster-1/node/other"
	api, err := NewHTTPAPI(wrongPeer)
	require.NoError(t, err)
	_, err = api.ClientAllocationPresent(t.Context(), testTarget())
	require.ErrorIs(t, err, errdefs.ErrUnavailable)

	wrongTarget := resolver
	wrongTarget.client.NodeID = "other-node"
	api, err = NewHTTPAPI(wrongTarget)
	require.NoError(t, err)
	_, err = api.ClientAllocationPresent(t.Context(), testTarget())
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
}

func newNomadMTLSTestServer(
	t *testing.T,
	state *nomadTestServerState,
) (*httptest.Server, staticResolver, string) {
	t.Helper()
	directory := t.TempDir()
	caCertificate, caKey, caPEM := newTestCA(t)
	serverCertificate := newTestCertificate(t, caCertificate, caKey, certificateRequest{
		commonName: "127.0.0.1", server: true,
		uris: []string{testServerURI, testClientURI},
	})
	clientCertificate, clientKey := newTestCertificatePEM(t, caCertificate, caKey, certificateRequest{
		commonName: "regional-manager", client: true,
		uris: []string{"spiffe://sandbox0.test/region/manager"},
	})
	caFile := filepath.Join(directory, "ca.pem")
	clientCertFile := filepath.Join(directory, "client.pem")
	clientKeyFile := filepath.Join(directory, "client-key.pem")
	tokenFile := filepath.Join(directory, "token")
	require.NoError(t, os.WriteFile(caFile, caPEM, 0o600))
	require.NoError(t, os.WriteFile(clientCertFile, clientCertificate, 0o600))
	require.NoError(t, os.WriteFile(clientKeyFile, clientKey, 0o600))
	state.mu.Lock()
	token := state.token
	state.mu.Unlock()
	require.NoError(t, os.WriteFile(tokenFile, []byte(token+"\n"), 0o600))

	clientRoots := x509.NewCertPool()
	require.True(t, clientRoots.AppendCertsFromPEM(caPEM))
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.TLS == nil || len(request.TLS.PeerCertificates) == 0 {
			t.Errorf("request did not present a verified TLS client certificate")
			http.Error(writer, "client identity missing", http.StatusUnauthorized)
			return
		}
		state.mu.Lock()
		defer state.mu.Unlock()
		if request.Header.Get("X-Nomad-Token") != state.token {
			http.Error(writer, "permission denied", http.StatusForbidden)
			return
		}
		switch request.URL.Path {
		case "/v1/allocation/allocation-1":
			if rejectUnexpectedNomadRequest(t, writer, request.Method, http.MethodGet, "method") ||
				rejectUnexpectedNomadRequest(t, writer, request.URL.Query().Get("namespace"), "default", "namespace") {
				return
			}
			if !state.serverPresent {
				http.NotFound(writer, request)
				return
			}
			_ = json.NewEncoder(writer).Encode(Allocation{
				ID: "allocation-1", Namespace: "default", NodeID: "node-1",
				DesiredStatus: state.desiredStatus,
			})
		case "/v1/allocation/allocation-1/stop":
			if rejectUnexpectedNomadRequest(t, writer, request.Method, http.MethodPost, "method") ||
				rejectUnexpectedNomadRequest(t, writer, request.URL.Query().Get("namespace"), "default", "namespace") ||
				rejectUnexpectedNomadRequest(t, writer, request.URL.Query().Get("no_shutdown_delay"), "true", "no_shutdown_delay") ||
				rejectUnexpectedNomadRequest(t, writer, request.URL.Query().Get("reschedule"), "false", "reschedule") {
				return
			}
			state.stopCalls++
			state.lastIdempotency = request.URL.Query().Get("idempotency_token")
			state.desiredStatus = "stop"
			writer.WriteHeader(http.StatusOK)
		case "/v1/client/fs/stat/allocation-1":
			if rejectUnexpectedNomadRequest(t, writer, request.Method, http.MethodGet, "method") ||
				rejectUnexpectedNomadRequest(t, writer, request.URL.Query().Get("path"), "alloc/", "path") {
				return
			}
			if !state.clientPresent {
				http.NotFound(writer, request)
				return
			}
			_, _ = writer.Write([]byte(`{"Tasks":{}}`))
		case "/v1/client/allocation/allocation-1/gc":
			if rejectUnexpectedNomadRequest(t, writer, request.Method, http.MethodGet, "method") {
				return
			}
			state.gcCalls++
			if !state.clientPresent {
				http.NotFound(writer, request)
				return
			}
			if state.gcNotEligible {
				http.Error(writer, "No such allocation on client, or allocation not eligible for GC", http.StatusInternalServerError)
				return
			}
			state.clientPresent = false
			writer.WriteHeader(http.StatusOK)
		default:
			http.NotFound(writer, request)
		}
	}))
	server.TLS = &tls.Config{
		MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{serverCertificate},
		ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clientRoots,
	}
	server.StartTLS()
	base := Endpoint{
		ClusterID: "cluster-1", BaseURL: server.URL, CAFile: caFile,
		ClientCertFile: clientCertFile, ClientKeyFile: clientKeyFile,
		TokenFile: tokenFile, Timeout: time.Second,
	}
	return server, staticResolver{
		server: endpointWithIdentity(base, "", testServerURI),
		client: endpointWithIdentity(base, "node-1", testClientURI),
	}, tokenFile
}

func rejectUnexpectedNomadRequest(
	t *testing.T,
	writer http.ResponseWriter,
	got, want, field string,
) bool {
	t.Helper()
	if got == want {
		return false
	}
	t.Errorf("Nomad request %s = %q, want %q", field, got, want)
	http.Error(writer, "unexpected request", http.StatusBadRequest)
	return true
}

func endpointWithIdentity(endpoint Endpoint, nodeID, peerURI string) Endpoint {
	endpoint.NodeID = nodeID
	endpoint.PeerURISAN = peerURI
	return endpoint
}

type certificateRequest struct {
	commonName string
	server     bool
	client     bool
	uris       []string
}

func newTestCA(t *testing.T) (*x509.Certificate, ed25519.PrivateKey, []byte) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	raw, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	require.NoError(t, err)
	certificate, err := x509.ParseCertificate(raw)
	require.NoError(t, err)
	return certificate, privateKey, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: raw})
}

func newTestCertificate(
	t *testing.T,
	ca *x509.Certificate,
	caKey ed25519.PrivateKey,
	request certificateRequest,
) tls.Certificate {
	t.Helper()
	certificatePEM, keyPEM := newTestCertificatePEM(t, ca, caKey, request)
	certificate, err := tls.X509KeyPair(certificatePEM, keyPEM)
	require.NoError(t, err)
	return certificate
}

func newTestCertificatePEM(
	t *testing.T,
	ca *x509.Certificate,
	caKey ed25519.PrivateKey,
	request certificateRequest,
) ([]byte, []byte) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: request.commonName},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature,
	}
	if request.server {
		template.ExtKeyUsage = append(template.ExtKeyUsage, x509.ExtKeyUsageServerAuth)
		template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	}
	if request.client {
		template.ExtKeyUsage = append(template.ExtKeyUsage, x509.ExtKeyUsageClientAuth)
	}
	for _, value := range request.uris {
		identity, err := url.Parse(value)
		require.NoError(t, err)
		template.URIs = append(template.URIs, identity)
	}
	raw, err := x509.CreateCertificate(rand.Reader, template, ca, publicKey, caKey)
	require.NoError(t, err)
	privateRaw, err := x509.MarshalPKCS8PrivateKey(privateKey)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: raw}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateRaw})
}
