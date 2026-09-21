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
	"fmt"
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
	"github.com/sandbox0-ai/sandbox0/pkg/nomadinventory"
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
	gcMissing       bool
	stopCalls       int
	evaluateCalls   int
	clientStatus    string
	jobID           string
	evaluateStatus  int
	emptyEvaluation bool
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
	target := httpTestTarget()

	allocation, err := api.ServerAllocation(t.Context(), target)
	require.NoError(t, err)
	expected := testAllocation()
	expected.ID = testHTTPAllocationID
	expected.JobID = nomadinventory.DefaultWarmJobID
	require.Equal(t, expected, allocation)
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

func TestHTTPAPIClientObservationBoundsUnresponsiveNode(t *testing.T) {
	state := &nomadTestServerState{token: "token"}
	server, resolver, _ := newNomadMTLSTestServer(t, state)
	defer server.Close()
	server.Config.Handler = http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
	})
	resolver.client.Timeout = time.Minute
	api, err := NewHTTPAPI(resolver)
	require.NoError(t, err)

	started := time.Now()
	_, err = api.ClientAllocationPresent(t.Context(), httpTestTarget())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(started), 10*time.Second, "a longer mutation timeout must not delay node observations")

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	started = time.Now()
	_, err = api.ClientAllocationPresent(ctx, httpTestTarget())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(started), time.Second, "the pass deadline must remain authoritative")
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

	before, err := controller.Observe(t.Context(), httpTestTarget())
	require.NoError(t, err)
	require.True(t, before.PhysicalPresent)
	request := runtimeslotreconciler.AllocationPurgeRequest{
		OperationID: "purge-operation", Target: httpTestTarget(),
	}
	require.ErrorIs(t, controller.Purge(t.Context(), request), runtimeslotreconciler.ErrAllocationStillPresent)
	require.NoError(t, controller.Purge(t.Context(), request))
	after, err := controller.Observe(t.Context(), httpTestTarget())
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

	allocation, err := api.ServerAllocation(t.Context(), httpTestTarget())
	require.NoError(t, err)
	require.Nil(t, allocation)
	present, err := api.ClientAllocationPresent(t.Context(), httpTestTarget())
	require.NoError(t, err)
	require.False(t, present)
	require.NoError(t, api.GarbageCollectAllocation(t.Context(), httpTestTarget()))
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

	err = api.GarbageCollectAllocation(t.Context(), httpTestTarget())
	require.ErrorIs(t, err, runtimeslotreconciler.ErrAllocationStillPresent)
}

func TestHTTPAPIResolvesAmbiguousClientGCMissingResponseByDirectObservation(t *testing.T) {
	state := &nomadTestServerState{
		token: "token", desiredStatus: "stop", serverPresent: true,
		clientPresent: false, gcMissing: true,
	}
	server, resolver, _ := newNomadMTLSTestServer(t, state)
	defer server.Close()
	api, err := NewHTTPAPI(resolver)
	require.NoError(t, err)

	require.NoError(t, api.GarbageCollectAllocation(t.Context(), httpTestTarget()))
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
	_, err = api.ClientAllocationPresent(t.Context(), httpTestTarget())
	require.ErrorIs(t, err, errdefs.ErrUnavailable)

	wrongTarget := resolver
	wrongTarget.client.NodeID = "other-node"
	api, err = NewHTTPAPI(wrongTarget)
	require.NoError(t, err)
	_, err = api.ClientAllocationPresent(t.Context(), httpTestTarget())
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
		case "/v1/allocations":
			if rejectUnexpectedNomadRequest(t, writer, request.Method, http.MethodGet, "method") ||
				rejectUnexpectedNomadRequest(t, writer, request.URL.Query().Get("namespace"), "default", "namespace") {
				return
			}
			if !state.serverPresent {
				_, _ = writer.Write([]byte("[]"))
				return
			}
			clientStatus := state.clientStatus
			if clientStatus == "" {
				clientStatus = "running"
			}
			jobID := state.jobID
			if jobID == "" {
				jobID = nomadinventory.DefaultWarmJobID
			}
			_ = json.NewEncoder(writer).Encode([]Allocation{{
				JobID: jobID,
				ID:    testHTTPAllocationID, Namespace: "default", NodeID: "node-1",
				DesiredStatus: state.desiredStatus, ClientStatus: clientStatus,
			}})
		case "/v1/job/" + nomadinventory.DefaultWarmJobID + "/evaluate", "/v1/job/" + nomadinventory.DefaultWarmJobID + "-shard-01/evaluate":
			require.Equal(t, http.MethodPost, request.Method)
			require.Equal(t, "default", request.URL.Query().Get("namespace"))
			require.Zero(t, request.ContentLength, "evaluation must not force-reschedule live allocations")
			state.evaluateCalls++
			if state.evaluateStatus != 0 {
				writer.WriteHeader(state.evaluateStatus)
				return
			}
			if state.emptyEvaluation {
				_, _ = writer.Write([]byte(`{}`))
				return
			}
			_, _ = writer.Write([]byte(`{"EvalID":"evaluation-receipt"}`))
		case "/v1/allocation/" + testHTTPAllocationID + "/stop":
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
		case "/v1/client/fs/stat/" + testHTTPAllocationID:
			if rejectUnexpectedNomadRequest(t, writer, request.Method, http.MethodGet, "method") ||
				rejectUnexpectedNomadRequest(t, writer, request.URL.Query().Get("path"), "alloc/", "path") {
				return
			}
			if !state.clientPresent {
				http.NotFound(writer, request)
				return
			}
			_, _ = writer.Write([]byte(`{"Tasks":{}}`))
		case "/v1/client/allocation/" + testHTTPAllocationID + "/gc":
			if rejectUnexpectedNomadRequest(t, writer, request.Method, http.MethodGet, "method") {
				return
			}
			state.gcCalls++
			if state.gcMissing {
				http.Error(writer, "No such allocation on client, or allocation not eligible for GC", http.StatusInternalServerError)
				return
			}
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

const testHTTPAllocationID = "40539b8a-a8f8-95c9-2e6b-eb123a3a07c6"

func httpTestTarget() runtimeslotreconciler.AllocationTarget {
	target := testTarget()
	target.AllocationID = testHTTPAllocationID
	return target
}

func TestHTTPAPIEvaluatesOnlyTerminalOwnedCarrierJobs(t *testing.T) {
	for _, tc := range []struct {
		name, client, desired, job string
		missing                    bool
		wantError                  bool
		wantCalls                  int
	}{
		{name: "live", client: "running", desired: "run", wantError: true},
		{name: "sharded", client: "failed", desired: "run", job: "sandbox0-warm-slots-shard-01", wantCalls: 1},
		{name: "completed", client: "complete", desired: "run", wantCalls: 1},
		{name: "failed", client: "failed", desired: "run", wantCalls: 1},
		{name: "stopped", client: "running", desired: "stop", wantCalls: 1},
		{name: "missing", missing: true},
		{name: "unowned", client: "complete", desired: "run", job: "other-job", wantError: true},
		{name: "invalid family suffix", client: "failed", desired: "run", job: "sandbox0-warm-slots-other", wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := &nomadTestServerState{token: "test", serverPresent: !tc.missing, clientStatus: tc.client, desiredStatus: tc.desired, jobID: tc.job}
			server, resolver, _ := newNomadMTLSTestServer(t, state)
			defer server.Close()
			api, err := NewHTTPAPI(resolver)
			require.NoError(t, err)
			err = api.EvaluateTerminalAllocation(t.Context(), httpTestTarget(), "replacement")
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.wantCalls, state.evaluateCalls)
			require.Zero(t, state.stopCalls)
			require.Zero(t, state.gcCalls)
		})
	}
}

func TestHTTPAPIEvaluationRequiresAcknowledgement(t *testing.T) {
	for _, code := range []int{0, http.StatusServiceUnavailable} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			state := &nomadTestServerState{token: "test", serverPresent: true, clientStatus: "failed", desiredStatus: "run", evaluateStatus: code, emptyEvaluation: true}
			server, resolver, _ := newNomadMTLSTestServer(t, state)
			defer server.Close()
			api, err := NewHTTPAPI(resolver)
			require.NoError(t, err)
			require.Error(t, api.EvaluateTerminalAllocation(t.Context(), httpTestTarget(), "replacement"))
			require.Equal(t, 1, state.evaluateCalls)
		})
	}
}
