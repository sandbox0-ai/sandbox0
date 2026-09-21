package nodeauthority

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauth"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/rootfswriterauthority"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotclaim"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotterminal"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	writerprotocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
	"github.com/stretchr/testify/require"
)

type batchRouteVerifier struct{}

func (batchRouteVerifier) Verify(context.Context, string) (nodeauth.Identity, error) {
	return nodeauth.Identity{NodeUID: "node-uid", AgentUID: "agent-uid"}, nil
}

type batchRouteStore struct {
	Store
	requests []*sandboxstore.RenewRootFSWriterGrantRequest
}

func (s *batchRouteStore) RenewRootFSWriterGrants(_ context.Context, requests []*sandboxstore.RenewRootFSWriterGrantRequest, policy sandboxstore.RootFSWriterLeaseRenewalPolicy) ([]sandboxstore.RenewRootFSWriterGrantResult, error) {
	s.requests = requests
	now := time.Now()
	results := make([]sandboxstore.RenewRootFSWriterGrantResult, len(requests))
	for i, request := range requests {
		results[i].Grant = &sandboxstore.RootFSWriterGrant{ID: request.GrantID, AuthorityObservedAt: now, LeaseExpiresAt: now.Add(policy.LeaseTTL)}
	}
	return results, nil
}

func TestNodeAuthorityMuxRoutesAuthenticatedWriterBatches(t *testing.T) {
	store := &batchRouteStore{}
	verifier := batchRouteVerifier{}
	writer, err := rootfswriterauthority.NewHandler(rootfswriterauthority.HandlerConfig{Store: store, Verifier: verifier, LeaseTTL: 30 * time.Second})
	require.NoError(t, err)
	lifecycle, err := rootfswriterauthority.NewLifecycleHandler(verifier, store, writer)
	require.NoError(t, err)
	mux := newNodeAuthorityMux(lifecycle, http.NotFoundHandler(), http.NotFoundHandler(), http.NotFoundHandler())
	payload, err := json.Marshal(writerprotocol.BatchRenewRequest{Items: []writerprotocol.BatchRenewItem{
		{GrantID: "grant-1", RenewRequest: writerprotocol.RenewRequest{WriterEpoch: 1, BindingVersion: 1, BindingDigest: strings.Repeat("ab", 32)}},
		{GrantID: "grant-2", RenewRequest: writerprotocol.RenewRequest{WriterEpoch: 2, BindingVersion: 1, BindingDigest: strings.Repeat("cd", 32)}},
	}})
	require.NoError(t, err)
	for _, authenticated := range []bool{false, true} {
		request := httptest.NewRequest(http.MethodPut, writerprotocol.BatchRenewPath, bytes.NewReader(payload))
		if authenticated {
			request.Header.Set("Authorization", "Bearer node-token")
		}
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, request)
		if !authenticated {
			require.Equal(t, http.StatusUnauthorized, response.Code, "batch route must retain node authentication")
			require.Empty(t, store.requests)
			continue
		}
		require.Equal(t, http.StatusOK, response.Code)
		var result writerprotocol.BatchRenewResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
		require.NoError(t, result.Validate(2))
		require.Len(t, store.requests, 2)
		require.Equal(t, "node-uid", store.requests[0].ConsumerNodeUID)
	}
}

type fakeStore struct {
	Store
	usage    sandboxstore.RootFSCompositeBacklogUsage
	usageErr error
}

func (f *fakeStore) GetRootFSCompositeBacklogUsage(context.Context) (sandboxstore.RootFSCompositeBacklogUsage, error) {
	return f.usage, f.usageErr
}

type fakeProber struct{}

func (fakeProber) ProbeCommandReady(context.Context, string, string) (*procdapi.CommandReadyProbeResult, error) {
	return nil, nil
}

func (fakeProber) MigrateRuntime(context.Context, string, procdapi.RuntimeMigrationRequest, string) (*procdapi.RuntimeMigrationResponse, error) {
	return nil, nil
}

type fakeTokenGenerator struct{}

func (fakeTokenGenerator) GenerateMigrationToken(procdapi.RuntimeMigrationRequest) (string, error) {
	return "scoped", nil
}

func (fakeTokenGenerator) GenerateToken(string, string, string) (string, error) {
	return "token", nil
}

func TestNewAssemblesSharedNodeAuthorityAndClaimPlanner(t *testing.T) {
	certFile, keyFile, caFile := writeTestAuthorityIdentity(t)
	component, err := New(Config{
		Store: &fakeStore{}, Address: "127.0.0.1:0",
		CertFile: certFile, KeyFile: keyFile, ClientCAFile: caFile,
		Identities: []nodeauth.CertificateIdentity{{
			CommonName: "node-agent", ClusterID: "cluster-1", NodeID: "node-1",
			NodeUID: "node-uid-1", AgentUID: "agent-1",
		}},
	})
	require.NoError(t, err)
	require.NotNil(t, component)
	require.NotNil(t, component.Ready())
	require.False(t, component.TerminalEnabled())

	planner, err := component.NewClaimPlanner(ClaimPlannerConfig{
		Prober: fakeProber{}, TokenGenerator: fakeTokenGenerator{},
		WriterTokenKey: make([]byte, 32),
	})
	require.NoError(t, err)
	require.IsType(t, &runtimeslotclaim.Planner{}, planner)
	require.NotNil(t, component.handovers)
	require.NotNil(t, component.cancellations)
	require.NotNil(t, component.sourceExecution)
	require.NotNil(t, component.evacuation)
	require.NotNil(t, component.preflights)
	require.NotNil(t, component.sourceRecovery)
	require.NotNil(t, component.staging)
	require.NotNil(t, component.stagingRelease)
	require.NotNil(t, component.destinations)
	require.NoError(t, component.hub.Close())
}

func TestNewRejectsSilentlyIgnoredTerminalCatalog(t *testing.T) {
	certFile, keyFile, caFile := writeTestAuthorityIdentity(t)
	component, err := New(Config{
		Store: &fakeStore{}, Address: "127.0.0.1:0",
		CertFile: certFile, KeyFile: keyFile, ClientCAFile: caFile,
		Identities: []nodeauth.CertificateIdentity{{
			CommonName: "node-agent", ClusterID: "cluster-1", NodeID: "node-1",
			NodeUID: "node-uid-1", AgentUID: "agent-1",
		}},
		Terminal: runtimeslotterminal.Config{NomadEndpointsFile: "/etc/sandbox0/nomad.json"},
	})
	require.Error(t, err)
	require.Nil(t, component)
}

func TestBacklogHealthHandlerExportsBoundedState(t *testing.T) {
	store := &fakeStore{usage: sandboxstore.RootFSCompositeBacklogUsage{
		UsedDescriptorBytes: 11, MaxDescriptorBytes: 22, GenerationCount: 3,
	}}
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	response := httptest.NewRecorder()
	backlogHealthHandler(store).ServeHTTP(response, request)

	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "11", response.Header().Get("X-Sandbox0-RootFS-Composite-Bytes"))
	require.Equal(t, "22", response.Header().Get("X-Sandbox0-RootFS-Composite-Limit"))
	require.Equal(t, "3", response.Header().Get("X-Sandbox0-RootFS-Composite-Generations"))
}

func writeTestAuthorityIdentity(t *testing.T) (string, string, string) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "authority.test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA:        true, BasicConstraintsValid: true, DNSNames: []string{"authority.test"},
	}
	raw, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	require.NoError(t, err)
	key, err := x509.MarshalPKCS8PrivateKey(privateKey)
	require.NoError(t, err)
	directory := t.TempDir()
	certFile := filepath.Join(directory, "tls.crt")
	keyFile := filepath.Join(directory, "tls.key")
	caFile := filepath.Join(directory, "ca.crt")
	certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: raw})
	require.NoError(t, os.WriteFile(certFile, certificatePEM, 0o600))
	require.NoError(t, os.WriteFile(caFile, certificatePEM, 0o600))
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0o600))
	return certFile, keyFile, caFile
}
