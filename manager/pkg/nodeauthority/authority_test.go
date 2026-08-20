package nodeauthority

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauth"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotclaim"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotterminal"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/stretchr/testify/require"
)

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

type fakeTokenGenerator struct{}

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
			NodeUID: "node-uid-1", PodUID: "agent-1",
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
	require.NoError(t, component.hub.Close())
}

func TestNewRejectsSilentlyIgnoredTerminalCatalog(t *testing.T) {
	certFile, keyFile, caFile := writeTestAuthorityIdentity(t)
	component, err := New(Config{
		Store: &fakeStore{}, Address: "127.0.0.1:0",
		CertFile: certFile, KeyFile: keyFile, ClientCAFile: caFile,
		Identities: []nodeauth.CertificateIdentity{{
			CommonName: "node-agent", ClusterID: "cluster-1", NodeID: "node-1",
			NodeUID: "node-uid-1", PodUID: "agent-1",
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
