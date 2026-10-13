//go:build linux

package nomadruntime

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/egressauthstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauth"
	managerauthority "github.com/sandbox0-ai/sandbox0/manager/pkg/rootfswriterauthority"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsartifact"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
	"github.com/stretchr/testify/require"
)

type autogrowMigrationLogger struct{}

func (autogrowMigrationLogger) Printf(string, ...any) {}
func (autogrowMigrationLogger) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}

func autogrowRemotePool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := dbpool.New(t.Context(), dbpool.Options{DatabaseURL: os.Getenv("INTEGRATION_DATABASE_URL"), Schema: "scheduler", MaxConns: 10})
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	var name string
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT current_database()`).Scan(&name))
	require.True(t, strings.HasPrefix(name, "s0_rootfs_autogrow"), "this destructive fixture requires its dedicated autogrow database")
	_, err = pool.Exec(t.Context(), `DROP SCHEMA IF EXISTS manager CASCADE; DROP SCHEMA IF EXISTS quota CASCADE;
		CREATE SCHEMA IF NOT EXISTS scheduler;
		CREATE OR REPLACE FUNCTION scheduler.update_updated_at_column() RETURNS TRIGGER AS $$
		BEGIN NEW.updated_at = NOW(); RETURN NEW; END; $$ LANGUAGE plpgsql;`)
	require.NoError(t, err)
	logger := autogrowMigrationLogger{}
	require.NoError(t, quota.RunMigrations(t.Context(), pool, logger))
	require.NoError(t, egressauthstore.RunMigrations(t.Context(), pool, logger))
	require.NoError(t, sandboxstore.RunSandboxStoreMigrations(t.Context(), pool, logger))
	return pool
}

// The local executable supplies the image contents. OCI identities are fixture
// provenance; object custody, sparse Ready CAS and generation creation use the
// real regional store. Registry unpacking has a separate importer gate.
func autogrowRemoteBase(t *testing.T, root, runID string, store *sandboxstore.PGSandboxStore, objects objectstore.ContextConditionalStore) *sandboxstore.RootFSBaseArtifact {
	t.Helper()
	ctx := t.Context()
	source := digest.FromString("autogrow-source-" + runID)
	executable, err := os.ReadFile(filepath.Join(root, "source", "writer"))
	require.NoError(t, err)
	spec := rootfsimporter.OperationSpec{SourceOCIRef: "fixture.invalid/writer@" + source.String(),
		Platform:         rootfsimporter.ReadyArtifactPlatform{OS: "linux", Architecture: "amd64"},
		FormatGeneration: 2, ProcdProtocol: "sandbox0.procd.v3", ProcdDigest: digest.FromBytes(executable).String(),
		LogicalSizeBytes: 64 << 40, InitialFilesystemSizeBytes: 300 << 20,
		BlockOptions: rootfsblock.BuildOptions{ObjectPrefix: "rootfs/autogrow/" + runID + "/import"}}
	operationID, spec, err := rootfsimporter.DeterministicOperation(spec)
	require.NoError(t, err)
	_, err = store.BeginRootFSImport(ctx, &sandboxstore.BeginRootFSImportRequest{OperationID: operationID, Spec: spec})
	require.NoError(t, err)
	operation, err := store.LeaseNextRootFSImport(ctx, "autogrow-fixture", 5*time.Minute)
	require.NoError(t, err)
	require.Equal(t, operationID, operation.ID)
	lease, err := operation.Lease()
	require.NoError(t, err)
	journal, err := sandboxstore.NewRootFSImportPublicationJournal(store, lease)
	require.NoError(t, err)
	publisher := rootfsimporter.JournaledPublisher{OperationID: operationID, Journal: journal, Publisher: rootfsblock.ObjectStorePublisher{Store: objects}}
	imagePath := filepath.Join(root, "base.xfs")
	require.NoError(t, (rootfsartifact.XFSBuilder{}).Build(ctx, filepath.Join(root, "source"), imagePath, spec.InitialFilesystemSizeBytes))
	image, err := os.Open(imagePath)
	require.NoError(t, err)
	base, buildErr := rootfsblock.BuildMaterializedFileGeneration(ctx, image, spec.InitialFilesystemSizeBytes, publisher, spec.BlockOptions, nil)
	require.NoError(t, buildErr)
	require.NoError(t, image.Close())
	require.NoError(t, os.Remove(imagePath))
	sparse, err := rootfsblock.GrowSparseGeneration(ctx, objects, base.Descriptor, spec.LogicalSizeBytes, publisher, spec.BlockOptions)
	require.NoError(t, err)
	refs := map[string]rootfsblock.ObjectReference{}
	for _, ref := range append(base.References, sparse.References...) {
		refs[ref.Key] = ref
	}
	var references []rootfsblock.ObjectReference
	var bytes int64
	for _, ref := range refs {
		references = append(references, ref)
		bytes += ref.Size
	}
	sort.Slice(references, func(i, j int) bool { return references[i].Key < references[j].Key })
	result := rootfsimporter.BuildResult{SourceOCIRef: spec.SourceOCIRef, SourceOCIDigest: source,
		ManifestDigest: digest.FromString("fixture-manifest-" + runID), ConfigDigest: digest.FromString("fixture-config-" + runID),
		Platform:     ocispec.Platform{OS: "linux", Architecture: "amd64"},
		LayerDigests: []digest.Digest{digest.FromString("fixture-layer-" + runID)}, DiffIDs: []digest.Digest{digest.FromString("fixture-diff-" + runID)},
		ProcdDigest: digest.Digest(spec.ProcdDigest), UnpackedBytes: int64(len(executable)), Files: 1,
		LogicalSizeBytes: spec.LogicalSizeBytes, InitialFilesystemSizeBytes: spec.InitialFilesystemSizeBytes,
		DescriptorDigest: digest.FromBytes(sparse.Payload), BaseBlockRoot: digest.Digest(sparse.Descriptor.MappingRoot.RootDigest),
		Descriptor: sparse.Descriptor, DescriptorBytes: sparse.Payload, Objects: len(references), Bytes: bytes, References: references}
	require.NoError(t, result.Validate())
	artifact, err := store.PublishReadyRootFSImport(ctx, &sandboxstore.PublishReadyRootFSImportRequest{Lease: lease, Result: result})
	require.NoError(t, err)
	require.Equal(t, int64(300<<20), artifact.InitialFilesystemSizeBytes)
	return artifact
}

func autogrowRemoteStage(t *testing.T, store *sandboxstore.PGSandboxStore, artifact *sandboxstore.RootFSBaseArtifact) rootfshandoff.StageRequest {
	return autogrowRemoteNamedStage(t, store, artifact, "")
}

func autogrowRemoteNamedStage(t *testing.T, store *sandboxstore.PGSandboxStore, artifact *sandboxstore.RootFSBaseArtifact, suffix string) rootfshandoff.StageRequest {
	t.Helper()
	if suffix != "" {
		require.NotEqual(t, "1", os.Getenv("SANDBOX0_ROOTFS_AUTOGROW_REGIONAL_FORK"), "named contention claims do not use the single-source fork fixture")
	}
	name := func(value string) string {
		if suffix == "" {
			return "autogrow-" + value
		}
		return "autogrow-" + suffix + "-" + value
	}
	sandboxID := name("sandbox")
	record := &sandboxstore.SandboxRecord{ID: sandboxID, TeamID: "autogrow-team", UserID: "autogrow-user", TemplateID: "autogrow-template",
		TemplateName: "autogrow-template", TemplateNamespace: "sandbox0", DesiredState: sandboxstore.SandboxDesiredStateActive,
		RuntimeGeneration: 1, RuntimeNamespace: "sandbox0", RuntimeID: name("pod"), CreatedAt: time.Now().UTC(), ResourceMillicpu: 1000, ResourceMemoryMiB: 1024}
	if os.Getenv("SANDBOX0_ROOTFS_AUTOGROW_REGIONAL_FORK") == "1" {
		record.ClusterID = "autogrow-cluster"
		record.RuntimeNamespace, record.RuntimeID = "", ""
		_, err := store.ReserveSandboxClaim(t.Context(), &sandboxstore.ReserveSandboxClaimRequest{
			Record: record, OperationID: "autogrow-claim-operation", LeaseTTL: time.Minute})
		require.NoError(t, err)
	} else {
		require.NoError(t, store.UpsertSandbox(t.Context(), record))
	}
	filesystem, initial, err := store.EnsureInitialRootFSGeneration(t.Context(), &sandboxstore.EnsureInitialRootFSGenerationRequest{
		SandboxID: sandboxID, TeamID: record.TeamID, SourceOCIRef: artifact.SourceOCIRef, SourceOCIDigest: artifact.SourceOCIDigest, BaseArtifactDigest: artifact.ArtifactDigest})
	require.NoError(t, err)
	token := digest.FromString("isolated-" + name("writer-token")).Encoded()
	stage := rootfshandoff.StageRequest{BindingVersion: rootfshandoff.WriterBindingVersion, Parent: digest.FromString(name("parent")).String(), InitialGeneration: initial.ID,
		Identity: rootfshandoff.Identity{NodeUID: "autogrow-node", BootID: "autogrow-boot", RuntimeGeneration: "1", AllocationID: name("allocation"),
			NetworkIncarnationID: sandboxID, TaskName: "app", SourceOCIDigest: artifact.SourceOCIDigest, RootFSDriver: "sandbox0-rootfs",
			RuntimeClass: "io.containerd.runsc.v1", SlotNonce: name("slot"), ClaimID: name("claim"), LaunchAttempt: name("attempt"),
			RootFSID: filesystem.ID, WriterEpoch: 1, WriterGrantID: name("grant"), WriterGrantToken: token, WriterGrantTokenDigest: rootfshandoff.WriterGrantTokenDigest(token)},
		ExpectedPolicyToken: rootfshandoff.NetworkPolicyToken{AllocationID: name("allocation"), NetworkIncarnationID: sandboxID, ClaimID: name("claim"),
			NetworkEpoch: 1, PolicyDigest: digest.FromString("isolated-policy").String(), SourceIP: "192.0.2.2", CtldGeneration: "autogrow-ctld", NetNSIdentity: name("netns")},
		Generation: &rootfshandoff.GenerationDescriptor{Version: rootfshandoff.GenerationDescriptorVersion, GenerationID: initial.ID, FilesystemID: filesystem.ID,
			SourceOCIDigest: initial.SourceOCIDigest, BaseArtifactDigest: initial.BaseArtifactDigest, BaseBlockRoot: initial.BaseBlockRoot,
			CurrentBlockHead: initial.CurrentBlockHead, WriterEpoch: initial.WriterEpoch, FormatGeneration: initial.FormatGeneration,
			DurabilityState: initial.DurabilityState, LocatorVersion: initial.LocatorVersion, Descriptor: initial.Descriptor}}
	require.NoError(t, stage.Validate())
	binding, err := stage.BindingDigest()
	require.NoError(t, err)
	if os.Getenv("SANDBOX0_ROOTFS_AUTOGROW_REGIONAL_FORK") == "1" {
		autogrowPrepareRegionalSlot(t, store, stage)
	}
	issued, err := store.IssueRootFSWriterGrant(t.Context(), &sandboxstore.IssueRootFSWriterGrantRequest{GrantID: stage.Identity.WriterGrantID,
		SandboxID: sandboxID, ExpectedFilesystemID: filesystem.ID, InitialGenerationID: initial.ID,
		ClaimID: stage.Identity.ClaimID, SlotID: stage.Identity.SlotNonce, OperationID: name("issue"), RawToken: token,
		BindingVersion: stage.BindingVersion, BindingDigest: binding[:], NodeUID: stage.Identity.NodeUID, NodeBootID: stage.Identity.BootID,
		RuntimeNamespace: "sandbox0", RuntimeID: name("pod"), RuntimeIncarnationID: stage.Identity.AllocationID,
		NodeName: "autogrow-node", GateParent: stage.Parent, RuntimeGeneration: stage.Identity.RuntimeGeneration, ConsumeExpiresAt: time.Now().Add(time.Minute)})
	require.NoError(t, err)
	require.Equal(t, stage.Identity.WriterEpoch, issued.Grant.WriterEpoch)
	return stage
}

type autogrowReplyLoss struct{ publish, install atomic.Bool }

type autogrowObjectOutage struct {
	down     atomic.Bool
	rejected atomic.Int64
}

func autogrowObjectEndpoint(t *testing.T, endpoint string) (string, *autogrowObjectOutage) {
	t.Helper()
	if os.Getenv("SANDBOX0_ROOTFS_AUTOGROW_OUTAGE") != "1" {
		return endpoint, nil
	}
	target, err := url.Parse(endpoint)
	require.NoError(t, err)
	proxy := httputil.NewSingleHostReverseProxy(target)
	outage := &autogrowObjectOutage{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if outage.down.Load() {
			outage.rejected.Add(1)
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("<Error><Code>ServiceUnavailable</Code><Message>isolated storage outage</Message></Error>"))
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	return server.URL, outage
}

func autogrowRemoteAuthority(t *testing.T, root string, store *sandboxstore.PGSandboxStore) (*managerauthority.ManagerClient, *autogrowReplyLoss) {
	t.Helper()
	return autogrowRemoteAuthorityForNode(t, root, store, "autogrow-node")
}

func autogrowRemoteAuthorityForNode(t *testing.T, root string, store *sandboxstore.PGSandboxStore, nodeUID string) (*managerauthority.ManagerClient, *autogrowReplyLoss) {
	t.Helper()
	require.NotEmpty(t, nodeUID)
	public, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "autogrow-ca"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caRaw, err := x509.CreateCertificate(rand.Reader, ca, ca, public, private)
	require.NoError(t, err)
	ca, err = x509.ParseCertificate(caRaw)
	require.NoError(t, err)
	cert := func(serial int64, name string, usage x509.ExtKeyUsage) (tls.Certificate, []byte, []byte) {
		pub, key, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		template := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: name}, NotBefore: ca.NotBefore, NotAfter: ca.NotAfter,
			KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
		raw, err := x509.CreateCertificate(rand.Reader, template, ca, pub, private)
		require.NoError(t, err)
		keyRaw, err := x509.MarshalPKCS8PrivateKey(key)
		require.NoError(t, err)
		certPEM, keyPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: raw}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyRaw})
		pair, err := tls.X509KeyPair(certPEM, keyPEM)
		require.NoError(t, err)
		return pair, certPEM, keyPEM
	}
	serverCert, _, _ := cert(2, "autogrow-authority", x509.ExtKeyUsageServerAuth)
	_, clientPEM, clientKey := cert(3, "autogrow-client", x509.ExtKeyUsageClientAuth)
	identities := []nodeauth.CertificateIdentity{{CommonName: "autogrow-client", NodeUID: nodeUID, AgentUID: "autogrow-ctld"}}
	verifier, err := nodeauth.NewCertificateVerifier(identities)
	require.NoError(t, err)
	handler, err := managerauthority.NewHandler(managerauthority.HandlerConfig{Verifier: verifier, Store: store, LeaseTTL: 10 * time.Minute})
	require.NoError(t, err)
	handler, err = managerauthority.NewLifecycleHandler(verifier, store, handler)
	require.NoError(t, err)
	handler, err = nodeauth.NewVerifiedCertificateMiddleware(verifier, handler)
	require.NoError(t, err)
	loss := &autogrowReplyLoss{}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var drop *atomic.Bool
		if strings.HasSuffix(r.URL.Path, "/live-commit") {
			drop = &loss.publish
		}
		if strings.HasSuffix(r.URL.Path, "/live-commit/installed") {
			drop = &loss.install
		}
		if drop != nil && !drop.Load() {
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, r)
			if recorder.Code == http.StatusOK && drop.CompareAndSwap(false, true) {
				http.Error(w, "isolated lost acknowledgement", http.StatusServiceUnavailable)
				return
			}
			for key, values := range recorder.Header() {
				w.Header()[key] = values
			}
			w.WriteHeader(recorder.Code)
			_, _ = w.Write(recorder.Body.Bytes())
			return
		}
		handler.ServeHTTP(w, r)
	}))
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{serverCert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	server.StartTLS()
	t.Cleanup(server.Close)
	paths := []string{filepath.Join(root, "authority-ca.pem"), filepath.Join(root, "node.pem"), filepath.Join(root, "node-key.pem"), filepath.Join(root, "token")}
	for i, data := range [][]byte{pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caRaw}), clientPEM, clientKey, []byte("certificate-identity")} {
		require.NoError(t, os.WriteFile(paths[i], data, 0600))
	}
	client, err := managerauthority.NewManagerClient(managerauthority.ManagerClientConfig{BaseURL: server.URL, CAFile: paths[0], ClientCertFile: paths[1], ClientKeyFile: paths[2], TokenFile: paths[3], Timeout: 2 * time.Second})
	require.NoError(t, err)
	return client, loss
}
