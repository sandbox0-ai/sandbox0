// Copyright 2026 Sandbox0 Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	managerauthority "github.com/sandbox0-ai/sandbox0/manager/pkg/rootfswriterauthority"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/nomad-driver-sandbox0/internal/writerauthority"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
)

type migrateLogger struct {
	logger *log.Logger
}

func (l migrateLogger) Printf(format string, args ...any) {
	l.logger.Printf(format, args...)
}

func (l migrateLogger) Fatalf(format string, args ...any) {
	l.logger.Fatalf(format, args...)
}

func main() {
	mode := flag.String("mode", "serve", "serve or issue")
	stageFile := flag.String("stage-file", "", "stage-digest: StageRequest JSON path")
	dbURL := flag.String("db-url", "", "PostgreSQL URL")
	address := flag.String("address", "172.16.100.2:8421", "mTLS listen address")
	certFile := flag.String("cert-file", "", "server certificate")
	keyFile := flag.String("key-file", "", "server private key")
	clientCAFile := flag.String("client-ca-file", "", "client CA bundle")
	leaseTTL := flag.Duration("lease-ttl", 30*time.Second, "writer lease TTL")
	renewalGrace := flag.Duration("renewal-grace", 0, "writer renewal grace; default lease-ttl/2 capped at 5s")
	allowedClients := flag.String("allowed-clients", "", "comma-separated cn:nodeUID:podUID")
	skipMigrations := flag.Bool("skip-migrations", false, "skip sandbox store migrations")

	sandboxID := flag.String("sandbox-id", "", "issue: sandbox ID")
	teamID := flag.String("team-id", "team-1", "issue: team ID")
	grantID := flag.String("grant-id", "", "issue: grant ID")
	claimID := flag.String("claim-id", "", "issue: claim ID")
	slotID := flag.String("slot-id", "", "issue: slot ID")
	operationID := flag.String("operation-id", "", "issue: operation ID")
	rawToken := flag.String("raw-token", "", "issue: one-time writer token")
	bindingHex := flag.String("binding-digest", "", "issue: 32-byte hexadecimal stage binding digest")
	nodeUID := flag.String("node-uid", "", "issue: node UID")
	nodeBootID := flag.String("node-boot-id", "", "issue: node boot ID")
	podUID := flag.String("pod-uid", "", "issue: node pod UID")
	runtimeGeneration := flag.String("runtime-generation", "1", "issue: runtime generation")
	gateParent := flag.String("gate-parent", "", "issue: gate parent")
	resume := flag.Bool("resume", false, "issue the next epoch on an existing block-cow filesystem")
	filesystemID := flag.String("filesystem-id", "", "resume: existing filesystem ID")
	initialGenerationID := flag.String("initial-generation-id", "", "resume: existing head generation ID")
	expectedWriterEpoch := flag.Int64("expected-writer-epoch", 0, "resume: previous writer epoch")
	flag.Parse()

	if *mode == "stage-digest" {
		digest, err := stageDigest(*stageFile)
		if err != nil {
			fatal("stage-digest: %v", err)
		}
		fmt.Println(digest)
		return
	}
	if strings.TrimSpace(*dbURL) == "" {
		fatal("db-url is required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	pool, err := pgxpool.New(ctx, strings.TrimSpace(*dbURL))
	if err != nil {
		fatal("connect PostgreSQL: %v", err)
	}
	defer pool.Close()
	store := sandboxstore.NewPGSandboxStore(pool)
	if !*skipMigrations {
		logger := log.New(os.Stderr, "writer-authority-migrate ", log.LstdFlags)
		if err := sandboxstore.RunSandboxStoreMigrations(ctx, pool, migrateLogger{logger: logger}); err != nil {
			fatal("run migrations: %v", err)
		}
	}

	switch *mode {
	case "serve":
		if err := serve(ctx, store, *address, *certFile, *keyFile, *clientCAFile, *leaseTTL, *renewalGrace, *allowedClients); err != nil {
			fatal("serve: %v", err)
		}
	case "issue":
		if err := issue(ctx, store, issueOptions{
			sandboxID: *sandboxID, teamID: *teamID, grantID: *grantID, claimID: *claimID,
			slotID: *slotID, operationID: *operationID, rawToken: *rawToken,
			bindingHex: *bindingHex, nodeUID: *nodeUID, nodeBootID: *nodeBootID,
			podUID: *podUID, runtimeGeneration: *runtimeGeneration, gateParent: *gateParent,
			stage:  readStageOption(*stageFile),
			resume: *resume, filesystemID: *filesystemID,
			initialGenerationID: *initialGenerationID, expectedWriterEpoch: *expectedWriterEpoch,
		}); err != nil {
			fatal("issue: %v", err)
		}
	case "stage-digest":
		digest, err := stageDigest(*stageFile)
		if err != nil {
			fatal("stage-digest: %v", err)
		}
		fmt.Println(digest)
	default:
		fatal("unknown mode %q", *mode)
	}
}

func stageDigest(path string) (string, error) {
	stage, err := readStage(path)
	if err != nil {
		return "", err
	}
	digest, err := stage.BindingDigest()
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(digest[:]), nil
}

func readStage(path string) (rootfshandoff.StageRequest, error) {
	payload, err := os.ReadFile(strings.TrimSpace(path))
	if err != nil {
		return rootfshandoff.StageRequest{}, err
	}
	var envelope struct {
		Stage rootfshandoff.StageRequest `json:"stage"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return rootfshandoff.StageRequest{}, err
	}
	stage := envelope.Stage
	if stage.Parent == "" {
		if err := json.Unmarshal(payload, &stage); err != nil {
			return rootfshandoff.StageRequest{}, err
		}
	}
	return stage, nil
}

func readStageOption(path string) *rootfshandoff.StageRequest {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	stage, err := readStage(path)
	if err != nil {
		fatal("read stage: %v", err)
	}
	return &stage
}

func serve(
	ctx context.Context,
	store *sandboxstore.PGSandboxStore,
	address, certFile, keyFile, clientCAFile string,
	leaseTTL time.Duration,
	renewalGrace time.Duration,
	allowedClients string,
) error {
	if strings.TrimSpace(address) == "" || strings.TrimSpace(certFile) == "" ||
		strings.TrimSpace(keyFile) == "" || strings.TrimSpace(clientCAFile) == "" {
		return fmt.Errorf("address, certificate, key, and client CA are required")
	}
	identities, err := parseAllowedClients(allowedClients)
	if err != nil {
		return err
	}
	if renewalGrace == 0 {
		renewalGrace = leaseTTL / 2
	}
	renewalGrace = min(renewalGrace, 5*time.Second)
	handler, err := managerauthority.NewHandler(managerauthority.HandlerConfig{
		Verifier: writerauthority.NewCertVerifier(identities),
		Store:    store, LeaseTTL: leaseTTL,
		RenewalPolicy: sandboxstore.RootFSWriterLeaseRenewalPolicy{
			LeaseTTL: leaseTTL, GracePeriod: renewalGrace,
		},
	})
	if err != nil {
		return fmt.Errorf("create writer handler: %w", err)
	}
	verifier := writerauthority.NewCertVerifier(identities)
	mux := http.NewServeMux()
	mux.Handle("/internal/v1/rootfs-writer-grants", http.NotFoundHandler())
	mux.Handle("/internal/v1/rootfs-writer-grants/", newPublishHandler(verifier, store, handler))
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ok\n"))
	})
	authorized, err := writerauthority.NewCertMiddleware(identities, mux)
	if err != nil {
		return err
	}
	serverCertificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return fmt.Errorf("load server identity: %w", err)
	}
	caPEM, err := os.ReadFile(clientCAFile)
	if err != nil {
		return fmt.Errorf("read client CA: %w", err)
	}
	clientRoots := x509.NewCertPool()
	if !clientRoots.AppendCertsFromPEM(caPEM) {
		return fmt.Errorf("client CA contains no certificates")
	}
	server := &http.Server{
		Addr: address, Handler: authorized, ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second,
		TLSConfig: &tls.Config{
			MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{serverCertificate},
			ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clientRoots,
		},
	}
	errCh := make(chan error, 1)
	go func() { errCh <- server.ListenAndServeTLS("", "") }()
	log.Printf("Nomad RootFS writer authority listening on %s", address)
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-errCh:
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	}
}

func newPublishHandler(
	verifier managerauthority.CallerVerifier,
	store *sandboxstore.PGSandboxStore,
	next http.Handler,
) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.EscapedPath() != "/internal/v1/rootfs-writer-grants:renew" &&
			strings.HasSuffix(request.URL.EscapedPath(), "/terminal/publish") {
			servePublish(verifier, store, writer, request)
			return
		}
		next.ServeHTTP(writer, request)
	})
}

func servePublish(
	verifier managerauthority.CallerVerifier,
	store *sandboxstore.PGSandboxStore,
	writer http.ResponseWriter,
	request *http.Request,
) {
	if request.Method != http.MethodPut {
		writer.Header().Set("Allow", http.MethodPut)
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	relative := strings.TrimPrefix(request.URL.EscapedPath(), "/internal/v1/rootfs-writer-grants/")
	grantID, err := url.PathUnescape(strings.TrimSuffix(relative, "/terminal/publish"))
	if err != nil || grantID == "" || strings.Contains(grantID, "/") {
		http.Error(writer, "invalid writer grant", http.StatusBadRequest)
		return
	}
	caller, err := verifier.Verify(request.Context(), request.Header.Get("Authorization"))
	if err != nil {
		http.Error(writer, "unknown writer authority client", http.StatusUnauthorized)
		return
	}
	var body managerauthority.PublishGenerationRequest
	if err := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 128<<10)).Decode(&body); err != nil {
		http.Error(writer, "invalid publish request", http.StatusBadRequest)
		return
	}
	proof, err := hex.DecodeString(strings.TrimSpace(body.ProofDigest))
	if err != nil || len(proof) != 32 || hex.EncodeToString(proof) != strings.TrimSpace(body.ProofDigest) {
		http.Error(writer, "proof_digest must be a canonical SHA-256 digest", http.StatusBadRequest)
		return
	}
	grant, err := store.GetRootFSWriterGrant(request.Context(), grantID)
	if err != nil || grant == nil || grant.NodeUID != caller.NodeUID ||
		grant.WriterEpoch != body.WriterEpoch || grant.BindingVersion != body.BindingVersion {
		http.Error(writer, "publish request does not match writer grant", http.StatusConflict)
		return
	}
	binding, err := hex.DecodeString(strings.TrimSpace(body.BindingDigest))
	if err != nil || !bytes.Equal(grant.BindingDigest, binding) {
		http.Error(writer, "publish binding does not match writer grant", http.StatusConflict)
		return
	}
	if grant.State == sandboxstore.RootFSWriterGrantStateRetired {
		writer.WriteHeader(http.StatusNoContent)
		return
	}
	oldGenerationID := grant.InitialGenerationID
	filesystemID := grant.FilesystemID
	generation := body.Generation
	generation.FilesystemID = filesystemID
	generation.ParentGenerationID = oldGenerationID
	generation.WriterEpoch = grant.WriterEpoch
	if generation.ID == "" || generation.Descriptor == nil || generation.CurrentBlockHead == "" {
		http.Error(writer, "sealed generation is incomplete", http.StatusBadRequest)
		return
	}
	if _, err := store.BeginRootFSWriterRetire(request.Context(), &sandboxstore.BeginRootFSWriterRetireRequest{
		GrantID: grantID, WriterEpoch: grant.WriterEpoch, OperationID: body.OperationID,
		BindingVersion: grant.BindingVersion, BindingDigest: grant.BindingDigest,
		ExpectedOldHeadLayerID: oldGenerationID,
	}); err != nil {
		http.Error(writer, "begin regional retire: "+err.Error(), http.StatusConflict)
		return
	}
	runtimeGeneration, err := strconv.ParseInt(grant.RuntimeGeneration, 10, 64)
	if err != nil {
		runtimeGeneration = 1
	}
	err = store.WithSandboxLock(request.Context(), grant.SandboxID, func(ctx context.Context, tx sandboxstore.SandboxStoreTx, _ *sandboxstore.SandboxRecord) error {
		if err := tx.BeginLifecycleTxn(ctx, &sandboxstore.SandboxLifecycleTxn{
			ID: body.OperationID, SandboxID: grant.SandboxID, Kind: sandboxstore.SandboxLifecycleKindPause,
			Phase: sandboxstore.SandboxLifecyclePhasePublishing, ExpectedHeadLayerID: oldGenerationID,
		}); err != nil {
			return err
		}
		writerTx, ok := tx.(sandboxstore.RootFSWriterGrantTx)
		if !ok {
			return fmt.Errorf("sandbox transaction cannot publish rootfs generations")
		}
		if _, err := writerTx.CompleteRootFSWriterRetireAndPublishGeneration(ctx, &sandboxstore.CompleteRootFSWriterRetireAndPublishGenerationRequest{
			LifecycleTxnID: body.OperationID, GrantID: grantID, WriterEpoch: grant.WriterEpoch,
			OperationID: body.OperationID, BindingVersion: grant.BindingVersion, BindingDigest: grant.BindingDigest,
			ProofDigest: proof, ExpectedOldGenerationID: oldGenerationID, Generation: &generation,
		}); err != nil {
			return err
		}
		return tx.MarkRuntimePaused(ctx, grant.SandboxID, runtimeGeneration, time.Now().UTC())
	})
	if err != nil {
		http.Error(writer, "publish regional retire: "+err.Error(), http.StatusConflict)
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}

type issueOptions struct {
	sandboxID           string
	teamID              string
	grantID             string
	claimID             string
	slotID              string
	operationID         string
	rawToken            string
	bindingHex          string
	nodeUID             string
	nodeBootID          string
	podUID              string
	runtimeGeneration   string
	gateParent          string
	stage               *rootfshandoff.StageRequest
	resume              bool
	filesystemID        string
	initialGenerationID string
	expectedWriterEpoch int64
}

func issue(ctx context.Context, store *sandboxstore.PGSandboxStore, options issueOptions) error {
	binding, err := hex.DecodeString(strings.TrimSpace(options.bindingHex))
	if err != nil || len(binding) != 32 || hex.EncodeToString(binding) != strings.TrimSpace(options.bindingHex) {
		return fmt.Errorf("binding-digest must be 32 canonical lowercase hexadecimal bytes")
	}
	if err := store.UpsertSandbox(ctx, &sandboxstore.SandboxRecord{
		ID: options.sandboxID, TeamID: options.teamID, UserID: "user-1",
		TemplateID: "nomad-poc", TemplateName: "nomad-poc", TemplateNamespace: "default",
		DesiredState: sandboxstore.SandboxDesiredStateActive, CreatedAt: time.Now().UTC(),
	}); err != nil {
		return fmt.Errorf("seed sandbox: %w", err)
	}
	var expectedFilesystemID, initialGenerationID string
	if options.resume {
		if options.stage == nil || strings.TrimSpace(options.filesystemID) == "" ||
			strings.TrimSpace(options.initialGenerationID) == "" || options.expectedWriterEpoch <= 0 {
			return fmt.Errorf("resume issue requires stage-file, filesystem-id, initial-generation-id, and expected-writer-epoch")
		}
		expectedFilesystemID = strings.TrimSpace(options.filesystemID)
		initialGenerationID = strings.TrimSpace(options.initialGenerationID)
	} else if options.stage != nil {
		if options.stage.Generation == nil {
			return fmt.Errorf("stage generation is required")
		}
		if err := options.stage.Generation.Validate(); err != nil {
			return fmt.Errorf("validate stage generation: %w", err)
		}
		sourceRef := "nomad-poc/alpine"
		generation := options.stage.Generation
		if _, err := store.PutReadyRootFSBaseArtifact(ctx, &sandboxstore.PutReadyRootFSBaseArtifactRequest{
			ArtifactDigest: generation.BaseArtifactDigest, SourceOCIRef: sourceRef,
			SourceOCIDigest: generation.SourceOCIDigest, BaseBlockRoot: generation.BaseBlockRoot,
			FormatGeneration: generation.FormatGeneration, Descriptor: generation.Descriptor,
		}); err != nil {
			return fmt.Errorf("seed base artifact: %w", err)
		}
		filesystem, initial, err := store.EnsureInitialRootFSGeneration(ctx, &sandboxstore.EnsureInitialRootFSGenerationRequest{
			SandboxID: options.sandboxID, TeamID: options.teamID, SourceOCIRef: sourceRef,
			SourceOCIDigest: generation.SourceOCIDigest, BaseArtifactDigest: generation.BaseArtifactDigest,
		})
		if err != nil {
			return fmt.Errorf("seed initial block generation: %w", err)
		}
		expectedFilesystemID = filesystem.ID
		initialGenerationID = initial.ID
	}
	issued, err := store.IssueRootFSWriterGrant(ctx, &sandboxstore.IssueRootFSWriterGrantRequest{
		GrantID: options.grantID, SandboxID: options.sandboxID, ClaimID: options.claimID,
		SlotID: options.slotID, OperationID: options.operationID, RawToken: options.rawToken,
		BindingVersion: sandboxstore.RootFSWriterBindingVersion, BindingDigest: binding,
		NodeUID: options.nodeUID, NodeBootID: options.nodeBootID, PodNamespace: "sandbox0-system",
		PodName: options.nodeUID + "-writer", PodUID: options.podUID, NodeName: options.nodeUID,
		GateParent: options.gateParent, RuntimeGeneration: options.runtimeGeneration,
		ConsumeExpiresAt:     time.Now().Add(2 * time.Minute),
		ExpectedFilesystemID: expectedFilesystemID, InitialGenerationID: initialGenerationID,
		ExpectedWriterEpoch: options.expectedWriterEpoch,
	})
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(issued)
}

func parseAllowedClients(raw string) ([]writerauthority.CertIdentity, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("at least one allowed client is required")
	}
	var identities []writerauthority.CertIdentity
	for index, item := range strings.Split(raw, ",") {
		parts := strings.Split(strings.TrimSpace(item), ":")
		if len(parts) != 3 || strings.TrimSpace(parts[0]) == "" ||
			strings.TrimSpace(parts[1]) == "" || strings.TrimSpace(parts[2]) == "" {
			return nil, fmt.Errorf("allowed client %d must be commonName:nodeUID:podUID", index)
		}
		identities = append(identities, writerauthority.CertIdentity{
			CommonName: strings.TrimSpace(parts[0]), NodeUID: strings.TrimSpace(parts[1]), PodUID: strings.TrimSpace(parts[2]),
		})
	}
	return identities, nil
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
