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
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
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
	payload, err := os.ReadFile(strings.TrimSpace(path))
	if err != nil {
		return "", err
	}
	var envelope struct {
		Stage rootfshandoff.StageRequest `json:"stage"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return "", err
	}
	stage := envelope.Stage
	if stage.Parent == "" {
		if err := json.Unmarshal(payload, &stage); err != nil {
			return "", err
		}
	}
	digest, err := stage.BindingDigest()
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(digest[:]), nil
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
	mux := http.NewServeMux()
	mux.Handle("/internal/v1/rootfs-writer-grants", http.NotFoundHandler())
	mux.Handle("/internal/v1/rootfs-writer-grants/", handler)
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

type issueOptions struct {
	sandboxID         string
	teamID            string
	grantID           string
	claimID           string
	slotID            string
	operationID       string
	rawToken          string
	bindingHex        string
	nodeUID           string
	nodeBootID        string
	podUID            string
	runtimeGeneration string
	gateParent        string
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
	issued, err := store.IssueRootFSWriterGrant(ctx, &sandboxstore.IssueRootFSWriterGrantRequest{
		GrantID: options.grantID, SandboxID: options.sandboxID, ClaimID: options.claimID,
		SlotID: options.slotID, OperationID: options.operationID, RawToken: options.rawToken,
		BindingVersion: sandboxstore.RootFSWriterBindingVersion, BindingDigest: binding,
		NodeUID: options.nodeUID, NodeBootID: options.nodeBootID, PodNamespace: "sandbox0-system",
		PodName: options.nodeUID + "-writer", PodUID: options.podUID, NodeName: options.nodeUID,
		GateParent: options.gateParent, RuntimeGeneration: options.runtimeGeneration,
		ConsumeExpiresAt: time.Now().Add(2 * time.Minute),
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
