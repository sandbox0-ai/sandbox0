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
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/rootfsmaterializer"
	managerauthority "github.com/sandbox0-ai/sandbox0/manager/pkg/rootfswriterauthority"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotauthority"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotnode"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotreconciler"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/nomad-driver-sandbox0/internal/writerauthority"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	runtimeslotprotocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
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
	mode := flag.String("mode", "serve", "serve, issue, fork, snapshot, restore, rebase-publish, rollback, cancel, or stage-digest")
	stageFile := flag.String("stage-file", "", "stage-digest: StageRequest JSON path")
	dbURL := flag.String("db-url", "", "PostgreSQL URL")
	address := flag.String("address", "172.16.100.2:8421", "mTLS listen address")
	certFile := flag.String("cert-file", "", "server certificate")
	keyFile := flag.String("key-file", "", "server private key")
	clientCAFile := flag.String("client-ca-file", "", "client CA bundle")
	leaseTTL := flag.Duration("lease-ttl", 30*time.Second, "writer lease TTL")
	runtimeSlotHeartbeatTTL := flag.Duration("runtime-slot-heartbeat-ttl", 30*time.Second, "runtime slot heartbeat TTL")
	renewalGrace := flag.Duration("renewal-grace", 0, "writer renewal grace; default lease-ttl/2 capped at 5s")
	allowedClients := flag.String("allowed-clients", "", "comma-separated cn:nodeUID:podUID or cn:nodeUID:podUID:clusterID:nodeID")
	runtimeSlotTerminal := flag.Bool("runtime-slot-terminal-reconciler", false, "enable plugin-independent runtime slot terminal reconciliation")
	nomadEndpointsFile := flag.String("runtime-slot-nomad-endpoints-file", "", "strict regional Nomad endpoint catalog JSON path")
	runtimeSlotReconcileInterval := flag.Duration("runtime-slot-reconcile-interval", runtimeslotreconciler.DefaultWorkerInterval, "delay between terminal reconciliation passes")
	runtimeSlotReconcileTimeout := flag.Duration("runtime-slot-reconcile-timeout", runtimeslotreconciler.DefaultWorkerPassTimeout, "timeout for one terminal reconciliation pass")
	runtimeSlotReconcileLimit := flag.Int("runtime-slot-reconcile-limit", 100, "maximum terminal candidates scanned per pass")
	skipMigrations := flag.Bool("skip-migrations", false, "skip sandbox store migrations")
	compositeBacklogBytes := flag.Int64("composite-backlog-bytes", sandboxstore.DefaultRootFSCompositeBacklogBytes, "regional PostgreSQL composite descriptor budget")
	materializerInterval := flag.Duration("materializer-interval", rootfsmaterializer.DefaultInterval, "S3 materializer scan interval")
	materializerScanLimit := flag.Int("materializer-scan-limit", rootfsmaterializer.DefaultScanLimit, "maximum composite generations scanned per pass")
	objectType := flag.String("object-type", "s3", "RootFS object-store type")
	objectBucket := flag.String("object-bucket", "", "RootFS object-store bucket (required in serve mode)")
	objectRegion := flag.String("object-region", "us-east-1", "RootFS object-store region")
	objectEndpoint := flag.String("object-endpoint", "", "RootFS object-store endpoint")

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
	sourceSandboxID := flag.String("source-sandbox-id", "", "fork: source sandbox")
	targetSandboxID := flag.String("target-sandbox-id", "", "fork: target sandbox")
	snapshotID := flag.String("snapshot-id", "", "snapshot/restore: immutable snapshot ID")
	rollbackTTL := flag.Duration("rollback-ttl", 24*time.Hour, "restore: old-head rollback retention")
	generationFile := flag.String("generation-file", "", "rebase-publish: prepared RootFSGeneration JSON path")
	expectedBaseArtifact := flag.String("expected-base-artifact", "", "rebase-publish: source Base artifact digest")
	healthCheckHex := flag.String("health-check-digest", "", "rebase-publish: 32-byte hexadecimal health proof")
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
		if err := store.SetRootFSCompositeBacklogLimit(ctx, *compositeBacklogBytes); err != nil {
			fatal("serve: configure composite backlog: %v", err)
		}
		materializer, err := newMaterializer(store, materializerConfig{
			objectType: *objectType, bucket: *objectBucket, region: *objectRegion,
			endpoint: *objectEndpoint, interval: *materializerInterval, scanLimit: *materializerScanLimit,
		})
		if err != nil {
			fatal("serve: create materializer: %v", err)
		}
		if err := serve(ctx, store, materializer, *address, *certFile, *keyFile, *clientCAFile,
			*leaseTTL, *renewalGrace, *runtimeSlotHeartbeatTTL, *allowedClients,
			runtimeSlotTerminalConfig{
				Enabled: *runtimeSlotTerminal, NomadEndpointsFile: *nomadEndpointsFile,
				Interval: *runtimeSlotReconcileInterval, PassTimeout: *runtimeSlotReconcileTimeout,
				ScanLimit: *runtimeSlotReconcileLimit,
			}); err != nil {
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
	case "fork":
		target := &sandboxstore.SandboxRecord{
			ID: *targetSandboxID, TeamID: *teamID, UserID: "user-1",
			TemplateID: "nomad-poc", TemplateName: "nomad-poc", TemplateNamespace: "default",
			DesiredState: sandboxstore.SandboxDesiredStatePaused, CreatedAt: time.Now().UTC(),
		}
		if err := store.UpsertSandbox(ctx, target); err != nil {
			fatal("fork: seed target: %v", err)
		}
		filesystem, err := store.ForkRootFSFilesystem(ctx, &sandboxstore.ForkRootFSFilesystemRequest{
			SourceSandboxID: *sourceSandboxID, TargetSandboxID: *targetSandboxID, TargetTeamID: *teamID,
		})
		if err != nil {
			fatal("fork: %v", err)
		}
		encodeResult("fork", filesystem)
	case "snapshot":
		snapshot, err := store.CreateRootFSSnapshot(ctx, &sandboxstore.CreateRootFSSnapshotRequest{
			SandboxID: *sandboxID, SnapshotID: *snapshotID,
		})
		if err != nil {
			fatal("snapshot: %v", err)
		}
		encodeResult("snapshot", snapshot)
	case "restore":
		restoreRequest := &sandboxstore.RestoreRootFSFromSnapshotRequest{
			SandboxID: *sandboxID, SnapshotID: *snapshotID, TeamID: *teamID,
			OperationID: *operationID,
		}
		if *rollbackTTL > 0 {
			restoreRequest.RollbackExpiresAt = time.Now().UTC().Add(*rollbackTTL)
		}
		filesystem, err := store.RestoreRootFSFromSnapshot(ctx, restoreRequest)
		if err != nil {
			fatal("restore: %v", err)
		}
		encodeResult("restore", filesystem)
	case "rebase-publish":
		payload, err := os.ReadFile(strings.TrimSpace(*generationFile))
		if err != nil {
			fatal("rebase-publish: read generation: %v", err)
		}
		var generation sandboxstore.RootFSGeneration
		if err := json.Unmarshal(payload, &generation); err != nil {
			fatal("rebase-publish: decode generation: %v", err)
		}
		healthDigest, err := hex.DecodeString(strings.TrimSpace(*healthCheckHex))
		if err != nil {
			fatal("rebase-publish: decode health-check-digest: %v", err)
		}
		rebaseRequest := &sandboxstore.PublishPausedRootFSRebaseRequest{
			SandboxID: *sandboxID, TeamID: *teamID, OperationID: *operationID,
			ExpectedSourceGenerationID: *initialGenerationID,
			ExpectedBaseArtifactDigest: *expectedBaseArtifact,
			Generation:                 &generation,
			HealthCheckDigest:          healthDigest,
		}
		if *rollbackTTL > 0 {
			rebaseRequest.RollbackExpiresAt = time.Now().UTC().Add(*rollbackTTL)
		}
		filesystem, err := store.PublishPausedRootFSRebase(ctx, rebaseRequest)
		if err != nil {
			fatal("rebase-publish: %v", err)
		}
		encodeResult("rebase-publish", filesystem)
	case "rollback":
		filesystem, err := store.RollbackRootFSHead(ctx, &sandboxstore.RollbackRootFSHeadRequest{
			SandboxID: *sandboxID, OperationID: *operationID, TeamID: *teamID,
		})
		if err != nil {
			fatal("rollback: %v", err)
		}
		encodeResult("rollback", filesystem)
	case "cancel":
		digest, err := hex.DecodeString(strings.TrimSpace(*bindingHex))
		if err != nil || len(digest) != 32 {
			fatal("cancel: binding-digest must be canonical")
		}
		if _, err := store.CancelRootFSWriterGrant(ctx, &sandboxstore.CancelRootFSWriterGrantRequest{
			GrantID: *grantID, WriterEpoch: int64(*expectedWriterEpoch), OperationID: *operationID,
			BindingVersion: sandboxstore.RootFSWriterBindingVersion, BindingDigest: digest,
		}); err != nil {
			fatal("cancel: %v", err)
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

func encodeResult(operation string, value any) {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		fatal("%s: encode: %v", operation, err)
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
	materializer *rootfsmaterializer.Worker,
	address, certFile, keyFile, clientCAFile string,
	leaseTTL time.Duration,
	renewalGrace time.Duration,
	runtimeSlotHeartbeatTTL time.Duration,
	allowedClients string,
	terminalConfig runtimeSlotTerminalConfig,
) error {
	if materializer == nil {
		return fmt.Errorf("rootfs materializer is required")
	}
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
	verifier := writerauthority.NewCertVerifier(identities)
	handler, err := managerauthority.NewHandler(managerauthority.HandlerConfig{
		Verifier: verifier,
		Store:    store, LeaseTTL: leaseTTL,
		RenewalPolicy: sandboxstore.RootFSWriterLeaseRenewalPolicy{
			LeaseTTL: leaseTTL, GracePeriod: renewalGrace,
		},
	})
	if err != nil {
		return fmt.Errorf("create writer handler: %w", err)
	}
	runtimeSlotHandler, err := runtimeslotauthority.NewHandler(runtimeslotauthority.HandlerConfig{
		Verifier: verifier, Store: store, HeartbeatTTL: runtimeSlotHeartbeatTTL,
	})
	if err != nil {
		return fmt.Errorf("create runtime slot handler: %w", err)
	}
	nodeChannelHub, err := runtimeslotnode.NewChannelHub(verifier)
	if err != nil {
		return fmt.Errorf("create runtime slot node channel: %w", err)
	}
	defer nodeChannelHub.Close()
	terminalWorker, err := newRuntimeSlotTerminalWorker(store, nodeChannelHub, terminalConfig)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle("/internal/v1/rootfs-writer-grants", http.NotFoundHandler())
	mux.Handle("/internal/v1/rootfs-writer-grants/", newPublishHandler(verifier, store, handler))
	mux.Handle(strings.TrimSuffix(runtimeslotprotocol.PathPrefix, "/"), http.NotFoundHandler())
	mux.Handle(runtimeslotprotocol.PathPrefix, runtimeSlotHandler)
	mux.Handle(runtimeslotprotocol.NodeChannelPath, nodeChannelHub)
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, request *http.Request) {
		usage, err := store.GetRootFSCompositeBacklogUsage(request.Context())
		if err != nil {
			http.Error(writer, "composite backlog unavailable", http.StatusServiceUnavailable)
			return
		}
		writer.Header().Set("X-Sandbox0-RootFS-Composite-Bytes", strconv.FormatInt(usage.UsedDescriptorBytes, 10))
		writer.Header().Set("X-Sandbox0-RootFS-Composite-Limit", strconv.FormatInt(usage.MaxDescriptorBytes, 10))
		writer.Header().Set("X-Sandbox0-RootFS-Composite-Generations", strconv.FormatInt(usage.GenerationCount, 10))
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
	defer server.Close()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen for writer authority: %w", err)
	}
	defer listener.Close()
	serviceCtx, cancelService := context.WithCancel(ctx)
	defer cancelService()
	go materializer.Run(serviceCtx, func(result rootfsmaterializer.Result, err error) {
		if err != nil {
			log.Printf("RootFS materializer: scanned=%d materialized=%d failed=%d error=%v",
				result.Scanned, result.Materialized, result.Failed, err)
		} else if result.Materialized > 0 {
			log.Printf("RootFS materializer: scanned=%d materialized=%d", result.Scanned, result.Materialized)
		}
	})
	var terminalErr <-chan error
	var terminalDone <-chan struct{}
	if terminalWorker != nil {
		errorsCh := make(chan error, 1)
		doneCh := make(chan struct{})
		terminalErr = errorsCh
		terminalDone = doneCh
		go func() {
			defer close(doneCh)
			errorsCh <- terminalWorker.Run(serviceCtx, logRuntimeSlotTerminalPass)
		}()
		log.Printf("Runtime slot terminal reconciler enabled: interval=%s timeout=%s limit=%d",
			terminalConfig.Interval, terminalConfig.PassTimeout, terminalConfig.ScanLimit)
	}
	defer func() {
		cancelService()
		if terminalDone != nil {
			select {
			case <-terminalDone:
			case <-time.After(5 * time.Second):
				log.Printf("Runtime slot terminal worker did not stop within 5s")
			}
		}
	}()
	errCh := make(chan error, 1)
	go func() { errCh <- server.ServeTLS(listener, "", "") }()
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
	case err := <-terminalErr:
		if serviceCtx.Err() != nil {
			return nil
		}
		return fmt.Errorf("runtime slot terminal worker stopped: %w", err)
	}
}

func logRuntimeSlotTerminalPass(report runtimeslotreconciler.WorkerReport) {
	if report.Error != nil {
		log.Printf("Runtime slot terminal reconcile: candidates=%d completed=%d skipped=%d failed=%d duration=%s error=%v",
			report.Result.Candidates, report.Result.Completed, report.Result.Skipped, report.Result.Failed,
			report.Duration.Round(time.Millisecond), report.Error)
		return
	}
	if report.Result.Completed > 0 || report.Result.Skipped > 0 {
		log.Printf("Runtime slot terminal reconcile: candidates=%d completed=%d skipped=%d failed=%d duration=%s",
			report.Result.Candidates, report.Result.Completed, report.Result.Skipped, report.Result.Failed,
			report.Duration.Round(time.Millisecond))
	}
}

func newPublishHandler(
	verifier managerauthority.CallerVerifier,
	store *sandboxstore.PGSandboxStore,
	next http.Handler,
) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		path := request.URL.EscapedPath()
		switch {
		case path != "/internal/v1/rootfs-writer-grants:renew" && strings.HasSuffix(path, "/terminal/publish"):
			servePublish(verifier, store, writer, request)
			return
		case strings.HasSuffix(path, "/terminal/crash-abandon/begin"):
			serveCrashAbandonBegin(verifier, store, writer, request)
			return
		case strings.HasSuffix(path, "/terminal/crash-abandon/complete"):
			serveCrashAbandonComplete(verifier, store, writer, request)
			return
		}
		next.ServeHTTP(writer, request)
	})
}

func serveCrashAbandonBegin(
	verifier managerauthority.CallerVerifier,
	store *sandboxstore.PGSandboxStore,
	writer http.ResponseWriter,
	request *http.Request,
) {
	grantID, caller, body, grant, ok := decodeCrashAbandonBegin(
		verifier, store, writer, request, "/terminal/crash-abandon/begin",
	)
	if !ok {
		return
	}
	if grant.State == sandboxstore.RootFSWriterGrantStateRetired &&
		grant.RetireKind == sandboxstore.RootFSWriterRetireKindCrashAbandon &&
		grant.RetireOperationID == body.OperationID {
		writer.WriteHeader(http.StatusNoContent)
		return
	}
	if err := ensureCrashLifecycle(request.Context(), store, grant, body); err != nil {
		http.Error(writer, "prepare crash lifecycle: "+err.Error(), http.StatusConflict)
		return
	}
	binding, _ := hex.DecodeString(body.BindingDigest)
	if _, err := store.BeginRootFSWriterCrashAbandon(request.Context(), &sandboxstore.BeginRootFSWriterCrashAbandonRequest{
		GrantID: grantID, WriterEpoch: body.WriterEpoch, OperationID: body.OperationID,
		BindingVersion: body.BindingVersion, BindingDigest: binding,
		NodeUID: caller.NodeUID, NodeBootID: grant.NodeBootID,
		ExpectedOldGenerationID: body.ExpectedOldGenerationID,
	}); err != nil {
		http.Error(writer, "begin crash abandon: "+err.Error(), http.StatusConflict)
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}

func serveCrashAbandonComplete(
	verifier managerauthority.CallerVerifier,
	store *sandboxstore.PGSandboxStore,
	writer http.ResponseWriter,
	request *http.Request,
) {
	grantID, caller, begin, grant, ok := decodeCrashAbandonComplete(verifier, store, writer, request)
	if !ok {
		return
	}
	proof := begin.Proof
	proofDigest, err := proof.Digest()
	if err != nil {
		http.Error(writer, "invalid crash fence proof: "+err.Error(), http.StatusBadRequest)
		return
	}
	if grant.State == sandboxstore.RootFSWriterGrantStateRetired {
		if grant.RetireKind == sandboxstore.RootFSWriterRetireKindCrashAbandon &&
			grant.RetireOperationID == begin.OperationID && bytes.Equal(grant.RetireProofDigest, proofDigest[:]) {
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		http.Error(writer, "writer grant has a different terminal result", http.StatusConflict)
		return
	}
	if proof.OperationID != begin.OperationID || proof.WriterGrantID != grantID ||
		proof.WriterEpoch != grant.WriterEpoch || proof.BindingVersion != grant.BindingVersion ||
		proof.BindingDigest != begin.BindingDigest || proof.Parent != grant.GateParent ||
		proof.ClaimID != grant.ClaimID || proof.NodeUID != caller.NodeUID ||
		proof.BootID != grant.NodeBootID || proof.InitialGeneration != grant.InitialGenerationID {
		http.Error(writer, "crash proof does not match writer grant", http.StatusConflict)
		return
	}
	generation, err := store.GetRootFSGeneration(request.Context(), grant.InitialGenerationID)
	if err != nil || generation.CurrentBlockHead != proof.InitialBlockHead {
		http.Error(writer, "crash proof does not match durable generation", http.StatusConflict)
		return
	}
	record, err := store.GetSandbox(request.Context(), grant.SandboxID)
	if err != nil || record.CurrentPodName != proof.PodUID {
		http.Error(writer, "crash proof does not match Nomad allocation", http.StatusConflict)
		return
	}
	binding, _ := hex.DecodeString(begin.BindingDigest)
	err = store.WithSandboxLock(request.Context(), grant.SandboxID, func(
		ctx context.Context,
		tx sandboxstore.SandboxStoreTx,
		_ *sandboxstore.SandboxRecord,
	) error {
		crashTx, ok := tx.(sandboxstore.RootFSWriterCrashAbandonTx)
		if !ok {
			return fmt.Errorf("sandbox transaction cannot abandon rootfs writers")
		}
		_, err := crashTx.CompleteRootFSWriterCrashAbandon(ctx, &sandboxstore.CompleteRootFSWriterCrashAbandonRequest{
			LifecycleTxnID: begin.OperationID, GrantID: grantID, WriterEpoch: grant.WriterEpoch,
			OperationID: begin.OperationID, BindingVersion: grant.BindingVersion, BindingDigest: binding,
			ProofVersion: sandboxstore.RootFSWriterCrashAbandonProofVersion, ProofDigest: proofDigest[:],
			NodeUID: caller.NodeUID, NodeBootID: grant.NodeBootID,
			ExpectedOldGenerationID: grant.InitialGenerationID,
		})
		return err
	})
	if err != nil {
		http.Error(writer, "complete crash abandon: "+err.Error(), http.StatusConflict)
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}

func decodeCrashAbandonBegin(
	verifier managerauthority.CallerVerifier,
	store *sandboxstore.PGSandboxStore,
	writer http.ResponseWriter,
	request *http.Request,
	suffix string,
) (string, managerauthority.CallerIdentity, managerauthority.CrashAbandonBeginRequest, *sandboxstore.RootFSWriterGrant, bool) {
	var body managerauthority.CrashAbandonBeginRequest
	if request.Method != http.MethodPut {
		writer.Header().Set("Allow", http.MethodPut)
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return "", managerauthority.CallerIdentity{}, body, nil, false
	}
	grantID, err := crashAbandonGrantID(request.URL.EscapedPath(), suffix)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return "", managerauthority.CallerIdentity{}, body, nil, false
	}
	caller, err := verifier.Verify(request.Context(), request.Header.Get("Authorization"))
	if err != nil {
		http.Error(writer, "unknown writer authority client", http.StatusUnauthorized)
		return "", managerauthority.CallerIdentity{}, body, nil, false
	}
	if err := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 128<<10)).Decode(&body); err != nil {
		http.Error(writer, "invalid crash abandon request", http.StatusBadRequest)
		return "", managerauthority.CallerIdentity{}, body, nil, false
	}
	if body.WriterEpoch <= 0 || body.BindingVersion != sandboxstore.RootFSWriterBindingVersion ||
		strings.TrimSpace(body.OperationID) == "" || strings.TrimSpace(body.ExpectedOldGenerationID) == "" {
		http.Error(writer, "invalid crash abandon binding", http.StatusBadRequest)
		return "", managerauthority.CallerIdentity{}, body, nil, false
	}
	binding, err := hex.DecodeString(strings.TrimSpace(body.BindingDigest))
	if err != nil || len(binding) != 32 || hex.EncodeToString(binding) != body.BindingDigest {
		http.Error(writer, "binding_digest must be canonical", http.StatusBadRequest)
		return "", managerauthority.CallerIdentity{}, body, nil, false
	}
	grant, err := store.GetRootFSWriterGrant(request.Context(), grantID)
	if err != nil || grant == nil || grant.NodeUID != caller.NodeUID ||
		grant.WriterEpoch != body.WriterEpoch || grant.BindingVersion != body.BindingVersion ||
		!bytes.Equal(grant.BindingDigest, binding) || grant.InitialGenerationID != body.ExpectedOldGenerationID {
		http.Error(writer, "crash abandon request does not match writer grant", http.StatusConflict)
		return "", managerauthority.CallerIdentity{}, body, nil, false
	}
	return grantID, caller, body, grant, true
}

func decodeCrashAbandonComplete(
	verifier managerauthority.CallerVerifier,
	store *sandboxstore.PGSandboxStore,
	writer http.ResponseWriter,
	request *http.Request,
) (string, managerauthority.CallerIdentity, managerauthority.CrashAbandonCompleteRequest, *sandboxstore.RootFSWriterGrant, bool) {
	var body managerauthority.CrashAbandonCompleteRequest
	if request.Method != http.MethodPut {
		writer.Header().Set("Allow", http.MethodPut)
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return "", managerauthority.CallerIdentity{}, body, nil, false
	}
	grantID, err := crashAbandonGrantID(request.URL.EscapedPath(), "/terminal/crash-abandon/complete")
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return "", managerauthority.CallerIdentity{}, body, nil, false
	}
	caller, err := verifier.Verify(request.Context(), request.Header.Get("Authorization"))
	if err != nil {
		http.Error(writer, "unknown writer authority client", http.StatusUnauthorized)
		return "", managerauthority.CallerIdentity{}, body, nil, false
	}
	if err := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 256<<10)).Decode(&body); err != nil {
		http.Error(writer, "invalid crash abandon completion", http.StatusBadRequest)
		return "", managerauthority.CallerIdentity{}, body, nil, false
	}
	grant, err := store.GetRootFSWriterGrant(request.Context(), grantID)
	binding, bindingErr := hex.DecodeString(strings.TrimSpace(body.BindingDigest))
	if err != nil || grant == nil || bindingErr != nil || len(binding) != 32 ||
		grant.NodeUID != caller.NodeUID || grant.WriterEpoch != body.WriterEpoch ||
		grant.BindingVersion != body.BindingVersion || !bytes.Equal(grant.BindingDigest, binding) ||
		grant.InitialGenerationID != body.ExpectedOldGenerationID || strings.TrimSpace(body.OperationID) == "" {
		http.Error(writer, "crash abandon completion does not match writer grant", http.StatusConflict)
		return "", managerauthority.CallerIdentity{}, body, nil, false
	}
	return grantID, caller, body, grant, true
}

func crashAbandonGrantID(path, suffix string) (string, error) {
	relative := strings.TrimPrefix(path, "/internal/v1/rootfs-writer-grants/")
	if relative == path || !strings.HasSuffix(relative, suffix) {
		return "", fmt.Errorf("invalid crash abandon path")
	}
	grantID, err := url.PathUnescape(strings.TrimSuffix(relative, suffix))
	if err != nil || grantID == "" || strings.Contains(grantID, "/") {
		return "", fmt.Errorf("invalid writer grant")
	}
	return grantID, nil
}

func ensureCrashLifecycle(
	ctx context.Context,
	store *sandboxstore.PGSandboxStore,
	grant *sandboxstore.RootFSWriterGrant,
	body managerauthority.CrashAbandonBeginRequest,
) error {
	runtimeGeneration, err := strconv.ParseInt(grant.RuntimeGeneration, 10, 64)
	if err != nil || runtimeGeneration <= 0 {
		return fmt.Errorf("invalid writer runtime generation")
	}
	return store.WithSandboxLock(ctx, grant.SandboxID, func(
		lockCtx context.Context,
		tx sandboxstore.SandboxStoreTx,
		record *sandboxstore.SandboxRecord,
	) error {
		active, activeErr := tx.GetActiveLifecycleTxn(lockCtx, grant.SandboxID)
		if activeErr != nil {
			return activeErr
		}
		if active != nil {
			if active.ID == body.OperationID && active.Kind == sandboxstore.SandboxLifecycleKindPause &&
				active.Source == sandboxstore.SandboxLifecycleSourceCrash &&
				active.ExpectedHeadLayerID == body.ExpectedOldGenerationID {
				return nil
			}
			return fmt.Errorf("another lifecycle transaction %s is active", active.ID)
		}
		if record == nil || record.DesiredState != sandboxstore.SandboxDesiredStateActive ||
			record.RuntimeGeneration != runtimeGeneration || record.CurrentPodNamespace == "" || record.CurrentPodName == "" {
			return fmt.Errorf("sandbox runtime does not match crashed writer")
		}
		return tx.BeginLifecycleTxn(lockCtx, &sandboxstore.SandboxLifecycleTxn{
			ID: body.OperationID, SandboxID: grant.SandboxID, Kind: sandboxstore.SandboxLifecycleKindPause,
			Phase: sandboxstore.SandboxLifecyclePhasePublishing, Source: sandboxstore.SandboxLifecycleSourceCrash,
			Cancelable: false, FromGeneration: runtimeGeneration,
			FromPodNamespace: record.CurrentPodNamespace, FromPodName: record.CurrentPodName,
			ExpectedHeadLayerID: body.ExpectedOldGenerationID,
		})
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
		status := http.StatusConflict
		if errors.Is(err, sandboxstore.ErrRootFSCompositeBacklogExhausted) {
			status = http.StatusInsufficientStorage
			writer.Header().Set("Retry-After", "1")
		}
		http.Error(writer, "publish regional retire: "+err.Error(), status)
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}

type materializerConfig struct {
	objectType string
	bucket     string
	region     string
	endpoint   string
	interval   time.Duration
	scanLimit  int
}

func newMaterializer(
	store *sandboxstore.PGSandboxStore,
	config materializerConfig,
) (*rootfsmaterializer.Worker, error) {
	if strings.TrimSpace(config.bucket) == "" {
		return nil, fmt.Errorf("object-bucket is required")
	}
	objects, err := objectstore.Create(objectstore.Config{
		Type: config.objectType, Bucket: config.bucket, Region: config.region, Endpoint: config.endpoint,
		AccessKey: os.Getenv("SANDBOX0_ROOTFS_OBJECT_ACCESS_KEY"),
		SecretKey: os.Getenv("SANDBOX0_ROOTFS_OBJECT_SECRET_KEY"),
	})
	if err != nil {
		return nil, fmt.Errorf("create RootFS object store: %w", err)
	}
	conditional, ok := objects.(objectstore.ConditionalStore)
	if !ok {
		return nil, fmt.Errorf("RootFS object store %s does not support conditional create", objects)
	}
	if err := objects.Create(); err != nil && !strings.Contains(strings.ToLower(err.Error()), "alreadyownedbyyou") {
		return nil, fmt.Errorf("create RootFS bucket: %w", err)
	}
	return rootfsmaterializer.New(rootfsmaterializer.Config{
		Store: store, Source: conditional,
		Publisher: rootfsblock.ObjectStorePublisher{Store: conditional},
		ScanLimit: config.scanLimit, Interval: config.interval,
	})
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
	runtimeGeneration, err := strconv.ParseInt(strings.TrimSpace(options.runtimeGeneration), 10, 64)
	if err != nil || runtimeGeneration <= 0 {
		return fmt.Errorf("runtime-generation must be a positive integer")
	}
	runtimeName := strings.TrimSpace(options.podUID)
	if options.stage != nil && strings.TrimSpace(options.stage.Identity.PodUID) != "" {
		runtimeName = strings.TrimSpace(options.stage.Identity.PodUID)
	}
	if err := store.UpsertSandbox(ctx, &sandboxstore.SandboxRecord{
		ID: options.sandboxID, TeamID: options.teamID, UserID: "user-1",
		TemplateID: "nomad-poc", TemplateName: "nomad-poc", TemplateNamespace: "default",
		DesiredState: sandboxstore.SandboxDesiredStateActive, CreatedAt: time.Now().UTC(),
		CurrentPodNamespace: "nomad", CurrentPodName: runtimeName, RuntimeGeneration: runtimeGeneration,
	}); err != nil {
		return fmt.Errorf("seed sandbox: %w", err)
	}
	var expectedFilesystemID, initialGenerationID string
	if options.resume {
		if options.stage == nil || strings.TrimSpace(options.filesystemID) == "" ||
			strings.TrimSpace(options.initialGenerationID) == "" || options.expectedWriterEpoch < 0 {
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
		if (len(parts) != 3 && len(parts) != 5) || strings.TrimSpace(parts[0]) == "" ||
			strings.TrimSpace(parts[1]) == "" || strings.TrimSpace(parts[2]) == "" {
			return nil, fmt.Errorf("allowed client %d must be commonName:nodeUID:podUID or commonName:nodeUID:podUID:clusterID:nodeID", index)
		}
		identity := writerauthority.CertIdentity{
			CommonName: strings.TrimSpace(parts[0]), NodeUID: strings.TrimSpace(parts[1]), PodUID: strings.TrimSpace(parts[2]),
		}
		if len(parts) == 5 {
			identity.ClusterID = strings.TrimSpace(parts[3])
			identity.NodeID = strings.TrimSpace(parts[4])
			if identity.ClusterID == "" || identity.NodeID == "" {
				return nil, fmt.Errorf("allowed client %d clusterID and nodeID must be non-empty", index)
			}
		}
		identities = append(identities, identity)
	}
	return identities, nil
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
