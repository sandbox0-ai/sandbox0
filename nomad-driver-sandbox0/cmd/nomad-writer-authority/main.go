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
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauth"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauthority"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/rootfsmaterializer"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotreconciler"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotterminal"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
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
	mode := flag.String("mode", "serve", "serve, issue, fork, snapshot, restore, rebase-request, rebase-publish, rollback, cancel, or stage-digest")
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
	materializerMinPackBytes := flag.Int64("materializer-min-pack-bytes", rootfsmaterializer.DefaultMinPackBytes, "minimum tenant-lane payload before shared pack publication")
	materializerMaxDelay := flag.Duration("materializer-max-delay", rootfsmaterializer.DefaultMaxDelay, "maximum age before a bounded small-pack flush")
	materializerForcedFlushes := flag.Int("materializer-forced-flushes-per-run", rootfsmaterializer.DefaultForcedFlushes, "maximum age-forced tenant-lane flushes per pass")
	materializerGarbageInterval := flag.Duration("materializer-garbage-interval", rootfsmaterializer.DefaultGarbageInterval, "interval between durable materialization garbage passes")
	materializerUploadingStale := flag.Duration("materializer-uploading-stale", rootfsmaterializer.DefaultUploadingStale, "stale age before a changed-locator upload batch can be abandoned")
	materializerTerminalRetention := flag.Duration("materializer-terminal-retention", rootfsmaterializer.DefaultTerminalRetention, "commit-response-loss retention for terminal materialization batches")
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
	rollbackTTL := flag.Duration("rollback-ttl", 24*time.Hour, "restore/rebase-request: old-head rollback retention")
	targetBaseArtifact := flag.String("target-base-artifact", "", "rebase-request: target Base artifact digest")
	rollbackExpiresAt := flag.String("rollback-expires-at", "", "rebase-publish: exact RFC3339Nano deadline returned by rebase-request")
	generationFile := flag.String("generation-file", "", "rebase-publish: prepared RootFSGeneration JSON path")
	expectedBaseArtifact := flag.String("expected-base-artifact", "", "rebase-publish: source Base artifact digest")
	healthCheckHex := flag.String("health-check-digest", "", "rebase-publish: 32-byte hexadecimal health proof")
	workerClusterID := flag.String("worker-cluster-id", "", "rebase: selected worker cluster ID")
	workerNodeID := flag.String("worker-node-id", "", "rebase: selected worker node ID")
	workerNodeUID := flag.String("worker-node-uid", "", "rebase: selected worker node UID")
	workerProofHex := flag.String("worker-proof-digest", "", "rebase-publish: 32-byte hexadecimal worker result proof")
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
			minPackBytes: *materializerMinPackBytes, maxDelay: *materializerMaxDelay,
			forcedFlushesPerRun: *materializerForcedFlushes,
			garbageInterval:     *materializerGarbageInterval,
			uploadingStale:      *materializerUploadingStale,
			terminalRetention:   *materializerTerminalRetention,
		})
		if err != nil {
			fatal("serve: create materializer: %v", err)
		}
		if err := serve(ctx, store, materializer, *address, *certFile, *keyFile, *clientCAFile,
			*leaseTTL, *renewalGrace, *runtimeSlotHeartbeatTTL, *allowedClients,
			runtimeslotterminal.Config{
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
	case "rebase-request":
		if *rollbackTTL <= 0 {
			fatal("rebase-request: rollback-ttl must be positive")
		}
		candidate, err := store.RequestNomadPausedRebase(ctx, &sandboxstore.NomadPausedRebaseRequest{
			OperationID: *operationID, SandboxID: *sandboxID, ExpectedTeamID: *teamID,
			TargetBaseArtifactDigest: *targetBaseArtifact,
			RollbackExpiresAt:        time.Now().UTC().Add(*rollbackTTL),
			WorkerClusterID:          *workerClusterID, WorkerNodeID: *workerNodeID, WorkerNodeUID: *workerNodeUID,
		})
		if err != nil {
			fatal("rebase-request: %v", err)
		}
		encodeResult("rebase-request", candidate)
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
		workerProof, err := hex.DecodeString(strings.TrimSpace(*workerProofHex))
		if err != nil {
			fatal("rebase-publish: decode worker-proof-digest: %v", err)
		}
		expiresAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(*rollbackExpiresAt))
		if err != nil {
			fatal("rebase-publish: decode rollback-expires-at: %v", err)
		}
		rebaseRequest := &sandboxstore.PublishPausedRootFSRebaseRequest{
			SandboxID: *sandboxID, TeamID: *teamID, OperationID: *operationID,
			ExpectedSourceGenerationID: *initialGenerationID,
			ExpectedBaseArtifactDigest: *expectedBaseArtifact,
			Generation:                 &generation,
			HealthCheckDigest:          healthDigest,
			RollbackExpiresAt:          expiresAt,
			WorkerClusterID:            *workerClusterID, WorkerNodeID: *workerNodeID, WorkerNodeUID: *workerNodeUID,
			WorkerProofDigest: workerProof,
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
	terminalConfig runtimeslotterminal.Config,
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
	authority, err := nodeauthority.New(nodeauthority.Config{
		Store: store, Address: address, CertFile: certFile, KeyFile: keyFile,
		ClientCAFile: clientCAFile, Identities: identities,
		WriterLeaseTTL: leaseTTL, WriterRenewalGrace: renewalGrace,
		RuntimeSlotHeartbeatTTL: runtimeSlotHeartbeatTTL, Terminal: terminalConfig,
	})
	if err != nil {
		return fmt.Errorf("create node authority: %w", err)
	}
	serviceCtx, cancelService := context.WithCancel(ctx)
	defer cancelService()
	go materializer.Run(serviceCtx, func(result rootfsmaterializer.Result, err error) {
		if err != nil {
			log.Printf("RootFS materializer: scanned=%d materialized=%d deferred=%d batches=%d failed=%d abandoned=%d purged=%d enqueued=%d error=%v",
				result.Scanned, result.Materialized, result.Deferred, result.Batches, result.Failed,
				result.Abandoned, result.Purged, result.Enqueued, err)
		} else if result.Materialized > 0 {
			log.Printf("RootFS materializer: scanned=%d materialized=%d deferred=%d batches=%d abandoned=%d purged=%d enqueued=%d",
				result.Scanned, result.Materialized, result.Deferred, result.Batches,
				result.Abandoned, result.Purged, result.Enqueued)
		}
	})
	var terminalErr <-chan error
	var terminalDone <-chan struct{}
	if authority.TerminalEnabled() {
		errorsCh := make(chan error, 1)
		doneCh := make(chan struct{})
		terminalErr = errorsCh
		terminalDone = doneCh
		go func() {
			defer close(doneCh)
			errorsCh <- authority.RunTerminal(serviceCtx, logRuntimeSlotTerminalPass)
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
	go func() { errCh <- authority.RunServer(serviceCtx) }()
	log.Printf("Nomad RootFS writer authority listening on %s", address)
	select {
	case err := <-errCh:
		if err == nil || errors.Is(err, context.Canceled) {
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

type materializerConfig struct {
	objectType          string
	bucket              string
	region              string
	endpoint            string
	interval            time.Duration
	scanLimit           int
	minPackBytes        int64
	maxDelay            time.Duration
	forcedFlushesPerRun int
	garbageInterval     time.Duration
	uploadingStale      time.Duration
	terminalRetention   time.Duration
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
	conditional, ok := objects.(objectstore.ContextConditionalStore)
	if !ok || !objectstore.SupportsContextConditionalCreate(objects) {
		return nil, fmt.Errorf("RootFS object store %s does not support contextual conditional access", objects)
	}
	if err := objects.Create(); err != nil && !strings.Contains(strings.ToLower(err.Error()), "alreadyownedbyyou") {
		return nil, fmt.Errorf("create RootFS bucket: %w", err)
	}
	return rootfsmaterializer.New(rootfsmaterializer.Config{
		Store: store, Source: conditional,
		Publisher: rootfsblock.ObjectStorePublisher{Store: conditional},
		ScanLimit: config.scanLimit, Interval: config.interval,
		MinPackBytes: config.minPackBytes, MaxDelay: config.maxDelay,
		ForcedFlushesPerRun: config.forcedFlushesPerRun,
		GarbageInterval:     config.garbageInterval, UploadingStale: config.uploadingStale,
		TerminalRetention: config.terminalRetention,
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
			FormatGeneration: generation.FormatGeneration,
			Platform:         sandboxstore.RootFSArtifactPlatform{OS: runtime.GOOS, Architecture: runtime.GOARCH},
			Descriptor:       generation.Descriptor,
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

func parseAllowedClients(raw string) ([]nodeauth.CertificateIdentity, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("at least one allowed client is required")
	}
	var identities []nodeauth.CertificateIdentity
	for index, item := range strings.Split(raw, ",") {
		parts := strings.Split(strings.TrimSpace(item), ":")
		if (len(parts) != 3 && len(parts) != 5) || strings.TrimSpace(parts[0]) == "" ||
			strings.TrimSpace(parts[1]) == "" || strings.TrimSpace(parts[2]) == "" {
			return nil, fmt.Errorf("allowed client %d must be commonName:nodeUID:podUID or commonName:nodeUID:podUID:clusterID:nodeID", index)
		}
		identity := nodeauth.CertificateIdentity{
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
