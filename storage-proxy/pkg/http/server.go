package http

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/auth"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/notify"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/pathnorm"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/snapshot"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volsync"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/client-go/kubernetes"
)

type volumeRepository interface {
	WithTx(ctx context.Context, fn func(pgx.Tx) error) error
	CreateSandboxVolumeTx(ctx context.Context, tx pgx.Tx, volume *db.SandboxVolume) error
	CreateSandboxVolumeOwnerTx(ctx context.Context, tx pgx.Tx, owner *db.SandboxVolumeOwner) error
	ListSandboxVolumesByTeam(ctx context.Context, teamID string) ([]*db.SandboxVolume, error)
	ListOwnedSandboxVolumes(ctx context.Context, clusterID string, cleanupRequested *bool) ([]*db.OwnedSandboxVolume, error)
	GetSandboxVolume(ctx context.Context, id string) (*db.SandboxVolume, error)
	GetSandboxVolumeOwner(ctx context.Context, volumeID string) (*db.SandboxVolumeOwner, error)
	GetOwnedSandboxVolumeByOwner(ctx context.Context, clusterID, sandboxID, purpose string) (*db.OwnedSandboxVolume, error)
	GetActiveMounts(ctx context.Context, volumeID string, heartbeatTimeout int) ([]*db.VolumeMount, error)
	DeleteMount(ctx context.Context, volumeID, clusterID, podID string) error
	DeleteSandboxVolumeTx(ctx context.Context, tx pgx.Tx, id string) error
	MarkOwnedSandboxVolumesForCleanup(ctx context.Context, clusterID, sandboxID, reason string) (int64, error)
	MarkOwnedSandboxVolumeCleanupAttempt(ctx context.Context, volumeID string, cleanupErr error) error
}

type meteringWriter interface {
	AppendEventTx(ctx context.Context, tx pgx.Tx, event *meteringpkg.Event) error
	UpsertProducerWatermarkTx(ctx context.Context, tx pgx.Tx, producer string, regionID string, completeBefore time.Time) error
}

type snapshotManager interface {
	CreateSnapshotSimple(ctx context.Context, req *snapshot.CreateSnapshotRequest) (*db.Snapshot, error)
	ListSnapshots(ctx context.Context, volumeID, teamID string) ([]*db.Snapshot, error)
	GetSnapshot(ctx context.Context, volumeID, snapshotID, teamID string) (*db.Snapshot, error)
	ListSnapshotCasefoldCollisions(ctx context.Context, req *snapshot.ListSnapshotCasefoldCollisionsRequest) ([]snapshot.SnapshotCasefoldCollision, error)
	ListSnapshotCompatibilityIssues(ctx context.Context, req *snapshot.ListSnapshotCompatibilityIssuesRequest) ([]pathnorm.CompatibilityIssue, error)
	ExportSnapshotArchive(ctx context.Context, req *snapshot.ExportSnapshotRequest, w io.Writer) error
	RestoreSnapshot(ctx context.Context, req *snapshot.RestoreSnapshotRequest) error
	DeleteSnapshot(ctx context.Context, volumeID, snapshotID, teamID string) error
	ForkVolume(ctx context.Context, req *snapshot.ForkVolumeRequest) (*db.SandboxVolume, error)
}

type syncManager interface {
	UpsertReplica(ctx context.Context, req *volsync.UpsertReplicaRequest) (*volsync.ReplicaEnvelope, error)
	GetReplica(ctx context.Context, volumeID, teamID, replicaID string) (*volsync.ReplicaEnvelope, error)
	GetHead(ctx context.Context, volumeID, teamID string) (int64, error)
	ListChanges(ctx context.Context, req *volsync.ListChangesRequest) (*volsync.ListChangesResponse, error)
	OpenReplayPayload(ctx context.Context, req *volsync.OpenReplayPayloadRequest) (io.ReadCloser, error)
	AppendReplicaChanges(ctx context.Context, req *volsync.AppendChangesRequest) (*volsync.AppendChangesResponse, error)
	UpdateReplicaCursor(ctx context.Context, req *volsync.UpdateCursorRequest) (*volsync.ReplicaEnvelope, error)
	ListConflicts(ctx context.Context, req *volsync.ListConflictsRequest) (*volsync.ListConflictsResponse, error)
	ResolveConflict(ctx context.Context, req *volsync.ResolveConflictRequest) (*db.SyncConflict, error)
}

type volumeMutationBarrier interface {
	WithExclusive(ctx context.Context, volumeID string, fn func(context.Context) error) error
}

type volumeMountManager interface {
	GetVolume(volumeID string) (*volume.VolumeContext, error)
	UnmountVolume(ctx context.Context, volumeID, sessionID string) error
	AcquireDirectVolumeFileMount(ctx context.Context, volumeID string, mountFn func(context.Context) (string, error)) (func(), error)
	CleanupIdleDirectVolumeFileMount(ctx context.Context, volumeID string) (bool, error)
}

type directVolumeMountSyncer interface {
	SyncDirectVolumeFileMount(ctx context.Context, volumeID string) error
}

type volumeFileRPC interface {
	MountVolume(ctx context.Context, req *pb.MountVolumeRequest) (*pb.MountVolumeResponse, error)
	GetAttr(ctx context.Context, req *pb.GetAttrRequest) (*pb.GetAttrResponse, error)
	Lookup(ctx context.Context, req *pb.LookupRequest) (*pb.NodeResponse, error)
	Open(ctx context.Context, req *pb.OpenRequest) (*pb.OpenResponse, error)
	Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error)
	Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error)
	Create(ctx context.Context, req *pb.CreateRequest) (*pb.NodeResponse, error)
	Mkdir(ctx context.Context, req *pb.MkdirRequest) (*pb.NodeResponse, error)
	Unlink(ctx context.Context, req *pb.UnlinkRequest) (*pb.Empty, error)
	Rmdir(ctx context.Context, req *pb.RmdirRequest) (*pb.Empty, error)
	ReadDir(ctx context.Context, req *pb.ReadDirRequest) (*pb.ReadDirResponse, error)
	OpenDir(ctx context.Context, req *pb.OpenDirRequest) (*pb.OpenDirResponse, error)
	ReleaseDir(ctx context.Context, req *pb.ReleaseDirRequest) (*pb.Empty, error)
	Rename(ctx context.Context, req *pb.RenameRequest) (*pb.Empty, error)
	Release(ctx context.Context, req *pb.ReleaseRequest) (*pb.Empty, error)
}

type volumeEventHub interface {
	Subscribe(req *pb.WatchRequest) (string, <-chan *pb.WatchEvent, func())
}

// Server provides HTTP management API for health checks and metrics
type Server struct {
	logger        *logrus.Logger
	mux           *http.ServeMux
	cfg           *config.StorageProxyConfig
	repo          volumeRepository
	meteringRepo  meteringWriter
	regionID      string
	authenticator *auth.HTTPAuthenticator
	snapshotMgr   snapshotManager
	syncMgr       syncManager
	barrier       volumeMutationBarrier
	volMgr        volumeMountManager
	fileRPC       volumeFileRPC
	eventHub      volumeEventHub
	podResolver   volumeFilePodResolver
	selfPodID     string
	selfClusterID string
}

// NewServer creates a new HTTP server
func NewServer(logger *logrus.Logger, cfg *config.StorageProxyConfig, k8sClient kubernetes.Interface, repo volumeRepository, meteringRepo meteringWriter, regionID string, authenticator *auth.HTTPAuthenticator, snapshotMgr snapshotManager, syncMgr syncManager, barrier volumeMutationBarrier, volMgr volumeMountManager, fileRPC volumeFileRPC, eventHub *notify.Hub) *Server {
	selfPodID, err := os.Hostname()
	if err != nil {
		selfPodID = ""
	}

	s := &Server{
		logger:        logger,
		mux:           http.NewServeMux(),
		cfg:           cfg,
		repo:          repo,
		meteringRepo:  meteringRepo,
		regionID:      regionID,
		authenticator: authenticator,
		snapshotMgr:   snapshotMgr,
		syncMgr:       syncMgr,
		barrier:       barrier,
		volMgr:        volMgr,
		fileRPC:       fileRPC,
		eventHub:      eventHub,
		podResolver:   newKubernetesVolumeFilePodResolver(logger, k8sClient, cfg),
		selfPodID:     selfPodID,
	}
	if cfg != nil {
		s.selfClusterID = cfg.DefaultClusterId
	}

	// Register handlers
	s.mux.HandleFunc("/healthz", s.handleHealth)
	s.mux.HandleFunc("/readyz", s.handleReady)
	s.mux.Handle("/metrics", promhttp.Handler())

	// Sandbox Volume handlers
	s.mux.HandleFunc("POST /sandboxvolumes", s.createSandboxVolume)
	s.mux.HandleFunc("GET /sandboxvolumes", s.listSandboxVolumes)
	s.mux.HandleFunc("GET /sandboxvolumes/{id}", s.getSandboxVolume)
	s.mux.HandleFunc("DELETE /sandboxvolumes/{id}", s.deleteSandboxVolume)
	s.mux.HandleFunc("POST /sandboxvolumes/{id}/fork", s.forkVolume)
	s.mux.HandleFunc("GET /sandboxvolumes/{id}/files", s.handleVolumeFileOperation)
	s.mux.HandleFunc("POST /sandboxvolumes/{id}/files", s.handleVolumeFileOperation)
	s.mux.HandleFunc("DELETE /sandboxvolumes/{id}/files", s.handleVolumeFileOperation)
	s.mux.HandleFunc("GET /sandboxvolumes/{id}/files/stat", s.handleVolumeFileStat)
	s.mux.HandleFunc("GET /sandboxvolumes/{id}/files/list", s.handleVolumeFileList)
	s.mux.HandleFunc("POST /sandboxvolumes/{id}/files/move", s.handleVolumeFileMove)
	s.mux.HandleFunc("GET /sandboxvolumes/{id}/files/watch", s.handleVolumeFileWatch)

	// Internal manager-owned SandboxVolume lifecycle handlers.
	s.mux.HandleFunc("POST /internal/v1/sandboxvolumes/owned", s.createOwnedSandboxVolume)
	s.mux.HandleFunc("GET /internal/v1/sandboxvolumes/owned", s.listOwnedSandboxVolumes)
	s.mux.HandleFunc("PUT /internal/v1/sandboxvolumes/owned/cleanup", s.markOwnedSandboxVolumesForCleanup)
	s.mux.HandleFunc("PUT /internal/v1/sandboxvolumes/owned/{id}/cleanup-attempt", s.markOwnedSandboxVolumeCleanupAttempt)
	s.mux.HandleFunc("DELETE /internal/v1/sandboxvolumes/owned/{id}", s.deleteOwnedSandboxVolume)

	// Snapshot handlers
	s.mux.HandleFunc("POST /sandboxvolumes/{volume_id}/snapshots", s.createSnapshot)
	s.mux.HandleFunc("GET /sandboxvolumes/{volume_id}/snapshots", s.listSnapshots)
	s.mux.HandleFunc("GET /sandboxvolumes/{volume_id}/snapshots/{snapshot_id}", s.getSnapshot)
	s.mux.HandleFunc("POST /sandboxvolumes/{volume_id}/snapshots/{snapshot_id}/restore", s.restoreSnapshot)
	s.mux.HandleFunc("DELETE /sandboxvolumes/{volume_id}/snapshots/{snapshot_id}", s.deleteSnapshot)

	// Volume sync handlers
	s.mux.HandleFunc("PUT /sandboxvolumes/{id}/sync/replicas/{replica_id}", s.upsertSyncReplica)
	s.mux.HandleFunc("GET /sandboxvolumes/{id}/sync/replicas/{replica_id}", s.getSyncReplica)
	s.mux.HandleFunc("POST /sandboxvolumes/{id}/sync/bootstrap", s.createSyncBootstrap)
	s.mux.HandleFunc("GET /sandboxvolumes/{id}/sync/bootstrap/archive", s.downloadSyncBootstrapArchive)
	s.mux.HandleFunc("GET /sandboxvolumes/{id}/sync/changes", s.listSyncChanges)
	s.mux.HandleFunc("GET /sandboxvolumes/{id}/sync/replay-payload", s.downloadSyncReplayPayload)
	s.mux.HandleFunc("GET /sandboxvolumes/{id}/sync/conflicts", s.listSyncConflicts)
	s.mux.HandleFunc("PUT /sandboxvolumes/{id}/sync/conflicts/{conflict_id}", s.resolveSyncConflict)
	s.mux.HandleFunc("POST /sandboxvolumes/{id}/sync/replicas/{replica_id}/changes", s.appendSyncChanges)
	s.mux.HandleFunc("PUT /sandboxvolumes/{id}/sync/replicas/{replica_id}/cursor", s.updateSyncReplicaCursor)

	return s
}

// ServeHTTP implements http.Handler
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Skip logging for health check, readiness check and metrics
	if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" || r.URL.Path == "/metrics" {
		s.serve(w, r)
		return
	}

	start := time.Now()
	wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

	s.serve(wrapped, r)

	fields := logrus.Fields{
		"method":   r.Method,
		"path":     r.URL.Path,
		"status":   wrapped.statusCode,
		"duration": time.Since(start),
		"remote":   r.RemoteAddr,
	}

	spanCtx := trace.SpanFromContext(r.Context()).SpanContext()
	if spanCtx.IsValid() {
		fields["trace_id"] = spanCtx.TraceID().String()
		fields["span_id"] = spanCtx.SpanID().String()
	}

	s.logger.WithFields(fields).Info("HTTP request")
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	if s.authenticator != nil {
		s.authenticator.HealthCheckMiddleware(s.mux).ServeHTTP(w, r)
	} else {
		s.mux.ServeHTTP(w, r)
	}
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Unwrap() http.ResponseWriter {
	return rw.ResponseWriter
}

func (rw *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := rw.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, http.ErrNotSupported
	}
	return hijacker.Hijack()
}

func (rw *responseWriter) Flush() {
	if flusher, ok := rw.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (rw *responseWriter) Push(target string, opts *http.PushOptions) error {
	if pusher, ok := rw.ResponseWriter.(http.Pusher); ok {
		return pusher.Push(target, opts)
	}
	return http.ErrNotSupported
}

// handleHealth handles health check requests
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	_ = spec.WriteSuccess(w, http.StatusOK, map[string]any{
		"status":    "healthy",
		"timestamp": time.Now().Unix(),
	})
}

// handleReady handles readiness check requests
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	_ = spec.WriteSuccess(w, http.StatusOK, map[string]any{
		"status":    "ready",
		"timestamp": time.Now().Unix(),
	})
}

func (s *Server) appendMeteringEventTx(ctx context.Context, tx pgx.Tx, event *meteringpkg.Event) error {
	if s.meteringRepo == nil || event == nil {
		return nil
	}
	if err := s.meteringRepo.AppendEventTx(ctx, tx, event); err != nil {
		return err
	}
	return s.meteringRepo.UpsertProducerWatermarkTx(ctx, tx, event.Producer, event.RegionID, event.OccurredAt)
}
