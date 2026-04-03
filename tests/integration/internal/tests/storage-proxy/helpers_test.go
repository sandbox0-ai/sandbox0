package storageproxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	storagemigrations "github.com/sandbox0-ai/sandbox0/storage-proxy/migrations"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	storagehttp "github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/http"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/pathnorm"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/snapshot"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volsync"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	"github.com/sirupsen/logrus"
)

type storageProxySyncTestEnv struct {
	ctx         context.Context
	repo        *db.Repository
	sync        *volsync.Service
	snapshotMgr *integrationSnapshotManager
	server      *storagehttp.Server
}

func newStorageProxySyncTestEnv(t *testing.T) *storageProxySyncTestEnv {
	t.Helper()

	ctx := context.Background()
	dbURL := requireIntegrationDatabaseURL(t)
	schema := fmt.Sprintf("storage_proxy_sync_test_%s", strings.ReplaceAll(uuid.NewString(), "-", ""))

	pool, err := dbpool.New(ctx, dbpool.Options{
		DatabaseURL: dbURL,
		Schema:      schema,
	})
	if err != nil {
		t.Fatalf("connect integration database: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", pgx.Identifier{schema}.Sanitize()))
		pool.Close()
	})

	if err := migrate.Up(ctx, pool, ".", migrate.WithBaseFS(storagemigrations.FS), migrate.WithSchema(schema)); err != nil {
		t.Fatalf("migrate storage-proxy schema: %v", err)
	}

	repo := db.NewRepository(pool)
	syncSvc := volsync.NewService(repo, logrus.New())
	replayStore, err := object.CreateStorage("mem", "", "", "", "")
	if err != nil {
		t.Fatalf("create replay payload storage: %v", err)
	}
	syncSvc.SetReplayPayloadStore(volsync.NewObjectReplayPayloadStore(replayStore))
	snapshotMgr := newIntegrationSnapshotManager()
	server := storagehttp.NewServer(logrus.New(), &config.StorageProxyConfig{}, nil, repo, nil, "test-region", nil, snapshotMgr, syncSvc, nil, nil, nil, nil)

	return &storageProxySyncTestEnv{
		ctx:         ctx,
		repo:        repo,
		sync:        syncSvc,
		snapshotMgr: snapshotMgr,
		server:      server,
	}
}

func newStorageProxyTestServer(env *storageProxySyncTestEnv) *storagehttp.Server {
	return storagehttp.NewServer(logrus.New(), &config.StorageProxyConfig{}, nil, env.repo, nil, "test-region", nil, env.snapshotMgr, env.sync, nil, nil, nil, nil)
}

func requireIntegrationDatabaseURL(t *testing.T) string {
	t.Helper()

	dbURL := os.Getenv("INTEGRATION_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("TEST_DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("missing INTEGRATION_DATABASE_URL or TEST_DATABASE_URL")
	}
	return dbURL
}

func (e *storageProxySyncTestEnv) createVolume(t *testing.T, volumeID string) {
	t.Helper()

	now := time.Now().UTC()
	if err := e.repo.CreateSandboxVolume(e.ctx, &db.SandboxVolume{
		ID:         volumeID,
		TeamID:     "team-1",
		UserID:     "user-1",
		AccessMode: "rwx",
		CreatedAt:  now,
		UpdatedAt:  now,
	}); err != nil {
		t.Fatalf("CreateSandboxVolume() error = %v", err)
	}
}

func (e *storageProxySyncTestEnv) newAuthedRequest(t *testing.T, method, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(string(body)))
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	recorder := httptest.NewRecorder()
	e.server.ServeHTTP(recorder, req)
	return recorder
}

func newJSONBody(t *testing.T, payload any) []byte {
	t.Helper()

	body, err := jsonMarshal(payload)
	if err != nil {
		t.Fatalf("marshal request body: %v", err)
	}
	return body
}

func jsonMarshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

type integrationSnapshotManager struct {
	snapshots            map[string]*db.Snapshot
	exportBody           []byte
	lastCreate           *snapshot.CreateSnapshotRequest
	lastExport           *snapshot.ExportSnapshotRequest
	lastCompatibility    *snapshot.ListSnapshotCompatibilityIssuesRequest
	compatibilityIssues  []pathnorm.CompatibilityIssue
	deletedSnapshotIDs   []string
	nextSnapshotSequence int
}

func newIntegrationSnapshotManager() *integrationSnapshotManager {
	return &integrationSnapshotManager{
		snapshots: make(map[string]*db.Snapshot),
	}
}

func (m *integrationSnapshotManager) CreateSnapshotSimple(_ context.Context, req *snapshot.CreateSnapshotRequest) (*db.Snapshot, error) {
	m.lastCreate = req
	m.nextSnapshotSequence++
	snap := &db.Snapshot{
		ID:          fmt.Sprintf("snap-%d", m.nextSnapshotSequence),
		VolumeID:    req.VolumeID,
		TeamID:      req.TeamID,
		UserID:      req.UserID,
		Name:        req.Name,
		Description: req.Description,
		CreatedAt:   time.Now().UTC(),
	}
	m.snapshots[snap.ID] = snap
	return snap, nil
}

func (m *integrationSnapshotManager) ListSnapshots(_ context.Context, volumeID, teamID string) ([]*db.Snapshot, error) {
	out := make([]*db.Snapshot, 0)
	for _, snap := range m.snapshots {
		if snap.VolumeID == volumeID && snap.TeamID == teamID {
			clone := *snap
			out = append(out, &clone)
		}
	}
	return out, nil
}

func (m *integrationSnapshotManager) GetSnapshot(_ context.Context, volumeID, snapshotID, teamID string) (*db.Snapshot, error) {
	snap, ok := m.snapshots[snapshotID]
	if !ok || snap.VolumeID != volumeID || snap.TeamID != teamID {
		return nil, snapshot.ErrSnapshotNotFound
	}
	clone := *snap
	return &clone, nil
}

func (m *integrationSnapshotManager) ListSnapshotCasefoldCollisions(_ context.Context, _ *snapshot.ListSnapshotCasefoldCollisionsRequest) ([]snapshot.SnapshotCasefoldCollision, error) {
	return nil, nil
}

func (m *integrationSnapshotManager) ListSnapshotCompatibilityIssues(_ context.Context, req *snapshot.ListSnapshotCompatibilityIssuesRequest) ([]pathnorm.CompatibilityIssue, error) {
	m.lastCompatibility = req
	return m.compatibilityIssues, nil
}

func (m *integrationSnapshotManager) ExportSnapshotArchive(_ context.Context, req *snapshot.ExportSnapshotRequest, w io.Writer) error {
	m.lastExport = req
	body := m.exportBody
	if len(body) == 0 {
		body = []byte("integration-bootstrap-archive")
	}
	_, err := w.Write(body)
	return err
}

func (m *integrationSnapshotManager) RestoreSnapshot(_ context.Context, _ *snapshot.RestoreSnapshotRequest) error {
	return nil
}

func (m *integrationSnapshotManager) DeleteSnapshot(_ context.Context, volumeID, snapshotID, teamID string) error {
	m.deletedSnapshotIDs = append(m.deletedSnapshotIDs, snapshotID)
	delete(m.snapshots, snapshotID)
	return nil
}

func (m *integrationSnapshotManager) ForkVolume(_ context.Context, _ *snapshot.ForkVolumeRequest) (*db.SandboxVolume, error) {
	return nil, nil
}

type integrationMountedVolumeManager struct {
	volumes map[string]*volume.VolumeContext
}

func (m *integrationMountedVolumeManager) MountVolume(_ context.Context, _, volumeID, _ string, _ *volume.VolumeConfig, _ volume.AccessMode) (string, time.Time, error) {
	if _, ok := m.volumes[volumeID]; !ok {
		return "", time.Time{}, fmt.Errorf("volume %s not found", volumeID)
	}
	return "session-test", time.Now().UTC(), nil
}

func (m *integrationMountedVolumeManager) UnmountVolume(_ context.Context, _, _ string) error {
	return nil
}

func (m *integrationMountedVolumeManager) AckInvalidate(_, _, _ string, _ bool, _ string) error {
	return nil
}

func (m *integrationMountedVolumeManager) GetVolume(volumeID string) (*volume.VolumeContext, error) {
	vol, ok := m.volumes[volumeID]
	if !ok {
		return nil, fmt.Errorf("volume %s not mounted", volumeID)
	}
	return vol, nil
}

func (m *integrationMountedVolumeManager) TrackVolumeSession(_, _, _ string) {}

func newMountedIntegrationVolumeContext(t *testing.T, volumeID, teamID string) *volume.VolumeContext {
	t.Helper()

	metaConf := meta.DefaultConf()
	metaConf.MountPoint = "/test"

	metaClient := meta.NewClient("memkv://"+uuid.NewString(), metaConf)
	format := &meta.Format{
		Name:        "test",
		UUID:        uuid.NewString(),
		Storage:     "mem",
		BlockSize:   4096,
		Compression: "none",
		DirStats:    true,
	}
	if err := metaClient.Init(format, true); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	if err := metaClient.NewSession(true); err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}

	chunkConf := chunk.Config{
		BlockSize:  format.BlockSize * 1024,
		Compress:   format.Compression,
		MaxUpload:  2,
		BufferSize: 8 << 20,
		CacheSize:  8 << 20,
		CacheDir:   "memory",
	}
	blob, err := object.CreateStorage("mem", "", "", "", "")
	if err != nil {
		t.Fatalf("CreateStorage() error = %v", err)
	}
	registry := prometheus.NewRegistry()
	store := chunk.NewCachedStore(blob, chunkConf, registry)
	vfsConf := &vfs.Config{
		Meta:            metaConf,
		Format:          *format,
		Version:         "test",
		Chunk:           &chunkConf,
		FuseOpts:        &vfs.FuseOptions{},
		AttrTimeout:     time.Second,
		EntryTimeout:    time.Second,
		DirEntryTimeout: time.Second,
	}

	t.Cleanup(func() {
		_ = metaClient.CloseSession()
		_ = metaClient.Shutdown()
	})

	return &volume.VolumeContext{
		VolumeID:  volumeID,
		TeamID:    teamID,
		Meta:      metaClient,
		Store:     store,
		VFS:       vfs.NewVFS(vfsConf, metaClient, store, registry, registry),
		MountedAt: time.Now(),
		RootInode: meta.RootInode,
		RootPath:  "/",
	}
}

func readMountedFile(t *testing.T, volCtx *volume.VolumeContext, logicalPath string) []byte {
	t.Helper()

	parentIno := meta.RootInode
	parts := strings.Split(strings.Trim(strings.TrimSpace(logicalPath), "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		t.Fatalf("invalid logical path %q", logicalPath)
	}

	var inode meta.Ino
	attr := &meta.Attr{}
	for _, part := range parts {
		if errno := volCtx.Meta.Lookup(meta.Background(), parentIno, part, &inode, attr, false); errno != 0 {
			t.Fatalf("Lookup(%q) error = %v", logicalPath, syscall.Errno(errno))
		}
		parentIno = inode
	}

	vfsCtx := vfs.NewLogContext(meta.Background())
	_, handleID, errno := volCtx.VFS.Open(vfsCtx, inode, 0)
	if errno != 0 {
		t.Fatalf("Open(%q) error = %v", logicalPath, syscall.Errno(errno))
	}
	defer volCtx.VFS.Release(vfsCtx, inode, handleID)

	data := make([]byte, attr.Length)
	if attr.Length == 0 {
		return data
	}
	n, errno := volCtx.VFS.Read(vfsCtx, inode, data, 0, handleID)
	if errno != 0 {
		t.Fatalf("Read(%q) error = %v", logicalPath, syscall.Errno(errno))
	}
	return data[:n]
}
