package coordinator

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/infra/infra-operator/api/config"
	"github.com/sandbox0-ai/infra/storage-proxy/pkg/db"
	"github.com/sirupsen/logrus"
)

// Mock implementations using simple Go structs
type MockRepository struct {
	createMountFunc              func(ctx context.Context, mount *db.VolumeMount) error
	updateMountHeartbeatFunc     func(ctx context.Context, volumeID, clusterID, podID string) error
	deleteMountFunc              func(ctx context.Context, volumeID, clusterID, podID string) error
	getActiveMountsFunc          func(ctx context.Context, volumeID string, timeout int) ([]*db.VolumeMount, error)
	deleteStaleMountsFunc        func(ctx context.Context, timeout int) (int64, error)
	createCoordinationFunc       func(ctx context.Context, coord *db.SnapshotCoordination) error
	getCoordinationFunc          func(ctx context.Context, id string) (*db.SnapshotCoordination, error)
	updateCoordinationStatusFunc func(ctx context.Context, id, status string) error
	createFlushResponseFunc      func(ctx context.Context, resp *db.FlushResponse) error
	countCompletedFlushesFunc    func(ctx context.Context, coordID string) (int, error)
	getFlushResponsesFunc        func(ctx context.Context, coordID string) ([]*db.FlushResponse, error)
}

func (m *MockRepository) CreateMount(ctx context.Context, mount *db.VolumeMount) error {
	if m.createMountFunc != nil {
		return m.createMountFunc(ctx, mount)
	}
	return nil
}

func (m *MockRepository) UpdateMountHeartbeat(ctx context.Context, volumeID, clusterID, podID string) error {
	if m.updateMountHeartbeatFunc != nil {
		return m.updateMountHeartbeatFunc(ctx, volumeID, clusterID, podID)
	}
	return nil
}

func (m *MockRepository) DeleteMount(ctx context.Context, volumeID, clusterID, podID string) error {
	if m.deleteMountFunc != nil {
		return m.deleteMountFunc(ctx, volumeID, clusterID, podID)
	}
	return nil
}

func (m *MockRepository) GetActiveMounts(ctx context.Context, volumeID string, timeout int) ([]*db.VolumeMount, error) {
	if m.getActiveMountsFunc != nil {
		return m.getActiveMountsFunc(ctx, volumeID, timeout)
	}
	return nil, nil
}

func (m *MockRepository) DeleteStaleMounts(ctx context.Context, timeout int) (int64, error) {
	if m.deleteStaleMountsFunc != nil {
		return m.deleteStaleMountsFunc(ctx, timeout)
	}
	return 0, nil
}

func (m *MockRepository) CreateCoordination(ctx context.Context, coord *db.SnapshotCoordination) error {
	if m.createCoordinationFunc != nil {
		return m.createCoordinationFunc(ctx, coord)
	}
	return nil
}

func (m *MockRepository) GetCoordination(ctx context.Context, id string) (*db.SnapshotCoordination, error) {
	if m.getCoordinationFunc != nil {
		return m.getCoordinationFunc(ctx, id)
	}
	return nil, nil
}

func (m *MockRepository) UpdateCoordinationStatus(ctx context.Context, id, status string) error {
	if m.updateCoordinationStatusFunc != nil {
		return m.updateCoordinationStatusFunc(ctx, id, status)
	}
	return nil
}

func (m *MockRepository) CreateFlushResponse(ctx context.Context, resp *db.FlushResponse) error {
	if m.createFlushResponseFunc != nil {
		return m.createFlushResponseFunc(ctx, resp)
	}
	return nil
}

func (m *MockRepository) CountCompletedFlushes(ctx context.Context, coordID string) (int, error) {
	if m.countCompletedFlushesFunc != nil {
		return m.countCompletedFlushesFunc(ctx, coordID)
	}
	return 0, nil
}

func (m *MockRepository) GetFlushResponses(ctx context.Context, coordID string) ([]*db.FlushResponse, error) {
	if m.getFlushResponsesFunc != nil {
		return m.getFlushResponsesFunc(ctx, coordID)
	}
	return nil, nil
}

type MockVolumeContext struct {
	flushAllFunc func(path string) error
}

func (m *MockVolumeContext) FlushAll(path string) error {
	if m.flushAllFunc != nil {
		return m.flushAllFunc(path)
	}
	return nil
}

type MockVolumeProvider struct {
	mu            sync.RWMutex
	volumes       map[string]VolumeContext
	getVolumeFunc func(volumeID string) (VolumeContext, error)
}

func (m *MockVolumeProvider) GetVolume(volumeID string) (VolumeContext, error) {
	if m.getVolumeFunc != nil {
		return m.getVolumeFunc(volumeID)
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if vol, ok := m.volumes[volumeID]; ok {
		return vol, nil
	}
	return nil, errors.New("volume not found")
}

func (m *MockVolumeProvider) ListVolumes() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	volumes := make([]string, 0, len(m.volumes))
	for vid := range m.volumes {
		volumes = append(volumes, vid)
	}
	return volumes
}

func (m *MockVolumeProvider) AddVolume(volumeID string, ctx VolumeContext) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.volumes == nil {
		m.volumes = make(map[string]VolumeContext)
	}
	m.volumes[volumeID] = ctx
}

func (m *MockVolumeProvider) RemoveVolume(volumeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.volumes, volumeID)
}

// Test helpers
func newTestCoordinator(t *testing.T) (*Coordinator, *MockRepository, *MockVolumeProvider) {
	cfg := &config.StorageProxyConfig{
		DefaultClusterId: "test-cluster",
	}
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel) // Suppress logs during tests

	mockRepo := &MockRepository{}
	mockVolProvider := &MockVolumeProvider{
		volumes: make(map[string]VolumeContext),
	}

	coord := &Coordinator{
		repo:           mockRepo,
		volMgr:         mockVolProvider,
		config:         cfg,
		logger:         logger,
		clusterID:      cfg.DefaultClusterId,
		podID:          "test-pod-" + uuid.New().String()[:8],
		stopCh:         make(chan struct{}),
		doneCh:         make(chan struct{}),
		flushCh:        make(chan *FlushRequest, 100),
		mountedVolumes: make(map[string]struct{}),
	}

	return coord, mockRepo, mockVolProvider
}

// Test Mount Registration
func TestRegisterMount_Success(t *testing.T) {
	coord, mockRepo, _ := newTestCoordinator(t)
	ctx := context.Background()
	volumeID := "test-volume-1"

	var capturedMount *db.VolumeMount
	mockRepo.createMountFunc = func(ctx context.Context, mount *db.VolumeMount) error {
		capturedMount = mount
		return nil
	}

	err := coord.RegisterMount(ctx, volumeID)
	if err != nil {
		t.Fatalf("RegisterMount failed: %v", err)
	}

	if capturedMount == nil {
		t.Fatal("CreateMount was not called")
	}

	if capturedMount.VolumeID != volumeID {
		t.Errorf("Expected volumeID %s, got %s", volumeID, capturedMount.VolumeID)
	}

	if capturedMount.ClusterID != coord.clusterID {
		t.Errorf("Expected clusterID %s, got %s", coord.clusterID, capturedMount.ClusterID)
	}

	if capturedMount.PodID != coord.podID {
		t.Errorf("Expected podID %s, got %s", coord.podID, capturedMount.PodID)
	}

	// Check internal state
	coord.mu.RLock()
	_, exists := coord.mountedVolumes[volumeID]
	coord.mu.RUnlock()

	if !exists {
		t.Error("Volume not registered in mountedVolumes map")
	}
}

func TestRegisterMount_DatabaseError(t *testing.T) {
	coord, mockRepo, _ := newTestCoordinator(t)
	ctx := context.Background()
	volumeID := "test-volume-1"

	dbError := errors.New("database connection failed")
	mockRepo.createMountFunc = func(ctx context.Context, mount *db.VolumeMount) error {
		return dbError
	}

	err := coord.RegisterMount(ctx, volumeID)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	// Should still track internally even if DB fails
	coord.mu.RLock()
	_, exists := coord.mountedVolumes[volumeID]
	coord.mu.RUnlock()

	if !exists {
		t.Error("Volume should be tracked internally even on DB error")
	}
}

func TestRegisterMount_AlreadyRegistered(t *testing.T) {
	coord, mockRepo, _ := newTestCoordinator(t)
	ctx := context.Background()
	volumeID := "test-volume-1"

	var callCount int32
	mockRepo.createMountFunc = func(ctx context.Context, mount *db.VolumeMount) error {
		atomic.AddInt32(&callCount, 1)
		return nil
	}

	// Register twice
	_ = coord.RegisterMount(ctx, volumeID)
	_ = coord.RegisterMount(ctx, volumeID)

	// Should only call CreateMount once
	if atomic.LoadInt32(&callCount) != 1 {
		t.Errorf("Expected CreateMount to be called once, got %d times", callCount)
	}
}

// Test Mount Unregistration
func TestUnregisterMount_Success(t *testing.T) {
	coord, mockRepo, _ := newTestCoordinator(t)
	ctx := context.Background()
	volumeID := "test-volume-1"

	// First register
	coord.mu.Lock()
	coord.mountedVolumes[volumeID] = struct{}{}
	coord.mu.Unlock()

	var deleted bool
	mockRepo.deleteMountFunc = func(ctx context.Context, vid, cid, pid string) error {
		if vid == volumeID && cid == coord.clusterID && pid == coord.podID {
			deleted = true
		}
		return nil
	}

	err := coord.UnregisterMount(ctx, volumeID)
	if err != nil {
		t.Fatalf("UnregisterMount failed: %v", err)
	}

	if !deleted {
		t.Error("DeleteMount was not called with correct parameters")
	}

	coord.mu.RLock()
	_, exists := coord.mountedVolumes[volumeID]
	coord.mu.RUnlock()

	if exists {
		t.Error("Volume still exists in mountedVolumes map")
	}
}

func TestUnregisterMount_NotRegistered(t *testing.T) {
	coord, mockRepo, _ := newTestCoordinator(t)
	ctx := context.Background()
	volumeID := "not-registered"

	var callCount int32
	mockRepo.deleteMountFunc = func(ctx context.Context, vid, cid, pid string) error {
		atomic.AddInt32(&callCount, 1)
		return nil
	}

	err := coord.UnregisterMount(ctx, volumeID)
	if err != nil {
		t.Fatalf("UnregisterMount failed: %v", err)
	}

	// Should not call DeleteMount if not registered
	if atomic.LoadInt32(&callCount) != 0 {
		t.Error("DeleteMount should not be called for unregistered volume")
	}
}

// Test Heartbeat Updates
func TestUpdateHeartbeats(t *testing.T) {
	coord, mockRepo, mockVolProvider := newTestCoordinator(t)
	ctx := context.Background()

	volume1 := "vol-1"
	volume2 := "vol-2"

	// Add volumes to provider
	mockVolProvider.AddVolume(volume1, &MockVolumeContext{})
	mockVolProvider.AddVolume(volume2, &MockVolumeContext{})

	// Register volumes
	coord.mu.Lock()
	coord.mountedVolumes[volume1] = struct{}{}
	coord.mountedVolumes[volume2] = struct{}{}
	coord.mu.Unlock()

	var updatedVolumes []string
	mockRepo.updateMountHeartbeatFunc = func(ctx context.Context, volumeID, clusterID, podID string) error {
		updatedVolumes = append(updatedVolumes, volumeID)
		return nil
	}

	coord.updateHeartbeats(ctx)

	if len(updatedVolumes) != 2 {
		t.Errorf("Expected 2 heartbeat updates, got %d", len(updatedVolumes))
	}
}

func TestUpdateHeartbeats_PartialFailure(t *testing.T) {
	coord, mockRepo, mockVolProvider := newTestCoordinator(t)
	ctx := context.Background()

	volume1 := "vol-1"
	volume2 := "vol-2"

	mockVolProvider.AddVolume(volume1, &MockVolumeContext{})
	mockVolProvider.AddVolume(volume2, &MockVolumeContext{})

	coord.mu.Lock()
	coord.mountedVolumes[volume1] = struct{}{}
	coord.mountedVolumes[volume2] = struct{}{}
	coord.mu.Unlock()

	mockRepo.updateMountHeartbeatFunc = func(ctx context.Context, volumeID, clusterID, podID string) error {
		if volumeID == volume2 {
			return errors.New("heartbeat failed")
		}
		return nil
	}

	// Should not panic or fail
	coord.updateHeartbeats(ctx)
}

// Test Flush Request Handling
func TestHandleFlushRequest_Success(t *testing.T) {
	t.Skip("Requires real pgxpool for NOTIFY - use integration tests")
	// This test requires a real PostgreSQL connection for pg_notify.
	// In production, handleFlushRequest is triggered via LISTEN/NOTIFY.
	// Unit tests should focus on individual components; integration tests
	// should verify the full coordination flow with a real database.
}

func TestHandleFlushRequest_VolumeNotMounted(t *testing.T) {
	t.Skip("Requires real pgxpool for NOTIFY - use integration tests")
}

func TestHandleFlushRequest_FlushError(t *testing.T) {
	t.Skip("Requires real pgxpool for NOTIFY - use integration tests")
}

// Test CoordinateFlush
func TestCoordinateFlush_NoMounts(t *testing.T) {
	coord, mockRepo, _ := newTestCoordinator(t)
	ctx := context.Background()
	volumeID := "unmounted-volume"

	// Return empty mounts list
	mockRepo.getActiveMountsFunc = func(ctx context.Context, vid string, timeout int) ([]*db.VolumeMount, error) {
		return []*db.VolumeMount{}, nil
	}

	err := coord.CoordinateFlush(ctx, volumeID)
	if err != nil {
		t.Fatalf("CoordinateFlush should succeed with no mounts: %v", err)
	}
}

func TestCoordinateFlush_AllSuccess(t *testing.T) {
	t.Skip("Requires real pgxpool for NOTIFY - use integration tests")
	// CoordinateFlush requires PostgreSQL LISTEN/NOTIFY for multi-replica coordination.
	// This should be tested in integration tests with a real database connection.
}

func TestCoordinateFlush_PartialFailure(t *testing.T) {
	t.Skip("Requires real pgxpool for NOTIFY - use integration tests")
	// CoordinateFlush requires PostgreSQL LISTEN/NOTIFY for multi-replica coordination.
	// This should be tested in integration tests with a real database connection.
}

// Test Concurrent Operations
func TestConcurrentRegisterUnregister(t *testing.T) {
	coord, mockRepo, _ := newTestCoordinator(t)
	ctx := context.Background()

	numOps := 20
	var wg sync.WaitGroup

	mockRepo.createMountFunc = func(ctx context.Context, mount *db.VolumeMount) error {
		return nil
	}

	mockRepo.deleteMountFunc = func(ctx context.Context, vid, cid, pid string) error {
		return nil
	}

	// Concurrent register operations
	wg.Add(numOps)
	for i := 0; i < numOps; i++ {
		volumeID := uuid.New().String()
		go func(vid string) {
			defer wg.Done()
			_ = coord.RegisterMount(ctx, vid)
		}(volumeID)
	}

	wg.Wait()

	// Concurrent unregister operations
	coord.mu.RLock()
	volumes := make([]string, 0, len(coord.mountedVolumes))
	for vid := range coord.mountedVolumes {
		volumes = append(volumes, vid)
	}
	coord.mu.RUnlock()

	wg.Add(len(volumes))
	for _, vid := range volumes {
		go func(volumeID string) {
			defer wg.Done()
			_ = coord.UnregisterMount(ctx, volumeID)
		}(vid)
	}

	wg.Wait()

	// All should be unregistered
	coord.mu.RLock()
	remaining := len(coord.mountedVolumes)
	coord.mu.RUnlock()

	if remaining != 0 {
		t.Errorf("Expected 0 mounted volumes, got %d", remaining)
	}
}

// Test IsVolumeMounted
func TestIsVolumeMounted(t *testing.T) {
	coord, _, _ := newTestCoordinator(t)

	volumeID := "test-volume"

	// Not mounted initially
	if coord.IsVolumeMounted(volumeID) {
		t.Error("Volume should not be mounted initially")
	}

	// Register volume
	coord.mu.Lock()
	coord.mountedVolumes[volumeID] = struct{}{}
	coord.mu.Unlock()

	// Should be mounted now
	if !coord.IsVolumeMounted(volumeID) {
		t.Error("Volume should be mounted after registration")
	}

	// Unregister volume
	coord.mu.Lock()
	delete(coord.mountedVolumes, volumeID)
	coord.mu.Unlock()

	// Should not be mounted
	if coord.IsVolumeMounted(volumeID) {
		t.Error("Volume should not be mounted after unregistration")
	}
}

// Test GetInstanceID
func TestGetInstanceID(t *testing.T) {
	coord, _, _ := newTestCoordinator(t)

	instanceID := coord.GetInstanceID()
	expected := coord.clusterID + "/" + coord.podID

	if instanceID != expected {
		t.Errorf("Expected instance ID %q, got %q", expected, instanceID)
	}
}

// Benchmark tests
func BenchmarkRegisterMount(b *testing.B) {
	coord, mockRepo, _ := newTestCoordinator(&testing.T{})
	ctx := context.Background()

	mockRepo.createMountFunc = func(ctx context.Context, mount *db.VolumeMount) error {
		return nil
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		volumeID := "vol-" + uuid.New().String()
		_ = coord.RegisterMount(ctx, volumeID)
	}
}

func BenchmarkUnregisterMount(b *testing.B) {
	coord, mockRepo, _ := newTestCoordinator(&testing.T{})
	ctx := context.Background()

	mockRepo.deleteMountFunc = func(ctx context.Context, vid, cid, pid string) error {
		return nil
	}

	// Pre-register volumes
	volumes := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		volumes[i] = "vol-" + uuid.New().String()
		coord.mu.Lock()
		coord.mountedVolumes[volumes[i]] = struct{}{}
		coord.mu.Unlock()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = coord.UnregisterMount(ctx, volumes[i])
	}
}

func BenchmarkIsVolumeMounted(b *testing.B) {
	coord, _, _ := newTestCoordinator(&testing.T{})
	volumeID := "test-volume"

	coord.mu.Lock()
	coord.mountedVolumes[volumeID] = struct{}{}
	coord.mu.Unlock()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = coord.IsVolumeMounted(volumeID)
	}
}
