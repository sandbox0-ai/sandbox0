// Package coordinator handles distributed coordination for snapshot operations
// across multiple storage-proxy instances using PostgreSQL LISTEN/NOTIFY.
//
// Key concept: Each storage-proxy instance (identified by cluster_id + pod_id)
// is an independent coordination unit. This is a multi-replica problem, not
// a cross-cluster problem - even replicas within the same cluster need coordination.
package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	"github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"

	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
)

const (
	// PostgreSQL NOTIFY channels
	ChannelFlushRequest  = "snapshot_flush_request"
	ChannelFlushResponse = "snapshot_flush_response"
	ChannelWatchEvent    = "volume_watch_event"

	// Heartbeat settings
	HeartbeatInterval = 5 * time.Second
	HeartbeatTimeout  = 15 // seconds - mounts older than this are considered stale

	// Coordination settings
	FlushTimeout    = 30 * time.Second
	CleanupInterval = 60 * time.Second
)

// Errors
var (
	ErrFlushTimeout      = errors.New("flush coordination timed out")
	ErrFlushFailed       = errors.New("flush failed on one or more nodes")
	ErrCoordinatorClosed = errors.New("coordinator is closed")
)

// FlushRequest is the payload for flush request notifications
type FlushRequest struct {
	CoordID  string `json:"coord_id"`
	VolumeID string `json:"volume_id"`
}

// FlushResponse is the payload for flush response notifications
type FlushResponse struct {
	CoordID   string `json:"coord_id"`
	ClusterID string `json:"cluster_id"`
	PodID     string `json:"pod_id"`
	Success   bool   `json:"success"`
	Error     string `json:"error,omitempty"`
}

// VolumeProvider provides access to volume contexts for flushing
type VolumeProvider interface {
	GetVolume(volumeID string) (VolumeContext, error)
	ListVolumes() []string
}

// VolumeContext provides the VFS interface for flushing
type VolumeContext interface {
	FlushAll(path string) error
}

// Coordinator manages distributed coordination for snapshot operations
type Coordinator struct {
	mu sync.RWMutex

	pool       *pgxpool.Pool
	repo       db.CoordinatorRepository
	volMgr     VolumeProvider
	config     *config.StorageProxyConfig
	logger     *logrus.Logger
	clusterID  string
	podID      string
	k8sClient  kubernetes.Interface
	listenConn *pgxpool.Conn // Dedicated connection for LISTEN
	metrics    *obsmetrics.StorageProxyMetrics

	// Mounted volumes on this instance
	mountedVolumes map[string]struct{}

	// Event hub for watch events
	eventHub EventHub

	// Channels
	stopCh   chan struct{}
	doneCh   chan struct{}
	flushCh  chan *FlushRequest // Incoming flush requests
	started  bool
	stopping bool
}

// EventHub receives watch events for local subscribers.
type EventHub interface {
	Publish(event *pb.WatchEvent)
}

// NewCoordinator creates a new coordinator
func NewCoordinator(
	pool *pgxpool.Pool,
	repo db.CoordinatorRepository,
	volMgr VolumeProvider,
	eventHub EventHub,
	k8sClient kubernetes.Interface,
	cfg *config.StorageProxyConfig,
	logger *logrus.Logger,
	metrics *obsmetrics.StorageProxyMetrics,
) *Coordinator {
	podID, err := os.Hostname()
	if err != nil || podID == "" {
		podID = uuid.New().String()
	}

	return &Coordinator{
		pool:           pool,
		repo:           repo,
		volMgr:         volMgr,
		eventHub:       eventHub,
		k8sClient:      k8sClient,
		config:         cfg,
		logger:         logger,
		clusterID:      cfg.DefaultClusterId,
		podID:          podID,
		metrics:        metrics,
		mountedVolumes: make(map[string]struct{}),
		stopCh:         make(chan struct{}),
		doneCh:         make(chan struct{}),
		flushCh:        make(chan *FlushRequest, 100),
	}
}

// Start starts the coordinator background goroutines
func (c *Coordinator) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return nil
	}
	c.started = true
	c.mu.Unlock()

	c.logger.WithFields(logrus.Fields{
		"cluster_id": c.clusterID,
		"pod_id":     c.podID,
	}).Info("Starting coordinator")

	// Acquire a dedicated connection for LISTEN
	var err error
	c.listenConn, err = c.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire listen connection: %w", err)
	}

	// Subscribe to flush request channel
	_, err = c.listenConn.Exec(ctx, fmt.Sprintf("LISTEN %s", ChannelFlushRequest))
	if err != nil {
		c.listenConn.Release()
		return fmt.Errorf("LISTEN %s: %w", ChannelFlushRequest, err)
	}

	// Subscribe to flush response channel (for coordinators waiting on responses)
	_, err = c.listenConn.Exec(ctx, fmt.Sprintf("LISTEN %s", ChannelFlushResponse))
	if err != nil {
		c.listenConn.Release()
		return fmt.Errorf("LISTEN %s: %w", ChannelFlushResponse, err)
	}

	// Subscribe to watch event channel
	_, err = c.listenConn.Exec(ctx, fmt.Sprintf("LISTEN %s", ChannelWatchEvent))
	if err != nil {
		c.listenConn.Release()
		return fmt.Errorf("LISTEN %s: %w", ChannelWatchEvent, err)
	}

	// Start background goroutines
	go c.runNotificationListener(ctx)
	go c.runFlushHandler(ctx)
	go c.runHeartbeat(ctx)
	go c.runCleanup(ctx)

	c.logger.Info("Coordinator started")
	return nil
}

// Stop stops the coordinator
func (c *Coordinator) Stop(ctx context.Context) error {
	c.mu.Lock()
	if c.stopping || !c.started {
		c.mu.Unlock()
		return nil
	}
	c.stopping = true
	c.mu.Unlock()

	c.logger.Info("Stopping coordinator")

	// Signal all goroutines to stop
	close(c.stopCh)

	// Unregister all mounts
	c.mu.RLock()
	volumes := make([]string, 0, len(c.mountedVolumes))
	for volumeID := range c.mountedVolumes {
		volumes = append(volumes, volumeID)
	}
	c.mu.RUnlock()

	for _, volumeID := range volumes {
		if err := c.UnregisterMount(ctx, volumeID); err != nil {
			c.logger.WithError(err).WithField("volume_id", volumeID).Warn("Failed to unregister mount")
		}
	}

	// Release listen connection
	if c.listenConn != nil {
		c.listenConn.Release()
	}

	// Wait for goroutines to finish (with timeout)
	select {
	case <-c.doneCh:
	case <-time.After(5 * time.Second):
		c.logger.Warn("Coordinator stop timed out")
	}

	c.logger.Info("Coordinator stopped")
	return nil
}

// RegisterMount registers a volume mount for this instance
func (c *Coordinator) RegisterMount(ctx context.Context, volumeID string, options volume.MountOptions) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	metrics := c.metrics

	// Already registered locally
	if _, exists := c.mountedVolumes[volumeID]; exists {
		return nil
	}

	// Register in memory first to ensure local tracking even if DB fails
	// This is critical for multi-replica coordination - the volume is mounted
	// locally regardless of DB state, and we need to track it for heartbeats
	// and flush coordination.
	c.mountedVolumes[volumeID] = struct{}{}
	if metrics != nil {
		metrics.CoordinatorMountsActive.Inc()
	}

	normalizedOptions := options
	normalizedOptions.AccessMode = volume.NormalizeAccessMode(string(options.AccessMode))
	if normalizedOptions.OwnerKind == "" {
		normalizedOptions.OwnerKind = volume.OwnerKindStorageProxy
	}
	if normalizedOptions.OwnerPort == 0 && c.config != nil && c.config.HTTPPort > 0 {
		normalizedOptions.OwnerPort = c.config.HTTPPort
	}
	rawOptions, err := json.Marshal(normalizedOptions)
	if err != nil {
		return fmt.Errorf("marshal mount options: %w", err)
	}
	rawMsg := json.RawMessage(rawOptions)

	mount := &db.VolumeMount{
		ID:            uuid.New().String(),
		VolumeID:      volumeID,
		ClusterID:     c.clusterID,
		PodID:         c.podID,
		LastHeartbeat: time.Now(),
		MountedAt:     time.Now(),
		MountOptions:  &rawMsg,
	}

	if err := c.repo.CreateMount(ctx, mount); err != nil {
		if metrics != nil {
			metrics.CoordinatorMountRegistrations.WithLabelValues("failure").Inc()
		}
		// Note: We still track locally even if DB registration fails
		// This ensures heartbeat updates and flush coordination work
		c.logger.WithFields(logrus.Fields{
			"volume_id":  volumeID,
			"cluster_id": c.clusterID,
			"pod_id":     c.podID,
		}).WithError(err).Warn("Failed to register mount in DB, but tracking locally")
		return fmt.Errorf("create mount: %w", err)
	}

	if metrics != nil {
		metrics.CoordinatorMountRegistrations.WithLabelValues("success").Inc()
	}

	c.logger.WithFields(logrus.Fields{
		"volume_id":  volumeID,
		"cluster_id": c.clusterID,
		"pod_id":     c.podID,
	}).Debug("Registered mount")

	return nil
}

// ValidateMount enforces access mode constraints across storage-proxy instances.
func (c *Coordinator) ValidateMount(ctx context.Context, volumeID string, accessMode volume.AccessMode) error {
	accessMode = volume.NormalizeAccessMode(string(accessMode))

	heartbeatTimeout := c.config.HeartbeatTimeout
	if heartbeatTimeout == 0 {
		heartbeatTimeout = HeartbeatTimeout
	}

	mounts, err := c.repo.GetActiveMounts(ctx, volumeID, heartbeatTimeout)
	if err != nil {
		return fmt.Errorf("get active mounts: %w", err)
	}

	switch accessMode {
	case volume.AccessModeRWO:
		for _, mount := range mounts {
			if mount.ClusterID != c.clusterID || mount.PodID != c.podID {
				return fmt.Errorf("volume %s already mounted on another instance", volumeID)
			}
		}
		return nil
	case volume.AccessModeROX:
		for _, mount := range mounts {
			if !isMountROX(mount) {
				return fmt.Errorf("volume %s already mounted read-write", volumeID)
			}
		}
		return nil
	case volume.AccessModeRWX:
		return nil
	default:
		return fmt.Errorf("invalid access_mode %q", accessMode)
	}
}

func isMountROX(mount *db.VolumeMount) bool {
	if mount == nil || mount.MountOptions == nil {
		return false
	}

	var options volume.MountOptions
	if err := json.Unmarshal(*mount.MountOptions, &options); err != nil {
		return false
	}
	return volume.NormalizeAccessMode(string(options.AccessMode)) == volume.AccessModeROX
}

// UnregisterMount unregisters a volume mount for this instance
func (c *Coordinator) UnregisterMount(ctx context.Context, volumeID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	metrics := c.metrics

	// Not registered locally
	if _, exists := c.mountedVolumes[volumeID]; !exists {
		return nil
	}

	if err := c.repo.DeleteMount(ctx, volumeID, c.clusterID, c.podID); err != nil {
		if metrics != nil {
			metrics.CoordinatorMountUnregistrations.WithLabelValues("failure").Inc()
		}
		return fmt.Errorf("delete mount: %w", err)
	}

	delete(c.mountedVolumes, volumeID)
	if metrics != nil {
		metrics.CoordinatorMountsActive.Dec()
		metrics.CoordinatorMountUnregistrations.WithLabelValues("success").Inc()
	}

	c.logger.WithFields(logrus.Fields{
		"volume_id":  volumeID,
		"cluster_id": c.clusterID,
		"pod_id":     c.podID,
	}).Debug("Unregistered mount")

	return nil
}

// IsVolumeMounted checks if a volume is mounted on this instance
func (c *Coordinator) IsVolumeMounted(volumeID string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, exists := c.mountedVolumes[volumeID]
	return exists
}

// CoordinateFlush coordinates a flush across all instances that have the volume mounted.
// Returns when all instances have flushed or timeout occurs.
func (c *Coordinator) CoordinateFlush(ctx context.Context, volumeID string) error {
	startTime := time.Now()
	metrics := c.metrics
	if metrics != nil {
		metrics.CoordinatorActiveCoordinations.Inc()
		defer metrics.CoordinatorActiveCoordinations.Dec()
	}

	heartbeatTimeout := c.config.HeartbeatTimeout
	if heartbeatTimeout == 0 {
		heartbeatTimeout = HeartbeatTimeout
	}

	flushTimeout, _ := time.ParseDuration(c.config.FlushTimeout)
	if flushTimeout == 0 {
		flushTimeout = FlushTimeout
	}

	// 1. Get all active mounts for this volume
	mounts, err := c.repo.GetActiveMounts(ctx, volumeID, heartbeatTimeout)
	if err != nil {
		return fmt.Errorf("get active mounts: %w", err)
	}

	// 2. If no active mounts, nothing to coordinate
	if len(mounts) == 0 {
		c.logger.WithField("volume_id", volumeID).Debug("No active mounts, skipping coordination")
		if metrics != nil {
			metrics.CoordinatorFlushCoordinationsTotal.WithLabelValues("success").Inc()
			metrics.CoordinatorFlushCoordinationDuration.Observe(time.Since(startTime).Seconds())
		}
		return nil
	}

	c.logger.WithFields(logrus.Fields{
		"volume_id":      volumeID,
		"expected_nodes": len(mounts),
	}).Info("Starting flush coordination")

	// 3. Create coordination record
	coord := &db.SnapshotCoordination{
		ID:            uuid.New().String(),
		VolumeID:      volumeID,
		Status:        db.CoordStatusFlushing,
		ExpectedNodes: len(mounts),
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
		ExpiresAt:     time.Now().Add(flushTimeout),
	}

	if err := c.repo.CreateCoordination(ctx, coord); err != nil {
		return fmt.Errorf("create coordination: %w", err)
	}

	// 4. Send NOTIFY to all instances
	req := FlushRequest{
		CoordID:  coord.ID,
		VolumeID: volumeID,
	}
	payload, _ := json.Marshal(req)

	_, err = c.pool.Exec(ctx, fmt.Sprintf("SELECT pg_notify('%s', $1)", ChannelFlushRequest), string(payload))
	if err != nil {
		c.repo.UpdateCoordinationStatus(ctx, coord.ID, db.CoordStatusFailed)
		if metrics != nil {
			metrics.CoordinatorFlushCoordinationsTotal.WithLabelValues("failure").Inc()
			metrics.CoordinatorFlushCoordinationDuration.Observe(time.Since(startTime).Seconds())
		}
		return fmt.Errorf("send flush request: %w", err)
	}

	if metrics != nil {
		metrics.CoordinatorFlushRequestsSent.Inc()
	}

	// 5. Wait for all responses
	err = c.waitForFlushCompletion(ctx, coord)

	// Record metrics based on result
	duration := time.Since(startTime).Seconds()
	if metrics != nil {
		metrics.CoordinatorFlushCoordinationDuration.Observe(duration)
	}

	if err != nil {
		if metrics != nil {
			if errors.Is(err, ErrFlushTimeout) {
				metrics.CoordinatorFlushCoordinationsTotal.WithLabelValues("timeout").Inc()
			} else {
				metrics.CoordinatorFlushCoordinationsTotal.WithLabelValues("failure").Inc()
			}
		}
		return err
	}

	if metrics != nil {
		metrics.CoordinatorFlushCoordinationsTotal.WithLabelValues("success").Inc()
	}
	return nil
}

// waitForFlushCompletion waits for all nodes to complete flushing
func (c *Coordinator) waitForFlushCompletion(ctx context.Context, coord *db.SnapshotCoordination) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	flushTimeout, _ := time.ParseDuration(c.config.FlushTimeout)
	if flushTimeout == 0 {
		flushTimeout = FlushTimeout
	}

	deadline := time.After(flushTimeout)

	for {
		select {
		case <-ctx.Done():
			c.repo.UpdateCoordinationStatus(ctx, coord.ID, db.CoordStatusFailed)
			return ctx.Err()

		case <-deadline:
			c.repo.UpdateCoordinationStatus(ctx, coord.ID, db.CoordStatusTimeout)
			return ErrFlushTimeout

		case <-ticker.C:
			// Check completion status
			completed, err := c.repo.CountCompletedFlushes(ctx, coord.ID)
			if err != nil {
				c.logger.WithError(err).Warn("Failed to count completed flushes")
				continue
			}

			c.logger.WithFields(logrus.Fields{
				"coord_id":  coord.ID,
				"completed": completed,
				"expected":  coord.ExpectedNodes,
			}).Debug("Checking flush completion")

			if completed >= coord.ExpectedNodes {
				// All nodes responded, verify all succeeded
				responses, err := c.repo.GetFlushResponses(ctx, coord.ID)
				if err != nil {
					c.repo.UpdateCoordinationStatus(ctx, coord.ID, db.CoordStatusFailed)
					return fmt.Errorf("get flush responses: %w", err)
				}

				for _, resp := range responses {
					if !resp.Success {
						c.repo.UpdateCoordinationStatus(ctx, coord.ID, db.CoordStatusFailed)
						return fmt.Errorf("%w: %s/%s: %s", ErrFlushFailed, resp.ClusterID, resp.PodID, resp.ErrorMessage)
					}
				}

				c.repo.UpdateCoordinationStatus(ctx, coord.ID, db.CoordStatusCompleted)
				c.logger.WithFields(logrus.Fields{
					"coord_id":        coord.ID,
					"volume_id":       coord.VolumeID,
					"completed_nodes": completed,
				}).Info("Flush coordination completed successfully")
				return nil
			}
		}
	}
}

// runNotificationListener listens for PostgreSQL notifications
func (c *Coordinator) runNotificationListener(ctx context.Context) {
	metrics := c.metrics
	for {
		select {
		case <-c.stopCh:
			return
		default:
		}

		// Wait for notification with timeout
		notification, err := c.listenConn.Conn().WaitForNotification(ctx)
		if err != nil {
			if c.stopping {
				return
			}
			c.logger.WithError(err).Warn("Error waiting for notification")
			time.Sleep(100 * time.Millisecond)
			continue
		}

		switch notification.Channel {
		case ChannelFlushRequest:
			if metrics != nil {
				metrics.CoordinatorNotificationsReceived.WithLabelValues(ChannelFlushRequest).Inc()
			}
			var req FlushRequest
			if err := json.Unmarshal([]byte(notification.Payload), &req); err != nil {
				if metrics != nil {
					metrics.CoordinatorNotificationErrors.Inc()
				}
				c.logger.WithError(err).Warn("Failed to parse flush request")
				continue
			}

			// Non-blocking send to flush handler
			select {
			case c.flushCh <- &req:
			default:
				c.logger.Warn("Flush channel full, dropping request")
			}

		case ChannelFlushResponse:
			if metrics != nil {
				metrics.CoordinatorNotificationsReceived.WithLabelValues(ChannelFlushResponse).Inc()
			}
			// Response notifications are handled by polling in waitForFlushCompletion
			// This is just for logging/debugging
			c.logger.WithField("payload", notification.Payload).Debug("Received flush response notification")
		case ChannelWatchEvent:
			var event pb.WatchEvent
			if err := json.Unmarshal([]byte(notification.Payload), &event); err != nil {
				c.logger.WithError(err).Warn("Failed to parse watch event")
				continue
			}
			if event.OriginInstance == c.GetInstanceID() {
				continue
			}
			if !c.IsVolumeMounted(event.VolumeId) {
				continue
			}
			if c.eventHub != nil {
				c.eventHub.Publish(&event)
			}
		}
	}
}

// runFlushHandler handles incoming flush requests
func (c *Coordinator) runFlushHandler(ctx context.Context) {
	for {
		select {
		case <-c.stopCh:
			return
		case req := <-c.flushCh:
			c.handleFlushRequest(ctx, req)
		}
	}
}

// handleFlushRequest handles a single flush request
func (c *Coordinator) handleFlushRequest(ctx context.Context, req *FlushRequest) {
	metrics := c.metrics
	if metrics != nil {
		metrics.CoordinatorFlushRequestsReceived.Inc()
	}
	startTime := time.Now()

	logger := c.logger.WithFields(logrus.Fields{
		"coord_id":   req.CoordID,
		"volume_id":  req.VolumeID,
		"cluster_id": c.clusterID,
		"pod_id":     c.podID,
	})

	// Check if we have this volume mounted
	if !c.IsVolumeMounted(req.VolumeID) {
		// Not mounted on this instance, skip
		logger.Debug("Volume not mounted on this instance, skipping flush")
		return
	}

	logger.Info("Processing flush request")

	// Get volume context and flush
	var success bool
	var errMsg string

	volCtx, err := c.volMgr.GetVolume(req.VolumeID)
	if err != nil {
		success = false
		errMsg = err.Error()
		logger.WithError(err).Error("Failed to get volume context")
	} else {
		if err := volCtx.FlushAll(""); err != nil {
			success = false
			errMsg = err.Error()
			logger.WithError(err).Error("Failed to flush volume")
		} else {
			success = true
			logger.Info("Volume flushed successfully")
		}
	}

	// Record flush latency
	if metrics != nil {
		metrics.CoordinatorFlushLatency.Observe(time.Since(startTime).Seconds())
	}

	if success {
		if metrics != nil {
			metrics.CoordinatorFlushResponsesTotal.WithLabelValues("true").Inc()
		}
	} else {
		if metrics != nil {
			metrics.CoordinatorFlushResponsesTotal.WithLabelValues("false").Inc()
		}
	}

	// Record response in database
	now := time.Now()
	resp := &db.FlushResponse{
		ID:           uuid.New().String(),
		CoordID:      req.CoordID,
		ClusterID:    c.clusterID,
		PodID:        c.podID,
		Success:      success,
		FlushedAt:    &now,
		ErrorMessage: errMsg,
	}

	if err := c.repo.CreateFlushResponse(ctx, resp); err != nil {
		logger.WithError(err).Error("Failed to record flush response")
		return
	}

	// Send NOTIFY for faster completion detection
	notifyResp := FlushResponse{
		CoordID:   req.CoordID,
		ClusterID: c.clusterID,
		PodID:     c.podID,
		Success:   success,
		Error:     errMsg,
	}
	payload, _ := json.Marshal(notifyResp)

	_, err = c.pool.Exec(ctx, fmt.Sprintf("SELECT pg_notify('%s', $1)", ChannelFlushResponse), string(payload))
	if err != nil {
		logger.WithError(err).Warn("Failed to send flush response notification")
	}
}

// runHeartbeat updates heartbeats for all mounted volumes
func (c *Coordinator) runHeartbeat(ctx context.Context) {
	interval, _ := time.ParseDuration(c.config.HeartbeatInterval)
	if interval == 0 {
		interval = HeartbeatInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.updateHeartbeats(ctx)
		}
	}
}

// updateHeartbeats updates heartbeat for all mounted volumes
func (c *Coordinator) updateHeartbeats(ctx context.Context) {
	metrics := c.metrics
	c.mu.RLock()
	volumes := make([]string, 0, len(c.mountedVolumes))
	for volumeID := range c.mountedVolumes {
		volumes = append(volumes, volumeID)
	}
	c.mu.RUnlock()

	for _, volumeID := range volumes {
		if err := c.repo.UpdateMountHeartbeat(ctx, volumeID, c.clusterID, c.podID); err != nil {
			if metrics != nil {
				metrics.CoordinatorHeartbeatErrors.Inc()
			}
			// If update fails (e.g., mount record deleted), re-register
			if strings.Contains(err.Error(), "not found") {
				c.logger.WithField("volume_id", volumeID).Debug("Mount record not found, re-registering")
				c.mu.Lock()
				delete(c.mountedVolumes, volumeID)
				c.mu.Unlock()

				// Try to re-register
				if err := c.RegisterMount(ctx, volumeID, volume.MountOptions{}); err != nil {
					c.logger.WithError(err).WithField("volume_id", volumeID).Warn("Failed to re-register mount")
				}
			} else {
				c.logger.WithError(err).WithField("volume_id", volumeID).Warn("Failed to update heartbeat")
			}
		} else {
			if metrics != nil {
				metrics.CoordinatorHeartbeatsTotal.Inc()
			}
		}
	}
}

// runCleanup periodically cleans up stale mounts
func (c *Coordinator) runCleanup(ctx context.Context) {
	metrics := c.metrics
	cleanupInterval, _ := time.ParseDuration(c.config.CleanupInterval)
	if cleanupInterval == 0 {
		cleanupInterval = CleanupInterval
	}
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()

	heartbeatTimeout := c.config.HeartbeatTimeout
	if heartbeatTimeout == 0 {
		heartbeatTimeout = HeartbeatTimeout
	}

	for {
		select {
		case <-c.stopCh:
			close(c.doneCh)
			return
		case <-ticker.C:
			mounts, err := c.repo.GetAllMounts(ctx)
			if err != nil {
				c.logger.WithError(err).Warn("Failed to list mounts for orphan cleanup")
			} else {
				var orphaned int64
				seenPods := make(map[string]struct{}, len(mounts))
				for _, mount := range mounts {
					podKey := mount.ClusterID + "/" + mount.PodID
					if _, ok := seenPods[podKey]; ok {
						continue
					}
					seenPods[podKey] = struct{}{}
					if c.podExists(ctx, mount.PodID) {
						continue
					}
					if err := c.repo.DeleteMountByPodID(ctx, mount.ClusterID, mount.PodID); err != nil {
						c.logger.WithError(err).WithFields(logrus.Fields{
							"cluster_id": mount.ClusterID,
							"pod_id":     mount.PodID,
						}).Warn("Failed to delete orphaned mounts by pod")
						continue
					}
					orphaned++
				}
				if orphaned > 0 {
					c.logger.WithField("orphaned_pods", orphaned).Info("Cleaned up orphaned mounts by pod lookup")
				}
			}

			deleted, err := c.repo.DeleteStaleMounts(ctx, heartbeatTimeout)
			if err != nil {
				c.logger.WithError(err).Warn("Failed to cleanup stale mounts")
			} else if deleted > 0 {
				if metrics != nil {
					metrics.CoordinatorStaleMountsCleaned.Add(float64(deleted))
				}
				c.logger.WithField("deleted_count", deleted).Info("Cleaned up stale mounts")
			}
		}
	}
}

func (c *Coordinator) podExists(ctx context.Context, podID string) bool {
	if c.k8sClient == nil || podID == "" {
		return true
	}

	if strings.Contains(podID, "/") {
		parts := strings.SplitN(podID, "/", 2)
		_, err := c.k8sClient.CoreV1().Pods(parts[0]).Get(ctx, parts[1], metav1.GetOptions{})
		if err == nil {
			return true
		}
		return !apierrors.IsNotFound(err)
	}

	pods, err := c.k8sClient.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: "metadata.name=" + podID,
		Limit:         1,
	})
	if err != nil {
		c.logger.WithError(err).WithField("pod_id", podID).Warn("Failed to check pod existence")
		return true
	}
	return len(pods.Items) > 0
}

// GetInstanceID returns the unique identifier for this instance
func (c *Coordinator) GetInstanceID() string {
	return fmt.Sprintf("%s/%s", c.clusterID, c.podID)
}

// Publish broadcasts a watch event locally and across replicas.
func (c *Coordinator) Publish(ctx context.Context, event *pb.WatchEvent) {
	if c == nil || event == nil {
		return
	}
	clone := &pb.WatchEvent{
		VolumeId:        event.VolumeId,
		EventType:       event.EventType,
		Path:            event.Path,
		OldPath:         event.OldPath,
		Inode:           event.Inode,
		TimestampUnix:   event.TimestampUnix,
		OriginInstance:  event.OriginInstance,
		OriginSandboxId: event.OriginSandboxId,
		InvalidateId:    event.InvalidateId,
	}
	if clone.OriginInstance == "" {
		clone.OriginInstance = c.GetInstanceID()
	}
	if clone.TimestampUnix == 0 {
		clone.TimestampUnix = time.Now().Unix()
	}
	if c.eventHub != nil {
		c.eventHub.Publish(clone)
	}
	payload, _ := json.Marshal(clone)
	if _, err := c.pool.Exec(ctx, fmt.Sprintf("SELECT pg_notify('%s', $1)", ChannelWatchEvent), string(payload)); err != nil {
		c.logger.WithError(err).Warn("Failed to send watch event notification")
	}
}
