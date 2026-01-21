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
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/infra/infra-operator/api/config"
	"github.com/sandbox0-ai/infra/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/infra/storage-proxy/pkg/metrics"
	"github.com/sirupsen/logrus"
)

const (
	// PostgreSQL NOTIFY channels
	ChannelFlushRequest  = "snapshot_flush_request"
	ChannelFlushResponse = "snapshot_flush_response"

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
	listenConn *pgxpool.Conn // Dedicated connection for LISTEN

	// Mounted volumes on this instance
	mountedVolumes map[string]struct{}

	// Channels
	stopCh   chan struct{}
	doneCh   chan struct{}
	flushCh  chan *FlushRequest // Incoming flush requests
	started  bool
	stopping bool
}

// NewCoordinator creates a new coordinator
func NewCoordinator(
	pool *pgxpool.Pool,
	repo db.CoordinatorRepository,
	volMgr VolumeProvider,
	cfg *config.StorageProxyConfig,
	logger *logrus.Logger,
) *Coordinator {
	return &Coordinator{
		pool:           pool,
		repo:           repo,
		volMgr:         volMgr,
		config:         cfg,
		logger:         logger,
		clusterID:      cfg.DefaultClusterId,
		podID:          uuid.New().String(),
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
func (c *Coordinator) RegisterMount(ctx context.Context, volumeID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Already registered locally
	if _, exists := c.mountedVolumes[volumeID]; exists {
		return nil
	}

	// Register in memory first to ensure local tracking even if DB fails
	// This is critical for multi-replica coordination - the volume is mounted
	// locally regardless of DB state, and we need to track it for heartbeats
	// and flush coordination.
	c.mountedVolumes[volumeID] = struct{}{}
	metrics.CoordinatorMountsActive.Inc()

	mount := &db.VolumeMount{
		ID:            uuid.New().String(),
		VolumeID:      volumeID,
		ClusterID:     c.clusterID,
		PodID:         c.podID,
		LastHeartbeat: time.Now(),
		MountedAt:     time.Now(),
	}

	if err := c.repo.CreateMount(ctx, mount); err != nil {
		metrics.CoordinatorMountRegistrations.WithLabelValues("failure").Inc()
		// Note: We still track locally even if DB registration fails
		// This ensures heartbeat updates and flush coordination work
		c.logger.WithFields(logrus.Fields{
			"volume_id":  volumeID,
			"cluster_id": c.clusterID,
			"pod_id":     c.podID,
		}).WithError(err).Warn("Failed to register mount in DB, but tracking locally")
		return fmt.Errorf("create mount: %w", err)
	}

	metrics.CoordinatorMountRegistrations.WithLabelValues("success").Inc()

	c.logger.WithFields(logrus.Fields{
		"volume_id":  volumeID,
		"cluster_id": c.clusterID,
		"pod_id":     c.podID,
	}).Debug("Registered mount")

	return nil
}

// UnregisterMount unregisters a volume mount for this instance
func (c *Coordinator) UnregisterMount(ctx context.Context, volumeID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Not registered locally
	if _, exists := c.mountedVolumes[volumeID]; !exists {
		return nil
	}

	if err := c.repo.DeleteMount(ctx, volumeID, c.clusterID, c.podID); err != nil {
		metrics.CoordinatorMountUnregistrations.WithLabelValues("failure").Inc()
		return fmt.Errorf("delete mount: %w", err)
	}

	delete(c.mountedVolumes, volumeID)
	metrics.CoordinatorMountsActive.Dec()
	metrics.CoordinatorMountUnregistrations.WithLabelValues("success").Inc()

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
	metrics.CoordinatorActiveCoordinations.Inc()
	defer metrics.CoordinatorActiveCoordinations.Dec()

	// 1. Get all active mounts for this volume
	mounts, err := c.repo.GetActiveMounts(ctx, volumeID, HeartbeatTimeout)
	if err != nil {
		return fmt.Errorf("get active mounts: %w", err)
	}

	// 2. If no active mounts, nothing to coordinate
	if len(mounts) == 0 {
		c.logger.WithField("volume_id", volumeID).Debug("No active mounts, skipping coordination")
		metrics.CoordinatorFlushCoordinationsTotal.WithLabelValues("success").Inc()
		metrics.CoordinatorFlushCoordinationDuration.Observe(time.Since(startTime).Seconds())
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
		ExpiresAt:     time.Now().Add(FlushTimeout),
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
		metrics.CoordinatorFlushCoordinationsTotal.WithLabelValues("failure").Inc()
		metrics.CoordinatorFlushCoordinationDuration.Observe(time.Since(startTime).Seconds())
		return fmt.Errorf("send flush request: %w", err)
	}

	metrics.CoordinatorFlushRequestsSent.Inc()

	// 5. Wait for all responses
	err = c.waitForFlushCompletion(ctx, coord)

	// Record metrics based on result
	duration := time.Since(startTime).Seconds()
	metrics.CoordinatorFlushCoordinationDuration.Observe(duration)

	if err != nil {
		if errors.Is(err, ErrFlushTimeout) {
			metrics.CoordinatorFlushCoordinationsTotal.WithLabelValues("timeout").Inc()
		} else {
			metrics.CoordinatorFlushCoordinationsTotal.WithLabelValues("failure").Inc()
		}
		return err
	}

	metrics.CoordinatorFlushCoordinationsTotal.WithLabelValues("success").Inc()
	return nil
}

// waitForFlushCompletion waits for all nodes to complete flushing
func (c *Coordinator) waitForFlushCompletion(ctx context.Context, coord *db.SnapshotCoordination) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	deadline := time.After(FlushTimeout)

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
			metrics.CoordinatorNotificationsReceived.WithLabelValues(ChannelFlushRequest).Inc()
			var req FlushRequest
			if err := json.Unmarshal([]byte(notification.Payload), &req); err != nil {
				metrics.CoordinatorNotificationErrors.Inc()
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
			metrics.CoordinatorNotificationsReceived.WithLabelValues(ChannelFlushResponse).Inc()
			// Response notifications are handled by polling in waitForFlushCompletion
			// This is just for logging/debugging
			c.logger.WithField("payload", notification.Payload).Debug("Received flush response notification")
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
	metrics.CoordinatorFlushRequestsReceived.Inc()
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
	metrics.CoordinatorFlushLatency.Observe(time.Since(startTime).Seconds())

	if success {
		metrics.CoordinatorFlushResponsesTotal.WithLabelValues("true").Inc()
	} else {
		metrics.CoordinatorFlushResponsesTotal.WithLabelValues("false").Inc()
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
	ticker := time.NewTicker(HeartbeatInterval)
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
	c.mu.RLock()
	volumes := make([]string, 0, len(c.mountedVolumes))
	for volumeID := range c.mountedVolumes {
		volumes = append(volumes, volumeID)
	}
	c.mu.RUnlock()

	for _, volumeID := range volumes {
		if err := c.repo.UpdateMountHeartbeat(ctx, volumeID, c.clusterID, c.podID); err != nil {
			metrics.CoordinatorHeartbeatErrors.Inc()
			// If update fails (e.g., mount record deleted), re-register
			if strings.Contains(err.Error(), "not found") {
				c.logger.WithField("volume_id", volumeID).Debug("Mount record not found, re-registering")
				c.mu.Lock()
				delete(c.mountedVolumes, volumeID)
				c.mu.Unlock()

				// Try to re-register
				if err := c.RegisterMount(ctx, volumeID); err != nil {
					c.logger.WithError(err).WithField("volume_id", volumeID).Warn("Failed to re-register mount")
				}
			} else {
				c.logger.WithError(err).WithField("volume_id", volumeID).Warn("Failed to update heartbeat")
			}
		} else {
			metrics.CoordinatorHeartbeatsTotal.Inc()
		}
	}
}

// runCleanup periodically cleans up stale mounts
func (c *Coordinator) runCleanup(ctx context.Context) {
	ticker := time.NewTicker(CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			close(c.doneCh)
			return
		case <-ticker.C:
			deleted, err := c.repo.DeleteStaleMounts(ctx, HeartbeatTimeout)
			if err != nil {
				c.logger.WithError(err).Warn("Failed to cleanup stale mounts")
			} else if deleted > 0 {
				metrics.CoordinatorStaleMountsCleaned.Add(float64(deleted))
				c.logger.WithField("deleted_count", deleted).Info("Cleaned up stale mounts")
			}
		}
	}
}

// GetInstanceID returns the unique identifier for this instance
func (c *Coordinator) GetInstanceID() string {
	return fmt.Sprintf("%s/%s", c.clusterID, c.podID)
}
