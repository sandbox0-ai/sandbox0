package rootfs

import "github.com/prometheus/client_golang/prometheus"

type rootFSSyncCollector struct {
	controller            *Controller
	sessions              *prometheus.Desc
	dirtyPaths            *prometheus.Desc
	dirtyBytes            *prometheus.Desc
	activeCaptures        *prometheus.Desc
	initialScanPending    *prometheus.Desc
	fullReconcileSessions *prometheus.Desc
	sealedSessions        *prometheus.Desc
	errorSessions         *prometheus.Desc
}

func newRootFSSyncCollector(controller *Controller) *rootFSSyncCollector {
	return &rootFSSyncCollector{
		controller: controller,
		sessions: prometheus.NewDesc(
			"ctld_rootfs_sync_sessions",
			"Current node-local rootfs synchronization sessions.", nil, nil,
		),
		dirtyPaths: prometheus.NewDesc(
			"ctld_rootfs_sync_dirty_paths",
			"Dirty rootfs paths waiting for capture across current sessions.", nil, nil,
		),
		dirtyBytes: prometheus.NewDesc(
			"ctld_rootfs_sync_dirty_bytes",
			"Estimated dirty rootfs bytes waiting for capture across current sessions.", nil, nil,
		),
		activeCaptures: prometheus.NewDesc(
			"ctld_rootfs_sync_active_captures",
			"Active rootfs path captures across current sessions.", nil, nil,
		),
		initialScanPending: prometheus.NewDesc(
			"ctld_rootfs_sync_initial_scan_pending_sessions",
			"Current rootfs synchronization sessions whose initial scan is incomplete.", nil, nil,
		),
		fullReconcileSessions: prometheus.NewDesc(
			"ctld_rootfs_sync_full_reconcile_sessions",
			"Current rootfs synchronization sessions requiring a full reconciliation before seal.", nil, nil,
		),
		sealedSessions: prometheus.NewDesc(
			"ctld_rootfs_sync_sealed_sessions",
			"Current rootfs synchronization sessions waiting for manager publication acknowledgement.", nil, nil,
		),
		errorSessions: prometheus.NewDesc(
			"ctld_rootfs_sync_error_sessions",
			"Current rootfs synchronization sessions exposing a capture error.", nil, nil,
		),
	}
}

func (c *rootFSSyncCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.sessions
	ch <- c.dirtyPaths
	ch <- c.dirtyBytes
	ch <- c.activeCaptures
	ch <- c.initialScanPending
	ch <- c.fullReconcileSessions
	ch <- c.sealedSessions
	ch <- c.errorSessions
}

func (c *rootFSSyncCollector) Collect(ch chan<- prometheus.Metric) {
	var sessions, dirtyPaths, dirtyBytes, activeCaptures float64
	var initialScanPending, fullReconcileSessions, sealedSessions, errorSessions float64
	if c != nil && c.controller != nil {
		c.controller.v3Mu.Lock()
		bindings := make([]*rootFSSyncBinding, 0, len(c.controller.v3Sessions))
		for _, binding := range c.controller.v3Sessions {
			bindings = append(bindings, binding)
		}
		c.controller.v3Mu.Unlock()
		for _, binding := range bindings {
			status := bindingStatus(binding)
			sessions++
			dirtyPaths += float64(status.DirtyPaths)
			dirtyBytes += float64(status.DirtyBytes)
			activeCaptures += float64(status.ActiveCaptures)
			if !status.InitialScanComplete {
				initialScanPending++
			}
			if status.NeedsFullReconcile {
				fullReconcileSessions++
			}
			if status.Sealed {
				sealedSessions++
			}
			if status.LastError != "" {
				errorSessions++
			}
		}
	}
	ch <- prometheus.MustNewConstMetric(c.sessions, prometheus.GaugeValue, sessions)
	ch <- prometheus.MustNewConstMetric(c.dirtyPaths, prometheus.GaugeValue, dirtyPaths)
	ch <- prometheus.MustNewConstMetric(c.dirtyBytes, prometheus.GaugeValue, dirtyBytes)
	ch <- prometheus.MustNewConstMetric(c.activeCaptures, prometheus.GaugeValue, activeCaptures)
	ch <- prometheus.MustNewConstMetric(c.initialScanPending, prometheus.GaugeValue, initialScanPending)
	ch <- prometheus.MustNewConstMetric(c.fullReconcileSessions, prometheus.GaugeValue, fullReconcileSessions)
	ch <- prometheus.MustNewConstMetric(c.sealedSessions, prometheus.GaugeValue, sealedSessions)
	ch <- prometheus.MustNewConstMetric(c.errorSessions, prometheus.GaugeValue, errorSessions)
}
