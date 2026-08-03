//go:build linux

package ha

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const metricsShutdownTimeout = 5 * time.Second

type metricsCollector struct {
	coordinator *Coordinator
	node        string
	slot        string

	primary         *prometheus.Desc
	role            *prometheus.Desc
	epoch           *prometheus.Desc
	synchronized    *prometheus.Desc
	standbys        *prometheus.Desc
	stateDuration   *prometheus.Desc
	roleTransitions *prometheus.Desc
	lockInfo        *prometheus.Desc
}

func newMetricsCollector(coordinator *Coordinator, node, slot string) (*metricsCollector, error) {
	if coordinator == nil {
		return nil, fmt.Errorf("ctld HA coordinator is required")
	}
	labels := []string{"node", "slot"}
	return &metricsCollector{
		coordinator: coordinator,
		node:        strings.TrimSpace(node),
		slot:        strings.TrimSpace(slot),
		primary: prometheus.NewDesc(
			"ctld_ha_primary",
			"Whether this ctld peer currently owns the node-local primary lock",
			labels,
			nil,
		),
		role: prometheus.NewDesc(
			"ctld_ha_role",
			"Current ctld HA role as a one-hot labeled gauge",
			append(labels, "role"),
			nil,
		),
		epoch: prometheus.NewDesc(
			"ctld_ha_epoch",
			"Current node-local ctld HA election epoch",
			labels,
			nil,
		),
		synchronized: prometheus.NewDesc(
			"ctld_ha_synchronized",
			"Whether the ctld peer has a synchronized HA counterpart",
			labels,
			nil,
		),
		standbys: prometheus.NewDesc(
			"ctld_ha_standbys",
			"Number of synchronized standbys connected to this primary",
			labels,
			nil,
		),
		stateDuration: prometheus.NewDesc(
			"ctld_ha_state_duration_seconds",
			"Seconds since the current ctld HA role was entered",
			labels,
			nil,
		),
		roleTransitions: prometheus.NewDesc(
			"ctld_ha_role_transitions_total",
			"Ctld HA role changes since process start",
			append(labels, "from", "to"),
			nil,
		),
		lockInfo: prometheus.NewDesc(
			"ctld_ha_lock_info",
			"Identity of the node-local filesystem object used for ctld primary election",
			append(labels, "device", "inode"),
			nil,
		),
	}, nil
}

func (c *metricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.primary
	ch <- c.role
	ch <- c.epoch
	ch <- c.synchronized
	ch <- c.standbys
	ch <- c.stateDuration
	ch <- c.roleTransitions
	ch <- c.lockInfo
}

func (c *metricsCollector) Collect(ch chan<- prometheus.Metric) {
	snapshot := c.coordinator.MetricsSnapshot()
	role := snapshot.State.Role
	if role == "" {
		role = RoleStarting
	}
	primary := 0.0
	if role == RolePrimary {
		primary = 1
	}
	synchronized := 0.0
	if snapshot.State.Synchronized {
		synchronized = 1
	}
	stateDuration := time.Since(snapshot.StateSince).Seconds()
	if snapshot.StateSince.IsZero() || stateDuration < 0 {
		stateDuration = 0
	}
	baseLabels := []string{c.node, c.slot}
	ch <- prometheus.MustNewConstMetric(c.primary, prometheus.GaugeValue, primary, baseLabels...)
	ch <- prometheus.MustNewConstMetric(c.role, prometheus.GaugeValue, 1, c.node, c.slot, string(role))
	ch <- prometheus.MustNewConstMetric(c.epoch, prometheus.GaugeValue, float64(snapshot.State.Epoch), baseLabels...)
	ch <- prometheus.MustNewConstMetric(c.synchronized, prometheus.GaugeValue, synchronized, baseLabels...)
	ch <- prometheus.MustNewConstMetric(c.standbys, prometheus.GaugeValue, float64(snapshot.State.Standbys), baseLabels...)
	ch <- prometheus.MustNewConstMetric(c.stateDuration, prometheus.GaugeValue, stateDuration, baseLabels...)
	for transition, count := range snapshot.Transitions {
		ch <- prometheus.MustNewConstMetric(
			c.roleTransitions,
			prometheus.CounterValue,
			float64(count),
			c.node,
			c.slot,
			string(transition.From),
			string(transition.To),
		)
	}
	if snapshot.LockIdentity.Known {
		ch <- prometheus.MustNewConstMetric(
			c.lockInfo,
			prometheus.GaugeValue,
			1,
			c.node,
			c.slot,
			strconv.FormatUint(snapshot.LockIdentity.Device, 10),
			strconv.FormatUint(snapshot.LockIdentity.Inode, 10),
		)
	}
}

// MetricsServer exposes election metrics for both primary and standby peers.
type MetricsServer struct {
	listener  net.Listener
	server    *http.Server
	errors    chan error
	closeOnce sync.Once
}

// StartMetricsServer starts a dedicated pre-election Prometheus endpoint.
func StartMetricsServer(ctx context.Context, addr string, coordinator *Coordinator, node, slot string) (*MetricsServer, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return nil, nil
	}
	collector, err := newMetricsCollector(coordinator, node, slot)
	if err != nil {
		return nil, err
	}
	registry := prometheus.NewRegistry()
	if err := registry.Register(collector); err != nil {
		return nil, fmt.Errorf("register ctld HA metrics: %w", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen for ctld HA metrics: %w", err)
	}
	metricsServer := &MetricsServer{
		listener: listener,
		server: &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
		errors: make(chan error, 1),
	}
	go func() {
		if err := metricsServer.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			metricsServer.errors <- err
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), metricsShutdownTimeout)
		defer cancel()
		_ = metricsServer.server.Shutdown(shutdownCtx)
	}()
	return metricsServer, nil
}

// Addr returns the bound listener address.
func (s *MetricsServer) Addr() string {
	if s == nil || s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// Errors reports unexpected serving failures.
func (s *MetricsServer) Errors() <-chan error {
	if s == nil {
		return nil
	}
	return s.errors
}

// Close gracefully stops the metrics endpoint.
func (s *MetricsServer) Close() error {
	if s == nil {
		return nil
	}
	var closeErr error
	s.closeOnce.Do(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), metricsShutdownTimeout)
		defer cancel()
		closeErr = s.server.Shutdown(shutdownCtx)
	})
	return closeErr
}
