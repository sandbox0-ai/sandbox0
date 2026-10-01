package networking

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/model"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/policy"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/proxy"
)

const livePolicySnapshotPath = "/internal/live-policy-snapshot"

func (d *Daemon) ImportLiveListeners(files []*os.File) error {
	if d.proxyServer != nil || d.inheritedListeners != nil {
		return fmt.Errorf("network runtime already started")
	}
	d.inheritedListeners = files
	return nil
}

func (d *Daemon) PrepareLiveUpdate() ([]*os.File, error) {
	if !d.Ready() || d.proxyServer == nil {
		return nil, fmt.Errorf("network runtime is not ready for live update")
	}
	if d.cfg.EgressBandwidthBytesPerSecond > 0 || d.cfg.IngressBandwidthBytesPerSecond > 0 {
		return nil, fmt.Errorf("live update requires shared bandwidth accounting; process-local sandbox token buckets cannot overlap")
	}
	return d.proxyServer.ExportListeners()
}

// CommitLiveUpdate relinquishes registry and redirect ownership, while the
// source process continues enforcing policies and credentials for old flows.
func (d *Daemon) CommitLiveUpdate(ctx context.Context) error {
	d.runtimeMu.Lock()
	control := d.runtimeSlotControl
	d.runtimeMu.Unlock()
	if control != nil {
		if err := control.Shutdown(ctx); err != nil {
			return err
		}
	}
	d.syncMu.Lock()
	defer d.syncMu.Unlock()
	d.draining.Store(true)
	d.ready.Store(false)
	d.runtimeMu.Lock()
	registry := d.runtimeSlotRegistry
	d.runtimeSlotRegistry, d.runtimeSlotControl = nil, nil
	d.runtimeMu.Unlock()
	if registry != nil {
		if err := registry.Close(); err != nil {
			return err
		}
	}
	var result error
	if d.healthServer != nil {
		result = errors.Join(result, d.healthServer.Shutdown(ctx))
	}
	if d.metricsServer != nil {
		result = errors.Join(result, d.metricsServer.Shutdown(ctx))
	}
	if err := d.proxyServer.Drain(); err != nil {
		return errors.Join(result, err)
	}
	close(d.drainStarted)
	go d.followLivePolicies(d.flowContext)
	return result
}

func (d *Daemon) followLivePolicies(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	lastSuccess := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		d.syncMu.Lock()
		err := d.syncDrainingPolicies(ctx, d.proxyServer.PolicyStore(), nil, d.proxyServer)
		if err == nil {
			lastSuccess = time.Now()
		} else if time.Since(lastSuccess) > 10*time.Second {
			d.proxyServer.PolicyStore().ReconcileSandboxes(nil)
			d.proxyServer.ReconcileActiveFlows()
		}
		d.syncMu.Unlock()
	}
}

func (d *Daemon) syncDrainingPolicies(ctx context.Context, store *policy.Store, platform *platformPolicyState, server *proxy.Server) error {
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", d.runtimeSlotControlSocket)
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://ctld"+livePolicySnapshotPath, nil)
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("live policy snapshot returned %d", response.StatusCode)
	}
	var sandboxes []*model.SandboxInfo
	if err := json.NewDecoder(io.LimitReader(response.Body, 16<<20)).Decode(&sandboxes); err != nil {
		return err
	}
	store.ReconcileSandboxes(sandboxes)
	server.ReconcileActiveFlows()
	if platform != nil {
		platform.Reconcile(sandboxes)
	}
	return nil
}
