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

package driver

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/containerd/errdefs"
	"github.com/hashicorp/go-hclog"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
)

const (
	rootFSSessionReconcileInterval  = time.Second
	rootFSSessionAttachGrace        = 2 * time.Minute
	rootFSSessionReconcileTimeout   = 3 * time.Minute
	nomadAllocationResponseMaxBytes = 64 << 20
)

type rootFSSessionDaemon struct {
	runtime rootFSSessionDaemonRuntime
	runner  Runsc
	mounter Mounter
	config  PluginConfig
	logger  hclog.Logger

	mu          sync.Mutex
	wg          sync.WaitGroup
	inflight    map[string]bool
	trigger     chan string
	allocations nomadAllocationSource
}

type rootFSSessionDaemonRuntime interface {
	RootFSRuntime
	RecoverySessions() ([]rootfssession.RecoverySession, error)
}

type nomadAllocationSource interface {
	ActiveAllocations(context.Context) (map[string]bool, error)
}

// NomadAllocationConfig identifies the server-side allocation catalog used to
// detect `stop -purge` even when the local task-driver process misses Destroy.
type NomadAllocationConfig struct {
	Address   string
	NodeID    string
	TokenFile string
	CAFile    string
	CertFile  string
	KeyFile   string
}

// RunRootFSSessionDaemon runs the node-scoped owner for writer leases,
// NBD/XFS/Overlay sessions, and terminal reconciliation. The Nomad task-driver
// process talks to it only over the root-owned Unix socket.
func RunRootFSSessionDaemon(
	ctx context.Context,
	config PluginConfig,
	socketPath string,
	nomadConfig NomadAllocationConfig,
	logger hclog.Logger,
) error {
	config.RootFSSessiondSocket = ""
	if strings.TrimSpace(config.RootFSConsumerMountRoot) == "" {
		return fmt.Errorf("rootfs_consumer_mount_root is required for the session daemon")
	}
	if strings.TrimSpace(config.RootFSAuthorityURL) == "" {
		return fmt.Errorf("rootfs_authority_url is required for the session daemon")
	}
	for name, value := range map[string]string{
		"runsc": config.RunscPath, "runsc_root": config.RunscRoot,
		"rootfs_consumer_mount_root": config.RootFSConsumerMountRoot,
	} {
		if !filepath.IsAbs(value) || filepath.Clean(value) == "/" {
			return fmt.Errorf("%s must be a non-root absolute path", name)
		}
	}
	consumerMountRoot, err := filepath.EvalSymlinks(config.RootFSConsumerMountRoot)
	if err != nil {
		return fmt.Errorf("resolve rootfs_consumer_mount_root: %w", err)
	}
	config.RootFSConsumerMountRoot = consumerMountRoot
	if err := validateRootFSConfig(&config); err != nil {
		return err
	}
	allocations, err := newNomadAllocationSource(nomadConfig)
	if err != nil {
		return err
	}
	if allocations == nil {
		return fmt.Errorf("Nomad allocation authority is required for the session daemon")
	}
	runtime, err := newEmbeddedRootFSRuntime(&config, logger.Named("runtime"))
	if err != nil {
		return err
	}
	defer runtime.Close()
	daemon := &rootFSSessionDaemon{
		runtime: runtime, runner: NewCommandRunsc(config), mounter: systemMounter{},
		config: config, logger: logger, inflight: make(map[string]bool), trigger: make(chan string, 128),
		allocations: allocations,
	}
	daemonCtx, cancelDaemon := context.WithCancel(ctx)
	defer cancelDaemon()
	daemon.wg.Add(1)
	go func() {
		defer daemon.wg.Done()
		daemon.reconcileLoop(daemonCtx)
	}()
	err = serveRootFSSessionRuntime(daemonCtx, socketPath, runtime, daemon.writerLeaseLost, daemon.health)
	cancelDaemon()
	daemon.wg.Wait()
	return err
}

func (d *rootFSSessionDaemon) health(ctx context.Context) error {
	if _, err := d.runtime.RecoverySessions(); err != nil {
		return fmt.Errorf("read durable RootFS recovery journal: %w: %w", err, errdefs.ErrUnavailable)
	}
	if d.allocations != nil {
		if _, err := d.allocations.ActiveAllocations(ctx); err != nil {
			return fmt.Errorf("read Nomad allocation authority: %w: %w", err, errdefs.ErrUnavailable)
		}
	}
	return nil
}

func (d *rootFSSessionDaemon) writerLeaseLost(stage rootfshandoff.StageRequest, cause error) {
	d.logger.Error("RootFS writer authority lease lost", "parent", stage.Parent, "error", cause)
	select {
	case d.trigger <- stage.Parent:
	default:
		d.logger.Error("RootFS terminal trigger queue is full", "parent", stage.Parent)
	}
}

func (d *rootFSSessionDaemon) reconcileLoop(ctx context.Context) {
	ticker := time.NewTicker(rootFSSessionReconcileInterval)
	defer ticker.Stop()
	d.scan(ctx, "")
	for {
		select {
		case <-ctx.Done():
			return
		case parent := <-d.trigger:
			d.scan(ctx, parent)
		case <-ticker.C:
			d.scan(ctx, "")
		}
	}
}

func (d *rootFSSessionDaemon) scan(ctx context.Context, onlyParent string) {
	sessions, err := d.runtime.RecoverySessions()
	if err != nil {
		d.logger.Error("list durable RootFS recovery sessions", "error", err)
		return
	}
	now := time.Now()
	var activeAllocations map[string]bool
	if d.allocations != nil {
		activeAllocations, err = d.allocations.ActiveAllocations(ctx)
		if err != nil {
			d.logger.Error("list active Nomad allocations for RootFS reconciliation", "error", err)
			activeAllocations = nil
		}
	}
	for _, session := range sessions {
		if onlyParent != "" && session.Stage.Parent != onlyParent {
			continue
		}
		if session.Kind == rootfssession.RecoveryUnavailable {
			d.logger.Error("legacy RootFS session lacks an independent recovery binding", "state", session.State)
			continue
		}
		allocationPurged := activeAllocations != nil && !activeAllocations[session.Stage.Identity.PodUID] &&
			now.Sub(session.CreatedAt) >= rootFSSessionReconcileInterval
		if !rootFSSessionNeedsReconciliation(session, now, onlyParent != "" || allocationPurged) {
			continue
		}
		parent := session.Stage.Parent
		d.mu.Lock()
		if d.inflight[parent] {
			d.mu.Unlock()
			continue
		}
		d.inflight[parent] = true
		d.wg.Add(1)
		d.mu.Unlock()
		go func(session rootfssession.RecoverySession) {
			defer func() {
				d.mu.Lock()
				delete(d.inflight, session.Stage.Parent)
				d.mu.Unlock()
				d.wg.Done()
			}()
			reconcileCtx, cancel := context.WithTimeout(ctx, rootFSSessionReconcileTimeout)
			defer cancel()
			if err := d.reconcile(reconcileCtx, session); err != nil && !errors.Is(err, context.Canceled) {
				d.logger.Error("reconcile orphan RootFS writer", "parent", session.Stage.Parent, "error", err)
			}
		}(session)
	}
}

type httpNomadAllocationSource struct {
	url       string
	tokenFile string
	http      *http.Client
}

func newNomadAllocationSource(config NomadAllocationConfig) (nomadAllocationSource, error) {
	address := strings.TrimSpace(config.Address)
	nodeID := strings.TrimSpace(config.NodeID)
	if address == "" && nodeID == "" {
		return nil, nil
	}
	if address == "" || nodeID == "" {
		return nil, fmt.Errorf("Nomad address and node ID must be configured together")
	}
	parsed, err := url.Parse(address)
	if err != nil || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, fmt.Errorf("Nomad address must be an HTTP(S) origin")
	}
	parsed.Path = strings.TrimSuffix(parsed.Path, "/") + "/v1/node/" + url.PathEscape(nodeID) + "/allocations"
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	if parsed.Scheme == "https" {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
		if strings.TrimSpace(config.CAFile) != "" {
			payload, err := os.ReadFile(strings.TrimSpace(config.CAFile))
			if err != nil {
				return nil, fmt.Errorf("read Nomad CA: %w", err)
			}
			roots := x509.NewCertPool()
			if !roots.AppendCertsFromPEM(payload) {
				return nil, fmt.Errorf("Nomad CA contains no certificates")
			}
			tlsConfig.RootCAs = roots
		}
		if strings.TrimSpace(config.CertFile) != "" || strings.TrimSpace(config.KeyFile) != "" {
			if strings.TrimSpace(config.CertFile) == "" || strings.TrimSpace(config.KeyFile) == "" {
				return nil, fmt.Errorf("Nomad client certificate and key must be configured together")
			}
			certificate, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
			if err != nil {
				return nil, fmt.Errorf("load Nomad client identity: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{certificate}
		}
		transport.TLSClientConfig = tlsConfig
	}
	return &httpNomadAllocationSource{
		url: parsed.String(), tokenFile: strings.TrimSpace(config.TokenFile),
		http: &http.Client{Timeout: 2 * time.Second, Transport: transport},
	}, nil
}

func (s *httpNomadAllocationSource) ActiveAllocations(ctx context.Context) (map[string]bool, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, s.url, nil)
	if err != nil {
		return nil, err
	}
	if s.tokenFile != "" {
		token, err := os.ReadFile(s.tokenFile)
		if err != nil {
			return nil, fmt.Errorf("read Nomad token: %w", err)
		}
		request.Header.Set("X-Nomad-Token", strings.TrimSpace(string(token)))
	}
	response, err := s.http.Do(request)
	if err != nil {
		return nil, fmt.Errorf("list Nomad node allocations: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		payload, _ := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		return nil, fmt.Errorf("list Nomad node allocations: HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(payload)))
	}
	var records []struct {
		ID string `json:"ID"`
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, nomadAllocationResponseMaxBytes))
	if err := decoder.Decode(&records); err != nil {
		return nil, fmt.Errorf("decode Nomad node allocations: %w", err)
	}
	active := make(map[string]bool, len(records))
	for _, record := range records {
		if id := strings.TrimSpace(record.ID); id != "" {
			// Presence, not scheduler status, is the purge fence. A normal stop
			// must retain its allocation long enough for planned publication;
			// treating DesiredStatus=stop as an orphan would discard writes.
			active[id] = true
		}
	}
	return active, nil
}

func rootFSSessionNeedsReconciliation(session rootfssession.RecoverySession, now time.Time, forced bool) bool {
	if forced || session.Kind == rootfssession.RecoveryPlannedRetire {
		return true
	}
	if session.Consumer != nil {
		deadline, err := session.Consumer.Validate()
		return err != nil || !session.Live || !now.Before(deadline)
	}
	return now.Sub(session.CreatedAt) >= rootFSSessionAttachGrace
}

func (d *rootFSSessionDaemon) reconcile(ctx context.Context, session rootfssession.RecoverySession) error {
	observation, err := d.fenceHostRuntime(ctx, session)
	if err != nil {
		return err
	}
	if session.Kind == rootfssession.RecoveryPlannedRetire {
		operationID := session.RetireOperationID
		if operationID == "" {
			return fmt.Errorf("planned RootFS recovery lacks its operation ID")
		}
		_, err := d.runtime.Retire(ctx, session.Stage, operationID)
		return err
	}
	_, err = d.runtime.CrashFence(ctx, session.Stage, crashOperationID(session.Stage), observation)
	return err
}

func (d *rootFSSessionDaemon) fenceHostRuntime(
	ctx context.Context,
	session rootfssession.RecoverySession,
) (crashTaskObservation, error) {
	consumer := session.Consumer
	if consumer == nil {
		namespace, err := os.Readlink("/proc/self/ns/mnt")
		if err != nil {
			return crashTaskObservation{}, fmt.Errorf("read session daemon mount namespace: %w", err)
		}
		return crashTaskObservation{
			ActiveKey: session.Stage.Identity.ClaimID, HostMountNamespaceID: namespace,
			ContainerAbsent: true, TaskAbsent: true, FrontendSnapshotAbsent: true, StableMountAbsent: true,
		}, nil
	}
	namespace, err := os.Readlink("/proc/self/ns/mnt")
	if err != nil {
		return crashTaskObservation{}, fmt.Errorf("read session daemon mount namespace: %w", err)
	}
	if namespace != consumer.HostMountNamespace {
		return crashTaskObservation{}, fmt.Errorf(
			"consumer mount namespace %s differs from session daemon %s: %w",
			consumer.HostMountNamespace, namespace, errdefs.ErrFailedPrecondition,
		)
	}
	if err := validateConsumerMountPath(consumer.StableMount, d.config.RootFSConsumerMountRoot); err != nil {
		return crashTaskObservation{}, err
	}
	// A naturally stopped container may reject kill. Forced delete plus the
	// subsequent state lookup, not kill's exit status, is the absence proof.
	_ = d.runner.Kill(ctx, consumer.ContainerID, "KILL")
	if err := d.runner.Delete(ctx, consumer.ContainerID, true); err != nil && !errdefs.IsNotFound(err) {
		return crashTaskObservation{}, fmt.Errorf("delete orphan gVisor container: %w", err)
	}
	if _, err := d.runner.State(ctx, consumer.ContainerID); err == nil {
		return crashTaskObservation{}, fmt.Errorf("gVisor container %s remains present: %w", consumer.ContainerID, errdefs.ErrFailedPrecondition)
	} else if !errdefs.IsNotFound(err) {
		return crashTaskObservation{}, fmt.Errorf("attest orphan gVisor container absence: %w", err)
	}
	if err := d.mounter.Unmount(consumer.StableMount); err != nil {
		return crashTaskObservation{}, err
	}
	attached, err := hostMountAttached(consumer.StableMount)
	if err != nil {
		return crashTaskObservation{}, err
	}
	if attached {
		return crashTaskObservation{}, fmt.Errorf("stable task root %s remains mounted: %w", consumer.StableMount, errdefs.ErrFailedPrecondition)
	}
	return crashTaskObservation{
		ActiveKey: consumer.ActiveKey, ContainerID: consumer.ContainerID,
		HostMountNamespaceID: consumer.HostMountNamespace, ContainerAbsent: true, TaskAbsent: true,
		FrontendSnapshotAbsent: true, StableMountAbsent: true,
	}, nil
}

func validateConsumerMountPath(path, root string) error {
	path = filepath.Clean(strings.TrimSpace(path))
	root = filepath.Clean(strings.TrimSpace(root))
	if !filepath.IsAbs(path) || !filepath.IsAbs(root) || path == root || root == "/" {
		return fmt.Errorf("consumer mount and root are invalid")
	}
	relative, err := filepath.Rel(root, path)
	if err != nil || relative == ".." || filepath.IsAbs(relative) || startsWithDotDot(relative) {
		return fmt.Errorf("consumer mount %s is outside %s", path, root)
	}
	return nil
}

func hostMountAttached(path string) (bool, error) {
	file, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return false, fmt.Errorf("open host mountinfo: %w", err)
	}
	defer file.Close()
	wanted := filepath.Clean(path)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 5 {
			continue
		}
		mountpoint := strings.NewReplacer(
			`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`,
		).Replace(fields[4])
		if filepath.Clean(mountpoint) == wanted {
			return true, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return false, fmt.Errorf("scan host mountinfo: %w", err)
	}
	return false, nil
}
