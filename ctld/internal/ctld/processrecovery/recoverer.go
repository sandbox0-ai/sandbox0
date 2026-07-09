// Package processrecovery coordinates procd replacement after ctld rebuilds a
// sandbox's FUSE portals.
package processrecovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/procdstate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const (
	defaultCRIEndpoint     = "/host-run/containerd/containerd.sock"
	defaultKubeletPodsRoot = "/var/lib/kubelet/pods"
	defaultPendingDir      = "/var/lib/sandbox0/ctld/process-recovery"
	defaultDialTimeout     = 10 * time.Second
	defaultStopTimeout     = 10 * time.Second
	kubeletCSIVolumeDir    = "kubernetes.io~csi"
)

type runtimeService interface {
	ListContainers(context.Context, *runtimeapi.ListContainersRequest, ...grpc.CallOption) (*runtimeapi.ListContainersResponse, error)
	StopContainer(context.Context, *runtimeapi.StopContainerRequest, ...grpc.CallOption) (*runtimeapi.StopContainerResponse, error)
}

// Target identifies a live sandbox pod's procd container and webhook-state portal.
type Target struct {
	Namespace       string
	PodName         string
	PodUID          string
	ContainerName   string
	StateVolumeName string
	ReplayProcesses bool
}

// Config controls node-local process recovery coordination.
type Config struct {
	CRIEndpoint     string
	KubeletPodsRoot string
	PendingDir      string
	DialTimeout     time.Duration
	StopTimeout     time.Duration
	RuntimeService  runtimeService
	DialContext     func(context.Context, string) (*grpc.ClientConn, error)
}

// Recoverer replaces stale procd containers and optionally requests context replay.
type Recoverer struct {
	criEndpoint       string
	kubeletPodsRoot   string
	pendingDir        string
	dialTimeout       time.Duration
	stopTimeout       time.Duration
	configuredRuntime runtimeService
	dialContext       func(context.Context, string) (*grpc.ClientConn, error)

	mu               sync.Mutex
	client           runtimeService
	conn             *grpc.ClientConn
	containersLoaded bool
	containers       map[string]string
	loadErr          error
}

type pendingRecovery struct {
	PodUID          string    `json:"pod_uid"`
	ContainerID     string    `json:"container_id,omitempty"`
	ReplayProcesses bool      `json:"replay_processes"`
	RequestedAt     time.Time `json:"requested_at"`
}

func New(cfg Config) *Recoverer {
	criEndpoint := strings.TrimSpace(cfg.CRIEndpoint)
	if criEndpoint == "" {
		criEndpoint = defaultCRIEndpoint
	}
	kubeletPodsRoot := strings.TrimSpace(cfg.KubeletPodsRoot)
	if kubeletPodsRoot == "" {
		kubeletPodsRoot = defaultKubeletPodsRoot
	}
	pendingDir := strings.TrimSpace(cfg.PendingDir)
	if pendingDir == "" {
		pendingDir = defaultPendingDir
	}
	dialTimeout := cfg.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = defaultDialTimeout
	}
	stopTimeout := cfg.StopTimeout
	if stopTimeout <= 0 {
		stopTimeout = defaultStopTimeout
	}
	return &Recoverer{
		criEndpoint:       criEndpoint,
		kubeletPodsRoot:   filepath.Clean(kubeletPodsRoot),
		pendingDir:        filepath.Clean(pendingDir),
		dialTimeout:       dialTimeout,
		stopTimeout:       stopTimeout,
		configuredRuntime: cfg.RuntimeService,
		dialContext:       cfg.DialContext,
	}
}

// Recover resumes a pending replacement, or starts one when portalsRecovered
// reports that this ctld instance rebuilt at least one portal for the pod.
func (r *Recoverer) Recover(ctx context.Context, target Target, portalsRecovered bool) error {
	if err := validateTarget(target); err != nil {
		return err
	}
	pendingPath := filepath.Join(r.pendingDir, target.PodUID+".json")
	pending, hasPending, err := readPending(pendingPath)
	if err != nil {
		return err
	}
	if !hasPending && !portalsRecovered {
		return nil
	}

	client, containerID, err := r.runtimeForTarget(ctx, target)
	if err != nil {
		return err
	}
	if hasPending && pending.PodUID != target.PodUID {
		return fmt.Errorf("pending process recovery pod UID %q does not match %q", pending.PodUID, target.PodUID)
	}
	if hasPending && pending.ContainerID != "" && containerID != "" && pending.ContainerID != containerID {
		return removePending(pendingPath)
	}
	if !hasPending {
		pending = pendingRecovery{
			PodUID:          target.PodUID,
			ContainerID:     containerID,
			ReplayProcesses: target.ReplayProcesses,
			RequestedAt:     time.Now().UTC(),
		}
		if err := writeJSONAtomic(r.pendingDir, pendingPath, pending); err != nil {
			return fmt.Errorf("write pending process recovery: %w", err)
		}
	}

	if pending.ReplayProcesses {
		markerPath, err := r.recoveryMarkerPath(target)
		if err != nil {
			return err
		}
		if err := writeJSONAtomic(filepath.Dir(markerPath), markerPath, pending); err != nil {
			return fmt.Errorf("write procd recovery marker: %w", err)
		}
	}
	if containerID == "" {
		return removePending(pendingPath)
	}
	if _, err := client.StopContainer(ctx, &runtimeapi.StopContainerRequest{
		ContainerId: containerID,
		Timeout:     int64(r.stopTimeout.Seconds()),
	}); err != nil {
		return fmt.Errorf("stop procd container %s: %w", containerID, err)
	}
	r.removeRunningContainer(target, containerID)
	return removePending(pendingPath)
}

func (r *Recoverer) recoveryMarkerPath(target Target) (string, error) {
	stateMount := filepath.Join(r.kubeletPodsRoot, target.PodUID, "volumes", kubeletCSIVolumeDir, target.StateVolumeName, "mount")
	rel, err := filepath.Rel(r.kubeletPodsRoot, stateMount)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("webhook-state portal escapes kubelet pod root")
	}
	if info, err := os.Stat(stateMount); err != nil {
		return "", fmt.Errorf("inspect webhook-state portal: %w", err)
	} else if !info.IsDir() {
		return "", fmt.Errorf("webhook-state portal is not a directory")
	}
	return filepath.Join(stateMount, procdstate.RecoveryRequestFilename), nil
}

func (r *Recoverer) runtimeForTarget(ctx context.Context, target Target) (runtimeService, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.containersLoaded {
		r.loadRunningContainersLocked(ctx)
	}
	if r.loadErr != nil {
		return nil, "", r.loadErr
	}
	return r.client, r.containers[containerKey(target.Namespace, target.PodName, target.PodUID, target.ContainerName)], nil
}

func (r *Recoverer) loadRunningContainersLocked(ctx context.Context) {
	r.containersLoaded = true
	client := r.configuredRuntime
	if client == nil {
		dialCtx, cancel := context.WithTimeout(ctx, r.dialTimeout)
		defer cancel()
		dial := r.dialContext
		if dial == nil {
			dial = func(ctx context.Context, endpoint string) (*grpc.ClientConn, error) {
				return grpc.DialContext(ctx, endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
			}
		}
		conn, err := dial(dialCtx, normalizeEndpoint(r.criEndpoint))
		if err != nil {
			r.loadErr = fmt.Errorf("dial CRI endpoint %s: %w", r.criEndpoint, err)
			return
		}
		r.conn = conn
		client = runtimeapi.NewRuntimeServiceClient(conn)
	}
	r.client = client
	resp, err := client.ListContainers(ctx, &runtimeapi.ListContainersRequest{
		Filter: &runtimeapi.ContainerFilter{State: &runtimeapi.ContainerStateValue{State: runtimeapi.ContainerState_CONTAINER_RUNNING}},
	})
	if err != nil {
		r.loadErr = fmt.Errorf("list CRI containers: %w", err)
		return
	}
	r.containers = make(map[string]string, len(resp.GetContainers()))
	for _, container := range resp.GetContainers() {
		metadata := container.GetMetadata()
		labels := container.GetLabels()
		if metadata == nil || container.GetState() != runtimeapi.ContainerState_CONTAINER_RUNNING {
			continue
		}
		key := containerKey(
			labels["io.kubernetes.pod.namespace"],
			labels["io.kubernetes.pod.name"],
			labels["io.kubernetes.pod.uid"],
			metadata.GetName(),
		)
		r.containers[key] = container.GetId()
	}
}

func (r *Recoverer) removeRunningContainer(target Target, containerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := containerKey(target.Namespace, target.PodName, target.PodUID, target.ContainerName)
	if r.containers[key] == containerID {
		delete(r.containers, key)
	}
}

// Close releases the shared CRI connection after startup reconciliation.
func (r *Recoverer) Close() error {
	r.mu.Lock()
	conn := r.conn
	r.conn = nil
	r.mu.Unlock()
	if conn == nil {
		return nil
	}
	return conn.Close()
}

func containerKey(namespace, podName, podUID, containerName string) string {
	return strings.Join([]string{namespace, podName, podUID, containerName}, "\x00")
}

func validateTarget(target Target) error {
	fields := []struct {
		name  string
		value string
	}{
		{name: "namespace", value: target.Namespace},
		{name: "pod name", value: target.PodName},
		{name: "pod UID", value: target.PodUID},
		{name: "container name", value: target.ContainerName},
		{name: "state volume name", value: target.StateVolumeName},
	}
	for _, field := range fields {
		value := strings.TrimSpace(field.value)
		if value == "" || filepath.Base(value) != value || strings.ContainsAny(value, `/\\`) {
			return fmt.Errorf("invalid %s %q", field.name, value)
		}
	}
	if !strings.HasPrefix(target.StateVolumeName, "sandbox0-volume-") {
		return fmt.Errorf("invalid state volume name %q", target.StateVolumeName)
	}
	return nil
}

func readPending(path string) (pendingRecovery, bool, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return pendingRecovery{}, false, nil
	}
	if err != nil {
		return pendingRecovery{}, false, fmt.Errorf("read pending process recovery: %w", err)
	}
	var pending pendingRecovery
	if err := json.Unmarshal(data, &pending); err != nil {
		return pendingRecovery{}, false, fmt.Errorf("decode pending process recovery: %w", err)
	}
	return pending, true, nil
}

func writeJSONAtomic(dir, path string, value any) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".recovery-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	return syncDirectory(dir)
}

func removePending(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove pending process recovery: %w", err)
	}
	return syncDirectory(filepath.Dir(path))
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func normalizeEndpoint(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	if strings.Contains(endpoint, "://") {
		return endpoint
	}
	if strings.HasPrefix(endpoint, "/") {
		return "unix://" + endpoint
	}
	return endpoint
}
