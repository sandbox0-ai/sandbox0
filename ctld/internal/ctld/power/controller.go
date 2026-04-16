package power

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/cgroup"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
)

var ErrNotImplemented = errors.New("ctld power resolver not implemented")
var ErrSandboxNotFound = errors.New("sandbox not found")
var ErrPodNotFound = errors.New("pod not found")

type Target struct {
	SandboxID    string
	Runtime      string
	CgroupDir    string
	PodNamespace string
	PodName      string
	PodUID       string
	PodIP        string
	ProcdPort    int32
}

type Resolver interface {
	Resolve(r *http.Request, sandboxID string) (Target, error)
	ResolvePod(r *http.Request, namespace, name string) (Target, error)
}

type SandboxStatsProvider interface {
	SandboxResourceUsage(ctx context.Context, target Target) (*ctldapi.SandboxResourceUsage, error)
}

type Controller struct {
	Resolver      Resolver
	FS            *cgroup.FS
	StatsProvider SandboxStatsProvider
	HTTPClient    *http.Client
}

func NewController(resolver Resolver, fs *cgroup.FS) *Controller {
	if fs == nil {
		fs = cgroup.NewFS()
	}
	return &Controller{Resolver: resolver, FS: fs}
}

func (c *Controller) Probe(r *http.Request, sandboxID string, kind sandboxprobe.Kind) (sandboxprobe.Response, int) {
	if !sandboxprobe.ValidKind(kind) {
		return sandboxprobe.Failed(kind, "InvalidProbeKind", fmt.Sprintf("unsupported probe kind %q", kind), nil), http.StatusBadRequest
	}
	target, status, errResp := c.resolveTarget(r, sandboxID)
	if status != http.StatusOK {
		return sandboxprobe.Failed(kind, "SandboxResolveFailed", errResp.Error, nil), status
	}
	return c.probeTarget(r, target, kind)
}

func (c *Controller) ProbePod(r *http.Request, namespace, name string, kind sandboxprobe.Kind) (sandboxprobe.Response, int) {
	if !sandboxprobe.ValidKind(kind) {
		return sandboxprobe.Failed(kind, "InvalidProbeKind", fmt.Sprintf("unsupported probe kind %q", kind), nil), http.StatusBadRequest
	}
	target, status, errResp := c.resolvePodTarget(r, namespace, name)
	if status != http.StatusOK {
		return sandboxprobe.Failed(kind, "PodResolveFailed", errResp.Error, nil), status
	}
	return c.probeTarget(r, target, kind)
}

func (c *Controller) probeTarget(r *http.Request, target Target, kind sandboxprobe.Kind) (sandboxprobe.Response, int) {
	if c.FS != nil {
		frozen, err := c.FS.IsFrozen(target.CgroupDir)
		if err == nil && frozen {
			return sandboxprobe.Suspended(kind, "SandboxPaused", "sandbox cgroup is frozen", nil), http.StatusOK
		}
	}
	if target.PodIP == "" {
		return sandboxprobe.Failed(kind, "PodIPMissing", "sandbox pod IP is not assigned", nil), http.StatusServiceUnavailable
	}
	port := target.ProcdPort
	if port <= 0 {
		port = 49983
	}
	url := fmt.Sprintf("http://%s/sandbox-probes/%s", net.JoinHostPort(target.PodIP, strconv.Itoa(int(port))), kind)
	ctx := context.Background()
	if r != nil {
		ctx = r.Context()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return sandboxprobe.Failed(kind, "InvalidProbeRequest", err.Error(), nil), http.StatusInternalServerError
	}
	client := c.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 2 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return sandboxprobe.Failed(kind, "ProcdProbeFailed", err.Error(), nil), http.StatusServiceUnavailable
	}
	defer resp.Body.Close()
	var result sandboxprobe.Response
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return sandboxprobe.Failed(kind, "ProcdProbeDecodeFailed", err.Error(), nil), http.StatusServiceUnavailable
	}
	if result.Kind == "" {
		result.Kind = kind
	}
	return result, http.StatusOK
}

func (c *Controller) Pause(r *http.Request, sandboxID string) (ctldapi.PauseResponse, int) {
	target, status, errResp := c.resolveTarget(r, sandboxID)
	if status != http.StatusOK {
		return errResp, status
	}
	ctx := context.Background()
	if r != nil {
		ctx = r.Context()
	}
	return c.PauseTarget(ctx, sandboxID, target)
}

func (c *Controller) PauseTarget(ctx context.Context, sandboxID string, target Target) (ctldapi.PauseResponse, int) {
	if ctx == nil {
		ctx = context.Background()
	}
	log.Printf("ctld pause start sandbox=%s runtime=%s cgroup=%s", sandboxID, target.Runtime, target.CgroupDir)
	if err := c.FS.Freeze(target.CgroupDir); err != nil {
		return ctldapi.PauseResponse{Paused: false, Error: fmt.Sprintf("freeze cgroup: %v", err)}, http.StatusInternalServerError
	}
	usage, err := c.pauseUsage(ctx, target)
	if err != nil {
		return ctldapi.PauseResponse{Paused: false, Error: err.Error()}, http.StatusInternalServerError
	}
	log.Printf("ctld pause complete sandbox=%s runtime=%s working_set=%d usage=%d", sandboxID, target.Runtime, usage.ContainerMemoryWorkingSet, usage.ContainerMemoryUsage)
	return ctldapi.PauseResponse{
		Paused:        true,
		ResourceUsage: usage,
	}, http.StatusOK
}

func (c *Controller) Resume(r *http.Request, sandboxID string) (ctldapi.ResumeResponse, int) {
	target, status, pauseErr := c.resolveTarget(r, sandboxID)
	if status != http.StatusOK {
		return ctldapi.ResumeResponse{Resumed: false, Error: pauseErr.Error}, status
	}
	return c.ResumeTarget(sandboxID, target)
}

func (c *Controller) ResumeTarget(sandboxID string, target Target) (ctldapi.ResumeResponse, int) {
	log.Printf("ctld resume start sandbox=%s runtime=%s cgroup=%s", sandboxID, target.Runtime, target.CgroupDir)
	if err := c.FS.Thaw(target.CgroupDir); err != nil {
		return ctldapi.ResumeResponse{Resumed: false, Error: fmt.Sprintf("thaw cgroup: %v", err)}, http.StatusInternalServerError
	}
	log.Printf("ctld resume complete sandbox=%s runtime=%s", sandboxID, target.Runtime)
	return ctldapi.ResumeResponse{Resumed: true}, http.StatusOK
}

func (c *Controller) resolveTarget(r *http.Request, sandboxID string) (Target, int, ctldapi.PauseResponse) {
	if c == nil || c.Resolver == nil {
		return Target{}, http.StatusNotImplemented, ctldapi.PauseResponse{Paused: false, Error: ErrNotImplemented.Error()}
	}
	target, err := c.Resolver.Resolve(r, sandboxID)
	return mapResolveResult(target, err)
}

func (c *Controller) resolvePodTarget(r *http.Request, namespace, name string) (Target, int, ctldapi.PauseResponse) {
	if c == nil || c.Resolver == nil {
		return Target{}, http.StatusNotImplemented, ctldapi.PauseResponse{Paused: false, Error: ErrNotImplemented.Error()}
	}
	target, err := c.Resolver.ResolvePod(r, namespace, name)
	return mapResolveResult(target, err)
}

func mapResolveResult(target Target, err error) (Target, int, ctldapi.PauseResponse) {
	if err == nil {
		return target, http.StatusOK, ctldapi.PauseResponse{}
	}
	if errors.Is(err, ErrNotImplemented) {
		return Target{}, http.StatusNotImplemented, ctldapi.PauseResponse{Paused: false, Error: err.Error()}
	}
	if errors.Is(err, ErrSandboxNotFound) || errors.Is(err, ErrPodNotFound) {
		return Target{}, http.StatusNotFound, ctldapi.PauseResponse{Paused: false, Error: err.Error()}
	}
	return Target{}, http.StatusInternalServerError, ctldapi.PauseResponse{Paused: false, Error: err.Error()}
}

func (c *Controller) pauseUsage(ctx context.Context, target Target) (*ctldapi.SandboxResourceUsage, error) {
	fallback, fallbackErr := c.cgroupPauseUsage(target.CgroupDir)
	if c == nil || c.StatsProvider == nil {
		if fallbackErr != nil {
			return nil, fmt.Errorf("read settled memory.current: %w", fallbackErr)
		}
		return fallback, nil
	}

	statsUsage, statsErr := c.StatsProvider.SandboxResourceUsage(ctx, target)
	usage := mergeSandboxResourceUsage(fallback, statsUsage)
	if usage != nil {
		return usage, nil
	}
	if statsErr == nil && fallbackErr == nil {
		return nil, fmt.Errorf("sandbox usage unavailable")
	}
	if statsErr != nil && fallbackErr != nil {
		return nil, fmt.Errorf("collect sandbox usage: cri stats: %v; cgroup fallback: %v", statsErr, fallbackErr)
	}
	if statsErr != nil {
		return nil, fmt.Errorf("collect sandbox usage from cri stats: %w", statsErr)
	}
	return nil, fmt.Errorf("read settled memory.current: %w", fallbackErr)
}

func (c *Controller) cgroupPauseUsage(dir string) (*ctldapi.SandboxResourceUsage, error) {
	if c == nil || c.FS == nil {
		return nil, fmt.Errorf("cgroup fs is not configured")
	}
	memoryCurrent, err := c.FS.SettledMemoryCurrent(dir)
	if err != nil {
		return nil, err
	}
	return &ctldapi.SandboxResourceUsage{
		ContainerMemoryUsage:      memoryCurrent,
		ContainerMemoryLimit:      memoryCurrent,
		ContainerMemoryWorkingSet: memoryCurrent,
		TotalMemoryRSS:            memoryCurrent,
	}, nil
}

func mergeSandboxResourceUsage(base, override *ctldapi.SandboxResourceUsage) *ctldapi.SandboxResourceUsage {
	if base == nil && override == nil {
		return nil
	}
	out := &ctldapi.SandboxResourceUsage{}
	if base != nil {
		*out = *base
	}
	if override == nil {
		return out
	}
	if override.ContainerMemoryUsage > 0 {
		out.ContainerMemoryUsage = override.ContainerMemoryUsage
	}
	if override.ContainerMemoryLimit > 0 {
		out.ContainerMemoryLimit = override.ContainerMemoryLimit
	}
	if override.ContainerMemoryWorkingSet > 0 {
		out.ContainerMemoryWorkingSet = override.ContainerMemoryWorkingSet
	}
	if override.TotalMemoryRSS > 0 {
		out.TotalMemoryRSS = override.TotalMemoryRSS
	}
	if override.TotalMemoryVMS > 0 {
		out.TotalMemoryVMS = override.TotalMemoryVMS
	}
	if override.TotalOpenFiles > 0 {
		out.TotalOpenFiles = override.TotalOpenFiles
	}
	if override.TotalThreadCount > 0 {
		out.TotalThreadCount = override.TotalThreadCount
	}
	if override.TotalIOReadBytes > 0 {
		out.TotalIOReadBytes = override.TotalIOReadBytes
	}
	if override.TotalIOWriteBytes > 0 {
		out.TotalIOWriteBytes = override.TotalIOWriteBytes
	}
	if override.ContextCount > 0 {
		out.ContextCount = override.ContextCount
	}
	if override.RunningContextCount > 0 {
		out.RunningContextCount = override.RunningContextCount
	}
	if override.PausedContextCount > 0 {
		out.PausedContextCount = override.PausedContextCount
	}
	return out
}
