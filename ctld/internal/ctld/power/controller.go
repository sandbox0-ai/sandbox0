package power

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
)

var ErrNotImplemented = errors.New("ctld power resolver not implemented")
var ErrSandboxNotFound = errors.New("sandbox not found")
var ErrPodNotFound = errors.New("pod not found")

type Target struct {
	SandboxID    string
	Runtime      string
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

type Controller struct {
	Resolver   Resolver
	HTTPClient *http.Client
}

func NewController(resolver Resolver) *Controller {
	return &Controller{Resolver: resolver}
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

func (c *Controller) Pause(_ *http.Request, _ string) (ctldapi.PauseResponse, int) {
	return ctldapi.PauseResponse{Paused: false, Error: "ctld cgroup pause has been removed"}, http.StatusNotImplemented
}

func (c *Controller) Resume(_ *http.Request, _ string) (ctldapi.ResumeResponse, int) {
	return ctldapi.ResumeResponse{Resumed: false, Error: "ctld cgroup resume has been removed"}, http.StatusNotImplemented
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
