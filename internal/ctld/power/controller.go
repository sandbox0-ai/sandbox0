package power

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/sandbox0-ai/sandbox0/internal/ctld/cgroup"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
)

var ErrNotImplemented = errors.New("ctld power resolver not implemented")
var ErrSandboxNotFound = errors.New("sandbox not found")

type Target struct {
	SandboxID string
	CgroupDir string
}

type Resolver interface {
	Resolve(r *http.Request, sandboxID string) (Target, error)
}

type Controller struct {
	Resolver Resolver
	FS       *cgroup.FS
}

func NewController(resolver Resolver, fs *cgroup.FS) *Controller {
	if fs == nil {
		fs = cgroup.NewFS()
	}
	return &Controller{Resolver: resolver, FS: fs}
}

func (c *Controller) Pause(r *http.Request, sandboxID string) (ctldapi.PauseResponse, int) {
	target, status, errResp := c.resolveTarget(r, sandboxID)
	if status != http.StatusOK {
		return errResp, status
	}
	if err := c.FS.Freeze(target.CgroupDir); err != nil {
		return ctldapi.PauseResponse{Paused: false, Error: fmt.Sprintf("freeze cgroup: %v", err)}, http.StatusInternalServerError
	}
	memoryCurrent, err := c.FS.MemoryCurrent(target.CgroupDir)
	if err != nil {
		return ctldapi.PauseResponse{Paused: false, Error: fmt.Sprintf("read memory.current: %v", err)}, http.StatusInternalServerError
	}
	return ctldapi.PauseResponse{
		Paused: true,
		ResourceUsage: &ctldapi.SandboxResourceUsage{
			ContainerMemoryUsage:      memoryCurrent,
			ContainerMemoryLimit:      memoryCurrent,
			ContainerMemoryWorkingSet: memoryCurrent,
			TotalMemoryRSS:            memoryCurrent,
		},
	}, http.StatusOK
}

func (c *Controller) Resume(r *http.Request, sandboxID string) (ctldapi.ResumeResponse, int) {
	target, status, pauseErr := c.resolveTarget(r, sandboxID)
	if status != http.StatusOK {
		return ctldapi.ResumeResponse{Resumed: false, Error: pauseErr.Error}, status
	}
	if err := c.FS.Thaw(target.CgroupDir); err != nil {
		return ctldapi.ResumeResponse{Resumed: false, Error: fmt.Sprintf("thaw cgroup: %v", err)}, http.StatusInternalServerError
	}
	return ctldapi.ResumeResponse{Resumed: true}, http.StatusOK
}

func (c *Controller) resolveTarget(r *http.Request, sandboxID string) (Target, int, ctldapi.PauseResponse) {
	if c == nil || c.Resolver == nil {
		return Target{}, http.StatusNotImplemented, ctldapi.PauseResponse{Paused: false, Error: ErrNotImplemented.Error()}
	}
	target, err := c.Resolver.Resolve(r, sandboxID)
	if err == nil {
		return target, http.StatusOK, ctldapi.PauseResponse{}
	}
	if errors.Is(err, ErrNotImplemented) {
		return Target{}, http.StatusNotImplemented, ctldapi.PauseResponse{Paused: false, Error: err.Error()}
	}
	if errors.Is(err, ErrSandboxNotFound) {
		return Target{}, http.StatusNotFound, ctldapi.PauseResponse{Paused: false, Error: err.Error()}
	}
	return Target{}, http.StatusInternalServerError, ctldapi.PauseResponse{Paused: false, Error: err.Error()}
}
