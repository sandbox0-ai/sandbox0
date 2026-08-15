package server

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
)

type Controller interface {
	Pause(r *http.Request, sandboxID string) (ctldapi.PauseResponse, int)
	Resume(r *http.Request, sandboxID string) (ctldapi.ResumeResponse, int)
	Probe(r *http.Request, sandboxID string, kind sandboxprobe.Kind) (sandboxprobe.Response, int)
	ProbePod(r *http.Request, namespace, name string, kind sandboxprobe.Kind) (sandboxprobe.Response, int)
}

type RootFSController interface {
	InspectRootFS(r *http.Request, req ctldapi.InspectRootFSRequest) (ctldapi.InspectRootFSResponse, int)
	SaveRootFS(r *http.Request, req ctldapi.SaveRootFSRequest) (ctldapi.SaveRootFSResponse, int)
	ApplyRootFS(r *http.Request, req ctldapi.ApplyRootFSRequest) (ctldapi.ApplyRootFSResponse, int)
}

type RootFSSnapshotController interface {
	PrepareRootFSSnapshot(r *http.Request, req ctldapi.PrepareRootFSSnapshotRequest) (ctldapi.PrepareRootFSSnapshotResponse, int)
	PublishRootFSSnapshot(r *http.Request, req ctldapi.PublishRootFSSnapshotRequest) (ctldapi.PublishRootFSSnapshotResponse, int)
	AbortRootFSSnapshot(r *http.Request, req ctldapi.AbortRootFSSnapshotRequest) (ctldapi.AbortRootFSSnapshotResponse, int)
}

// ReadinessController contributes primary service state to the ctld ready
// endpoint.
type ReadinessController interface {
	Ready() bool
}

// HealthController contributes fatal primary service state to the ctld health
// endpoint. A failed health check causes the HA liveness probe to restart the
// primary so its peer can take over.
type HealthController interface {
	Healthy() bool
}

type NotImplementedController struct{}

func (NotImplementedController) Pause(_ *http.Request, _ string) (ctldapi.PauseResponse, int) {
	return ctldapi.PauseResponse{Paused: false, Error: "ctld pause not implemented"}, http.StatusNotImplemented
}

func (NotImplementedController) Resume(_ *http.Request, _ string) (ctldapi.ResumeResponse, int) {
	return ctldapi.ResumeResponse{Resumed: false, Error: "ctld resume not implemented"}, http.StatusNotImplemented
}

func (NotImplementedController) Probe(_ *http.Request, _ string, kind sandboxprobe.Kind) (sandboxprobe.Response, int) {
	return sandboxprobe.Failed(kind, "ProbeNotImplemented", "ctld sandbox probe not implemented", nil), http.StatusNotImplemented
}

func (NotImplementedController) ProbePod(_ *http.Request, _, _ string, kind sandboxprobe.Kind) (sandboxprobe.Response, int) {
	return sandboxprobe.Failed(kind, "ProbeNotImplemented", "ctld pod probe not implemented", nil), http.StatusNotImplemented
}

func (NotImplementedController) InspectRootFS(_ *http.Request, _ ctldapi.InspectRootFSRequest) (ctldapi.InspectRootFSResponse, int) {
	return ctldapi.InspectRootFSResponse{Error: "ctld rootfs inspect not implemented"}, http.StatusNotImplemented
}

func (NotImplementedController) SaveRootFS(_ *http.Request, _ ctldapi.SaveRootFSRequest) (ctldapi.SaveRootFSResponse, int) {
	return ctldapi.SaveRootFSResponse{Error: "ctld rootfs save not implemented"}, http.StatusNotImplemented
}

func (NotImplementedController) PrepareRootFSSnapshot(_ *http.Request, _ ctldapi.PrepareRootFSSnapshotRequest) (ctldapi.PrepareRootFSSnapshotResponse, int) {
	return ctldapi.PrepareRootFSSnapshotResponse{Error: "ctld rootfs snapshot prepare not implemented"}, http.StatusNotImplemented
}

func (NotImplementedController) PublishRootFSSnapshot(_ *http.Request, _ ctldapi.PublishRootFSSnapshotRequest) (ctldapi.PublishRootFSSnapshotResponse, int) {
	return ctldapi.PublishRootFSSnapshotResponse{Error: "ctld rootfs snapshot publish not implemented"}, http.StatusNotImplemented
}

func (NotImplementedController) AbortRootFSSnapshot(_ *http.Request, _ ctldapi.AbortRootFSSnapshotRequest) (ctldapi.AbortRootFSSnapshotResponse, int) {
	return ctldapi.AbortRootFSSnapshotResponse{Error: "ctld rootfs snapshot abort not implemented"}, http.StatusNotImplemented
}

func (NotImplementedController) ApplyRootFS(_ *http.Request, _ ctldapi.ApplyRootFSRequest) (ctldapi.ApplyRootFSResponse, int) {
	return ctldapi.ApplyRootFSResponse{Error: "ctld rootfs apply not implemented"}, http.StatusNotImplemented
}

// registerJSONPostRoute keeps the control-plane JSON routes consistent while
// preserving each route's request and response types.
func registerJSONPostRoute[Request any, Response any, Target any](
	mux *http.ServeMux,
	path string,
	controller Controller,
	resolve func(Controller) (Target, bool),
	unsupportedResponse any,
	invalidRequestResponse func(error) any,
	handle func(Target, *http.Request, Request) (Response, int),
) {
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		target, ok := resolve(controller)
		if !ok {
			writeJSONResponse(w, http.StatusNotImplemented, unsupportedResponse)
			return
		}

		var req Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSONResponse(w, http.StatusBadRequest, invalidRequestResponse(err))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		resp, status := handle(target, r, req)
		writeJSONResponse(w, status, resp)
	})
}

func writeJSONResponse(w http.ResponseWriter, status int, response any) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(response)
}

func rootFSController(controller Controller) (RootFSController, bool) {
	rootFSController, ok := controller.(RootFSController)
	return rootFSController, ok
}

func rootFSSnapshotController(controller Controller) (RootFSSnapshotController, bool) {
	rootFSController, ok := controller.(RootFSSnapshotController)
	return rootFSController, ok
}

func NewMux(controller Controller) http.Handler {
	if controller == nil {
		controller = NotImplementedController{}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if healthController, ok := controller.(HealthController); ok && !healthController.Healthy() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("unhealthy"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if readinessController, ok := controller.(ReadinessController); ok && !readinessController.Ready() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("/metrics", promhttp.Handler())
	registerJSONPostRoute(mux, "/api/v1/rootfs/inspect", controller, rootFSController,
		ctldapi.InspectRootFSResponse{Error: "ctld rootfs inspect not implemented"},
		func(err error) any { return ctldapi.InspectRootFSResponse{Error: err.Error()} },
		func(c RootFSController, r *http.Request, req ctldapi.InspectRootFSRequest) (ctldapi.InspectRootFSResponse, int) {
			return c.InspectRootFS(r, req)
		},
	)
	registerJSONPostRoute(mux, "/api/v1/rootfs/save", controller, rootFSController,
		ctldapi.SaveRootFSResponse{Error: "ctld rootfs save not implemented"},
		func(err error) any { return ctldapi.SaveRootFSResponse{Error: err.Error()} },
		func(c RootFSController, r *http.Request, req ctldapi.SaveRootFSRequest) (ctldapi.SaveRootFSResponse, int) {
			return c.SaveRootFS(r, req)
		},
	)
	registerJSONPostRoute(mux, "/api/v1/rootfs/snapshots/prepare", controller, rootFSSnapshotController,
		ctldapi.PrepareRootFSSnapshotResponse{Error: "ctld rootfs snapshot prepare not implemented"},
		func(err error) any { return ctldapi.PrepareRootFSSnapshotResponse{Error: err.Error()} },
		func(c RootFSSnapshotController, r *http.Request, req ctldapi.PrepareRootFSSnapshotRequest) (ctldapi.PrepareRootFSSnapshotResponse, int) {
			return c.PrepareRootFSSnapshot(r, req)
		},
	)
	registerJSONPostRoute(mux, "/api/v1/rootfs/snapshots/publish", controller, rootFSSnapshotController,
		ctldapi.PublishRootFSSnapshotResponse{Error: "ctld rootfs snapshot publish not implemented"},
		func(err error) any { return ctldapi.PublishRootFSSnapshotResponse{Error: err.Error()} },
		func(c RootFSSnapshotController, r *http.Request, req ctldapi.PublishRootFSSnapshotRequest) (ctldapi.PublishRootFSSnapshotResponse, int) {
			return c.PublishRootFSSnapshot(r, req)
		},
	)
	registerJSONPostRoute(mux, "/api/v1/rootfs/snapshots/abort", controller, rootFSSnapshotController,
		ctldapi.AbortRootFSSnapshotResponse{Error: "ctld rootfs snapshot abort not implemented"},
		func(err error) any { return ctldapi.AbortRootFSSnapshotResponse{Error: err.Error()} },
		func(c RootFSSnapshotController, r *http.Request, req ctldapi.AbortRootFSSnapshotRequest) (ctldapi.AbortRootFSSnapshotResponse, int) {
			return c.AbortRootFSSnapshot(r, req)
		},
	)
	registerJSONPostRoute(mux, "/api/v1/rootfs/apply", controller, rootFSController,
		ctldapi.ApplyRootFSResponse{Error: "ctld rootfs apply not implemented"},
		func(err error) any { return ctldapi.ApplyRootFSResponse{Error: err.Error()} },
		func(c RootFSController, r *http.Request, req ctldapi.ApplyRootFSRequest) (ctldapi.ApplyRootFSResponse, int) {
			return c.ApplyRootFS(r, req)
		},
	)
	mux.HandleFunc("/api/v1/sandboxes/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		path := strings.TrimPrefix(r.URL.Path, "/api/v1/sandboxes/")
		parts := strings.Split(strings.Trim(path, "/"), "/")
		if len(parts) < 2 || parts[0] == "" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		sandboxID := parts[0]
		switch {
		case len(parts) == 2 && parts[1] == "pause":
			resp, status := controller.Pause(r, sandboxID)
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(resp)
		case len(parts) == 2 && parts[1] == "resume":
			resp, status := controller.Resume(r, sandboxID)
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(resp)
		case len(parts) == 3 && parts[1] == "probes":
			resp, status := controller.Probe(r, sandboxID, sandboxprobe.Kind(parts[2]))
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(resp)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	mux.HandleFunc("/api/v1/pods/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		path := strings.TrimPrefix(r.URL.Path, "/api/v1/pods/")
		parts := strings.Split(strings.Trim(path, "/"), "/")
		if len(parts) != 4 || parts[0] == "" || parts[1] == "" || parts[2] != "probes" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		resp, status := controller.ProbePod(r, parts[0], parts[1], sandboxprobe.Kind(parts[3]))
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(resp)
	})
	return mux
}
