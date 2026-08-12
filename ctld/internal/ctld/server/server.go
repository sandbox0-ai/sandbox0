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

type VolumePortalController interface {
	BindVolumePortal(r *http.Request, req ctldapi.BindVolumePortalRequest) (ctldapi.BindVolumePortalResponse, int)
	UnbindVolumePortal(r *http.Request, req ctldapi.UnbindVolumePortalRequest) (ctldapi.UnbindVolumePortalResponse, int)
	CheckVolumePortals(r *http.Request, req ctldapi.CheckVolumePortalsRequest) (ctldapi.CheckVolumePortalsResponse, int)
	AttachVolumeOwner(r *http.Request, req ctldapi.AttachVolumeOwnerRequest) (ctldapi.AttachVolumeOwnerResponse, int)
	ReleaseVolumeOwner(r *http.Request, req ctldapi.ReleaseVolumeOwnerRequest) (ctldapi.ReleaseVolumeOwnerResponse, int)
	PrepareVolumeSnapshotCheckpoint(r *http.Request, req ctldapi.PrepareVolumeSnapshotCheckpointRequest) (ctldapi.PrepareVolumeSnapshotCheckpointResponse, int)
	CompleteVolumeSnapshotCheckpoint(r *http.Request, req ctldapi.CompleteVolumeSnapshotCheckpointRequest) (ctldapi.CompleteVolumeSnapshotCheckpointResponse, int)
	AbortVolumeSnapshotCheckpoint(r *http.Request, req ctldapi.AbortVolumeSnapshotCheckpointRequest) (ctldapi.AbortVolumeSnapshotCheckpointResponse, int)
}

type RootFSSyncController interface {
	BindRootFSSync(r *http.Request, req ctldapi.BindRootFSSyncRequest) (ctldapi.BindRootFSSyncResponse, int)
	GetRootFSSyncStatus(r *http.Request, req ctldapi.GetRootFSSyncStatusRequest) (ctldapi.GetRootFSSyncStatusResponse, int)
	SealRootFSHead(r *http.Request, req ctldapi.SealRootFSHeadRequest) (ctldapi.SealRootFSHeadResponse, int)
	AcknowledgeRootFSHead(r *http.Request, req ctldapi.AcknowledgeRootFSHeadRequest) (ctldapi.AcknowledgeRootFSHeadResponse, int)
	MaterializeRootFSHead(r *http.Request, req ctldapi.MaterializeRootFSHeadRequest) (ctldapi.MaterializeRootFSHeadResponse, int)
	ImportRootFSImage(r *http.Request, req ctldapi.ImportRootFSImageRequest) (ctldapi.ImportRootFSImageResponse, int)
	ReleaseCarrierGate(r *http.Request, req ctldapi.ReleaseCarrierGateRequest) (ctldapi.ReleaseCarrierGateResponse, int)
}

type MountedVolumeController interface {
	MountedVolumeHandler() http.Handler
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
	registerJSONRoute(mux, http.MethodPost, path, controller, resolve, unsupportedResponse, invalidRequestResponse, handle)
}

func registerJSONPutRoute[Request any, Response any, Target any](
	mux *http.ServeMux,
	path string,
	controller Controller,
	resolve func(Controller) (Target, bool),
	unsupportedResponse any,
	invalidRequestResponse func(error) any,
	handle func(Target, *http.Request, Request) (Response, int),
) {
	registerJSONRoute(mux, http.MethodPut, path, controller, resolve, unsupportedResponse, invalidRequestResponse, handle)
}

func registerJSONRoute[Request any, Response any, Target any](
	mux *http.ServeMux,
	method string,
	path string,
	controller Controller,
	resolve func(Controller) (Target, bool),
	unsupportedResponse any,
	invalidRequestResponse func(error) any,
	handle func(Target, *http.Request, Request) (Response, int),
) {
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
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

func volumePortalController(controller Controller) (VolumePortalController, bool) {
	volumeController, ok := controller.(VolumePortalController)
	return volumeController, ok
}

func rootFSSyncController(controller Controller) (RootFSSyncController, bool) {
	rootFSController, ok := controller.(RootFSSyncController)
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
	if mountedController, ok := controller.(MountedVolumeController); ok {
		if mountedHandler := mountedController.MountedVolumeHandler(); mountedHandler != nil {
			mux.Handle("/sandboxvolumes/", mountedHandler)
		}
	}
	registerJSONPostRoute(mux, "/api/v1/volume-portals/bind", controller, volumePortalController,
		ctldapi.BindVolumePortalResponse{},
		func(err error) any { return map[string]string{"error": err.Error()} },
		func(c VolumePortalController, r *http.Request, req ctldapi.BindVolumePortalRequest) (ctldapi.BindVolumePortalResponse, int) {
			return c.BindVolumePortal(r, req)
		},
	)
	registerJSONPostRoute(mux, "/api/v1/volume-portals/unbind", controller, volumePortalController,
		ctldapi.UnbindVolumePortalResponse{Error: "ctld volume portals not implemented"},
		func(err error) any { return ctldapi.UnbindVolumePortalResponse{Error: err.Error()} },
		func(c VolumePortalController, r *http.Request, req ctldapi.UnbindVolumePortalRequest) (ctldapi.UnbindVolumePortalResponse, int) {
			return c.UnbindVolumePortal(r, req)
		},
	)
	registerJSONPostRoute(mux, "/api/v1/volume-portals/check", controller, volumePortalController,
		ctldapi.CheckVolumePortalsResponse{Error: "ctld volume portals not implemented"},
		func(err error) any { return ctldapi.CheckVolumePortalsResponse{Error: err.Error()} },
		func(c VolumePortalController, r *http.Request, req ctldapi.CheckVolumePortalsRequest) (ctldapi.CheckVolumePortalsResponse, int) {
			return c.CheckVolumePortals(r, req)
		},
	)
	registerJSONPostRoute(mux, "/api/v1/volume-portals/owners/attach", controller, volumePortalController,
		ctldapi.AttachVolumeOwnerResponse{Error: "ctld volume owners not implemented"},
		func(err error) any { return ctldapi.AttachVolumeOwnerResponse{Error: err.Error()} },
		func(c VolumePortalController, r *http.Request, req ctldapi.AttachVolumeOwnerRequest) (ctldapi.AttachVolumeOwnerResponse, int) {
			return c.AttachVolumeOwner(r, req)
		},
	)
	registerJSONPostRoute(mux, "/api/v1/volume-portals/owners/release", controller, volumePortalController,
		ctldapi.ReleaseVolumeOwnerResponse{Error: "ctld volume owners not implemented"},
		func(err error) any { return ctldapi.ReleaseVolumeOwnerResponse{Error: err.Error()} },
		func(c VolumePortalController, r *http.Request, req ctldapi.ReleaseVolumeOwnerRequest) (ctldapi.ReleaseVolumeOwnerResponse, int) {
			return c.ReleaseVolumeOwner(r, req)
		},
	)
	registerJSONPostRoute(mux, "/api/v1/volume-portals/snapshot-checkpoints/prepare", controller, volumePortalController,
		ctldapi.PrepareVolumeSnapshotCheckpointResponse{Error: "ctld volume snapshot checkpoint not implemented"},
		func(err error) any { return ctldapi.PrepareVolumeSnapshotCheckpointResponse{Error: err.Error()} },
		func(c VolumePortalController, r *http.Request, req ctldapi.PrepareVolumeSnapshotCheckpointRequest) (ctldapi.PrepareVolumeSnapshotCheckpointResponse, int) {
			return c.PrepareVolumeSnapshotCheckpoint(r, req)
		},
	)
	registerJSONPostRoute(mux, "/api/v1/volume-portals/snapshot-checkpoints/complete", controller, volumePortalController,
		ctldapi.CompleteVolumeSnapshotCheckpointResponse{Error: "ctld volume snapshot checkpoint not implemented"},
		func(err error) any { return ctldapi.CompleteVolumeSnapshotCheckpointResponse{Error: err.Error()} },
		func(c VolumePortalController, r *http.Request, req ctldapi.CompleteVolumeSnapshotCheckpointRequest) (ctldapi.CompleteVolumeSnapshotCheckpointResponse, int) {
			return c.CompleteVolumeSnapshotCheckpoint(r, req)
		},
	)
	registerJSONPostRoute(mux, "/api/v1/volume-portals/snapshot-checkpoints/abort", controller, volumePortalController,
		ctldapi.AbortVolumeSnapshotCheckpointResponse{Error: "ctld volume snapshot checkpoint not implemented"},
		func(err error) any { return ctldapi.AbortVolumeSnapshotCheckpointResponse{Error: err.Error()} },
		func(c VolumePortalController, r *http.Request, req ctldapi.AbortVolumeSnapshotCheckpointRequest) (ctldapi.AbortVolumeSnapshotCheckpointResponse, int) {
			return c.AbortVolumeSnapshotCheckpoint(r, req)
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
	registerJSONPutRoute(mux, "/api/v1/rootfs/sync/bind", controller, rootFSSyncController,
		ctldapi.BindRootFSSyncResponse{Error: "ctld rootfs sync not implemented"},
		func(err error) any { return ctldapi.BindRootFSSyncResponse{Error: err.Error()} },
		func(c RootFSSyncController, r *http.Request, req ctldapi.BindRootFSSyncRequest) (ctldapi.BindRootFSSyncResponse, int) {
			return c.BindRootFSSync(r, req)
		})
	registerJSONPostRoute(mux, "/api/v1/rootfs/sync/status", controller, rootFSSyncController,
		ctldapi.GetRootFSSyncStatusResponse{Error: "ctld rootfs sync not implemented"},
		func(err error) any { return ctldapi.GetRootFSSyncStatusResponse{Error: err.Error()} },
		func(c RootFSSyncController, r *http.Request, req ctldapi.GetRootFSSyncStatusRequest) (ctldapi.GetRootFSSyncStatusResponse, int) {
			return c.GetRootFSSyncStatus(r, req)
		})
	registerJSONPutRoute(mux, "/api/v1/rootfs/heads/seal", controller, rootFSSyncController,
		ctldapi.SealRootFSHeadResponse{Error: "ctld rootfs sync not implemented"},
		func(err error) any { return ctldapi.SealRootFSHeadResponse{Error: err.Error()} },
		func(c RootFSSyncController, r *http.Request, req ctldapi.SealRootFSHeadRequest) (ctldapi.SealRootFSHeadResponse, int) {
			return c.SealRootFSHead(r, req)
		})
	registerJSONPutRoute(mux, "/api/v1/rootfs/heads/acknowledge", controller, rootFSSyncController,
		ctldapi.AcknowledgeRootFSHeadResponse{Error: "ctld rootfs sync not implemented"},
		func(err error) any { return ctldapi.AcknowledgeRootFSHeadResponse{Error: err.Error()} },
		func(c RootFSSyncController, r *http.Request, req ctldapi.AcknowledgeRootFSHeadRequest) (ctldapi.AcknowledgeRootFSHeadResponse, int) {
			return c.AcknowledgeRootFSHead(r, req)
		})
	registerJSONPutRoute(mux, "/api/v1/rootfs/heads/materialize", controller, rootFSSyncController,
		ctldapi.MaterializeRootFSHeadResponse{Error: "ctld rootfs sync not implemented"},
		func(err error) any { return ctldapi.MaterializeRootFSHeadResponse{Error: err.Error()} },
		func(c RootFSSyncController, r *http.Request, req ctldapi.MaterializeRootFSHeadRequest) (ctldapi.MaterializeRootFSHeadResponse, int) {
			return c.MaterializeRootFSHead(r, req)
		})
	registerJSONPutRoute(mux, "/api/v1/rootfs/images/import", controller, rootFSSyncController,
		ctldapi.ImportRootFSImageResponse{Error: "ctld S0FS ImageFS importer not implemented"},
		func(err error) any { return ctldapi.ImportRootFSImageResponse{Error: err.Error()} },
		func(c RootFSSyncController, r *http.Request, req ctldapi.ImportRootFSImageRequest) (ctldapi.ImportRootFSImageResponse, int) {
			return c.ImportRootFSImage(r, req)
		})
	registerJSONPutRoute(mux, "/api/v1/carriers/gate/release", controller, rootFSSyncController,
		ctldapi.ReleaseCarrierGateResponse{Error: "ctld carrier gate not implemented"},
		func(err error) any { return ctldapi.ReleaseCarrierGateResponse{Error: err.Error()} },
		func(c RootFSSyncController, r *http.Request, req ctldapi.ReleaseCarrierGateRequest) (ctldapi.ReleaseCarrierGateResponse, int) {
			return c.ReleaseCarrierGate(r, req)
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
