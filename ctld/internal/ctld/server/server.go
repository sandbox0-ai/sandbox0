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
	AttachVolumeOwner(r *http.Request, req ctldapi.AttachVolumeOwnerRequest) (ctldapi.AttachVolumeOwnerResponse, int)
	PrepareVolumePortalHandoff(r *http.Request, req ctldapi.PrepareVolumePortalHandoffRequest) (ctldapi.PrepareVolumePortalHandoffResponse, int)
	CompleteVolumePortalHandoff(r *http.Request, req ctldapi.CompleteVolumePortalHandoffRequest) (ctldapi.CompleteVolumePortalHandoffResponse, int)
	AbortVolumePortalHandoff(r *http.Request, req ctldapi.AbortVolumePortalHandoffRequest) (ctldapi.AbortVolumePortalHandoffResponse, int)
}

type MountedVolumeController interface {
	MountedVolumeHandler() http.Handler
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

func NewMux(controller Controller) http.Handler {
	if controller == nil {
		controller = NotImplementedController{}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("/metrics", promhttp.Handler())
	if mountedController, ok := controller.(MountedVolumeController); ok {
		if mountedHandler := mountedController.MountedVolumeHandler(); mountedHandler != nil {
			mux.Handle("/sandboxvolumes/", mountedHandler)
		}
	}
	mux.HandleFunc("/api/v1/volume-portals/bind", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		volumeController, ok := controller.(VolumePortalController)
		if !ok {
			w.WriteHeader(http.StatusNotImplemented)
			_ = json.NewEncoder(w).Encode(ctldapi.BindVolumePortalResponse{})
			return
		}
		var req ctldapi.BindVolumePortalRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		resp, status := volumeController.BindVolumePortal(r, req)
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/api/v1/volume-portals/unbind", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		volumeController, ok := controller.(VolumePortalController)
		if !ok {
			w.WriteHeader(http.StatusNotImplemented)
			_ = json.NewEncoder(w).Encode(ctldapi.UnbindVolumePortalResponse{Error: "ctld volume portals not implemented"})
			return
		}
		var req ctldapi.UnbindVolumePortalRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(ctldapi.UnbindVolumePortalResponse{Error: err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		resp, status := volumeController.UnbindVolumePortal(r, req)
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/api/v1/volume-portals/owners/attach", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		volumeController, ok := controller.(VolumePortalController)
		if !ok {
			w.WriteHeader(http.StatusNotImplemented)
			_ = json.NewEncoder(w).Encode(ctldapi.AttachVolumeOwnerResponse{Error: "ctld volume owners not implemented"})
			return
		}
		var req ctldapi.AttachVolumeOwnerRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(ctldapi.AttachVolumeOwnerResponse{Error: err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		resp, status := volumeController.AttachVolumeOwner(r, req)
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/api/v1/volume-portals/handoffs/prepare", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		volumeController, ok := controller.(VolumePortalController)
		if !ok {
			w.WriteHeader(http.StatusNotImplemented)
			_ = json.NewEncoder(w).Encode(ctldapi.PrepareVolumePortalHandoffResponse{Error: "ctld volume portal handoff not implemented"})
			return
		}
		var req ctldapi.PrepareVolumePortalHandoffRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(ctldapi.PrepareVolumePortalHandoffResponse{Error: err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		resp, status := volumeController.PrepareVolumePortalHandoff(r, req)
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/api/v1/volume-portals/handoffs/complete", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		volumeController, ok := controller.(VolumePortalController)
		if !ok {
			w.WriteHeader(http.StatusNotImplemented)
			_ = json.NewEncoder(w).Encode(ctldapi.CompleteVolumePortalHandoffResponse{Error: "ctld volume portal handoff not implemented"})
			return
		}
		var req ctldapi.CompleteVolumePortalHandoffRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(ctldapi.CompleteVolumePortalHandoffResponse{Error: err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		resp, status := volumeController.CompleteVolumePortalHandoff(r, req)
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/api/v1/volume-portals/handoffs/abort", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		volumeController, ok := controller.(VolumePortalController)
		if !ok {
			w.WriteHeader(http.StatusNotImplemented)
			_ = json.NewEncoder(w).Encode(ctldapi.AbortVolumePortalHandoffResponse{Error: "ctld volume portal handoff not implemented"})
			return
		}
		var req ctldapi.AbortVolumePortalHandoffRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(ctldapi.AbortVolumePortalHandoffResponse{Error: err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		resp, status := volumeController.AbortVolumePortalHandoff(r, req)
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(resp)
	})
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
