package server

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
)

type Controller interface {
	Pause(r *http.Request, sandboxID string) (ctldapi.PauseResponse, int)
	Resume(r *http.Request, sandboxID string) (ctldapi.ResumeResponse, int)
	Probe(r *http.Request, sandboxID string, kind sandboxprobe.Kind) (sandboxprobe.Response, int)
	ProbePod(r *http.Request, namespace, name string, kind sandboxprobe.Kind) (sandboxprobe.Response, int)
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
