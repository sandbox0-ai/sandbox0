// Package server exposes ctld process health and Prometheus metrics. Privileged
// runtime control stays on root-owned Unix sockets.
package server

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Controller contributes primary service state to ctld health endpoints.
type Controller interface {
	Ready() bool
	Healthy() bool
}

// NewMux creates the unprivileged ctld HTTP surface.
func NewMux(controller Controller) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
		if controller != nil && !controller.Healthy() {
			http.Error(writer, "unhealthy", http.StatusServiceUnavailable)
			return
		}
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(writer http.ResponseWriter, _ *http.Request) {
		if controller != nil && !controller.Ready() {
			http.Error(writer, "not ready", http.StatusServiceUnavailable)
			return
		}
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ok"))
	})
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}
