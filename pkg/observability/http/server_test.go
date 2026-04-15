package http

import (
	stdhttp "net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestGinMiddlewareRecordsServerMetrics(t *testing.T) {
	gin.SetMode(gin.TestMode)
	registry := prometheus.NewRegistry()
	router := gin.New()
	router.Use(GinMiddleware(ServerConfig{
		ServiceName: "cluster-gateway",
		Tracer:      noop.NewTracerProvider().Tracer("test"),
		Registry:    registry,
	}))
	router.POST("/api/v1/sandboxes/:id", func(c *gin.Context) {
		c.String(stdhttp.StatusCreated, "ok")
	})

	req := httptest.NewRequest(stdhttp.MethodPost, "/api/v1/sandboxes/sandbox-12345678", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	got, ok := metricValue(t, registry, "cluster_gateway_http_server_requests_total", map[string]string{
		"method": "POST",
		"route":  "/api/v1/sandboxes/:id",
		"status": "201",
	})
	if !ok || got != 1 {
		t.Fatalf("server request metric = %v, ok=%v; want 1", got, ok)
	}
}

func TestServerMiddlewareNormalizesUnknownRoutes(t *testing.T) {
	registry := prometheus.NewRegistry()
	handler := ServerMiddleware(ServerConfig{
		ServiceName: "procd",
		Tracer:      noop.NewTracerProvider().Tracer("test"),
		Registry:    registry,
	})(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.WriteHeader(stdhttp.StatusAccepted)
	}))

	req := httptest.NewRequest(stdhttp.MethodPost, "/api/v1/contexts/ctx-12345678/restart", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	got, ok := metricValue(t, registry, "procd_http_server_requests_total", map[string]string{
		"method": "POST",
		"route":  "/api/v1/contexts/{id}/restart",
		"status": "202",
	})
	if !ok || got != 1 {
		t.Fatalf("server request metric = %v, ok=%v; want 1", got, ok)
	}
}

func TestOutboundHostLabelCollapsesIPAddresses(t *testing.T) {
	req := httptest.NewRequest(stdhttp.MethodGet, "http://10.0.0.12:8080/api/v1/sandboxes", nil)
	if got := outboundHostLabel(req); got != "ip" {
		t.Fatalf("outboundHostLabel(ip) = %q, want ip", got)
	}

	req = httptest.NewRequest(stdhttp.MethodGet, "http://manager:8080/api/v1/sandboxes", nil)
	if got := outboundHostLabel(req); got != "manager" {
		t.Fatalf("outboundHostLabel(service) = %q, want manager", got)
	}
}

func metricValue(t *testing.T, registry *prometheus.Registry, name string, labels map[string]string) (float64, bool) {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if labelsMatch(metric, labels) {
				if metric.Counter != nil {
					return metric.Counter.GetValue(), true
				}
				if metric.Gauge != nil {
					return metric.Gauge.GetValue(), true
				}
			}
		}
	}
	return 0, false
}

func labelsMatch(metric *dto.Metric, labels map[string]string) bool {
	for wantName, wantValue := range labels {
		found := false
		for _, label := range metric.GetLabel() {
			if label.GetName() == wantName && label.GetValue() == wantValue {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
