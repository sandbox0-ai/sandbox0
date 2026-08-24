package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

type testController struct{ healthy, ready bool }

func (c testController) Healthy() bool { return c.healthy }
func (c testController) Ready() bool   { return c.ready }

func TestNewMuxExposesOnlyProcessEndpoints(t *testing.T) {
	handler := NewMux(testController{healthy: true, ready: false})
	for _, test := range []struct {
		path string
		want int
	}{
		{path: "/healthz", want: http.StatusOK},
		{path: "/readyz", want: http.StatusServiceUnavailable},
		{path: "/api/v1/rootfs/inspect", want: http.StatusNotFound},
		{path: "/api/v1/sandboxes/sandbox-1/probes/readiness", want: http.StatusNotFound},
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, test.path, nil))
		if recorder.Code != test.want {
			t.Fatalf("GET %s status = %d, want %d", test.path, recorder.Code, test.want)
		}
	}
}
