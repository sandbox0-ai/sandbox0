package clustergateway

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	gatewayhttp "github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/http"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/metering"
	"go.uber.org/zap"
)

func TestClusterGatewayIntegration_MeteringExportContract(t *testing.T) {
	baseTime := time.Date(2026, 3, 12, 10, 0, 0, 0, time.UTC)
	completeBefore := baseTime.Add(10 * time.Minute)
	reader := &testMeteringReader{
		status: &metering.Status{
			RegionID:           "aws-us-east-1",
			LatestEventCursor:  "event-2",
			LatestWindowCursor: "window-2",
			CompleteBefore:     &completeBefore,
			ProducerCount:      1,
		},
		events: []*metering.Event{
			{
				Sequence:    2,
				EventID:     "sandbox/sb-1/paused/2",
				Producer:    "manager.sandbox_lifecycle",
				RegionID:    "aws-us-east-1",
				EventType:   metering.EventTypeSandboxPaused,
				SubjectType: metering.SubjectTypeSandbox,
				SubjectID:   "sb-1",
				TeamID:      "team-1",
				UserID:      "user-1",
				SandboxID:   "sb-1",
				TemplateID:  "tpl-1",
				ClusterID:   "cluster-a",
				OccurredAt:  baseTime.Add(5 * time.Minute),
			},
		},
		windows: []*metering.Window{
			{
				Sequence:    2,
				WindowID:    "sandbox/sb-1/runtime/2",
				Producer:    "manager.sandbox_lifecycle",
				RegionID:    "aws-us-east-1",
				WindowType:  metering.WindowTypeSandboxRuntimeMiBMilliseconds,
				SubjectType: metering.SubjectTypeSandbox,
				SubjectID:   "sb-1",
				TeamID:      "team-1",
				UserID:      "user-1",
				SandboxID:   "sb-1",
				TemplateID:  "tpl-1",
				ClusterID:   "cluster-a",
				WindowStart: baseTime.Add(5 * time.Minute),
				WindowEnd:   baseTime.Add(7 * time.Minute),
				Value:       120_000,
				Unit:        metering.WindowUnitMiBMilliseconds,
			},
		},
	}

	keys := gatewayKeyPair{}
	keys.privateKey, keys.publicKey = writeClusterGatewayKeys(t)

	managerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(managerServer.Close)

	cfg := &config.ClusterGatewayConfig{
		ManagerURL:     managerServer.URL,
		AllowedCallers: []string{"regional-gateway"},
		GatewayConfig: config.GatewayConfig{
			RegionID: "aws-us-east-1",
		},
	}
	obsProvider := newTestObservability(t, "cluster-gateway-metering-test")
	server, err := gatewayhttp.NewServer(cfg, nil, zap.NewNop(), obsProvider, gatewayhttp.WithMeteringReader(reader))
	if err != nil {
		t.Fatalf("create cluster-gateway server: %v", err)
	}

	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)

	token := newInternalToken(t, internalauthGenerator(keys.privateKey), []string{"*:*"})

	resp, body := doGatewayRequest(t, httpServer.Client(), http.MethodGet, httpServer.URL+"/internal/v1/metering/status", token, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status endpoint returned %d: %s", resp.StatusCode, string(body))
	}
	status, apiErr, err := spec.DecodeResponse[metering.Status](bytes.NewReader(body))
	if err != nil {
		t.Fatalf("decode status response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected status api error: %+v", apiErr)
	}
	if status.LatestEventCursor != "event-2" {
		t.Fatalf("latest_event_cursor = %q, want event-2", status.LatestEventCursor)
	}
	if status.LatestWindowCursor != "window-2" {
		t.Fatalf("latest_window_cursor = %q, want window-2", status.LatestWindowCursor)
	}
	if status.ProducerCount != 1 {
		t.Fatalf("producer_count = %d, want 1", status.ProducerCount)
	}
	if status.CompleteBefore == nil || !status.CompleteBefore.Equal(completeBefore) {
		t.Fatalf("complete_before = %v, want %v", status.CompleteBefore, completeBefore)
	}

	resp, body = doGatewayRequest(t, httpServer.Client(), http.MethodGet, httpServer.URL+"/internal/v1/metering/events?cursor=event-1&limit=10", token, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("events endpoint returned %d: %s", resp.StatusCode, string(body))
	}
	eventsResp, apiErr, err := spec.DecodeResponse[struct {
		Events []*metering.Event `json:"events"`
	}](bytes.NewReader(body))
	if err != nil {
		t.Fatalf("decode events response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected events api error: %+v", apiErr)
	}
	if len(eventsResp.Events) != 1 {
		t.Fatalf("event count = %d, want 1", len(eventsResp.Events))
	}
	if reader.eventCursor != "event-1" {
		t.Fatalf("event cursor = %q, want event-1", reader.eventCursor)
	}
	if eventsResp.Events[0].Sequence != 2 || eventsResp.Events[0].EventType != metering.EventTypeSandboxPaused {
		t.Fatalf("unexpected event: %+v", eventsResp.Events[0])
	}

	resp, body = doGatewayRequest(t, httpServer.Client(), http.MethodGet, httpServer.URL+"/internal/v1/metering/windows?cursor=window-1&limit=10", token, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("windows endpoint returned %d: %s", resp.StatusCode, string(body))
	}
	windowsResp, apiErr, err := spec.DecodeResponse[struct {
		Windows []*metering.Window `json:"windows"`
	}](bytes.NewReader(body))
	if err != nil {
		t.Fatalf("decode windows response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected windows api error: %+v", apiErr)
	}
	if len(windowsResp.Windows) != 1 {
		t.Fatalf("window count = %d, want 1", len(windowsResp.Windows))
	}
	if reader.windowCursor != "window-1" {
		t.Fatalf("window cursor = %q, want window-1", reader.windowCursor)
	}
	if windowsResp.Windows[0].Sequence != 2 || windowsResp.Windows[0].WindowType != metering.WindowTypeSandboxRuntimeMiBMilliseconds {
		t.Fatalf("unexpected window: %+v", windowsResp.Windows[0])
	}
}

type testMeteringReader struct {
	status       *metering.Status
	events       []*metering.Event
	windows      []*metering.Window
	eventCursor  string
	windowCursor string
}

func (r *testMeteringReader) GetStatus(context.Context, string) (*metering.Status, error) {
	return r.status, nil
}

func (r *testMeteringReader) ListEvents(_ context.Context, cursor string, _ int) ([]*metering.Event, string, error) {
	r.eventCursor = cursor
	return r.events, "", nil
}

func (r *testMeteringReader) ListWindows(_ context.Context, cursor string, _ int) ([]*metering.Window, string, error) {
	r.windowCursor = cursor
	return r.windows, "", nil
}

func internalauthGenerator(privateKey internalauth.PrivateKeyType) *internalauth.Generator {
	return internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "regional-gateway",
		PrivateKey: privateKey,
		TTL:        time.Minute,
	})
}
