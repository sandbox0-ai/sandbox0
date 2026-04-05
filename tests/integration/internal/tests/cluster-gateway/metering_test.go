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
	ctx := context.Background()
	pool, _ := newIsolatedTestDatabasePool(t, "intgw_metering")
	if err := metering.RunMigrations(ctx, pool, testMigrateLogger{t: t}); err != nil {
		t.Fatalf("migrate metering schema: %v", err)
	}
	repo := metering.NewRepository(pool)

	baseTime := time.Date(2026, 3, 12, 10, 0, 0, 0, time.UTC)
	completeBefore := baseTime.Add(10 * time.Minute)

	if err := repo.AppendEvent(ctx, &metering.Event{
		EventID:     "sandbox/sb-1/claimed/1",
		Producer:    "manager.sandbox_lifecycle",
		RegionID:    "aws-us-east-1",
		EventType:   metering.EventTypeSandboxClaimed,
		SubjectType: metering.SubjectTypeSandbox,
		SubjectID:   "sb-1",
		TeamID:      "team-1",
		UserID:      "user-1",
		SandboxID:   "sb-1",
		TemplateID:  "tpl-1",
		ClusterID:   "cluster-a",
		OccurredAt:  baseTime,
	}); err != nil {
		t.Fatalf("append first event: %v", err)
	}
	if err := repo.AppendEvent(ctx, &metering.Event{
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
	}); err != nil {
		t.Fatalf("append second event: %v", err)
	}
	if err := repo.AppendWindow(ctx, &metering.Window{
		WindowID:    "sandbox/sb-1/active/1",
		Producer:    "manager.sandbox_lifecycle",
		RegionID:    "aws-us-east-1",
		WindowType:  metering.WindowTypeSandboxActiveSeconds,
		SubjectType: metering.SubjectTypeSandbox,
		SubjectID:   "sb-1",
		TeamID:      "team-1",
		UserID:      "user-1",
		SandboxID:   "sb-1",
		TemplateID:  "tpl-1",
		ClusterID:   "cluster-a",
		WindowStart: baseTime,
		WindowEnd:   baseTime.Add(5 * time.Minute),
		Value:       300,
		Unit:        metering.WindowUnitSeconds,
	}); err != nil {
		t.Fatalf("append first window: %v", err)
	}
	if err := repo.AppendWindow(ctx, &metering.Window{
		WindowID:    "sandbox/sb-1/paused/2",
		Producer:    "manager.sandbox_lifecycle",
		RegionID:    "aws-us-east-1",
		WindowType:  metering.WindowTypeSandboxPausedSeconds,
		SubjectType: metering.SubjectTypeSandbox,
		SubjectID:   "sb-1",
		TeamID:      "team-1",
		UserID:      "user-1",
		SandboxID:   "sb-1",
		TemplateID:  "tpl-1",
		ClusterID:   "cluster-a",
		WindowStart: baseTime.Add(5 * time.Minute),
		WindowEnd:   baseTime.Add(7 * time.Minute),
		Value:       120,
		Unit:        metering.WindowUnitSeconds,
	}); err != nil {
		t.Fatalf("append second window: %v", err)
	}
	if err := repo.UpsertProducerWatermark(ctx, "manager.sandbox_lifecycle", "aws-us-east-1", completeBefore); err != nil {
		t.Fatalf("upsert watermark: %v", err)
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
	server, err := gatewayhttp.NewServer(cfg, pool, zap.NewNop(), obsProvider)
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
	if status.LatestEventSequence != 2 {
		t.Fatalf("latest_event_sequence = %d, want 2", status.LatestEventSequence)
	}
	if status.LatestWindowSequence != 2 {
		t.Fatalf("latest_window_sequence = %d, want 2", status.LatestWindowSequence)
	}
	if status.ProducerCount != 1 {
		t.Fatalf("producer_count = %d, want 1", status.ProducerCount)
	}
	if status.CompleteBefore == nil || !status.CompleteBefore.Equal(completeBefore) {
		t.Fatalf("complete_before = %v, want %v", status.CompleteBefore, completeBefore)
	}

	resp, body = doGatewayRequest(t, httpServer.Client(), http.MethodGet, httpServer.URL+"/internal/v1/metering/events?after_sequence=1&limit=10", token, nil)
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
	if eventsResp.Events[0].Sequence != 2 || eventsResp.Events[0].EventType != metering.EventTypeSandboxPaused {
		t.Fatalf("unexpected event: %+v", eventsResp.Events[0])
	}

	resp, body = doGatewayRequest(t, httpServer.Client(), http.MethodGet, httpServer.URL+"/internal/v1/metering/windows?after_sequence=1&limit=10", token, nil)
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
	if windowsResp.Windows[0].Sequence != 2 || windowsResp.Windows[0].WindowType != metering.WindowTypeSandboxPausedSeconds {
		t.Fatalf("unexpected window: %+v", windowsResp.Windows[0])
	}
}

type testMigrateLogger struct {
	t *testing.T
}

func (l testMigrateLogger) Printf(string, ...any) {}

func (l testMigrateLogger) Fatalf(format string, args ...any) {
	l.t.Fatalf(format, args...)
}

func internalauthGenerator(privateKey internalauth.PrivateKeyType) *internalauth.Generator {
	return internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "regional-gateway",
		PrivateKey: privateKey,
		TTL:        time.Minute,
	})
}
