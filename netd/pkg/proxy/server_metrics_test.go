package proxy

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"
)

func TestAcceptLoopTracksActiveTCPProxyConnectionsByListener(t *testing.T) {
	registry := prometheus.NewRegistry()
	connections := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "netd_proxy_connections_active",
		Help: "Current number of active downstream TCP proxy connections by listener.",
	}, []string{"listener"})
	registry.MustRegister(connections)

	server := &Server{
		logger: zap.NewNop(),
		metrics: &proxyMetricsRegistry{
			proxyConnectionsActive: connections,
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handlerStarted := make(chan struct{}, 2)
	releaseHandlers := make(chan struct{})
	handler := func(conn net.Conn) {
		defer conn.Close()
		handlerStarted <- struct{}{}
		<-releaseHandlers
	}

	type activeListener struct {
		name     string
		listener net.Listener
		client   net.Conn
		done     chan struct{}
	}
	listeners := make([]activeListener, 0, 2)
	for _, name := range []string{"http", "https"} {
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen %s: %v", name, err)
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			server.acceptLoop(ctx, name, listener, handler)
		}()
		client, err := net.Dial("tcp4", listener.Addr().String())
		if err != nil {
			_ = listener.Close()
			t.Fatalf("dial %s: %v", name, err)
		}
		listeners = append(listeners, activeListener{
			name:     name,
			listener: listener,
			client:   client,
			done:     done,
		})
	}

	for range listeners {
		select {
		case <-handlerStarted:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for proxy connection handler")
		}
	}
	for _, item := range listeners {
		if got := testutil.ToFloat64(connections.WithLabelValues(item.name)); got != 1 {
			t.Fatalf("active %s connections = %v, want 1", item.name, got)
		}
	}
	if got, err := testutil.GatherAndCount(registry, "netd_proxy_connections_active"); err != nil || got != 2 {
		t.Fatalf("gather netd_proxy_connections_active = %d, %v; want 2 labeled metrics", got, err)
	}

	close(releaseHandlers)
	for _, item := range listeners {
		waitForProxyGauge(t, connections.WithLabelValues(item.name), 0)
		_ = item.client.Close()
		_ = item.listener.Close()
		select {
		case <-item.done:
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for %s accept loop", item.name)
		}
	}
}

func waitForProxyGauge(t *testing.T, gauge prometheus.Gauge, want float64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if got := testutil.ToFloat64(gauge); got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("gauge = %v, want %v", testutil.ToFloat64(gauge), want)
}
