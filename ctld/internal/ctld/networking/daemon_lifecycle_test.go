package networking

import (
	"context"
	"testing"
	"time"

	apiconfig "github.com/sandbox0-ai/sandbox0/pkg/config"
	"go.uber.org/zap"
)

type orderedRuntimeResource struct {
	name   string
	events chan<- string
}

func (r *orderedRuntimeResource) Close() {
	r.events <- r.name
}

func TestShutdownClosesRuntimeResourcesAfterMeteringLoopStops(t *testing.T) {
	events := make(chan string, 3)
	meteringDone := make(chan struct{})
	d := &Daemon{
		cfg:    &apiconfig.NetworkRuntimeConfig{},
		logger: zap.NewNop(),
	}
	d.registerRuntimeResources(
		&orderedRuntimeResource{name: "conntrack", events: events},
		&orderedRuntimeResource{name: "metering", events: events},
	)
	d.runtimeMu.Lock()
	d.meteringDone = meteringDone
	d.runtimeMu.Unlock()

	go func() {
		events <- "flush"
		close(meteringDone)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := d.shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	want := []string{"flush", "metering", "conntrack"}
	for _, expected := range want {
		select {
		case got := <-events:
			if got != expected {
				t.Fatalf("event = %q, want %q", got, expected)
			}
		default:
			t.Fatalf("missing event %q", expected)
		}
	}
}

func TestReadyReflectsSynchronizedRuntimeState(t *testing.T) {
	d := &Daemon{}
	if d.Ready() {
		t.Fatal("new daemon is ready")
	}

	d.ready.Store(true)
	if !d.Ready() {
		t.Fatal("daemon did not report synchronized state")
	}

	d.ready.Store(false)
	if d.Ready() {
		t.Fatal("daemon remained ready after synchronization was lost")
	}
}

func TestNewCopiesRuntimeSlotNetworkPaths(t *testing.T) {
	d := New(&apiconfig.NetworkRuntimeConfig{}, zap.NewNop(), nil, Options{
		RuntimeSlotStatePath:     "/var/lib/sandbox0/ctld/runtime-slot-network.db",
		RuntimeSlotControlSocket: "/host-run/sandbox0/ctld-runtime-slot-network.sock",
		RuntimeSlotNetNSRoot:     "/host-run/netns",
	})
	if d.runtimeSlotStatePath != "/var/lib/sandbox0/ctld/runtime-slot-network.db" ||
		d.runtimeSlotControlSocket != "/host-run/sandbox0/ctld-runtime-slot-network.sock" ||
		d.runtimeSlotNetNSRoot != "/host-run/netns" {
		t.Fatalf("runtime slot network paths = %q, %q, %q", d.runtimeSlotStatePath, d.runtimeSlotControlSocket, d.runtimeSlotNetNSRoot)
	}
}

func TestRedirectBypassCIDRsCombinesConfiguredRanges(t *testing.T) {
	got := redirectBypassCIDRs(
		[]string{"10.96.0.10", "10.244.0.53"},
		[]string{"10.96.0.20/32", "192.168.1.1"},
	)
	want := []string{"10.96.0.10", "10.244.0.53", "10.96.0.20/32", "192.168.1.1"}
	if len(got) != len(want) {
		t.Fatalf("cidrs = %#v, want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("cidrs = %#v, want %#v", got, want)
		}
	}
}
