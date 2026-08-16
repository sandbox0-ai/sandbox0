package networking

import (
	"context"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/watcher"
	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
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

func TestNewCopiesRuntimeWatchTCPPorts(t *testing.T) {
	ports := []int{8096}
	d := New(&apiconfig.NetworkRuntimeConfig{}, zap.NewNop(), nil, Options{
		RuntimeWatchTCPPorts: ports,
	})
	ports[0] = 1

	if len(d.runtimeWatchTCPPorts) != 1 || d.runtimeWatchTCPPorts[0] != 8096 {
		t.Fatalf("runtime watch ports = %#v, want [8096]", d.runtimeWatchTCPPorts)
	}
}

func TestRedirectBypassCIDRsIncludesClusterDNSCIDRs(t *testing.T) {
	got := redirectBypassCIDRs(
		[]string{"10.96.0.10", "10.244.0.53"},
		[]string{"10.96.0.20/32"},
		[]string{"192.168.1.1"},
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

func TestClusterDNSCIDRsIncludesServiceAndEndpointIPs(t *testing.T) {
	got := clusterDNSCIDRs(
		"10.96.0.10",
		[]*watcher.ServiceInfo{{
			Namespace: "kube-system",
			Name:      "kube-dns",
			ClusterIP: "10.96.0.10",
		}, {
			Namespace: "sandbox0-system",
			Name:      "fullmode-manager",
			ClusterIP: "10.96.0.20",
		}},
		[]*watcher.EndpointsInfo{{
			Namespace: "kube-system",
			Name:      "kube-dns",
			Addresses: []string{"10.244.0.53", "10.244.1.53"},
		}, {
			Namespace: "sandbox0-system",
			Name:      "fullmode-manager",
			Addresses: []string{"10.244.0.20"},
		}},
	)
	want := []string{"10.96.0.10", "10.96.0.10", "10.244.0.53", "10.244.1.53"}
	if len(got) != len(want) {
		t.Fatalf("cidrs = %#v, want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("cidrs = %#v, want %#v", got, want)
		}
	}
}
