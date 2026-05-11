package daemon

import (
	"context"
	"testing"
	"time"

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
		cfg:    &apiconfig.NetdConfig{},
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
