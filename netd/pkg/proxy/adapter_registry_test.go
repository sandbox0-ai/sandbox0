package proxy

import "testing"

type stubAdapter struct {
	name       string
	transport  string
	protocol   string
	capability adapterCapability
}

func (a stubAdapter) Name() string                  { return a.name }
func (a stubAdapter) Transport() string             { return a.transport }
func (a stubAdapter) Protocol() string              { return a.protocol }
func (a stubAdapter) Capability() adapterCapability { return a.capability }
func (a stubAdapter) Handle(*adapterRequest) error  { return nil }

func TestNewAdapterRegistryResolvesAdapters(t *testing.T) {
	registry, err := newAdapterRegistry(
		[]proxyAdapter{&httpAdapter{}, &udpAdapter{}},
		[]proxyAdapter{&tcpPassThroughAdapter{}, &udpPassThroughAdapter{}},
	)
	if err != nil {
		t.Fatalf("newAdapterRegistry returned error: %v", err)
	}

	httpAdapter, err := registry.Resolve(trafficDecision{
		Action:    decisionActionUseAdapter,
		Transport: "tcp",
		Protocol:  "http",
	})
	if err != nil {
		t.Fatalf("resolve http adapter: %v", err)
	}
	if httpAdapter.Name() != "http" {
		t.Fatalf("resolved adapter = %q, want http", httpAdapter.Name())
	}

	fallbackAdapter, err := registry.Resolve(trafficDecision{
		Action:    decisionActionPassThrough,
		Transport: "udp",
	})
	if err != nil {
		t.Fatalf("resolve udp fallback: %v", err)
	}
	if fallbackAdapter.Name() != "udp-pass-through" {
		t.Fatalf("resolved fallback = %q, want udp-pass-through", fallbackAdapter.Name())
	}
}

func TestNewAdapterRegistryRejectsDuplicateProtocolRegistration(t *testing.T) {
	_, err := newAdapterRegistry(
		[]proxyAdapter{&httpAdapter{}, &httpAdapter{}},
		[]proxyAdapter{&tcpPassThroughAdapter{}},
	)
	if err == nil {
		t.Fatalf("expected duplicate adapter registration to fail")
	}
}

func TestNewAdapterRegistryRejectsNonPassthroughFallback(t *testing.T) {
	_, err := newAdapterRegistry(
		nil,
		[]proxyAdapter{
			stubAdapter{
				name:       "tcp-inspect-fallback",
				transport:  "tcp",
				protocol:   "unknown",
				capability: adapterCapabilityInspect,
			},
		},
	)
	if err == nil {
		t.Fatalf("expected non-passthrough fallback registration to fail")
	}
}

func TestNewAdapterRegistryRejectsFallbackInPrimaryRegistry(t *testing.T) {
	_, err := newAdapterRegistry(
		[]proxyAdapter{&tcpPassThroughAdapter{}},
		nil,
	)
	if err == nil {
		t.Fatalf("expected fallback adapter in primary registry to fail")
	}
}

func TestResolveReturnsErrorForMissingAdapter(t *testing.T) {
	registry, err := newAdapterRegistry(
		[]proxyAdapter{&httpAdapter{}},
		[]proxyAdapter{&tcpPassThroughAdapter{}},
	)
	if err != nil {
		t.Fatalf("newAdapterRegistry returned error: %v", err)
	}

	_, err = registry.Resolve(trafficDecision{
		Action:    decisionActionUseAdapter,
		Transport: "tcp",
		Protocol:  "postgres",
	})
	if err == nil {
		t.Fatalf("expected missing adapter lookup to fail")
	}
}

func TestResolveRejectsFallbackCapabilityMismatch(t *testing.T) {
	registry := &adapterRegistry{
		fallbacks: map[string]proxyAdapter{
			"tcp": stubAdapter{
				name:       "bad-fallback",
				transport:  "tcp",
				protocol:   "unknown",
				capability: adapterCapabilityInspect,
			},
		},
	}

	_, err := registry.Resolve(trafficDecision{
		Action:    decisionActionPassThrough,
		Transport: "tcp",
	})
	if err == nil {
		t.Fatalf("expected fallback capability mismatch to fail")
	}
}
