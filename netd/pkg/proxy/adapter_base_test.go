package proxy

import "testing"

func TestAdapterCapabilities(t *testing.T) {
	tests := []struct {
		name       string
		adapter    proxyAdapter
		capability adapterCapability
	}{
		{name: "http inspect", adapter: &httpAdapter{}, capability: adapterCapabilityInspect},
		{name: "postgres passthrough", adapter: &postgresAdapter{}, capability: adapterCapabilityPassThrough},
		{name: "tls passthrough", adapter: &tlsAdapter{}, capability: adapterCapabilityPassThrough},
		{name: "ssh passthrough", adapter: &sshAdapter{}, capability: adapterCapabilityPassThrough},
		{name: "udp passthrough", adapter: &udpAdapter{}, capability: adapterCapabilityPassThrough},
		{name: "tcp fallback passthrough", adapter: &tcpPassThroughAdapter{}, capability: adapterCapabilityPassThrough},
		{name: "udp fallback passthrough", adapter: &udpPassThroughAdapter{}, capability: adapterCapabilityPassThrough},
	}

	for _, tt := range tests {
		if got := tt.adapter.Capability(); got != tt.capability {
			t.Fatalf("%s capability = %q, want %q", tt.name, got, tt.capability)
		}
	}
}

func TestAdapterCapabilityOfNil(t *testing.T) {
	if got := adapterCapabilityOf(nil); got != "" {
		t.Fatalf("adapterCapabilityOf(nil) = %q, want empty", got)
	}
}
