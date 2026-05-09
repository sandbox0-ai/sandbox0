package proxy

import "testing"

func TestAdapterCapabilities(t *testing.T) {
	tests := []struct {
		name       string
		adapter    proxyAdapter
		capability adapterCapability
	}{
		{name: "amqp passthrough", adapter: &amqpAdapter{}, capability: adapterCapabilityPassThrough},
		{name: "dns passthrough", adapter: &dnsAdapter{}, capability: adapterCapabilityPassThrough},
		{name: "http inspect", adapter: &httpAdapter{}, capability: adapterCapabilityInspect},
		{name: "mongodb passthrough", adapter: &mongodbAdapter{}, capability: adapterCapabilityPassThrough},
		{name: "mqtt inspect", adapter: &mqttAdapter{}, capability: adapterCapabilityInspect},
		{name: "redis inspect", adapter: &redisAdapter{}, capability: adapterCapabilityInspect},
		{name: "socks5 inspect", adapter: &socks5Adapter{}, capability: adapterCapabilityInspect},
		{name: "tls terminate", adapter: &tlsAdapter{}, capability: adapterCapabilityTerminate},
		{name: "ssh terminate", adapter: &sshAdapter{}, capability: adapterCapabilityTerminate},
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
