package promutil

import "testing"

func TestMetricPrefix(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "hyphenated service", in: "cluster-gateway", want: "cluster_gateway"},
		{name: "mixed case", in: "StorageProxy", want: "storageproxy"},
		{name: "leading digit", in: "9service", want: "_9service"},
		{name: "empty", in: "", want: "sandbox0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MetricPrefix(tt.in); got != tt.want {
				t.Fatalf("MetricPrefix(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
