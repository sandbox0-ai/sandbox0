package sandboxobservability

import "testing"

func TestRuntimeMetricCatalogIsBoundedAndDefensivelyCopied(t *testing.T) {
	catalog := RuntimeMetricCatalogSnapshot()
	if len(catalog.Metrics) != 14 {
		t.Fatalf("metric count = %d, want 14", len(catalog.Metrics))
	}
	seen := map[RuntimeMetricName]struct{}{}
	for _, descriptor := range catalog.Metrics {
		if _, duplicate := seen[descriptor.Name]; duplicate {
			t.Fatalf("duplicate metric %q", descriptor.Name)
		}
		seen[descriptor.Name] = struct{}{}
		if descriptor.Kind == "" || descriptor.Unit == "" || descriptor.Description == "" {
			t.Fatalf("incomplete descriptor = %+v", descriptor)
		}
	}

	network, ok := RuntimeMetricDescriptorFor(RuntimeMetricNetworkIO)
	if !ok || len(network.Dimensions) != 1 || network.Dimensions[0] != "direction" {
		t.Fatalf("network descriptor = %+v", network)
	}
	network.Dimensions[0] = "mutated"
	again, _ := RuntimeMetricDescriptorFor(RuntimeMetricNetworkIO)
	if again.Dimensions[0] != "direction" {
		t.Fatalf("catalog dimensions were mutated: %+v", again)
	}
}
