package cases

import (
	"io"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestBuildNetdHTTPFixtureManifestParsesYAML(t *testing.T) {
	for _, tc := range []struct {
		name     string
		nodeName string
	}{
		{name: "without node name"},
		{name: "with node name", nodeName: "worker-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manifest := buildNetdHTTPFixtureManifest("sandbox0-e2e-netd-test", "fixture:latest", tc.nodeName)
			decoder := yaml.NewDecoder(strings.NewReader(manifest))
			count := 0
			for {
				var doc map[string]any
				err := decoder.Decode(&doc)
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("manifest did not parse as YAML: %v\n%s", err, manifest)
				}
				if len(doc) > 0 {
					count++
				}
			}
			if count != 3 {
				t.Fatalf("parsed %d manifest documents, want 3", count)
			}
		})
	}
}
