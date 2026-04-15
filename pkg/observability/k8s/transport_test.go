package k8s

import "testing"

func TestExtractK8sResource(t *testing.T) {
	tests := []struct {
		name string
		path string
		want string
	}{
		{name: "core namespaced collection", path: "/api/v1/namespaces/default/pods", want: "pods"},
		{name: "core namespaced object", path: "/api/v1/namespaces/default/pods/sandbox-123", want: "pods"},
		{name: "group namespaced object", path: "/apis/apps/v1/namespaces/default/replicasets/rs-123", want: "replicasets"},
		{name: "core cluster collection", path: "/api/v1/nodes", want: "nodes"},
		{name: "group cluster collection", path: "/apis/storage.k8s.io/v1/storageclasses", want: "storageclasses"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractK8sResource(tt.path); got != tt.want {
				t.Fatalf("extractK8sResource(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}
