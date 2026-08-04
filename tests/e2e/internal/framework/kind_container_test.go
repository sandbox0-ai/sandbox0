package framework

import (
	"strings"
	"testing"
)

func TestExtractKindPodContainerTarget(t *testing.T) {
	target, err := extractKindPodContainerTarget([]byte(`{
		"spec": {"nodeName": "e2e-control-plane"},
		"status": {
			"containerStatuses": [
				{"name": "sidecar", "containerID": "containerd://sidecar-id"},
				{"name": "procd", "containerID": "containerd://procd-id"}
			]
		}
	}`), "procd")
	if err != nil {
		t.Fatalf("extractKindPodContainerTarget() error = %v", err)
	}
	want := kindPodContainerTarget{NodeName: "e2e-control-plane", ContainerID: "procd-id"}
	if target != want {
		t.Fatalf("extractKindPodContainerTarget() = %#v, want %#v", target, want)
	}
}

func TestExtractKindPodContainerTargetRejectsInvalidPodData(t *testing.T) {
	tests := []struct {
		name      string
		pod       string
		container string
		wantError string
	}{
		{
			name:      "invalid JSON",
			pod:       `{`,
			container: "procd",
			wantError: "decode pod",
		},
		{
			name:      "missing node name",
			pod:       `{"status":{"containerStatuses":[{"name":"procd","containerID":"containerd://procd-id"}]}}`,
			container: "procd",
			wantError: "pod node name is empty",
		},
		{
			name:      "missing container status",
			pod:       `{"spec":{"nodeName":"e2e-control-plane"},"status":{"containerStatuses":[]}}`,
			container: "procd",
			wantError: "container status is missing",
		},
		{
			name:      "missing container ID",
			pod:       `{"spec":{"nodeName":"e2e-control-plane"},"status":{"containerStatuses":[{"name":"procd"}]}}`,
			container: "procd",
			wantError: "container ID is empty",
		},
		{
			name:      "missing requested name",
			pod:       `{"spec":{"nodeName":"e2e-control-plane"},"status":{"containerStatuses":[{"name":"procd","containerID":"containerd://procd-id"}]}}`,
			container: "",
			wantError: "container name is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := extractKindPodContainerTarget([]byte(tt.pod), tt.container)
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("extractKindPodContainerTarget() error = %v, want containing %q", err, tt.wantError)
			}
		})
	}
}
