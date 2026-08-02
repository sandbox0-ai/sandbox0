package framework

import (
	"encoding/json"
	"testing"
)

func TestSandboxRuntimeClassPatch(t *testing.T) {
	got, err := sandboxRuntimeClassPatch(`gvisor-rootfs`)
	if err != nil {
		t.Fatalf("sandboxRuntimeClassPatch returned error: %v", err)
	}

	var patch struct {
		Spec struct {
			Services struct {
				Ctld struct {
					RootFSSnapshotter struct {
						RuntimeClassName string `json:"runtimeClassName"`
						Handler          string `json:"handler"`
					} `json:"rootfsSnapshotter"`
				} `json:"ctld"`
			} `json:"services"`
		} `json:"spec"`
	}
	if err := json.Unmarshal([]byte(got), &patch); err != nil {
		t.Fatalf("patch is not valid JSON: %v", err)
	}
	if patch.Spec.Services.Ctld.RootFSSnapshotter.RuntimeClassName != "gvisor-rootfs" ||
		patch.Spec.Services.Ctld.RootFSSnapshotter.Handler != "gvisor-rootfs" {
		t.Fatalf("rootfs snapshotter runtime = %+v, want gvisor-rootfs", patch.Spec.Services.Ctld.RootFSSnapshotter)
	}
}
