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
				Manager struct {
					Config struct {
						SandboxRuntimeClassName string `json:"sandboxRuntimeClassName"`
					} `json:"config"`
				} `json:"manager"`
			} `json:"services"`
		} `json:"spec"`
	}
	if err := json.Unmarshal([]byte(got), &patch); err != nil {
		t.Fatalf("patch is not valid JSON: %v", err)
	}
	if patch.Spec.Services.Manager.Config.SandboxRuntimeClassName != "gvisor-rootfs" {
		t.Fatalf("sandbox runtime class = %q, want gvisor-rootfs", patch.Spec.Services.Manager.Config.SandboxRuntimeClassName)
	}
}
