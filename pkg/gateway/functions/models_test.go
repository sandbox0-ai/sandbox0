package functions

import (
	"strings"
	"testing"

	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
)

func TestNormalizeScalePolicyDefaultsToScaleToZero(t *testing.T) {
	got := NormalizeScalePolicy(FunctionScalePolicy{})
	if got.MaxInstances != 1 {
		t.Fatalf("MaxInstances = %d, want 1", got.MaxInstances)
	}
	if got.TargetConcurrency != 1 {
		t.Fatalf("TargetConcurrency = %d, want 1", got.TargetConcurrency)
	}
	if got.IdleTimeoutSeconds != 300 {
		t.Fatalf("IdleTimeoutSeconds = %d, want 300", got.IdleTimeoutSeconds)
	}
	if got.StartupTimeoutSeconds != 90 {
		t.Fatalf("StartupTimeoutSeconds = %d, want 90", got.StartupTimeoutSeconds)
	}
}

func TestNormalizeSlug(t *testing.T) {
	got, err := NormalizeSlug("  My Function_v1!!  ")
	if err != nil {
		t.Fatalf("NormalizeSlug: %v", err)
	}
	if got != "my-function-v1" {
		t.Fatalf("slug = %q, want my-function-v1", got)
	}
	if _, err := NormalizeSlug("___"); err == nil {
		t.Fatal("NormalizeSlug accepted an empty slug")
	}
}

func TestNewDomainLabelFitsDNSLabel(t *testing.T) {
	slug := strings.Repeat("a", 48)
	got, err := NewDomainLabel(slug)
	if err != nil {
		t.Fatalf("NewDomainLabel: %v", err)
	}
	if len(got) > 63 {
		t.Fatalf("domain label length = %d, want <= 63", len(got))
	}
	if !strings.HasPrefix(got, slug+"-") {
		t.Fatalf("domain label = %q, want slug prefix", got)
	}
}

func TestPublicURL(t *testing.T) {
	got := PublicURL("hello-1234", "aws-us-east-1", "sandbox0.example")
	want := "https://hello-1234.fn.aws-us-east-1.sandbox0.example"
	if got != want {
		t.Fatalf("PublicURL = %q, want %q", got, want)
	}
}

func TestNormalizeRevisionSpecDefaultsFunctionIngress(t *testing.T) {
	spec, err := NormalizeRevisionSpec(FunctionRevisionSpec{
		Template: "node",
		Service: mgr.SandboxAppService{
			ID:   "web",
			Port: 3000,
			Runtime: &mgr.SandboxAppServiceRuntime{
				Type:    mgr.SandboxAppServiceRuntimeCMD,
				Command: []string{"npm", "start"},
			},
		},
		Mounts: []FunctionRevisionMount{{
			SnapshotID: "snap-1",
			MountPath:  "/app",
		}},
	})
	if err != nil {
		t.Fatalf("NormalizeRevisionSpec: %v", err)
	}
	if !spec.Service.Ingress.Public {
		t.Fatal("service ingress is not public")
	}
	if len(spec.Service.Ingress.Routes) != 1 {
		t.Fatalf("routes len = %d, want 1", len(spec.Service.Ingress.Routes))
	}
	if !spec.Service.Ingress.Routes[0].Resume {
		t.Fatal("function route should resume runtime sandboxes")
	}
	if !spec.Mounts[0].ReadOnly {
		t.Fatal("function snapshot mount should be read-only")
	}
}

func TestNormalizeRevisionSpecRejectsUnpublishableService(t *testing.T) {
	_, err := NormalizeRevisionSpec(FunctionRevisionSpec{
		Template: "node",
		Service: mgr.SandboxAppService{
			ID:   "web",
			Port: 3000,
			Runtime: &mgr.SandboxAppServiceRuntime{
				Type: mgr.SandboxAppServiceRuntimeManual,
			},
		},
	})
	if err == nil {
		t.Fatal("NormalizeRevisionSpec accepted a manual service runtime")
	}
}

func TestNormalizeRevisionSpecRejectsDuplicateMountPaths(t *testing.T) {
	_, err := NormalizeRevisionSpec(FunctionRevisionSpec{
		Template: "node",
		Service: mgr.SandboxAppService{
			ID:   "web",
			Port: 3000,
			Runtime: &mgr.SandboxAppServiceRuntime{
				Type:    mgr.SandboxAppServiceRuntimeCMD,
				Command: []string{"npm", "start"},
			},
		},
		Mounts: []FunctionRevisionMount{
			{SnapshotID: "snap-1", MountPath: "/app"},
			{SnapshotID: "snap-2", MountPath: "/app/"},
		},
	})
	if err == nil {
		t.Fatal("NormalizeRevisionSpec accepted duplicate mount paths")
	}
}
