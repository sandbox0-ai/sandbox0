package template

import (
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
)

func TestResolveEphemeralMountsReturnsExactLimits(t *testing.T) {
	mounts, err := ResolveEphemeralMounts(sandboxspec.TemplateSpec{EphemeralMounts: []sandboxspec.EphemeralMountSpec{
		{MountPath: "/var/lib/docker", SizeLimit: "16Gi"},
		{MountPath: "/dev/shm", SizeLimit: "2Gi"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(mounts) != 2 || mounts[0].SizeBytes != 16<<30 || mounts[1].SizeBytes != 2<<30 {
		t.Fatalf("mounts = %#v", mounts)
	}
}

func TestResolveEphemeralMountsRejectsUnsafeShapes(t *testing.T) {
	tests := []struct {
		name   string
		mounts []sandboxspec.EphemeralMountSpec
		want   string
	}{
		{name: "relative", mounts: []sandboxspec.EphemeralMountSpec{{MountPath: "workspace", SizeLimit: "1Gi"}}, want: "canonical absolute"},
		{name: "reserved", mounts: []sandboxspec.EphemeralMountSpec{{MountPath: "/proc/memory", SizeLimit: "1Gi"}}, want: "reserved runtime path"},
		{name: "unsupported dev", mounts: []sandboxspec.EphemeralMountSpec{{MountPath: "/dev/custom", SizeLimit: "1Gi"}}, want: "reserved runtime path"},
		{name: "overlap", mounts: []sandboxspec.EphemeralMountSpec{{MountPath: "/workspace", SizeLimit: "1Gi"}, {MountPath: "/workspace/cache", SizeLimit: "1Gi"}}, want: "overlaps"},
		{name: "too small", mounts: []sandboxspec.EphemeralMountSpec{{MountPath: "/workspace", SizeLimit: "4Ki"}}, want: "between 1Mi and 1Ti"},
		{name: "fractional byte", mounts: []sandboxspec.EphemeralMountSpec{{MountPath: "/workspace", SizeLimit: "1.1Mi"}}, want: "exact byte quantity"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ResolveEphemeralMounts(sandboxspec.TemplateSpec{EphemeralMounts: test.mounts})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ResolveEphemeralMounts() error = %v, want containing %q", err, test.want)
			}
		})
	}
}
