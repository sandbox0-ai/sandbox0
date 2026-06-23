package runtimeconfig

import (
	"testing"
	"time"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestToManagerPreservesEgressAuthDefaultResolveTTL(t *testing.T) {
	cfg := ToManager(&infrav1alpha1.ManagerConfig{
		EgressAuthDefaultResolveTTL: metav1.Duration{Duration: 90 * time.Second},
	})
	if cfg.EgressAuthDefaultResolveTTL.Duration != 90*time.Second {
		t.Fatalf("egress auth default resolve ttl = %s, want 90s", cfg.EgressAuthDefaultResolveTTL.Duration)
	}
}

func TestToStorageProxyDefaultsObjectEncryptionEnabled(t *testing.T) {
	cfg := ToStorageProxy(nil)
	if !cfg.ObjectEncryptionEnabled {
		t.Fatal("expected object encryption to be enabled by default")
	}
}

func TestToStorageProxyPreservesExplicitObjectEncryptionDisabled(t *testing.T) {
	cfg := ToStorageProxy(&infrav1alpha1.StorageProxyConfig{ObjectEncryptionEnabled: false})
	if cfg.ObjectEncryptionEnabled {
		t.Fatal("expected explicit object encryption disabled setting to be preserved")
	}
}

func TestToStorageProxyPreservesLocalStorageLimits(t *testing.T) {
	cfg := ToStorageProxy(&infrav1alpha1.StorageProxyConfig{
		CacheSizeLimit:             "512Mi",
		LogSizeLimit:               "64Mi",
		VolumePortalCacheSizeLimit: "2Gi",
		VolumePortalRootMinFree:    "1Gi",
	})
	if cfg.CacheSizeLimit != "512Mi" || cfg.LogSizeLimit != "64Mi" || cfg.VolumePortalCacheSizeLimit != "2Gi" || cfg.VolumePortalRootMinFree != "1Gi" {
		t.Fatalf("local storage limits were not preserved: %#v", cfg)
	}
}

func TestToStorageProxyPreservesS0FSLayoutConfig(t *testing.T) {
	cfg := ToStorageProxy(&infrav1alpha1.StorageProxyConfig{
		S0FSSegmentTargetSize:        "8Mi",
		S0FSCompactionInterval:       "30s",
		S0FSCompactionMinDeadRatio:   "0.25",
		S0FSCompactionMinReclaimSize: "2Mi",
	})
	if cfg.S0FSSegmentTargetSize != "8Mi" ||
		cfg.S0FSCompactionInterval != "30s" ||
		cfg.S0FSCompactionMinDeadRatio != "0.25" ||
		cfg.S0FSCompactionMinReclaimSize != "2Mi" {
		t.Fatalf("s0fs layout config was not preserved: %#v", cfg)
	}
}

func TestToNetdPreservesBandwidthLimits(t *testing.T) {
	cfg := ToNetd(&infrav1alpha1.NetdConfig{
		EgressBandwidthBytesPerSecond:      1024,
		IngressBandwidthBytesPerSecond:     2048,
		BandwidthBurstBytes:                4096,
		TeamEgressBandwidthBytesPerSecond:  8192,
		TeamIngressBandwidthBytesPerSecond: 16384,
		TeamBandwidthBurstBytes:            32768,
	})
	if cfg.EgressBandwidthBytesPerSecond != 1024 ||
		cfg.IngressBandwidthBytesPerSecond != 2048 ||
		cfg.BandwidthBurstBytes != 4096 ||
		cfg.TeamEgressBandwidthBytesPerSecond != 8192 ||
		cfg.TeamIngressBandwidthBytesPerSecond != 16384 ||
		cfg.TeamBandwidthBurstBytes != 32768 {
		t.Fatalf("bandwidth limits were not preserved: %#v", cfg)
	}
}
