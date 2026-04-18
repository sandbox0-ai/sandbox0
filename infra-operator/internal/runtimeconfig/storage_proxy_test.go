package runtimeconfig

import (
	"testing"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
)

func TestToStorageProxyCopiesSmallFileDebugKnobs(t *testing.T) {
	cfg := ToStorageProxy(&infrav1alpha1.StorageProxyConfig{
		SharedMutationBarrierDisabled: true,
		AsyncRemoteSyncRecord:         true,
		JuiceFSSkipDirMtime:           "30s",
	})

	if !cfg.SharedMutationBarrierDisabled {
		t.Fatal("expected SharedMutationBarrierDisabled to be copied")
	}
	if !cfg.AsyncRemoteSyncRecord {
		t.Fatal("expected AsyncRemoteSyncRecord to be copied")
	}
	if cfg.JuiceFSSkipDirMtime != "30s" {
		t.Fatalf("JuiceFSSkipDirMtime = %q, want 30s", cfg.JuiceFSSkipDirMtime)
	}
}
