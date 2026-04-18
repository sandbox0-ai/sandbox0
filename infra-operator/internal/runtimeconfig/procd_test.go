package runtimeconfig

import (
	"testing"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
)

func TestToManagerCopiesProcdFuseLatencyKnobs(t *testing.T) {
	cfg := ToManager(&infrav1alpha1.ManagerConfig{
		ProcdConfig: infrav1alpha1.ProcdConfig{
			FuseDeferFlushToRelease: true,
			FuseAsyncRelease:        true,
			FuseWritebackCache:      true,
		},
	})

	if !cfg.ProcdConfig.FuseDeferFlushToRelease {
		t.Fatal("expected FuseDeferFlushToRelease to be copied")
	}
	if !cfg.ProcdConfig.FuseAsyncRelease {
		t.Fatal("expected FuseAsyncRelease to be copied")
	}
	if !cfg.ProcdConfig.FuseWritebackCache {
		t.Fatal("expected FuseWritebackCache to be copied")
	}
}
