package runtimeconfig

import (
	"testing"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
)

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
