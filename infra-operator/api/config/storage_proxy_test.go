package config

import "testing"

func TestStorageProxyConfigValidateAcceptsJuiceFSUploadDelay(t *testing.T) {
	cfg := &StorageProxyConfig{JuiceFSUploadDelay: "30s"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestStorageProxyConfigValidateRejectsInvalidJuiceFSUploadDelay(t *testing.T) {
	cfg := &StorageProxyConfig{JuiceFSUploadDelay: "soon"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() should reject invalid juicefs upload delay")
	}
}

func TestStorageProxyConfigValidateAcceptsJuiceFSSkipDirMtime(t *testing.T) {
	cfg := &StorageProxyConfig{JuiceFSSkipDirMtime: "30s"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestStorageProxyConfigValidateRejectsInvalidJuiceFSSkipDirMtime(t *testing.T) {
	cfg := &StorageProxyConfig{JuiceFSSkipDirMtime: "later"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() should reject invalid juicefs skip dir mtime")
	}
}
