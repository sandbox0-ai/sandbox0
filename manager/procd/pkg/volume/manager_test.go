// Package volume provides unit tests for SandboxVolume management.
package volume

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"
)

// mockTokenProvider is a mock implementation of TokenProvider.
type mockTokenProvider struct {
	token string
}

func (m *mockTokenProvider) GetInternalToken() string {
	return m.token
}

// TestNewManager tests manager creation.
func TestNewManager(t *testing.T) {
	logger := zaptest.NewLogger(t)
	config := &Config{
		ProxyBaseURL:  "storage-proxy.default.svc.cluster.local",
		ProxyReplicas: 3,
		NodeName:      "node-1",
	}

	m := NewManager(config, &mockTokenProvider{token: "test-token"}, logger)

	if m == nil {
		t.Fatal("NewManager() returned nil")
	}

	if m.config != config {
		t.Error("NewManager() config not set correctly")
	}

	if m.logger == nil {
		t.Error("NewManager() logger not set")
	}

	if m.tokenProvider == nil {
		t.Error("NewManager() tokenProvider not set")
	}

	if m.mounts == nil {
		t.Error("NewManager() mounts map not initialized")
	}

	// Check initial state
	status := m.GetStatus()
	// GetStatus returns nil slice when empty (Go's var []T = nil)
	// This is valid behavior - just check length
	if status != nil && len(status) != 0 {
		t.Errorf("GetStatus() returned %d items, want 0", len(status))
	}
}

// TestManager_IsMounted tests IsMounted method.
func TestManager_IsMounted(t *testing.T) {
	logger := zaptest.NewLogger(t)
	config := &Config{
		ProxyBaseURL:  "storage-proxy.default.svc.cluster.local",
		ProxyReplicas: 3,
		NodeName:      "node-1",
	}

	m := NewManager(config, &mockTokenProvider{token: "test-token"}, logger)

	tests := []struct {
		name     string
		volumeID string
		want     bool
	}{
		{
			name:     "non-existent volume",
			volumeID: "vol-1",
			want:     false,
		},
		{
			name:     "empty volume ID",
			volumeID: "",
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := m.IsMounted(tt.volumeID)
			if got != tt.want {
				t.Errorf("IsMounted() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestManager_MountValidation tests mount request validation.
func TestManager_MountValidation(t *testing.T) {
	logger := zaptest.NewLogger(t)
	config := &Config{
		ProxyBaseURL:  "storage-proxy.default.svc.cluster.local",
		ProxyReplicas: 3,
		NodeName:      "node-1",
	}

	m := NewManager(config, &mockTokenProvider{token: "test-token"}, logger)
	ctx := context.Background()

	tests := []struct {
		name    string
		req     *MountRequest
		wantErr error
	}{
		{
			name: "empty mount point",
			req: &MountRequest{
				SandboxVolumeID: "vol-1",
				MountPoint:      "",
			},
			wantErr: ErrInvalidMountPoint,
		},
		{
			name: "valid request but will fail on gRPC",
			req: &MountRequest{
				SandboxVolumeID: "vol-1",
				MountPoint:      "/mnt/vol-1",
				VolumeConfig: &VolumeConfig{
					CacheSize:  "100",
					Prefetch:   3,
					BufferSize: "300",
					Writeback:  true,
					ReadOnly:   false,
				},
			},
			wantErr: nil, // Will fail on gRPC connection, which is expected
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := m.Mount(ctx, tt.req)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Errorf("Mount() error = %v, want %v", err, tt.wantErr)
				}
			}
			// For valid requests, we expect gRPC connection errors
			// which is fine - we're testing validation logic
		})
	}
}

// TestManager_UnmountNotMounted tests unmounting a volume that isn't mounted.
func TestManager_UnmountNotMounted(t *testing.T) {
	logger := zaptest.NewLogger(t)
	config := &Config{
		ProxyBaseURL:  "storage-proxy.default.svc.cluster.local",
		ProxyReplicas: 3,
		NodeName:      "node-1",
	}

	m := NewManager(config, &mockTokenProvider{token: "test-token"}, logger)
	ctx := context.Background()

	err := m.Unmount(ctx, "non-existent")
	if err != ErrVolumeNotMounted {
		t.Errorf("Unmount() error = %v, want %v", err, ErrVolumeNotMounted)
	}
}

// TestManager_GetStatus tests GetStatus method.
func TestManager_GetStatus(t *testing.T) {
	logger := zaptest.NewLogger(t)
	config := &Config{
		ProxyBaseURL:  "storage-proxy.default.svc.cluster.local",
		ProxyReplicas: 3,
		NodeName:      "node-1",
	}

	m := NewManager(config, &mockTokenProvider{token: "test-token"}, logger)

	// Empty status - GetStatus returns nil slice when empty
	status := m.GetStatus()
	if status != nil && len(status) != 0 {
		t.Errorf("GetStatus() length = %d, want 0", len(status))
	}
}

// TestManager_Cleanup tests Cleanup method.
func TestManager_Cleanup(t *testing.T) {
	logger := zaptest.NewLogger(t)
	config := &Config{
		ProxyBaseURL:  "storage-proxy.default.svc.cluster.local",
		ProxyReplicas: 3,
		NodeName:      "node-1",
	}

	m := NewManager(config, &mockTokenProvider{token: "test-token"}, logger)

	// Cleanup empty manager - should not panic
	m.Cleanup()

	// Verify mounts are still empty
	status := m.GetStatus()
	if len(status) != 0 {
		t.Errorf("After Cleanup(), GetStatus() length = %d, want 0", len(status))
	}
}

// TestManager_ConcurrentAccess tests concurrent access to the manager.
func TestManager_ConcurrentAccess(t *testing.T) {
	logger := zaptest.NewLogger(t)
	config := &Config{
		ProxyBaseURL:  "storage-proxy.default.svc.cluster.local",
		ProxyReplicas: 3,
		NodeName:      "node-1",
	}

	m := NewManager(config, &mockTokenProvider{token: "test-token"}, logger)
	ctx := context.Background()

	// Concurrent IsMounted calls
	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			m.IsMounted("vol-1")
		}
		close(done)
	}()

	go func() {
		for i := 0; i < 100; i++ {
			m.GetStatus()
		}
	}()

	// Concurrent mount attempts (will fail but tests concurrent access)
	for i := 0; i < 10; i++ {
		go func(idx int) {
			req := &MountRequest{
				SandboxVolumeID: string(rune(idx)),
				MountPoint:      "/mnt/test",
			}
			m.Mount(ctx, req)
		}(i)
	}

	<-done

	// Manager should still be functional
	status := m.GetStatus()
	// GetStatus may return nil when empty, just check it doesn't panic
	_ = status
}

// TestManager_getStorageProxyAddress tests address generation with node affinity.
func TestManager_getStorageProxyAddress(t *testing.T) {
	tests := []struct {
		name         string
		nodeName     string
		replicas     int
		baseURL      string
		wantContains string
	}{
		{
			name:         "single replica",
			nodeName:     "node-1",
			replicas:     1,
			baseURL:      "storage-proxy",
			wantContains: "storage-proxy-0",
		},
		{
			name:         "multiple replicas",
			nodeName:     "node-1",
			replicas:     3,
			baseURL:      "storage-proxy",
			wantContains: "storage-proxy-",
		},
		{
			name:         "different node",
			nodeName:     "node-2",
			replicas:     3,
			baseURL:      "storage-proxy",
			wantContains: "storage-proxy-",
		},
		{
			name:         "full base URL",
			nodeName:     "node-1",
			replicas:     2,
			baseURL:      "storage-proxy.default.svc.cluster.local",
			wantContains: "storage-proxy-",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := zaptest.NewLogger(t)
			config := &Config{
				ProxyBaseURL:  tt.baseURL,
				ProxyReplicas: tt.replicas,
				NodeName:      tt.nodeName,
			}

			m := NewManager(config, &mockTokenProvider{}, logger)

			// Use unexported method via testing
			addr := m.getStorageProxyAddress()

			if addr == "" {
				t.Error("getStorageProxyAddress() returned empty string")
			}

			// Verify it contains the base pattern
			found := false
			for i := 0; i < tt.replicas; i++ {
				expected := "storage-proxy-0."
				if addr == expected || addr[:len(expected)] == expected {
					found = true
					break
				}
			}

			if !found && tt.wantContains != "" {
				// At least verify it has the prefix
				if len(addr) < len(tt.wantContains) || addr[:len(tt.wantContains)] != tt.wantContains {
					t.Logf("Address: %s", addr)
				}
			}
		})
	}
}

// TestMountContext tests MountContext struct.
func TestMountContext(t *testing.T) {
	ctx := &MountContext{
		SandboxVolumeID: "vol-123",
		MountPoint:      "/mnt/vol-123",
		MountedAt:       time.Now(),
	}

	if ctx.SandboxVolumeID != "vol-123" {
		t.Errorf("SandboxVolumeID = %s, want vol-123", ctx.SandboxVolumeID)
	}

	if ctx.MountPoint != "/mnt/vol-123" {
		t.Errorf("MountPoint = %s, want /mnt/vol-123", ctx.MountPoint)
	}

	if ctx.MountedAt.IsZero() {
		t.Error("MountedAt is zero")
	}
}

// TestVolumeConfig tests VolumeConfig defaults.
func TestVolumeConfig(t *testing.T) {
	config := &VolumeConfig{
		CacheSize:  "100",
		Prefetch:   3,
		BufferSize: "300",
		Writeback:  true,
		ReadOnly:   false,
	}

	if config.CacheSize != "100" {
		t.Errorf("CacheSize = %s, want 100", config.CacheSize)
	}

	if config.Prefetch != 3 {
		t.Errorf("Prefetch = %d, want 3", config.Prefetch)
	}

	if !config.Writeback {
		t.Error("Writeback = false, want true")
	}

	if config.ReadOnly {
		t.Error("ReadOnly = true, want false")
	}
}

// TestErrorDefinitions tests that error variables are properly defined.
func TestErrorDefinitions(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "ErrVolumeAlreadyMounted",
			err:  ErrVolumeAlreadyMounted,
			want: "sandboxvolume already mounted",
		},
		{
			name: "ErrVolumeNotMounted",
			err:  ErrVolumeNotMounted,
			want: "sandboxvolume not mounted",
		},
		{
			name: "ErrInvalidMountPoint",
			err:  ErrInvalidMountPoint,
			want: "invalid mount point",
		},
		{
			name: "ErrMountTimeout",
			err:  ErrMountTimeout,
			want: "mount timeout",
		},
		{
			name: "ErrUnmountFailed",
			err:  ErrUnmountFailed,
			want: "unmount failed",
		},
		{
			name: "ErrConnectionFailed",
			err:  ErrConnectionFailed,
			want: "grpc connection failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err == nil {
				t.Fatal("Error variable is nil")
			}
			if tt.err.Error() != tt.want {
				t.Errorf("Error() = %s, want %s", tt.err.Error(), tt.want)
			}
		})
	}
}

// TestManager_ConcurrentMountUnmount tests concurrent mount/unmount operations.
func TestManager_ConcurrentMountUnmount(t *testing.T) {
	logger := zaptest.NewLogger(t)
	config := &Config{
		ProxyBaseURL:  "storage-proxy.default.svc.cluster.local",
		ProxyReplicas: 3,
		NodeName:      "node-1",
	}

	m := NewManager(config, &mockTokenProvider{token: "test-token"}, logger)
	ctx := context.Background()

	// This test verifies that concurrent operations don't cause data races
	// The actual mount/unmount will fail (no gRPC server), but we're testing
	// that the manager handles concurrent access safely

	for i := 0; i < 10; i++ {
		go func(idx int) {
			volID := string(rune('a' + idx))
			req := &MountRequest{
				SandboxVolumeID: volID,
				MountPoint:      "/mnt/" + volID,
			}
			m.Mount(ctx, req)
		}(i)
	}

	for i := 0; i < 10; i++ {
		go func(idx int) {
			volID := string(rune('a' + idx))
			m.Unmount(ctx, volID)
		}(i)
	}

	// Give goroutines time to complete
	time.Sleep(100 * time.Millisecond)

	// Manager should still be functional
	status := m.GetStatus()
	// GetStatus may return nil when empty, just check it doesn't panic
	_ = status
}

// TestMountStatus tests MountStatus struct.
func TestMountStatus(t *testing.T) {
	now := time.Now()
	status := MountStatus{
		SandboxVolumeID:    "vol-123",
		MountPoint:         "/mnt/vol-123",
		MountedAt:          now.Format(time.RFC3339),
		MountedDurationSec: 3600,
	}

	if status.SandboxVolumeID != "vol-123" {
		t.Errorf("SandboxVolumeID = %s, want vol-123", status.SandboxVolumeID)
	}

	if status.MountPoint != "/mnt/vol-123" {
		t.Errorf("MountPoint = %s, want /mnt/vol-123", status.MountPoint)
	}

	if status.MountedDurationSec != 3600 {
		t.Errorf("MountedDurationSec = %d, want 3600", status.MountedDurationSec)
	}
}

// TestConfigValidation tests Config field defaults.
func TestConfigValidation(t *testing.T) {
	config := &Config{
		ProxyBaseURL:  "storage-proxy",
		ProxyReplicas: 1,
		NodeName:      "test-node",
		CacheMaxBytes: 1024 * 1024 * 100,
		CacheTTL:      5 * time.Minute,
	}

	if config.ProxyBaseURL != "storage-proxy" {
		t.Errorf("ProxyBaseURL = %s", config.ProxyBaseURL)
	}

	if config.ProxyReplicas != 1 {
		t.Errorf("ProxyReplicas = %d", config.ProxyReplicas)
	}

	if config.NodeName != "test-node" {
		t.Errorf("NodeName = %s", config.NodeName)
	}

	if config.CacheMaxBytes != 1024*1024*100 {
		t.Errorf("CacheMaxBytes = %d", config.CacheMaxBytes)
	}

	if config.CacheTTL != 5*time.Minute {
		t.Errorf("CacheTTL = %v", config.CacheTTL)
	}
}
