package authn

import "testing"

func TestRolePermissionsIncludeSandboxVolumeFilePermissions(t *testing.T) {
	tests := []struct {
		role      string
		wantRead  bool
		wantWrite bool
	}{
		{role: "admin", wantRead: true, wantWrite: true},
		{role: "developer", wantRead: true, wantWrite: true},
		{role: "viewer", wantRead: true, wantWrite: false},
	}

	for _, tt := range tests {
		t.Run(tt.role, func(t *testing.T) {
			permissions := ExpandRolePermissions(tt.role)
			gotRead := containsPermission(permissions, PermSandboxVolumeFileRead)
			gotWrite := containsPermission(permissions, PermSandboxVolumeFileWrite)
			if gotRead != tt.wantRead {
				t.Fatalf("read permission = %v, want %v", gotRead, tt.wantRead)
			}
			if gotWrite != tt.wantWrite {
				t.Fatalf("write permission = %v, want %v", gotWrite, tt.wantWrite)
			}
		})
	}
}

func containsPermission(permissions []string, target string) bool {
	for _, permission := range permissions {
		if permission == target {
			return true
		}
	}
	return false
}
