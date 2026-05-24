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
		{role: "builder", wantRead: false, wantWrite: false},
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

func TestRolePermissionsIncludeRegistryWrite(t *testing.T) {
	tests := []struct {
		role string
		want bool
	}{
		{role: "admin", want: true},
		{role: "developer", want: true},
		{role: "builder", want: true},
		{role: "viewer", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.role, func(t *testing.T) {
			permissions := ExpandRolePermissions(tt.role)
			got := containsPermission(permissions, PermRegistryWrite)
			if got != tt.want {
				t.Fatalf("registry write permission = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRolePermissionsIncludeArtifactPermissions(t *testing.T) {
	tests := []struct {
		role       string
		wantRead   bool
		wantMutate bool
	}{
		{role: "admin", wantRead: true, wantMutate: true},
		{role: "developer", wantRead: true, wantMutate: true},
		{role: "builder", wantRead: true, wantMutate: false},
		{role: "viewer", wantRead: true, wantMutate: false},
	}

	for _, tt := range tests {
		t.Run(tt.role, func(t *testing.T) {
			permissions := ExpandRolePermissions(tt.role)
			if got := containsPermission(permissions, PermArtifactRead); got != tt.wantRead {
				t.Fatalf("artifact read permission = %v, want %v", got, tt.wantRead)
			}
			if got := containsPermission(permissions, PermArtifactCreate); got != tt.wantMutate {
				t.Fatalf("artifact create permission = %v, want %v", got, tt.wantMutate)
			}
			if got := containsPermission(permissions, PermArtifactDelete); got != tt.wantMutate {
				t.Fatalf("artifact delete permission = %v, want %v", got, tt.wantMutate)
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
