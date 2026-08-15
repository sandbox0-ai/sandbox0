package authn

import "testing"

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

func TestRolePermissionsIncludeAPIKeyManageForAdminsOnly(t *testing.T) {
	tests := []struct {
		role string
		want bool
	}{
		{role: "admin", want: true},
		{role: "developer", want: false},
		{role: "builder", want: false},
		{role: "viewer", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.role, func(t *testing.T) {
			permissions := ExpandRolePermissions(tt.role)
			got := containsPermission(permissions, PermAPIKeyManage)
			if got != tt.want {
				t.Fatalf("api key manage permission = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRolePermissionsIncludeUsageRead(t *testing.T) {
	tests := []struct {
		role string
		want bool
	}{
		{role: "admin", want: true},
		{role: "developer", want: true},
		{role: "builder", want: false},
		{role: "viewer", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.role, func(t *testing.T) {
			permissions := ExpandRolePermissions(tt.role)
			if got := containsPermission(permissions, PermUsageRead); got != tt.want {
				t.Fatalf("usage read permission = %v, want %v", got, tt.want)
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
