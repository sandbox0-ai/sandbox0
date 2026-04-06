package naming

import "testing"

func TestTeamScopedImageRegistry(t *testing.T) {
	t.Parallel()

	got := TeamScopedImageRegistry("https://registry.example.com/", "team-123")
	want := "registry.example.com/" + TeamImageRepositoryPrefix("team-123")
	if got != want {
		t.Fatalf("TeamScopedImageRegistry() = %q, want %q", got, want)
	}
}

func TestValidateTeamScopedImageReference(t *testing.T) {
	t.Parallel()

	privateHosts := []string{"registry.internal.svc:5000", "registry.example.com"}
	prefix := TeamImageRepositoryPrefix("team-123")

	tests := []struct {
		name     string
		imageRef string
		wantErr  bool
	}{
		{
			name:     "public image bypasses validation",
			imageRef: "ubuntu:24.04",
		},
		{
			name:     "team scoped private image accepted",
			imageRef: "registry.internal.svc:5000/" + prefix + "/my-app:v1",
		},
		{
			name:     "team scoped digest image accepted",
			imageRef: "registry.example.com/" + prefix + "/my-app@sha256:0123",
		},
		{
			name:     "other team private image rejected",
			imageRef: "registry.internal.svc:5000/t-other/my-app:v1",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateTeamScopedImageReference(tt.imageRef, "team-123", privateHosts)
			if tt.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
