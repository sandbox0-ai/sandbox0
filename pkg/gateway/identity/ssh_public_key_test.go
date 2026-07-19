package identity

import (
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/resourceguard"
)

const authorizedKeyWithoutComment = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIJ4dLZLZOA/asaP+5QO6t81jzbe5G4jrI2F+jbjL6TY8"

func TestValidateUserSSHPublicKeyBoundaries(t *testing.T) {
	key := &UserSSHPublicKey{
		Name:      strings.Repeat("n", int(MaxSSHPublicKeyNameBytes)),
		PublicKey: strings.Repeat("k", int(MaxAuthorizedSSHPublicKeyBytes)),
		Comment:   strings.Repeat("c", int(MaxSSHPublicKeyCommentBytes)),
	}
	if err := ValidateUserSSHPublicKey(key); err != nil {
		t.Fatalf("ValidateUserSSHPublicKey(boundary) error = %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*UserSSHPublicKey)
	}{
		{
			name: "name one byte over",
			mutate: func(value *UserSSHPublicKey) {
				value.Name += "n"
			},
		},
		{
			name: "key one byte over",
			mutate: func(value *UserSSHPublicKey) {
				value.PublicKey += "k"
			},
		},
		{
			name: "comment one byte over",
			mutate: func(value *UserSSHPublicKey) {
				value.Comment += "c"
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			over := *key
			tt.mutate(&over)
			if err := ValidateUserSSHPublicKey(&over); !resourceguard.IsTooLarge(err) {
				t.Fatalf("ValidateUserSSHPublicKey() error = %v, want TooLargeError", err)
			}
		})
	}
}

func TestNormalizeAuthorizedSSHPublicKeyCommentBoundary(t *testing.T) {
	boundary := authorizedKeyWithoutComment + " " +
		strings.Repeat("c", int(MaxSSHPublicKeyCommentBytes))
	_, _, _, comment, err := NormalizeAuthorizedSSHPublicKey(boundary)
	if err != nil {
		t.Fatalf("NormalizeAuthorizedSSHPublicKey(boundary) error = %v", err)
	}
	if len(comment) != int(MaxSSHPublicKeyCommentBytes) {
		t.Fatalf("comment length = %d, want %d", len(comment), MaxSSHPublicKeyCommentBytes)
	}

	over := authorizedKeyWithoutComment + " " +
		strings.Repeat("c", int(MaxSSHPublicKeyCommentBytes)+1)
	if _, _, _, _, err := NormalizeAuthorizedSSHPublicKey(over); !resourceguard.IsTooLarge(err) {
		t.Fatalf("NormalizeAuthorizedSSHPublicKey(one byte over) error = %v, want TooLargeError", err)
	}
}

func TestNormalizeAuthorizedSSHPublicKeyRejectsOversizedRawInputWithoutLeakingIt(t *testing.T) {
	secretMarker := "do-not-leak-ssh-key"
	raw := authorizedKeyWithoutComment + " " +
		strings.Repeat("x", int(MaxAuthorizedSSHPublicKeyBytes)) + secretMarker
	_, _, _, _, err := NormalizeAuthorizedSSHPublicKey(raw)
	if !resourceguard.IsTooLarge(err) {
		t.Fatalf("NormalizeAuthorizedSSHPublicKey() error = %v, want TooLargeError", err)
	}
	if strings.Contains(err.Error(), secretMarker) {
		t.Fatalf("error leaked SSH key input: %v", err)
	}
}
