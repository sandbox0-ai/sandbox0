package cases

import (
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"
)

func TestSSHFixtureKeyConstantsAreValid(t *testing.T) {
	tests := []struct {
		name       string
		privateKey string
		publicKey  string
	}{
		{
			name:       "fixture",
			privateKey: sshFixturePrivateKey,
			publicKey:  sshFixturePublicKey,
		},
		{
			name:       "fake",
			privateKey: sshProxyFakePrivateKey,
			publicKey:  sshProxyFakePublicKey,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			signer, err := ssh.ParsePrivateKey([]byte(strings.TrimSpace(tt.privateKey)))
			if err != nil {
				t.Fatalf("parse private key: %v", err)
			}
			publicKey, _, _, _, err := ssh.ParseAuthorizedKey([]byte(tt.publicKey))
			if err != nil {
				t.Fatalf("parse public key: %v", err)
			}
			if !sshPublicKeyBytesEqual(signer.PublicKey(), publicKey) {
				t.Fatal("public key does not match private key")
			}
		})
	}
}

func sshPublicKeyBytesEqual(a, b ssh.PublicKey) bool {
	if a == nil || b == nil {
		return false
	}
	return string(a.Marshal()) == string(b.Marshal())
}
