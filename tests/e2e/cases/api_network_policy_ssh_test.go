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

func TestSSHFixtureImageNameCandidates(t *testing.T) {
	tests := []struct {
		name     string
		imageRef string
		want     []string
	}{
		{
			name:     "docker hub namespace",
			imageRef: "sandbox0ai/e2e-openssh-server:68b605929e83",
			want: []string{
				"sandbox0ai/e2e-openssh-server:68b605929e83",
				"docker.io/sandbox0ai/e2e-openssh-server:68b605929e83",
			},
		},
		{
			name:     "explicit registry",
			imageRef: "lscr.io/linuxserver/openssh-server:latest",
			want:     []string{"lscr.io/linuxserver/openssh-server:latest"},
		},
		{
			name:     "localhost registry",
			imageRef: "localhost:5000/e2e-openssh-server:latest",
			want:     []string{"localhost:5000/e2e-openssh-server:latest"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sshFixtureImageNameCandidates(tt.imageRef)
			if strings.Join(got, "\n") != strings.Join(tt.want, "\n") {
				t.Fatalf("candidates = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestSSHFixtureImageListContains(t *testing.T) {
	images := strings.Join([]string{
		"docker.io/library/postgres:16-alpine",
		"docker.io/sandbox0ai/e2e-openssh-server:68b605929e83",
	}, "\n")
	candidates := sshFixtureImageNameCandidates("sandbox0ai/e2e-openssh-server:68b605929e83")
	if !sshFixtureImageListContains(images, candidates) {
		t.Fatal("expected docker.io-normalized image to match")
	}
	if sshFixtureImageListContains(images, []string{"sandbox0ai/missing:latest"}) {
		t.Fatal("expected missing image not to match")
	}
}

func sshPublicKeyBytesEqual(a, b ssh.PublicKey) bool {
	if a == nil || b == nil {
		return false
	}
	return string(a.Marshal()) == string(b.Marshal())
}
