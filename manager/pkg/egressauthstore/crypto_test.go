package egressauthstore

import (
	"bytes"
	"context"
	"testing"
)

func TestAESGCMCodecRoundTrip(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	codec, err := NewAESGCMCodec("v1", map[string][]byte{"v1": key})
	if err != nil {
		t.Fatalf("new codec: %v", err)
	}

	spec := CredentialSourceSecretSpec{
		StaticUsernamePassword: &StaticUsernamePasswordSourceSpec{
			Username: "alice",
			Password: "secret",
		},
	}
	aad := []byte("team/source/version")
	payload, err := codec.Encrypt(context.Background(), aad, spec)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if bytes.Contains(payload, []byte("secret")) {
		t.Fatalf("encrypted payload contains plaintext secret: %s", payload)
	}

	got, err := codec.Decrypt(context.Background(), aad, payload)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if got.StaticUsernamePassword == nil || got.StaticUsernamePassword.Password != "secret" {
		t.Fatalf("unexpected decrypted spec: %#v", got)
	}
	if _, err := codec.Decrypt(context.Background(), []byte("other-aad"), payload); err == nil {
		t.Fatal("decrypt with different AAD succeeded")
	}
}
