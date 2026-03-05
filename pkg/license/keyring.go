package license

import (
	"crypto/ed25519"
	"fmt"
	"sync"

	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

const (
	// CurrentKeyID is the default signing key id for production-issued licenses.
	CurrentKeyID = "s0-2026-01"
)

var (
	trustedKeysOnce sync.Once
	trustedKeys     map[string]ed25519.PublicKey
	trustedKeysErr  error
)

// trustedPublicKeyPEM stores built-in trusted public keys for license verification.
// Keep old keys during rollout windows to allow seamless key rotation across versions.
var trustedPublicKeyPEM = map[string]string{
	CurrentKeyID: `-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEALPvtxouU0yE64w5T4nU6o/yFrzMBmTmDQVyEhxTE3zk=
-----END PUBLIC KEY-----`,
}

// TrustedPublicKeys returns the built-in trusted keyring.
// This function panics only when embedded keys are malformed, which is a build-time issue.
func TrustedPublicKeys() map[string]ed25519.PublicKey {
	trustedKeysOnce.Do(func() {
		keys := make(map[string]ed25519.PublicKey, len(trustedPublicKeyPEM))
		for keyID, pemData := range trustedPublicKeyPEM {
			key, err := internalauth.LoadEd25519PublicKey([]byte(pemData))
			if err != nil {
				trustedKeysErr = fmt.Errorf("invalid embedded public key %q: %w", keyID, err)
				return
			}
			keys[keyID] = key
		}
		trustedKeys = keys
	})
	if trustedKeysErr != nil {
		panic(trustedKeysErr)
	}
	return trustedKeys
}
