package internalauth

import "os"

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

var (
	// DefaultInternalJWTPublicKeyPath is the default path inside containers where the
	// internal auth public key is mounted.
	DefaultInternalJWTPublicKeyPath = getEnv("INTERNAL_JWT_PUBLIC_KEY_PATH", "/config/internal_jwt_public.key")

	// DefaultInternalJWTPrivateKeyPath is the default path inside containers where the
	// internal auth private key is mounted.
	DefaultInternalJWTPrivateKeyPath = getEnv("INTERNAL_JWT_PRIVATE_KEY_PATH", "/secrets/internal_jwt_private.key")

	// DefaultAuditJWTPublicKeyPath is mounted only into cluster-gateway.
	DefaultAuditJWTPublicKeyPath = getEnv("AUDIT_JWT_PUBLIC_KEY_PATH", "/config/audit_jwt_public.key")
	// DefaultAuditJWTPrivateKeyPath is mounted only into netd.
	DefaultAuditJWTPrivateKeyPath = getEnv("AUDIT_JWT_PRIVATE_KEY_PATH", "/secrets/audit_jwt_private.key")

	// DefaultAuditSigningPrivateKeyPath is mounted only into cluster-gateway and
	// is never reused for service authentication.
	DefaultAuditSigningPrivateKeyPath = getEnv("AUDIT_SIGNING_PRIVATE_KEY_PATH", "/secrets/audit_signing_private.key")
	// DefaultAuditSigningPublicKeyPath is mounted into cluster-gateway for
	// verification and can be retained separately for offline audit validation.
	DefaultAuditSigningPublicKeyPath = getEnv("AUDIT_SIGNING_PUBLIC_KEY_PATH", "/config/audit_signing_public.key")
)
