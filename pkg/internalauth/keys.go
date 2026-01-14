package internalauth

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
)

// PublicKeyType is the type alias for Ed25519 public key.
type PublicKeyType = ed25519.PublicKey

// PrivateKeyType is the type alias for Ed25519 private key.
type PrivateKeyType = ed25519.PrivateKey

var (
	// ErrInvalidKeyType is returned when the key type is not supported.
	ErrInvalidKeyType = errors.New("internalauth: invalid key type")

	// ErrInvalidPEMFormat is returned when the PEM format is invalid.
	ErrInvalidPEMFormat = errors.New("internalauth: invalid PEM format")
)

// GenerateEd25519KeyPair generates a new Ed25519 key pair.
// Returns (privateKeyPEM, publicKeyPEM, error).
func GenerateEd25519KeyPair() ([]byte, []byte, error) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate Ed25519 key pair: %w", err)
	}

	privateKeyPEM, err := marshalPrivateKey(privateKey)
	if err != nil {
		return nil, nil, err
	}

	publicKeyPEM, err := marshalPublicKey(publicKey)
	if err != nil {
		return nil, nil, err
	}

	return privateKeyPEM, publicKeyPEM, nil
}

// LoadEd25519PrivateKey loads an Ed25519 private key from PEM format.
func LoadEd25519PrivateKey(pemData []byte) (ed25519.PrivateKey, error) {
	key, err := parsePrivateKey(pemData)
	if err != nil {
		return nil, err
	}

	privKey, ok := key.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("%w: expected Ed25519 private key", ErrInvalidKeyType)
	}

	return privKey, nil
}

// LoadEd25519PublicKey loads an Ed25519 public key from PEM format.
func LoadEd25519PublicKey(pemData []byte) (ed25519.PublicKey, error) {
	key, err := parsePublicKey(pemData)
	if err != nil {
		return nil, err
	}

	pubKey, ok := key.(ed25519.PublicKey)
	if !ok {
		return nil, fmt.Errorf("%w: expected Ed25519 public key", ErrInvalidKeyType)
	}

	return pubKey, nil
}

// LoadEd25519PrivateKeyFromFile loads an Ed25519 private key from a file.
func LoadEd25519PrivateKeyFromFile(path string) (ed25519.PrivateKey, error) {
	pemData, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read private key file: %w", err)
	}
	return LoadEd25519PrivateKey(pemData)
}

// LoadEd25519PublicKeyFromFile loads an Ed25519 public key from a file.
func LoadEd25519PublicKeyFromFile(path string) (ed25519.PublicKey, error) {
	pemData, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read public key file: %w", err)
	}
	return LoadEd25519PublicKey(pemData)
}

// parsePrivateKey parses a PEM-encoded private key.
func parsePrivateKey(pemData []byte) (any, error) {
	block, _ := pem.Decode(pemData)
	if block == nil {
		return nil, ErrInvalidPEMFormat
	}

	// Try PKCS8 format first (Ed25519 uses PKCS8)
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err == nil {
		return key, nil
	}

	return nil, fmt.Errorf("%w: failed to parse private key", ErrInvalidPEMFormat)
}

// parsePublicKey parses a PEM-encoded public key.
func parsePublicKey(pemData []byte) (any, error) {
	block, _ := pem.Decode(pemData)
	if block == nil {
		return nil, ErrInvalidPEMFormat
	}

	// Try PKIX format
	key, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to parse public key: %w", ErrInvalidPEMFormat, err)
	}

	return key, nil
}

// marshalPrivateKey encodes a private key to PEM format.
func marshalPrivateKey(key ed25519.PrivateKey) ([]byte, error) {
	bytes, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal private key: %w", err)
	}

	block := &pem.Block{
		Type:  "ED25519 PRIVATE KEY",
		Bytes: bytes,
	}

	return pem.EncodeToMemory(block), nil
}

// marshalPublicKey encodes a public key to PEM format.
func marshalPublicKey(key ed25519.PublicKey) ([]byte, error) {
	bytes, err := x509.MarshalPKIXPublicKey(key)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal public key: %w", err)
	}

	block := &pem.Block{
		Type:  "ED25519 PUBLIC KEY",
		Bytes: bytes,
	}

	return pem.EncodeToMemory(block), nil
}
