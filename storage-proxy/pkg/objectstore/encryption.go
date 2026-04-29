package objectstore

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strings"
)

const (
	EncryptionAlgoAES256GCMRSA = "aes256gcm-rsa"
	EncryptionAlgoCHACHA20RSA  = "chacha20-rsa"
)

func LoadEncryptionKey(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("encryption key path is empty")
	}
	pemBytes, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read encryption key: %w", err)
	}
	if len(pemBytes) == 0 {
		return "", fmt.Errorf("encryption key is empty")
	}
	return string(pemBytes), nil
}

func NewKeyEncryptor(keyPEM, passphrase string) (Encryptor, error) {
	if strings.TrimSpace(keyPEM) == "" {
		return nil, fmt.Errorf("encryption key is empty")
	}
	privateKey, err := parsePrivateKeyFromPEM([]byte(keyPEM), []byte(passphrase))
	if err != nil {
		return nil, err
	}
	return newRSAEncryptor(privateKey), nil
}

func parsePrivateKeyFromPEM(enc []byte, passphrase []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(enc)
	if block == nil {
		return nil, errors.New("failed to parse PEM block containing the key")
	}
	buf := block.Bytes
	if strings.Contains(block.Headers["Proc-Type"], "ENCRYPTED") ||
		strings.Contains(block.Type, "ENCRYPTED") {
		if len(passphrase) == 0 {
			return nil, errors.New("passphrase is required to private key")
		}
		return nil, errors.New("encrypted PEM is not supported; use unencrypted PKCS1/PKCS8")
	}
	privateKey, err := x509.ParsePKCS1PrivateKey(buf)
	if err == nil {
		return privateKey, nil
	}
	key, err := x509.ParsePKCS8PrivateKey(buf)
	if err != nil {
		return nil, err
	}
	rsaKey, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("unsupported private key type %T", key)
	}
	return rsaKey, nil
}

type rsaEncryptor struct {
	privKey *rsa.PrivateKey
	label   []byte
}

func newRSAEncryptor(privKey *rsa.PrivateKey) Encryptor {
	return &rsaEncryptor{privKey: privKey, label: []byte("keys")}
}

func (e *rsaEncryptor) Encrypt(plaintext []byte) ([]byte, error) {
	return rsa.EncryptOAEP(sha256.New(), rand.Reader, &e.privKey.PublicKey, plaintext, e.label)
}

func (e *rsaEncryptor) Decrypt(ciphertext []byte) ([]byte, error) {
	return rsa.DecryptOAEP(sha256.New(), rand.Reader, e.privKey, ciphertext, e.label)
}
