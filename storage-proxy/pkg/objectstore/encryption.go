package objectstore

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/crypto/chacha20poly1305"
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

func NewEncryptor(keyPEM, passphrase, algo string) (Encryptor, error) {
	if strings.TrimSpace(keyPEM) == "" {
		return nil, fmt.Errorf("encryption key is empty")
	}
	privateKey, err := parsePrivateKeyFromPEM([]byte(keyPEM), []byte(passphrase))
	if err != nil {
		return nil, err
	}
	return newDataEncryptor(privateKey, algo)
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

type dataEncryptor struct {
	keyEncryptor Encryptor
	keyLen       int
	aead         func(key []byte) (cipher.AEAD, error)
}

func newDataEncryptor(privKey *rsa.PrivateKey, algo string) (Encryptor, error) {
	switch algo {
	case "", EncryptionAlgoAES256GCMRSA:
		aead := func(key []byte) (cipher.AEAD, error) {
			block, err := aes.NewCipher(key)
			if err != nil {
				return nil, err
			}
			return cipher.NewGCM(block)
		}
		return &dataEncryptor{keyEncryptor: newRSAEncryptor(privKey), keyLen: 32, aead: aead}, nil
	case EncryptionAlgoCHACHA20RSA:
		return &dataEncryptor{keyEncryptor: newRSAEncryptor(privKey), keyLen: chacha20poly1305.KeySize, aead: chacha20poly1305.New}, nil
	default:
		return nil, fmt.Errorf("unsupported encryption algorithm: %s", algo)
	}
}

func (e *dataEncryptor) Encrypt(plaintext []byte) ([]byte, error) {
	key := make([]byte, e.keyLen)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return nil, err
	}
	cipherKey, err := e.keyEncryptor.Encrypt(key)
	if err != nil {
		return nil, err
	}
	aead, err := e.aead(key)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	headerSize := 3 + len(cipherKey) + len(nonce)
	buf := make([]byte, headerSize+len(plaintext)+aead.Overhead())
	buf[0] = byte(len(cipherKey) >> 8)
	buf[1] = byte(len(cipherKey) & 0xFF)
	buf[2] = byte(len(nonce))
	p := buf[3:]
	copy(p, cipherKey)
	p = p[len(cipherKey):]
	copy(p, nonce)
	p = p[len(nonce):]
	ciphertext := aead.Seal(p[:0], nonce, plaintext, nil)
	return buf[:headerSize+len(ciphertext)], nil
}

func (e *dataEncryptor) Decrypt(ciphertext []byte) ([]byte, error) {
	if len(ciphertext) < 3 {
		return nil, fmt.Errorf("encrypted payload is too short")
	}
	keyLen := int(ciphertext[0])<<8 + int(ciphertext[1])
	nonceLen := int(ciphertext[2])
	if 3+keyLen+nonceLen >= len(ciphertext) {
		return nil, fmt.Errorf("malformed ciphertext: %d %d", keyLen, nonceLen)
	}
	ciphertext = ciphertext[3:]
	cipherKey := ciphertext[:keyLen]
	nonce := ciphertext[keyLen : keyLen+nonceLen]
	ciphertext = ciphertext[keyLen+nonceLen:]
	key, err := e.keyEncryptor.Decrypt(cipherKey)
	if err != nil {
		return nil, fmt.Errorf("decrypt key: %w", err)
	}
	aead, err := e.aead(key)
	if err != nil {
		return nil, err
	}
	return aead.Open(ciphertext[:0], nonce, ciphertext, nil)
}
