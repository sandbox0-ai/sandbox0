package proxy

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"sync"
	"time"
)

type tlsInterceptAuthority interface {
	CertificateForHost(string) (*tls.Certificate, error)
}

type certificateAuthority struct {
	cert       *x509.Certificate
	signer     crypto.Signer
	leafTTL    time.Duration
	mu         sync.Mutex
	cache      map[string]*tlsCertificateCacheEntry
	now        func() time.Time
	serialBits int
}

type tlsCertificateCacheEntry struct {
	cert      *tls.Certificate
	expiresAt time.Time
}

func newCertificateAuthorityFromFiles(certPath, keyPath string, leafTTL time.Duration) (*certificateAuthority, error) {
	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		return nil, fmt.Errorf("read mitm ca cert: %w", err)
	}
	keyPEM, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("read mitm ca key: %w", err)
	}
	return newCertificateAuthority(certPEM, keyPEM, leafTTL)
}

func newCertificateAuthority(certPEM, keyPEM []byte, leafTTL time.Duration) (*certificateAuthority, error) {
	if leafTTL <= 0 {
		leafTTL = time.Hour
	}
	certBlock, _ := pem.Decode(certPEM)
	if certBlock == nil {
		return nil, fmt.Errorf("decode mitm ca cert pem")
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse mitm ca cert: %w", err)
	}
	key, err := parsePrivateKey(keyPEM)
	if err != nil {
		return nil, fmt.Errorf("parse mitm ca key: %w", err)
	}
	signer, ok := key.(crypto.Signer)
	if !ok {
		return nil, fmt.Errorf("mitm ca key does not implement crypto.Signer")
	}
	return &certificateAuthority{
		cert:       cert,
		signer:     signer,
		leafTTL:    leafTTL,
		cache:      make(map[string]*tlsCertificateCacheEntry),
		now:        func() time.Time { return time.Now().UTC() },
		serialBits: 128,
	}, nil
}

func (a *certificateAuthority) CertificateForHost(host string) (*tls.Certificate, error) {
	if a == nil {
		return nil, fmt.Errorf("tls intercept authority is nil")
	}
	host = normalizeHost(host)
	if host == "" {
		return nil, fmt.Errorf("tls intercept host is required")
	}
	now := a.now()
	a.mu.Lock()
	defer a.mu.Unlock()
	if cached := a.cache[host]; cached != nil && cached.cert != nil && cached.expiresAt.After(now.Add(30*time.Second)) {
		return cached.cert, nil
	}
	cert, expiresAt, err := a.issueLeafCertificate(host, now)
	if err != nil {
		return nil, err
	}
	a.cache[host] = &tlsCertificateCacheEntry{
		cert:      cert,
		expiresAt: expiresAt,
	}
	return cert, nil
}

func (a *certificateAuthority) issueLeafCertificate(host string, now time.Time) (*tls.Certificate, time.Time, error) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("generate mitm leaf key: %w", err)
	}
	serialLimit := new(big.Int).Lsh(big.NewInt(1), uint(a.serialBits))
	serialNumber, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("generate mitm leaf serial: %w", err)
	}
	notBefore := now.Add(-5 * time.Minute)
	notAfter := now.Add(a.leafTTL)
	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName: host,
		},
		DNSNames:              []string{host},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, a.cert, privateKey.Public(), a.signer)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("issue mitm leaf cert: %w", err)
	}
	tlsCert := &tls.Certificate{
		Certificate: [][]byte{der, a.cert.Raw},
		PrivateKey:  privateKey,
		Leaf:        template,
	}
	return tlsCert, notAfter, nil
}

func parsePrivateKey(pemData []byte) (any, error) {
	block, _ := pem.Decode(pemData)
	if block == nil {
		return nil, fmt.Errorf("decode private key pem")
	}
	if key, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	if key, err := x509.ParseECPrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	return nil, fmt.Errorf("unsupported private key format")
}

func cloneTLSConfig(cfg *tls.Config) *tls.Config {
	if cfg == nil {
		return &tls.Config{}
	}
	return cfg.Clone()
}

func newSelfSignedCertificateAuthority(commonName string, leafTTL time.Duration) ([]byte, []byte, error) {
	if commonName == "" {
		commonName = "sandbox0-netd-root"
	}
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, fmt.Errorf("generate ca key: %w", err)
	}
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, fmt.Errorf("generate ca serial: %w", err)
	}
	now := time.Now().UTC()
	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName: commonName,
		},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA:                  true,
		BasicConstraintsValid: true,
		MaxPathLen:            1,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return nil, nil, fmt.Errorf("create ca cert: %w", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	if leafTTL <= 0 {
		_ = leafTTL
	}
	return certPEM, keyPEM, nil
}
