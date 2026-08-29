package nodebootstrap

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"slices"
	"time"
)

func loadSigner(file string) (crypto.Signer, error) {
	payload, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	block, rest := pem.Decode(payload)
	if block == nil || len(bytes.TrimSpace(rest)) != 0 {
		return nil, errors.New("node bootstrap key file must contain one PEM private key")
	}
	var parsed any
	switch block.Type {
	case "PRIVATE KEY":
		parsed, err = x509.ParsePKCS8PrivateKey(block.Bytes)
	case "EC PRIVATE KEY":
		parsed, err = x509.ParseECPrivateKey(block.Bytes)
	case "RSA PRIVATE KEY":
		parsed, err = x509.ParsePKCS1PrivateKey(block.Bytes)
	default:
		return nil, errors.New("node bootstrap key type is unsupported")
	}
	if err != nil {
		return nil, err
	}
	signer, ok := parsed.(crypto.Signer)
	if !ok {
		return nil, errors.New("node bootstrap key is not a signer")
	}
	return signer, nil
}

func ensureAuthoritySigner(file string) (crypto.Signer, error) {
	if signer, err := loadSigner(file); err == nil {
		return signer, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	key, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		return nil, err
	}
	raw, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, err
	}
	if err := atomicWriteFile(file, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: raw}), 0o600); err != nil {
		return nil, err
	}
	return key, nil
}

func makeCSR(signer crypto.Signer) ([]byte, error) {
	raw, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{}, SignatureAlgorithm: signatureAlgorithm(signer),
	}, signer)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: raw}), nil
}

func signatureAlgorithm(signer crypto.Signer) x509.SignatureAlgorithm {
	switch key := signer.(type) {
	case *ecdsa.PrivateKey:
		if key.Curve == elliptic.P384() {
			return x509.ECDSAWithSHA384
		}
		return x509.ECDSAWithSHA256
	case *rsa.PrivateKey:
		return x509.SHA256WithRSA
	default:
		return x509.UnknownSignatureAlgorithm
	}
}

func parseOneCertificate(payload []byte) (*x509.Certificate, error) {
	block, rest := pem.Decode(payload)
	if block == nil || block.Type != "CERTIFICATE" || len(bytes.TrimSpace(rest)) != 0 {
		return nil, errors.New("certificate response must contain one PEM certificate")
	}
	return x509.ParseCertificate(block.Bytes)
}

func verifyIssuedCertificate(
	certificatePEM, caPEM []byte,
	signer crypto.Signer,
	commonName, privateIP, uriValue string,
	usage x509.ExtKeyUsage,
) error {
	certificate, err := parseOneCertificate(certificatePEM)
	if err != nil {
		return err
	}
	ca, err := parseOneCertificate(caPEM)
	if err != nil {
		return err
	}
	certPublic, err := x509.MarshalPKIXPublicKey(certificate.PublicKey)
	if err != nil {
		return err
	}
	keyPublic, err := x509.MarshalPKIXPublicKey(signer.Public())
	if err != nil || !bytes.Equal(certPublic, keyPublic) {
		return errors.New("issued certificate does not match the node private key")
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	if _, err := certificate.Verify(x509.VerifyOptions{
		Roots: roots, KeyUsages: []x509.ExtKeyUsage{usage}, CurrentTime: time.Now(),
	}); err != nil {
		return fmt.Errorf("verify issued node certificate: %w", err)
	}
	if certificate.Subject.CommonName != commonName {
		return errors.New("issued certificate common name is not exact")
	}
	if privateIP != "" {
		ip := net.ParseIP(privateIP)
		if ip == nil || !slices.ContainsFunc(certificate.IPAddresses, func(item net.IP) bool { return item.Equal(ip) }) {
			return errors.New("issued certificate does not contain the exact private address")
		}
	}
	if uriValue != "" {
		expected, err := url.Parse(uriValue)
		if err != nil || !slices.ContainsFunc(certificate.URIs, func(item *url.URL) bool {
			return item.String() == expected.String()
		}) {
			return errors.New("issued certificate does not contain the exact identity URI")
		}
	}
	return nil
}

func certificateNeedsRenewal(file string, renewBefore time.Duration) (bool, error) {
	payload, err := os.ReadFile(file)
	if err != nil {
		return false, err
	}
	certificate, err := parseOneCertificate(payload)
	if err != nil {
		return false, err
	}
	return time.Until(certificate.NotAfter) <= renewBefore, nil
}
