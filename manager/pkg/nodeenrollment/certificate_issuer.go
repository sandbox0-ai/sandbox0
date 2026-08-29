package nodeenrollment

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"strings"
	"time"
)

type X509IssuerConfig struct {
	RegionID            string
	NomadCACertFile     string
	NomadCAKeyFile      string
	AuthorityCACertFile string
	AuthorityCAKeyFile  string
	ExactTTL            time.Duration
	Now                 func() time.Time
}

type X509Issuer struct {
	regionID     string
	nomadCA      *x509.Certificate
	nomadKey     crypto.Signer
	authorityCA  *x509.Certificate
	authorityKey crypto.Signer
	nomadCAPEM   []byte
	exactTTL     time.Duration
	now          func() time.Time
}

func NewX509Issuer(config X509IssuerConfig) (*X509Issuer, error) {
	config.RegionID = strings.TrimSpace(config.RegionID)
	if config.RegionID == "" {
		return nil, errors.New("certificate issuer region ID is required")
	}
	nomadCA, nomadKey, nomadPEM, err := loadCertificateAuthority(config.NomadCACertFile, config.NomadCAKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load Nomad CA: %w", err)
	}
	authorityCA, authorityKey, _, err := loadCertificateAuthority(config.AuthorityCACertFile, config.AuthorityCAKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load node authority CA: %w", err)
	}
	if config.ExactTTL == 0 {
		config.ExactTTL = 24 * time.Hour
	}
	if config.ExactTTL < time.Hour || config.ExactTTL > 7*24*time.Hour {
		return nil, errors.New("exact node certificate TTL must be between one hour and seven days")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &X509Issuer{regionID: config.RegionID, nomadCA: nomadCA, nomadKey: nomadKey,
		authorityCA: authorityCA, authorityKey: authorityKey, nomadCAPEM: nomadPEM,
		exactTTL: config.ExactTTL, now: config.Now}, nil
}

func (i *X509Issuer) IssueNomadBootstrap(
	ctx context.Context,
	nodeName, privateIP string,
	csrPEM []byte,
) ([]byte, []byte, error) {
	uri := fmt.Sprintf("spiffe://sandbox0.internal/%s/nomad/client/bootstrap/%s", i.regionID, nodeName)
	certificate, err := i.issue(ctx, i.nomadCA, i.nomadKey, csrPEM,
		"client."+strings.ReplaceAll(i.regionID, "-", "_")+".nomad", privateIP, uri, 30*time.Minute,
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth})
	return certificate, append([]byte(nil), i.nomadCAPEM...), err
}

func (i *X509Issuer) IssueNomadExact(
	ctx context.Context,
	nomadNodeID, privateIP, _ string,
	csrPEM []byte,
) ([]byte, error) {
	uri := fmt.Sprintf("spiffe://sandbox0.internal/%s/nomad/client/%s", i.regionID, nomadNodeID)
	return i.issue(ctx, i.nomadCA, i.nomadKey, csrPEM,
		"client."+strings.ReplaceAll(i.regionID, "-", "_")+".nomad", privateIP, uri, i.exactTTL,
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth})
}

func (i *X509Issuer) IssueNodeAuthority(
	ctx context.Context,
	commonName, agentUID string,
	csrPEM []byte,
) ([]byte, error) {
	uri := fmt.Sprintf("spiffe://sandbox0.internal/%s/node-authority/%s", i.regionID, url.PathEscape(agentUID))
	return i.issue(ctx, i.authorityCA, i.authorityKey, csrPEM,
		commonName, "", uri, i.exactTTL, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
}

func (i *X509Issuer) issue(
	ctx context.Context,
	ca *x509.Certificate,
	key crypto.Signer,
	csrPEM []byte,
	commonName, privateIP, uriValue string,
	ttl time.Duration,
	usages []x509.ExtKeyUsage,
) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	block, rest := pem.Decode(csrPEM)
	if block == nil || block.Type != "CERTIFICATE REQUEST" || len(strings.TrimSpace(string(rest))) != 0 {
		return nil, errors.New("node CSR must contain one PEM certificate request")
	}
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil || csr.CheckSignature() != nil {
		return nil, errors.New("node CSR signature is invalid")
	}
	if csr.Subject.String() != "" || len(csr.DNSNames) != 0 || len(csr.IPAddresses) != 0 || len(csr.URIs) != 0 ||
		len(csr.EmailAddresses) != 0 || len(csr.Extensions) != 0 {
		return nil, errors.New("node CSR must not request caller-controlled identity attributes")
	}
	uri, err := url.Parse(uriValue)
	if err != nil {
		return nil, err
	}
	serialLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		return nil, err
	}
	now := i.now().UTC()
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: commonName, Organization: []string{"Sandbox0"}},
		NotBefore: now.Add(-2 * time.Minute), NotAfter: now.Add(ttl),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: usages, URIs: []*url.URL{uri},
	}
	if privateIP != "" {
		ip := net.ParseIP(privateIP)
		if ip == nil || ip.To4() == nil {
			return nil, errors.New("node certificate private IP is invalid")
		}
		template.IPAddresses = []net.IP{ip.To4()}
	}
	raw, err := x509.CreateCertificate(rand.Reader, template, ca, csr.PublicKey, key)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: raw}), nil
}

func loadCertificateAuthority(certFile, keyFile string) (*x509.Certificate, crypto.Signer, []byte, error) {
	certPEM, err := os.ReadFile(certFile)
	if err != nil {
		return nil, nil, nil, err
	}
	block, rest := pem.Decode(certPEM)
	if block == nil || block.Type != "CERTIFICATE" || len(strings.TrimSpace(string(rest))) != 0 {
		return nil, nil, nil, errors.New("CA file must contain one certificate")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil || !certificate.IsCA {
		return nil, nil, nil, errors.New("CA certificate is invalid")
	}
	keyPEM, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, nil, nil, err
	}
	keyBlock, keyRest := pem.Decode(keyPEM)
	if keyBlock == nil || len(strings.TrimSpace(string(keyRest))) != 0 {
		return nil, nil, nil, errors.New("CA key file must contain one private key")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
	if err != nil {
		switch keyBlock.Type {
		case "EC PRIVATE KEY":
			parsed, err = x509.ParseECPrivateKey(keyBlock.Bytes)
		case "RSA PRIVATE KEY":
			parsed, err = x509.ParsePKCS1PrivateKey(keyBlock.Bytes)
		}
		if err != nil {
			return nil, nil, nil, err
		}
	}
	signer, ok := parsed.(crypto.Signer)
	if !ok {
		return nil, nil, nil, errors.New("CA private key is not a signer")
	}
	return certificate, signer, certPEM, nil
}
