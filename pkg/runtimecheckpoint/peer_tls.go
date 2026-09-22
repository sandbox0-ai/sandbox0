package runtimecheckpoint

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"time"

	"github.com/opencontainers/go-digest"
)

const PeerImagePath = "/internal/v1/migration-image"
const peerServerName = "migration-peer.sandbox0.internal"

// PeerEndpoint is public transport information returned on the authenticated
// node channel and bound into the regional publication receipt. It contains
// no private key or bearer secret. The exact certificate is the trust anchor;
// hostnames, ambient proxy settings and redirects cannot redirect image data.
type PeerEndpoint struct {
	Address     string `json:"address"`
	Certificate string `json:"certificate"`
}

func (p PeerEndpoint) certificate() (*x509.Certificate, error) {
	if len(p.Certificate) > 4096 {
		return nil, fmt.Errorf("peer certificate exceeds limit")
	}
	der, err := base64.StdEncoding.Strict().DecodeString(p.Certificate)
	if err != nil || base64.StdEncoding.EncodeToString(der) != p.Certificate {
		return nil, fmt.Errorf("invalid peer certificate encoding")
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	if err := cert.CheckSignatureFrom(cert); err != nil {
		return nil, fmt.Errorf("peer certificate must be its own pinned trust anchor: %w", err)
	}
	if err := cert.VerifyHostname(peerServerName); err != nil {
		return nil, err
	}
	return cert, nil
}

func (p PeerEndpoint) Validate() error {
	u, err := url.Parse(p.Address)
	if err != nil || u.Scheme != "https" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return fmt.Errorf("peer address must be a canonical HTTPS origin")
	}
	if err := ValidatePeerAddress(u.Host); err != nil {
		return err
	}
	if p.Address != "https://"+u.Host {
		return fmt.Errorf("noncanonical peer origin")
	}
	_, err = p.certificate()
	return err
}

// CertificateDigest binds an advertised endpoint to the certificate already
// committed with its staging reservation. It never consults ambient PKI.
func (p PeerEndpoint) CertificateDigest() (string, error) {
	if err := p.Validate(); err != nil {
		return "", err
	}
	cert, err := p.certificate()
	if err != nil {
		return "", err
	}
	return digest.FromBytes(cert.Raw).String(), nil
}

// ValidatePeerAddress requires a concrete private interface and fixed port.
// Loopback is supported for isolated tests; DNS and unspecified binds are not.
func ValidatePeerAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("invalid migration peer address: %w", err)
	}
	ip, err := netip.ParseAddr(host)
	if err != nil || ip.Zone() != "" || ip.Is4In6() || (!ip.IsPrivate() && !ip.IsLoopback()) || ip.String() != host {
		return fmt.Errorf("migration peer must use a concrete private IP")
	}
	p, err := strconv.Atoi(port)
	if err != nil || p < 1 || p > 65535 || strconv.Itoa(p) != port {
		return fmt.Errorf("migration peer requires a fixed port")
	}
	return nil
}

func ValidatePeerCertificateDigest(value string) error { return validateDigest(value) }

// NewPeerIdentity generates a process-owned key. Only its certificate is
// exchanged over the manager channel. Restarting a node deliberately changes
// this key; an older operation falls back to its durable regional image.
func NewPeerIdentity() (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, err
	}
	now := time.Now()
	template := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: peerServerName},
		DNSNames: []string{peerServerName}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(365 * 24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, nil
}

func PeerCertificateDigest(cert tls.Certificate) string {
	if len(cert.Certificate) == 0 {
		return ""
	}
	return digest.FromBytes(cert.Certificate[0]).String()
}

func NewPeerEndpoint(address string, cert tls.Certificate) (PeerEndpoint, error) {
	if len(cert.Certificate) == 0 {
		return PeerEndpoint{}, fmt.Errorf("peer certificate is required")
	}
	p := PeerEndpoint{Address: "https://" + address, Certificate: base64.StdEncoding.EncodeToString(cert.Certificate[0])}
	return p, p.Validate()
}

// NewPeerClient authenticates both directions using manager-distributed public
// certificates. The source handler must compare the client's leaf digest with
// the exact destination digest in its journaled publication command.
func NewPeerClient(peer PeerEndpoint, identity tls.Certificate) (*http.Client, error) {
	if err := peer.Validate(); err != nil {
		return nil, err
	}
	if len(identity.Certificate) == 0 || identity.PrivateKey == nil {
		return nil, fmt.Errorf("peer client identity is required")
	}
	cert, err := peer.certificate()
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	transport := &http.Transport{Proxy: nil, DisableCompression: true,
		DialContext:         (&net.Dialer{Timeout: time.Second}).DialContext,
		TLSClientConfig:     &tls.Config{MinVersion: tls.VersionTLS13, ServerName: peerServerName, RootCAs: roots, Certificates: []tls.Certificate{identity}},
		TLSHandshakeTimeout: time.Second, ResponseHeaderTimeout: 2 * time.Second,
		MaxResponseHeaderBytes: 4096, MaxConnsPerHost: 1, IdleConnTimeout: time.Second}
	// The migration operation owns the body deadline through request.Context.
	// A fixed client timeout would reject large but valid checkpoint images.
	return &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return fmt.Errorf("peer redirects are forbidden") }}, nil
}
