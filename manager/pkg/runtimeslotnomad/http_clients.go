package runtimeslotnomad

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/containerd/errdefs"
)

const maxNomadHTTPClients = 64

type nomadHTTPClients struct {
	mu      sync.Mutex
	entries map[Endpoint]*nomadHTTPClient
}

type nomadHTTPClient struct {
	identity  [32]byte
	client    *http.Client
	baseURL   *url.URL
	transport *nomadExpiringTransport
}

// Connections belong to an exact resolver endpoint and exact credential bytes.
// Credentials and ACL tokens are still read on every operation; rotation never
// falls back to a transport authenticated with older credentials.
func (a *HTTPAPI) nomadHTTPClient(endpoint Endpoint) (*http.Client, *url.URL, error) {
	var identityFiles [3][]byte
	hash := sha256.New()
	for i, path := range []string{endpoint.CAFile, endpoint.ClientCertFile, endpoint.ClientKeyFile} {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, nil, fmt.Errorf("read Nomad TLS identity: %w: %w", err, errdefs.ErrUnavailable)
		}
		identityFiles[i] = data
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(data)))
		_, _ = hash.Write(size[:])
		_, _ = hash.Write(data)
	}
	var identity [32]byte
	copy(identity[:], hash.Sum(nil))
	a.clients.mu.Lock()
	defer a.clients.mu.Unlock()
	if a.clients.entries == nil {
		a.clients.entries = make(map[Endpoint]*nomadHTTPClient)
	}
	if entry := a.clients.entries[endpoint]; entry != nil {
		if entry.identity == identity && time.Now().UnixNano() < entry.transport.expiresAt.Load() {
			return entry.client, entry.baseURL, nil
		}
		entry.transport.CloseIdleConnections()
		delete(a.clients.entries, endpoint)
	}
	client, baseURL, err := newNomadHTTPClient(endpoint, identityFiles[0], identityFiles[1], identityFiles[2])
	if err != nil {
		return nil, nil, err
	}
	transport := &nomadExpiringTransport{Transport: client.Transport.(*http.Transport)}
	transport.expiresAt.Store(time.Now().Add(30 * time.Second).UnixNano())
	for _, certificate := range transport.TLSClientConfig.Certificates {
		for _, raw := range certificate.Certificate {
			parsed, err := x509.ParseCertificate(raw)
			if err != nil {
				return nil, nil, fmt.Errorf("parse Nomad client certificate: %w", err)
			}
			transport.shortenExpiry(parsed.NotAfter)
		}
	}
	client.Transport = transport
	if len(a.clients.entries) >= maxNomadHTTPClients {
		for key, entry := range a.clients.entries {
			entry.transport.CloseIdleConnections()
			delete(a.clients.entries, key)
			break
		}
	}
	a.clients.entries[endpoint] = &nomadHTTPClient{identity: identity, client: client, baseURL: baseURL, transport: transport}
	return client, baseURL, nil
}

// A reused connection may not outlive its verified peer or client certificate.
// Expiry rejects before sending another operation; the next lookup rebuilds it.
type nomadExpiringTransport struct {
	*http.Transport
	expiresAt atomic.Int64
}

func (t *nomadExpiringTransport) shortenExpiry(expires time.Time) {
	for old := t.expiresAt.Load(); expires.UnixNano() < old; old = t.expiresAt.Load() {
		if t.expiresAt.CompareAndSwap(old, expires.UnixNano()) {
			return
		}
	}
}

func (t *nomadExpiringTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if time.Now().UnixNano() >= t.expiresAt.Load() {
		return nil, fmt.Errorf("Nomad TLS transport expired: %w", errdefs.ErrUnavailable)
	}
	response, err := t.Transport.RoundTrip(request)
	if err == nil && response.TLS != nil {
		for _, chain := range response.TLS.VerifiedChains {
			for _, cert := range chain {
				t.shortenExpiry(cert.NotAfter)
			}
		}
	}
	return response, err
}
