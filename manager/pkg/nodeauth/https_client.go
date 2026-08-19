package nodeauth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/containerd/errdefs"
)

// HTTPSClientConfig configures one credential-rotating node-to-region client.
type HTTPSClientConfig struct {
	Authority      string
	BaseURL        string
	CAFile         string
	ClientCertFile string
	ClientKeyFile  string
	TokenFile      string
	Timeout        time.Duration
}

// HTTPSClient owns transport security and reloads its bearer token per request.
type HTTPSClient struct {
	authority string
	baseURL   *url.URL
	tokenFile string
	http      *http.Client
}

func NewHTTPSClient(config HTTPSClientConfig) (*HTTPSClient, error) {
	authority := strings.TrimSpace(config.Authority)
	if authority == "" {
		return nil, fmt.Errorf("node authority name is required: %w", errdefs.ErrInvalidArgument)
	}
	baseURL, err := url.Parse(strings.TrimSpace(config.BaseURL))
	if err != nil || baseURL.Scheme != "https" || baseURL.Host == "" || baseURL.User != nil ||
		baseURL.RawQuery != "" || baseURL.Fragment != "" {
		return nil, fmt.Errorf("%s URL must be an HTTPS origin: %w", authority, errdefs.ErrInvalidArgument)
	}
	if strings.TrimSpace(config.CAFile) == "" || strings.TrimSpace(config.TokenFile) == "" {
		return nil, fmt.Errorf("%s CA and bearer token files are required: %w", authority, errdefs.ErrInvalidArgument)
	}
	var certificates []tls.Certificate
	if strings.TrimSpace(config.ClientCertFile) != "" || strings.TrimSpace(config.ClientKeyFile) != "" {
		if strings.TrimSpace(config.ClientCertFile) == "" || strings.TrimSpace(config.ClientKeyFile) == "" {
			return nil, fmt.Errorf("%s client certificate and key must be configured together: %w", authority, errdefs.ErrInvalidArgument)
		}
		certificate, err := tls.LoadX509KeyPair(config.ClientCertFile, config.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load %s client identity: %w", authority, err)
		}
		certificates = append(certificates, certificate)
	}
	caPEM, err := os.ReadFile(strings.TrimSpace(config.CAFile))
	if err != nil {
		return nil, fmt.Errorf("read %s CA: %w", authority, err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("%s CA contains no certificates: %w", authority, errdefs.ErrInvalidArgument)
	}
	if config.Timeout <= 0 {
		config.Timeout = 2 * time.Second
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// Node authority credentials must never traverse ambient proxy settings.
	transport.Proxy = nil
	transport.TLSClientConfig = &tls.Config{
		MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: certificates,
	}
	return &HTTPSClient{
		authority: authority, baseURL: baseURL, tokenFile: strings.TrimSpace(config.TokenFile),
		http: &http.Client{Transport: transport, Timeout: config.Timeout},
	}, nil
}

// NewRequest creates an authenticated request for one already-escaped absolute
// protocol path. It reloads the token so projected credential rotation works.
func (c *HTTPSClient) NewRequest(ctx context.Context, method, escapedPath string, body io.Reader) (*http.Request, error) {
	if c == nil || c.baseURL == nil || c.http == nil {
		return nil, fmt.Errorf("node authority client is not initialized: %w", errdefs.ErrUnavailable)
	}
	if !strings.HasPrefix(escapedPath, "/") || strings.ContainsAny(escapedPath, "?#") {
		return nil, fmt.Errorf("%s request path is invalid: %w", c.authority, errdefs.ErrInvalidArgument)
	}
	token, err := os.ReadFile(c.tokenFile)
	if err != nil {
		return nil, fmt.Errorf("read %s bearer token: %w: %w", c.authority, err, errdefs.ErrUnavailable)
	}
	bearer := strings.TrimSpace(string(token))
	if bearer == "" {
		return nil, fmt.Errorf("%s bearer token is empty: %w", c.authority, errdefs.ErrUnavailable)
	}
	target := *c.baseURL
	rawPath := strings.TrimRight(target.EscapedPath(), "/") + escapedPath
	decodedPath, err := url.PathUnescape(rawPath)
	if err != nil {
		return nil, fmt.Errorf("%s request path is invalid: %w", c.authority, errdefs.ErrInvalidArgument)
	}
	target.Path = decodedPath
	target.RawPath = rawPath
	request, err := http.NewRequestWithContext(ctx, method, target.String(), body)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+bearer)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	return request, nil
}

func (c *HTTPSClient) Do(request *http.Request) (*http.Response, error) {
	if c == nil || c.http == nil {
		return nil, fmt.Errorf("node authority client is not initialized: %w", errdefs.ErrUnavailable)
	}
	return c.http.Do(request)
}
