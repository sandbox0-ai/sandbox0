package runtimeslotnomad

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotreconciler"
)

const (
	defaultNomadTimeout   = 5 * time.Second
	maxNomadResponseBytes = 2 << 20
	maxNomadErrorBytes    = 64 << 10
	maxNomadTokenBytes    = 64 << 10
)

// Endpoint is trusted resolver output for one Nomad server or exact Nomad
// client. Standard DNS/IP certificate verification and the exact SPIFFE URI
// SAN are both required.
type Endpoint struct {
	ClusterID      string
	NodeID         string
	BaseURL        string
	CAFile         string
	ClientCertFile string
	ClientKeyFile  string
	TokenFile      string
	PeerURISAN     string
	Timeout        time.Duration
}

// EndpointResolver maps region-owned cluster and node identities to trusted
// Nomad HTTPS endpoints. Implementations must not use an endpoint supplied by
// an allocation or task-driver request.
type EndpointResolver interface {
	ServerEndpoint(context.Context, string) (Endpoint, error)
	ClientEndpoint(context.Context, string, string) (Endpoint, error)
}

// HTTPAPI calls Nomad's server allocation endpoint and the exact client's
// allocation-filesystem/GC endpoints over mutually authenticated TLS.
type HTTPAPI struct {
	resolver EndpointResolver
}

var _ API = (*HTTPAPI)(nil)

// NewHTTPAPI constructs a secure Nomad API implementation.
func NewHTTPAPI(resolver EndpointResolver) (*HTTPAPI, error) {
	if resolver == nil {
		return nil, errors.New("Nomad endpoint resolver is required")
	}
	return &HTTPAPI{resolver: resolver}, nil
}

func (a *HTTPAPI) ServerAllocation(
	ctx context.Context,
	target runtimeslotreconciler.AllocationTarget,
) (*Allocation, error) {
	if err := validateTarget(target); err != nil {
		return nil, err
	}
	endpoint, err := a.serverEndpoint(ctx, target)
	if err != nil {
		return nil, err
	}
	status, payload, err := exchangeNomad(ctx, endpoint, http.MethodGet,
		allocationPath(target.AllocationID), namespaceQuery(target.AllocationNamespace), true)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNotFound {
		return nil, nil
	}
	if status/100 != 2 {
		return nil, nomadResponseError("read server allocation", status, payload)
	}
	var allocation Allocation
	decoder := json.NewDecoder(bytes.NewReader(payload))
	if err := decoder.Decode(&allocation); err != nil {
		return nil, fmt.Errorf("decode Nomad server allocation: %w: %w", err, errdefs.ErrUnavailable)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("Nomad server allocation contains trailing data: %w", errdefs.ErrUnavailable)
	}
	return &allocation, nil
}

func (a *HTTPAPI) ClientAllocationPresent(
	ctx context.Context,
	target runtimeslotreconciler.AllocationTarget,
) (bool, error) {
	if err := validateTarget(target); err != nil {
		return false, err
	}
	endpoint, err := a.clientEndpoint(ctx, target)
	if err != nil {
		return false, err
	}
	query := namespaceQuery(target.AllocationNamespace)
	query.Set("path", "alloc/")
	status, payload, err := exchangeNomad(ctx, endpoint, http.MethodGet,
		clientAllocationFSStatPath(target.AllocationID), query, false)
	if err != nil {
		return false, err
	}
	if status == http.StatusNotFound {
		return false, nil
	}
	if status/100 != 2 {
		return false, nomadResponseError("observe client allocation", status, payload)
	}
	return true, nil
}

func (a *HTTPAPI) StopAllocation(
	ctx context.Context,
	target runtimeslotreconciler.AllocationTarget,
	operationID string,
) error {
	if err := validateTarget(target); err != nil {
		return err
	}
	if err := validateOperationID(operationID); err != nil {
		return err
	}
	endpoint, err := a.serverEndpoint(ctx, target)
	if err != nil {
		return err
	}
	query := namespaceQuery(target.AllocationNamespace)
	query.Set("idempotency_token", operationID)
	query.Set("no_shutdown_delay", "true")
	query.Set("reschedule", "false")
	status, payload, err := exchangeNomad(ctx, endpoint, http.MethodPost,
		allocationPath(target.AllocationID)+"/stop", query, false)
	if err != nil {
		return err
	}
	if status == http.StatusNotFound || status/100 == 2 {
		return nil
	}
	return nomadResponseError("stop server allocation", status, payload)
}

func (a *HTTPAPI) GarbageCollectAllocation(
	ctx context.Context,
	target runtimeslotreconciler.AllocationTarget,
) error {
	if err := validateTarget(target); err != nil {
		return err
	}
	endpoint, err := a.clientEndpoint(ctx, target)
	if err != nil {
		return err
	}
	status, payload, err := exchangeNomad(ctx, endpoint, http.MethodGet,
		clientAllocationPath(target.AllocationID, "gc"), namespaceQuery(target.AllocationNamespace), false)
	if err != nil {
		return err
	}
	if status == http.StatusNotFound || status/100 == 2 {
		return nil
	}
	message := strings.ToLower(string(payload))
	if strings.Contains(message, "no such allocation on client") ||
		strings.Contains(message, "not eligible for gc") {
		present, observeErr := a.ClientAllocationPresent(ctx, target)
		if observeErr != nil {
			return fmt.Errorf("resolve ambiguous Nomad client GC response: %w", observeErr)
		}
		if !present {
			return nil
		}
		return runtimeslotreconciler.ErrAllocationStillPresent
	}
	return nomadResponseError("garbage collect client allocation", status, payload)
}

func (a *HTTPAPI) serverEndpoint(
	ctx context.Context,
	target runtimeslotreconciler.AllocationTarget,
) (Endpoint, error) {
	endpoint, err := a.resolver.ServerEndpoint(ctx, target.ClusterID)
	if err != nil {
		return Endpoint{}, fmt.Errorf("resolve Nomad server endpoint: %w", err)
	}
	if endpoint.ClusterID != target.ClusterID || endpoint.NodeID != "" {
		return Endpoint{}, fmt.Errorf("Nomad server resolver returned another target: %w", errdefs.ErrFailedPrecondition)
	}
	if err := endpoint.validate(); err != nil {
		return Endpoint{}, fmt.Errorf("validate Nomad server endpoint: %w", err)
	}
	return endpoint, nil
}

func (a *HTTPAPI) clientEndpoint(
	ctx context.Context,
	target runtimeslotreconciler.AllocationTarget,
) (Endpoint, error) {
	endpoint, err := a.resolver.ClientEndpoint(ctx, target.ClusterID, target.NodeID)
	if err != nil {
		return Endpoint{}, fmt.Errorf("resolve Nomad client endpoint: %w", err)
	}
	if endpoint.ClusterID != target.ClusterID || endpoint.NodeID != target.NodeID {
		return Endpoint{}, fmt.Errorf("Nomad client resolver returned another target: %w", errdefs.ErrFailedPrecondition)
	}
	if err := endpoint.validate(); err != nil {
		return Endpoint{}, fmt.Errorf("validate Nomad client endpoint: %w", err)
	}
	return endpoint, nil
}

func (e Endpoint) validate() error {
	baseURL := strings.TrimSpace(e.BaseURL)
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil ||
		parsed.Path != "" || parsed.RawPath != "" || parsed.RawQuery != "" || parsed.Fragment != "" || baseURL != e.BaseURL {
		return fmt.Errorf("Nomad endpoint must be a canonical HTTPS origin: %w", errdefs.ErrInvalidArgument)
	}
	if err := validateID("Nomad endpoint cluster_id", e.ClusterID); err != nil {
		return err
	}
	if e.NodeID != "" {
		if err := validateID("Nomad endpoint node_id", e.NodeID); err != nil {
			return err
		}
	}
	files := []struct{ name, path string }{
		{name: "CA", path: e.CAFile},
		{name: "client certificate", path: e.ClientCertFile},
		{name: "client key", path: e.ClientKeyFile},
		{name: "ACL token", path: e.TokenFile},
	}
	for _, file := range files {
		path := strings.TrimSpace(file.path)
		if path != file.path || !filepath.IsAbs(path) || path == string(filepath.Separator) || filepath.Clean(path) != path {
			return fmt.Errorf("Nomad endpoint %s file must be a canonical non-root absolute path: %w", file.name, errdefs.ErrInvalidArgument)
		}
	}
	peerURI, err := url.Parse(e.PeerURISAN)
	if err != nil || peerURI.Scheme != "spiffe" || peerURI.Host == "" || peerURI.User != nil ||
		peerURI.RawQuery != "" || peerURI.Fragment != "" || peerURI.String() != e.PeerURISAN || len(e.PeerURISAN) > 2048 {
		return fmt.Errorf("Nomad endpoint peer URI SAN must be canonical SPIFFE: %w", errdefs.ErrInvalidArgument)
	}
	if e.Timeout < 0 || e.Timeout > maxNomadEndpointTimeout {
		return fmt.Errorf("Nomad endpoint timeout must be non-negative and at most %s: %w", maxNomadEndpointTimeout, errdefs.ErrInvalidArgument)
	}
	return nil
}

func exchangeNomad(
	ctx context.Context,
	endpoint Endpoint,
	method, escapedPath string,
	query url.Values,
	readSuccessBody bool,
) (int, []byte, error) {
	client, baseURL, err := newNomadHTTPClient(endpoint)
	if err != nil {
		return 0, nil, err
	}
	target := *baseURL
	target.RawPath = escapedPath
	target.Path, err = url.PathUnescape(escapedPath)
	if err != nil {
		return 0, nil, fmt.Errorf("decode Nomad request path: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	target.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, method, target.String(), nil)
	if err != nil {
		return 0, nil, fmt.Errorf("create Nomad request: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	token, err := readNomadToken(endpoint.TokenFile)
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("X-Nomad-Token", token)
	response, err := client.Do(request)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return 0, nil, err
		}
		return 0, nil, fmt.Errorf("call Nomad endpoint: %w: %w", err, errdefs.ErrUnavailable)
	}
	defer response.Body.Close()
	if response.StatusCode/100 == 2 && !readSuccessBody {
		return response.StatusCode, nil, nil
	}
	limit := int64(maxNomadErrorBytes)
	if response.StatusCode/100 == 2 {
		limit = maxNomadResponseBytes
	}
	payload, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		return 0, nil, fmt.Errorf("read Nomad response: %w: %w", err, errdefs.ErrUnavailable)
	}
	if int64(len(payload)) > limit {
		return 0, nil, fmt.Errorf("Nomad response exceeds %d bytes: %w", limit, errdefs.ErrUnavailable)
	}
	return response.StatusCode, payload, nil
}

func newNomadHTTPClient(endpoint Endpoint) (*http.Client, *url.URL, error) {
	baseURL, err := url.Parse(endpoint.BaseURL)
	if err != nil {
		return nil, nil, fmt.Errorf("parse Nomad endpoint URL: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	caPEM, err := os.ReadFile(endpoint.CAFile)
	if err != nil {
		return nil, nil, fmt.Errorf("read Nomad endpoint CA: %w: %w", err, errdefs.ErrUnavailable)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, nil, fmt.Errorf("Nomad endpoint CA has no certificates: %w", errdefs.ErrInvalidArgument)
	}
	certificate, err := tls.LoadX509KeyPair(endpoint.ClientCertFile, endpoint.ClientKeyFile)
	if err != nil {
		return nil, nil, fmt.Errorf("load Nomad client identity: %w: %w", err, errdefs.ErrUnavailable)
	}
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{certificate},
		ServerName: baseURL.Hostname(),
		VerifyConnection: func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 {
				return errors.New("Nomad endpoint presented no peer certificate")
			}
			for _, identity := range state.PeerCertificates[0].URIs {
				if identity.String() == endpoint.PeerURISAN {
					return nil
				}
			}
			return fmt.Errorf("Nomad endpoint certificate lacks URI SAN %q", endpoint.PeerURISAN)
		},
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableKeepAlives = true
	transport.TLSClientConfig = tlsConfig
	timeout := endpoint.Timeout
	if timeout == 0 {
		timeout = defaultNomadTimeout
	}
	return &http.Client{
		Transport: transport, Timeout: timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("Nomad endpoint redirects are forbidden")
		},
	}, baseURL, nil
}

func readNomadToken(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open Nomad ACL token: %w: %w", err, errdefs.ErrUnavailable)
	}
	defer file.Close()
	payload, err := io.ReadAll(io.LimitReader(file, maxNomadTokenBytes+1))
	if err != nil {
		return "", fmt.Errorf("read Nomad ACL token: %w: %w", err, errdefs.ErrUnavailable)
	}
	if len(payload) > maxNomadTokenBytes {
		return "", fmt.Errorf("Nomad ACL token exceeds %d bytes: %w", maxNomadTokenBytes, errdefs.ErrInvalidArgument)
	}
	token := strings.TrimSpace(string(payload))
	if token == "" || len(strings.Fields(token)) != 1 || strings.ContainsAny(token, "\r\n") {
		return "", fmt.Errorf("Nomad ACL token is empty or non-canonical: %w", errdefs.ErrInvalidArgument)
	}
	return token, nil
}

func allocationPath(allocationID string) string {
	return "/v1/allocation/" + url.PathEscape(allocationID)
}

func clientAllocationPath(allocationID, action string) string {
	return "/v1/client/allocation/" + url.PathEscape(allocationID) + "/" + action
}

func clientAllocationFSStatPath(allocationID string) string {
	return "/v1/client/fs/stat/" + url.PathEscape(allocationID)
}

func namespaceQuery(namespace string) url.Values {
	return url.Values{"namespace": []string{namespace}}
}

func nomadResponseError(action string, status int, payload []byte) error {
	message := strings.TrimSpace(string(payload))
	if message == "" {
		message = http.StatusText(status)
	}
	base := fmt.Sprintf("%s returned HTTP %d: %s", action, status, message)
	switch status {
	case http.StatusBadRequest, http.StatusMethodNotAllowed:
		return fmt.Errorf("%s: %w", base, errdefs.ErrInvalidArgument)
	case http.StatusUnauthorized, http.StatusForbidden:
		return fmt.Errorf("%s: %w", base, errdefs.ErrPermissionDenied)
	case http.StatusNotFound:
		return fmt.Errorf("%s: %w", base, errdefs.ErrNotFound)
	case http.StatusConflict, http.StatusPreconditionFailed:
		return fmt.Errorf("%s: %w", base, errdefs.ErrFailedPrecondition)
	default:
		return fmt.Errorf("%s: %w", base, errdefs.ErrUnavailable)
	}
}
