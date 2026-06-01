package egressauth

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

type VaultConnectionConfig struct {
	Name                string
	Provider            string
	Address             string
	TokenFile           string
	CACertFile          string
	Namespace           string
	DefaultMount        string
	KVVersion           int
	SkipTLSVerify       bool
	AllowedPathPrefixes []string
}

type VaultResolver struct {
	connections map[string]*vaultConnection
}

type vaultConnection struct {
	cfg    VaultConnectionConfig
	client *http.Client
}

type vaultKV2ReadResponse struct {
	Data struct {
		Data map[string]any `json:"data"`
	} `json:"data"`
}

func NewVaultResolver(configs []VaultConnectionConfig) (*VaultResolver, error) {
	if len(configs) == 0 {
		return nil, nil
	}
	resolver := &VaultResolver{connections: make(map[string]*vaultConnection, len(configs))}
	for _, cfg := range configs {
		cfg.Name = strings.TrimSpace(cfg.Name)
		cfg.Provider = strings.TrimSpace(cfg.Provider)
		cfg.Address = strings.TrimRight(strings.TrimSpace(cfg.Address), "/")
		cfg.TokenFile = strings.TrimSpace(cfg.TokenFile)
		cfg.CACertFile = strings.TrimSpace(cfg.CACertFile)
		cfg.Namespace = strings.TrimSpace(cfg.Namespace)
		cfg.DefaultMount = strings.Trim(strings.TrimSpace(cfg.DefaultMount), "/")
		if cfg.Name == "" {
			return nil, fmt.Errorf("vault connection name is required")
		}
		if cfg.Provider == "" {
			cfg.Provider = CredentialSourceExternalProviderHashiCorpVault
		}
		if cfg.Provider != CredentialSourceExternalProviderHashiCorpVault {
			return nil, fmt.Errorf("vault connection %q provider %q is not supported", cfg.Name, cfg.Provider)
		}
		if cfg.Address == "" {
			return nil, fmt.Errorf("vault connection %q address is required", cfg.Name)
		}
		if cfg.TokenFile == "" {
			return nil, fmt.Errorf("vault connection %q token file is required", cfg.Name)
		}
		if cfg.DefaultMount == "" {
			cfg.DefaultMount = "secret"
		}
		if cfg.KVVersion == 0 {
			cfg.KVVersion = 2
		}
		if cfg.KVVersion != 2 {
			return nil, fmt.Errorf("vault connection %q kv version %d is not supported", cfg.Name, cfg.KVVersion)
		}
		client, err := newVaultHTTPClient(cfg)
		if err != nil {
			return nil, fmt.Errorf("create vault client %q: %w", cfg.Name, err)
		}
		resolver.connections[cfg.Name] = &vaultConnection{cfg: cfg, client: client}
	}
	return resolver, nil
}

func (r *VaultResolver) Resolve(ctx context.Context, teamID, resolverKind string, ref *CredentialSourceExternalRefSpec) (CredentialSourceSecretSpec, error) {
	if r == nil {
		return CredentialSourceSecretSpec{}, fmt.Errorf("vault resolver is not configured")
	}
	conn, err := r.connection(ref)
	if err != nil {
		return CredentialSourceSecretSpec{}, err
	}
	values, err := conn.readKV2(ctx, teamID, ref)
	if err != nil {
		return CredentialSourceSecretSpec{}, err
	}
	return sourceSpecFromVaultValues(resolverKind, values, ref.Fields)
}

func (r *VaultResolver) Put(ctx context.Context, teamID, resolverKind string, ref *CredentialSourceExternalRefSpec, spec CredentialSourceSecretSpec) error {
	if r == nil {
		return fmt.Errorf("vault resolver is not configured")
	}
	conn, err := r.connection(ref)
	if err != nil {
		return err
	}
	values, err := vaultValuesFromSourceSpec(resolverKind, spec)
	if err != nil {
		return err
	}
	values = applyVaultFieldMapping(values, ref.Fields)
	return conn.writeKV2(ctx, teamID, ref, values)
}

func (r *VaultResolver) connection(ref *CredentialSourceExternalRefSpec) (*vaultConnection, error) {
	if ref == nil {
		return nil, fmt.Errorf("vault reference is required")
	}
	name := strings.TrimSpace(ref.Connection)
	if name == "" {
		name = "default"
	}
	conn := r.connections[name]
	if conn == nil {
		return nil, fmt.Errorf("vault connection %q is not configured", name)
	}
	return conn, nil
}

func (c *vaultConnection) readKV2(ctx context.Context, teamID string, ref *CredentialSourceExternalRefSpec) (map[string]any, error) {
	if err := c.validateRef(teamID, ref); err != nil {
		return nil, err
	}
	reqURL, err := c.kv2DataURL(ref)
	if err != nil {
		return nil, err
	}
	if version := strings.TrimSpace(ref.Version); version != "" && version != "latest" {
		values := reqURL.Query()
		values.Set("version", version)
		reqURL.RawQuery = values.Encode()
	}
	req, err := c.newRequest(ctx, http.MethodGet, reqURL.String(), nil)
	if err != nil {
		return nil, err
	}
	var out vaultKV2ReadResponse
	if err := c.doJSON(req, http.StatusOK, &out); err != nil {
		return nil, err
	}
	return out.Data.Data, nil
}

func (c *vaultConnection) writeKV2(ctx context.Context, teamID string, ref *CredentialSourceExternalRefSpec, values map[string]any) error {
	if err := c.validateRef(teamID, ref); err != nil {
		return err
	}
	reqURL, err := c.kv2DataURL(ref)
	if err != nil {
		return err
	}
	body := map[string]any{"data": values}
	req, err := c.newRequest(ctx, http.MethodPost, reqURL.String(), body)
	if err != nil {
		return err
	}
	return c.doJSON(req, http.StatusOK, nil)
}

func (c *vaultConnection) validateRef(teamID string, ref *CredentialSourceExternalRefSpec) error {
	if ref == nil {
		return fmt.Errorf("vault reference is required")
	}
	if strings.TrimSpace(ref.Path) == "" {
		return fmt.Errorf("vault reference path is required")
	}
	cleanPath := cleanVaultPath(ref.Path)
	if cleanPath == "" {
		return fmt.Errorf("vault reference path is required")
	}
	for _, rawPrefix := range c.cfg.AllowedPathPrefixes {
		prefix := strings.ReplaceAll(rawPrefix, "{{teamID}}", teamID)
		prefix = cleanVaultPath(prefix)
		if prefix == "" {
			continue
		}
		if cleanPath == prefix || strings.HasPrefix(cleanPath, strings.TrimSuffix(prefix, "/")+"/") {
			return nil
		}
	}
	if len(c.cfg.AllowedPathPrefixes) > 0 {
		return fmt.Errorf("vault reference path %q is outside allowed prefixes", cleanPath)
	}
	return nil
}

func (c *vaultConnection) kv2DataURL(ref *CredentialSourceExternalRefSpec) (*url.URL, error) {
	base, err := url.Parse(c.cfg.Address)
	if err != nil {
		return nil, fmt.Errorf("parse vault address: %w", err)
	}
	mount := strings.Trim(strings.TrimSpace(ref.Mount), "/")
	if mount == "" {
		mount = c.cfg.DefaultMount
	}
	base.Path = path.Join(base.Path, "v1", mount, "data", cleanVaultPath(ref.Path))
	return base, nil
}

func (c *vaultConnection) newRequest(ctx context.Context, method, rawURL string, body any) (*http.Request, error) {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal vault request: %w", err)
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, rawURL, reader)
	if err != nil {
		return nil, err
	}
	token, err := os.ReadFile(c.cfg.TokenFile)
	if err != nil {
		return nil, fmt.Errorf("read vault token file: %w", err)
	}
	req.Header.Set("X-Vault-Token", strings.TrimSpace(string(token)))
	if c.cfg.Namespace != "" {
		req.Header.Set("X-Vault-Namespace", c.cfg.Namespace)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req, nil
}

func (c *vaultConnection) doJSON(req *http.Request, okStatus int, out any) error {
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("vault request failed: %w", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != okStatus {
		return fmt.Errorf("vault request returned %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("decode vault response: %w", err)
	}
	return nil
}

func newVaultHTTPClient(cfg VaultConnectionConfig) (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if cfg.CACertFile != "" || cfg.SkipTLSVerify {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
		if cfg.SkipTLSVerify {
			tlsConfig.InsecureSkipVerify = true
		}
		if cfg.CACertFile != "" {
			caPEM, err := os.ReadFile(cfg.CACertFile)
			if err != nil {
				return nil, fmt.Errorf("read vault ca cert file: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(caPEM) {
				return nil, fmt.Errorf("vault ca cert file does not contain PEM certificates")
			}
			tlsConfig.RootCAs = pool
		}
		transport.TLSClientConfig = tlsConfig
	}
	return &http.Client{Transport: transport, Timeout: 10 * time.Second}, nil
}

func sourceSpecFromVaultValues(resolverKind string, values map[string]any, fields map[string]string) (CredentialSourceSecretSpec, error) {
	mapped := make(map[string]string, len(values))
	if len(fields) == 0 {
		for key := range values {
			if value, ok := stringValue(values[key]); ok {
				mapped[key] = value
			}
		}
	} else {
		for logical, vaultField := range fields {
			value, ok := stringValue(values[vaultField])
			if !ok {
				return CredentialSourceSecretSpec{}, fmt.Errorf("vault field %q for %q is missing or not a string", vaultField, logical)
			}
			mapped[logical] = value
		}
	}
	switch resolverKind {
	case "static_headers":
		return CredentialSourceSecretSpec{StaticHeaders: &StaticHeadersSourceSpec{Values: mapped}}, nil
	case "static_username_password":
		return CredentialSourceSecretSpec{StaticUsernamePassword: &StaticUsernamePasswordSourceSpec{
			Username: mapped["username"],
			Password: mapped["password"],
		}}, nil
	case "static_tls_client_certificate":
		return CredentialSourceSecretSpec{StaticTLSClientCertificate: &StaticTLSClientCertificateSourceSpec{
			CertificatePEM: mapped["certificatePem"],
			PrivateKeyPEM:  mapped["privateKeyPem"],
			CAPEM:          mapped["caPem"],
		}}, nil
	case "static_ssh_private_key":
		return CredentialSourceSecretSpec{StaticSSHPrivateKey: &StaticSSHPrivateKeySourceSpec{
			PrivateKeyPEM: mapped["privateKeyPem"],
			Passphrase:    mapped["passphrase"],
		}}, nil
	default:
		return CredentialSourceSecretSpec{}, fmt.Errorf("credential source resolver_kind %q is not supported", resolverKind)
	}
}

func vaultValuesFromSourceSpec(resolverKind string, spec CredentialSourceSecretSpec) (map[string]any, error) {
	switch resolverKind {
	case "static_headers":
		if spec.StaticHeaders == nil {
			return nil, fmt.Errorf("static_headers spec is required")
		}
		out := make(map[string]any, len(spec.StaticHeaders.Values))
		for key, value := range spec.StaticHeaders.Values {
			out[key] = value
		}
		return out, nil
	case "static_username_password":
		if spec.StaticUsernamePassword == nil {
			return nil, fmt.Errorf("static_username_password spec is required")
		}
		return map[string]any{
			"username": spec.StaticUsernamePassword.Username,
			"password": spec.StaticUsernamePassword.Password,
		}, nil
	case "static_tls_client_certificate":
		if spec.StaticTLSClientCertificate == nil {
			return nil, fmt.Errorf("static_tls_client_certificate spec is required")
		}
		return map[string]any{
			"certificatePem": spec.StaticTLSClientCertificate.CertificatePEM,
			"privateKeyPem":  spec.StaticTLSClientCertificate.PrivateKeyPEM,
			"caPem":          spec.StaticTLSClientCertificate.CAPEM,
		}, nil
	case "static_ssh_private_key":
		if spec.StaticSSHPrivateKey == nil {
			return nil, fmt.Errorf("static_ssh_private_key spec is required")
		}
		return map[string]any{
			"privateKeyPem": spec.StaticSSHPrivateKey.PrivateKeyPEM,
			"passphrase":    spec.StaticSSHPrivateKey.Passphrase,
		}, nil
	default:
		return nil, fmt.Errorf("credential source resolver_kind %q is not supported", resolverKind)
	}
}

func applyVaultFieldMapping(values map[string]any, fields map[string]string) map[string]any {
	if len(values) == 0 || len(fields) == 0 {
		return values
	}
	out := make(map[string]any, len(values))
	for logical, value := range values {
		vaultField := strings.TrimSpace(fields[logical])
		if vaultField == "" {
			vaultField = logical
		}
		out[vaultField] = value
	}
	return out
}

func stringValue(value any) (string, bool) {
	switch typed := value.(type) {
	case string:
		return typed, true
	case json.Number:
		return typed.String(), true
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64), true
	default:
		return "", false
	}
}

func cleanVaultPath(value string) string {
	return strings.Trim(path.Clean("/"+strings.TrimSpace(value)), "/")
}
