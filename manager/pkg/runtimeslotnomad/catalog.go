package runtimeslotnomad

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/containerd/errdefs"
)

const (
	nomadEndpointCatalogVersion = 1
	maxNomadEndpointCatalogSize = 2 << 20
	maxNomadEndpointCount       = 4096
	maxNomadEndpointTimeout     = time.Minute
)

type endpointCatalog struct {
	Version   int                     `json:"version"`
	Endpoints []endpointCatalogRecord `json:"endpoints"`
}

type endpointCatalogRecord struct {
	ClusterID      string `json:"cluster_id"`
	NodeID         string `json:"node_id,omitempty"`
	BaseURL        string `json:"base_url"`
	CAFile         string `json:"ca_file"`
	ClientCertFile string `json:"client_cert_file"`
	ClientKeyFile  string `json:"client_key_file"`
	TokenFile      string `json:"token_file"`
	PeerURISAN     string `json:"peer_uri_san"`
	Timeout        string `json:"timeout,omitempty"`
}

// LoadStaticEndpointResolver loads a bounded, strict, immutable regional
// Nomad endpoint catalog. Credential file contents are still loaded on every
// request so certificate and ACL token rotation does not require a restart.
func LoadStaticEndpointResolver(path string) (*StaticEndpointResolver, error) {
	rawPath := path
	path = strings.TrimSpace(path)
	if path != rawPath || path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) {
		return nil, fmt.Errorf("Nomad endpoint catalog must be a canonical non-root absolute path: %w", errdefs.ErrInvalidArgument)
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect Nomad endpoint catalog: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("Nomad endpoint catalog must resolve to a regular file: %w", errdefs.ErrInvalidArgument)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open Nomad endpoint catalog: %w", err)
	}
	defer file.Close()
	payload, err := io.ReadAll(io.LimitReader(file, maxNomadEndpointCatalogSize+1))
	if err != nil {
		return nil, fmt.Errorf("read Nomad endpoint catalog: %w", err)
	}
	if len(payload) > maxNomadEndpointCatalogSize {
		return nil, fmt.Errorf("Nomad endpoint catalog exceeds %d bytes: %w", maxNomadEndpointCatalogSize, errdefs.ErrResourceExhausted)
	}
	var catalog endpointCatalog
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&catalog); err != nil {
		return nil, fmt.Errorf("decode Nomad endpoint catalog: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("Nomad endpoint catalog must contain exactly one JSON value: %w", errdefs.ErrInvalidArgument)
	}
	if catalog.Version != nomadEndpointCatalogVersion {
		return nil, fmt.Errorf("unsupported Nomad endpoint catalog version %d: %w", catalog.Version, errdefs.ErrInvalidArgument)
	}
	if len(catalog.Endpoints) == 0 || len(catalog.Endpoints) > maxNomadEndpointCount {
		return nil, fmt.Errorf("Nomad endpoint catalog must contain between 1 and %d endpoints: %w", maxNomadEndpointCount, errdefs.ErrInvalidArgument)
	}
	endpoints := make([]Endpoint, 0, len(catalog.Endpoints))
	for index, record := range catalog.Endpoints {
		timeout, err := parseCatalogTimeout(record.Timeout)
		if err != nil {
			return nil, fmt.Errorf("Nomad endpoint %d timeout: %w", index, err)
		}
		endpoints = append(endpoints, Endpoint{
			ClusterID: record.ClusterID, NodeID: record.NodeID, BaseURL: record.BaseURL,
			CAFile: record.CAFile, ClientCertFile: record.ClientCertFile,
			ClientKeyFile: record.ClientKeyFile, TokenFile: record.TokenFile,
			PeerURISAN: record.PeerURISAN, Timeout: timeout,
		})
	}
	return NewStaticEndpointResolver(endpoints)
}

func parseCatalogTimeout(value string) (time.Duration, error) {
	if value == "" {
		return 0, nil
	}
	if strings.TrimSpace(value) != value {
		return 0, fmt.Errorf("timeout must be canonical: %w", errdefs.ErrInvalidArgument)
	}
	timeout, err := time.ParseDuration(value)
	if err != nil || timeout <= 0 || timeout > maxNomadEndpointTimeout {
		return 0, fmt.Errorf("timeout must be greater than zero and at most %s: %w", maxNomadEndpointTimeout, errdefs.ErrInvalidArgument)
	}
	if timeout.String() != value {
		return 0, fmt.Errorf("timeout must use canonical Go duration syntax %q: %w", timeout, errdefs.ErrInvalidArgument)
	}
	return timeout, nil
}
