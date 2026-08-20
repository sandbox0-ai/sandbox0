package runtimeslotnomad

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadStaticEndpointResolver(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "nomad-endpoints.json")
	payload := `{
  "version": 1,
  "endpoints": [
    {
      "cluster_id": "cluster-1",
      "base_url": "https://nomad-server.example:4646",
      "ca_file": "/etc/sandbox0/nomad-ca.pem",
      "client_cert_file": "/etc/sandbox0/nomad-client.pem",
      "client_key_file": "/etc/sandbox0/nomad-client-key.pem",
      "token_file": "/run/secrets/sandbox0/nomad.token",
      "peer_uri_san": "spiffe://sandbox0.example/nomad/server",
      "timeout": "3s"
    },
    {
      "cluster_id": "cluster-1",
      "node_id": "node-1",
      "base_url": "https://nomad-client-1.example:4646",
      "ca_file": "/etc/sandbox0/nomad-ca.pem",
      "client_cert_file": "/etc/sandbox0/nomad-client.pem",
      "client_key_file": "/etc/sandbox0/nomad-client-key.pem",
      "token_file": "/run/secrets/sandbox0/nomad.token",
      "peer_uri_san": "spiffe://sandbox0.example/nomad/client/node-1"
    }
  ]
}`
	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatal(err)
	}
	resolver, err := LoadStaticEndpointResolver(path)
	if err != nil {
		t.Fatal(err)
	}
	server, err := resolver.ServerEndpoint(t.Context(), "cluster-1")
	if err != nil {
		t.Fatal(err)
	}
	if server.Timeout != 3*time.Second || server.NodeID != "" {
		t.Fatalf("server endpoint = %+v", server)
	}
	client, err := resolver.ClientEndpoint(t.Context(), "cluster-1", "node-1")
	if err != nil {
		t.Fatal(err)
	}
	if client.Timeout != 0 || client.NodeID != "node-1" {
		t.Fatalf("client endpoint = %+v", client)
	}
}

func TestLoadStaticEndpointResolverRejectsUnsafeCatalog(t *testing.T) {
	directory := t.TempDir()
	validEndpoint := `{
    "cluster_id":"cluster-1",
    "base_url":"https://nomad.example:4646",
    "ca_file":"/etc/ca.pem",
    "client_cert_file":"/etc/client.pem",
    "client_key_file":"/etc/client-key.pem",
    "token_file":"/run/secrets/token",
    "peer_uri_san":"spiffe://sandbox0.example/nomad/server"
  }`
	for name, payload := range map[string]string{
		"relative path":        `{"version":1,"endpoints":[` + validEndpoint + `]}`,
		"whitespace path":      `{"version":1,"endpoints":[` + validEndpoint + `]}`,
		"unknown field":        `{"version":1,"endpoints":[` + validEndpoint + `],"extra":true}`,
		"trailing value":       `{"version":1,"endpoints":[` + validEndpoint + `]} {}`,
		"wrong version":        `{"version":2,"endpoints":[` + validEndpoint + `]}`,
		"empty endpoints":      `{"version":1,"endpoints":[]}`,
		"invalid timeout":      `{"version":1,"endpoints":[` + validEndpoint[:len(validEndpoint)-1] + `,"timeout":"2h"}]}`,
		"noncanonical timeout": `{"version":1,"endpoints":[` + validEndpoint[:len(validEndpoint)-1] + `,"timeout":"3000ms"}]}`,
		"numeric timeout":      `{"version":1,"endpoints":[` + validEndpoint[:len(validEndpoint)-1] + `,"timeout":3}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(directory, name+".json")
			if name == "relative path" {
				path = filepath.Base(path)
				if _, err := LoadStaticEndpointResolver(path); err == nil {
					t.Fatal("relative catalog path unexpectedly succeeded")
				}
				return
			}
			if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
				t.Fatal(err)
			}
			loadPath := path
			if name == "whitespace path" {
				loadPath = " " + path
			}
			if _, err := LoadStaticEndpointResolver(loadPath); err == nil {
				t.Fatal("unsafe endpoint catalog unexpectedly succeeded")
			}
		})
	}
}
