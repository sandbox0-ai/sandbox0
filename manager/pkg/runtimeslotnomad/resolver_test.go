package runtimeslotnomad

import (
	"errors"
	"testing"

	"github.com/containerd/errdefs"
)

func TestStaticEndpointResolverBindsClusterAndNode(t *testing.T) {
	server := validResolverEndpoint("cluster-1", "")
	client := validResolverEndpoint("cluster-1", "node-1")
	resolver, err := NewStaticEndpointResolver([]Endpoint{server, client})
	if err != nil {
		t.Fatal(err)
	}
	gotServer, err := resolver.ServerEndpoint(t.Context(), "cluster-1")
	if err != nil || gotServer != server {
		t.Fatalf("ServerEndpoint() = %+v, %v", gotServer, err)
	}
	gotClient, err := resolver.ClientEndpoint(t.Context(), "cluster-1", "node-1")
	if err != nil || gotClient != client {
		t.Fatalf("ClientEndpoint() = %+v, %v", gotClient, err)
	}
	_, err = resolver.ClientEndpoint(t.Context(), "cluster-1", "other")
	if !errors.Is(err, errdefs.ErrNotFound) {
		t.Fatalf("ClientEndpoint(other) error = %v", err)
	}
}

func TestStaticEndpointResolverRejectsDuplicatesAndMissingServers(t *testing.T) {
	server := validResolverEndpoint("cluster-1", "")
	_, err := NewStaticEndpointResolver([]Endpoint{server, server})
	if !errors.Is(err, errdefs.ErrAlreadyExists) {
		t.Fatalf("duplicate error = %v", err)
	}
	_, err = NewStaticEndpointResolver([]Endpoint{validResolverEndpoint("cluster-1", "node-1")})
	if !errors.Is(err, errdefs.ErrInvalidArgument) {
		t.Fatalf("missing server error = %v", err)
	}
	_, err = NewStaticEndpointResolver([]Endpoint{
		validResolverEndpoint("cluster-1", ""), validResolverEndpoint("cluster-2", "node-2"),
	})
	if !errors.Is(err, errdefs.ErrInvalidArgument) {
		t.Fatalf("client without matching server error = %v", err)
	}
}

func validResolverEndpoint(clusterID, nodeID string) Endpoint {
	peerPath := "server"
	if nodeID != "" {
		peerPath = "node/" + nodeID
	}
	return Endpoint{
		ClusterID: clusterID, NodeID: nodeID, BaseURL: "https://nomad.internal:4646",
		CAFile: "/etc/nomad/ca.pem", ClientCertFile: "/etc/nomad/client.pem",
		ClientKeyFile: "/etc/nomad/client-key.pem", TokenFile: "/run/nomad/token",
		PeerURISAN: "spiffe://sandbox0.test/nomad/" + clusterID + "/" + peerPath,
	}
}
