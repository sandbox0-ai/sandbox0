package runtimeslotnomad

import (
	"context"
	"fmt"
	"strings"

	"github.com/containerd/errdefs"
)

// StaticEndpointResolver is an immutable region-owned Nomad endpoint catalog.
// A server endpoint has an empty NodeID; a client endpoint has its exact Nomad
// node ID. It is suitable for declarative deployment config and tests.
type StaticEndpointResolver struct {
	servers map[string]Endpoint
	clients map[string]Endpoint
}

// NewStaticEndpointResolver validates and indexes an immutable endpoint set.
func NewStaticEndpointResolver(endpoints []Endpoint) (*StaticEndpointResolver, error) {
	resolver := &StaticEndpointResolver{
		servers: make(map[string]Endpoint), clients: make(map[string]Endpoint),
	}
	for index, endpoint := range endpoints {
		if err := endpoint.validate(); err != nil {
			return nil, fmt.Errorf("validate Nomad endpoint %d: %w", index, err)
		}
		key := endpoint.ClusterID
		catalog := resolver.servers
		if endpoint.NodeID != "" {
			key += "\x00" + endpoint.NodeID
			catalog = resolver.clients
		}
		if _, exists := catalog[key]; exists {
			return nil, fmt.Errorf("duplicate Nomad endpoint for cluster %q node %q: %w", endpoint.ClusterID, endpoint.NodeID, errdefs.ErrAlreadyExists)
		}
		catalog[key] = endpoint
	}
	if len(resolver.servers) == 0 {
		return nil, fmt.Errorf("at least one Nomad server endpoint is required: %w", errdefs.ErrInvalidArgument)
	}
	for _, endpoint := range resolver.clients {
		if _, ok := resolver.servers[endpoint.ClusterID]; !ok {
			return nil, fmt.Errorf("nomad client cluster %q has no server endpoint: %w", endpoint.ClusterID, errdefs.ErrInvalidArgument)
		}
	}
	return resolver, nil
}

func (r *StaticEndpointResolver) ServerEndpoint(_ context.Context, clusterID string) (Endpoint, error) {
	if r == nil {
		return Endpoint{}, fmt.Errorf("nomad endpoint resolver is unavailable: %w", errdefs.ErrUnavailable)
	}
	endpoint, ok := r.servers[strings.TrimSpace(clusterID)]
	if !ok || endpoint.ClusterID != clusterID {
		return Endpoint{}, fmt.Errorf("nomad server endpoint for cluster %q is absent: %w", clusterID, errdefs.ErrNotFound)
	}
	return endpoint, nil
}

func (r *StaticEndpointResolver) ClientEndpoint(_ context.Context, clusterID, nodeID string) (Endpoint, error) {
	if r == nil {
		return Endpoint{}, fmt.Errorf("nomad endpoint resolver is unavailable: %w", errdefs.ErrUnavailable)
	}
	endpoint, ok := r.clients[strings.TrimSpace(clusterID)+"\x00"+strings.TrimSpace(nodeID)]
	if !ok || endpoint.ClusterID != clusterID || endpoint.NodeID != nodeID {
		return Endpoint{}, fmt.Errorf("nomad client endpoint for cluster %q node %q is absent: %w", clusterID, nodeID, errdefs.ErrNotFound)
	}
	return endpoint, nil
}
