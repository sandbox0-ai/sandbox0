package runtimeslotnomad

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
)

type DynamicNodeStore interface {
	GetRuntimeNodeEndpointIdentity(context.Context, string, string) (*sandboxstore.RuntimeNodeEndpointIdentity, error)
}

// DynamicEndpointResolver keeps the server endpoint deployment-pinned while
// resolving disposable client addresses from the durable admission catalog.
type DynamicEndpointResolver struct {
	servers  *StaticEndpointResolver
	store    DynamicNodeStore
	regionID string
}

func NewDynamicEndpointResolver(
	servers *StaticEndpointResolver,
	store DynamicNodeStore,
	regionID string,
) (*DynamicEndpointResolver, error) {
	regionID = strings.TrimSpace(regionID)
	if servers == nil || store == nil || regionID == "" {
		return nil, errors.New("dynamic Nomad endpoint server catalog and node store are required")
	}
	return &DynamicEndpointResolver{servers: servers, store: store, regionID: regionID}, nil
}

func (r *DynamicEndpointResolver) ServerEndpoint(ctx context.Context, clusterID string) (Endpoint, error) {
	return r.servers.ServerEndpoint(ctx, clusterID)
}

func (r *DynamicEndpointResolver) ClientEndpoint(
	ctx context.Context,
	clusterID, nodeID string,
) (Endpoint, error) {
	identity, err := r.store.GetRuntimeNodeEndpointIdentity(ctx, clusterID, nodeID)
	if err != nil {
		// Fixed migration nodes remain available from the static catalog until
		// they are enrolled into the same dynamic admission table.
		return r.servers.ClientEndpoint(ctx, clusterID, nodeID)
	}
	if identity.ClusterID != clusterID || identity.NodeID != nodeID ||
		net.ParseIP(identity.PrivateIP) == nil {
		return Endpoint{}, errors.New("dynamic Nomad endpoint identity is inconsistent")
	}
	server, err := r.servers.ServerEndpoint(ctx, clusterID)
	if err != nil {
		return Endpoint{}, err
	}
	return Endpoint{
		ClusterID: clusterID, NodeID: nodeID,
		BaseURL: "https://" + net.JoinHostPort(identity.PrivateIP, "4646"),
		CAFile:  server.CAFile, ClientCertFile: server.ClientCertFile,
		ClientKeyFile: server.ClientKeyFile, TokenFile: server.TokenFile,
		PeerURISAN: fmt.Sprintf("spiffe://sandbox0.internal/%s/nomad/client/%s",
			r.regionID, nodeID),
		Timeout: server.Timeout,
	}, nil
}
