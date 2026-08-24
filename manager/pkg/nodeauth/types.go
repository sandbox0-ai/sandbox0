// Package nodeauth defines the authenticated node identity shared by internal
// data-plane authority handlers.
package nodeauth

import "context"

// Identity is derived from a verified workload credential, never from an API
// request body.
type Identity struct {
	ClusterID string
	NodeID    string
	NodeUID   string
	AgentUID  string
}

// Verifier resolves a bearer credential to its durable node incarnation.
type Verifier interface {
	Verify(context.Context, string) (Identity, error)
}
