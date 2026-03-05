package setup

import (
	"time"

	"github.com/sandbox0-ai/sandbox0/tests/integration/internal/clients"
)

// ClientOptions configures service clients for integration tests.
type ClientOptions struct {
	ManagerURL string
	StorageURL string
	GatewayURL string
	Token      string
	Timeout    time.Duration
}

// Clients bundles service clients used in integration tests.
type Clients struct {
	Manager *clients.ManagerClient
	Storage *clients.StorageClient
	Gateway *clients.GatewayClient
}

// NewClients initializes service clients with consistent defaults.
func NewClients(opts ClientOptions) *Clients {
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}

	return &Clients{
		Manager: clients.NewManagerClient(opts.ManagerURL, opts.Token, timeout),
		Storage: clients.NewStorageClient(opts.StorageURL, opts.Token, timeout),
		Gateway: clients.NewGatewayClient(opts.GatewayURL, opts.Token, timeout),
	}
}
