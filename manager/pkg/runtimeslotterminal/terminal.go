// Package runtimeslotterminal assembles the plugin-independent regional
// runtime slot terminal worker.
package runtimeslotterminal

import (
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotnode"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotnomad"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotreconciler"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotwriter"
)

// Store combines the regional slot registry and RootFS writer transaction
// boundaries used by terminal reconciliation.
type Store interface {
	runtimeslotreconciler.Store
	runtimeslotwriter.Store
}

// Config controls the bounded terminal worker and its strict Nomad endpoint
// catalog. Destructive reconciliation is disabled unless Enabled is true.
type Config struct {
	Enabled            bool
	NomadEndpointsFile string
	Interval           time.Duration
	PassTimeout        time.Duration
	ScanLimit          int
	DynamicNodeStore   runtimeslotnomad.DynamicNodeStore
	DynamicRegionID    string
}

// New constructs the complete plugin-independent terminal path. A disabled
// worker returns nil, but rejects configuration that would otherwise be
// silently ignored.
func New(
	store Store,
	transport runtimeslotnode.Transport,
	config Config,
) (*runtimeslotreconciler.Worker, error) {
	worker, _, err := NewWithAllocation(store, transport, config)
	return worker, err
}

// NewWithAllocation constructs the terminal worker and returns the exact same
// Nomad controller for planned lifecycle operations. This prevents manager
// pause and terminal purge from loading divergent endpoint catalogs.
func NewWithAllocation(
	store Store,
	transport runtimeslotnode.Transport,
	config Config,
) (*runtimeslotreconciler.Worker, *runtimeslotnomad.Controller, error) {
	if !config.Enabled {
		if strings.TrimSpace(config.NomadEndpointsFile) != "" {
			return nil, nil, fmt.Errorf("runtime slot terminal reconciler must be enabled when a Nomad endpoint catalog is configured")
		}
		return nil, nil, nil
	}
	if store == nil || transport == nil {
		return nil, nil, fmt.Errorf("runtime slot terminal store and node transport are required")
	}
	staticResolver, err := runtimeslotnomad.LoadStaticEndpointResolver(config.NomadEndpointsFile)
	if err != nil {
		return nil, nil, fmt.Errorf("load runtime slot Nomad endpoints: %w", err)
	}
	var resolver runtimeslotnomad.EndpointResolver = staticResolver
	if config.DynamicNodeStore != nil {
		resolver, err = runtimeslotnomad.NewDynamicEndpointResolver(staticResolver, config.DynamicNodeStore, config.DynamicRegionID)
		if err != nil {
			return nil, nil, fmt.Errorf("create dynamic runtime slot Nomad resolver: %w", err)
		}
	}
	nomadAPI, err := runtimeslotnomad.NewHTTPAPI(resolver)
	if err != nil {
		return nil, nil, fmt.Errorf("create runtime slot Nomad API: %w", err)
	}
	allocation, err := runtimeslotnomad.New(nomadAPI)
	if err != nil {
		return nil, nil, fmt.Errorf("create runtime slot Nomad controller: %w", err)
	}
	node, err := runtimeslotnode.New(transport)
	if err != nil {
		return nil, nil, fmt.Errorf("create runtime slot node controller: %w", err)
	}
	writer, err := runtimeslotwriter.New(store)
	if err != nil {
		return nil, nil, fmt.Errorf("create runtime slot writer controller: %w", err)
	}
	reconciler, err := runtimeslotreconciler.New(runtimeslotreconciler.Config{
		Store: store, Allocation: allocation, Node: node, Writer: writer, Limit: config.ScanLimit,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("create runtime slot terminal reconciler: %w", err)
	}
	worker, err := runtimeslotreconciler.NewWorker(runtimeslotreconciler.WorkerConfig{
		Runner: reconciler, Interval: config.Interval, PassTimeout: config.PassTimeout,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("create runtime slot terminal worker: %w", err)
	}
	return worker, allocation, nil
}
