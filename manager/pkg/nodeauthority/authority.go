// Package nodeauthority assembles the manager's dedicated mTLS node listener.
package nodeauthority

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauth"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/rootfswriterauthority"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotauthority"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotclaim"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotnode"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotnomad"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotreconciler"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotterminal"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

const (
	defaultWriterLeaseTTL          = 30 * time.Second
	defaultRuntimeSlotHeartbeatTTL = 30 * time.Second
)

// Store is the single PostgreSQL authority shared by writer, runtime-slot
// claim, heartbeat, and terminal transitions.
type Store interface {
	rootfswriterauthority.GrantStore
	rootfswriterauthority.LifecycleStore
	runtimeslotauthority.Store
	runtimeslotclaim.Store
	runtimeslotterminal.Store
	GetRootFSCompositeBacklogUsage(context.Context) (sandboxstore.RootFSCompositeBacklogUsage, error)
}

// Config defines the dedicated verified-mTLS listener and regional lease
// policy. It is intentionally separate from manager's normal internal HTTP
// listener.
type Config struct {
	Store                   Store
	Address                 string
	CertFile                string
	KeyFile                 string
	ClientCAFile            string
	Identities              []nodeauth.CertificateIdentity
	WriterLeaseTTL          time.Duration
	WriterRenewalGrace      time.Duration
	RuntimeSlotHeartbeatTTL time.Duration
	Terminal                runtimeslotterminal.Config
}

// ClaimPlannerConfig provides the non-listener dependencies needed by the
// request path. Node and network delivery are always the component's own hub.
type ClaimPlannerConfig struct {
	Prober         runtimeslotclaim.CommandProber
	TokenGenerator runtimeslotclaim.TokenGenerator
	Observer       runtimeslotclaim.Observer
	WriterTokenKey []byte
	ClaimTTL       time.Duration
	SLO            time.Duration
	Now            func() time.Time
}

// Component owns one listener-local node channel registry. Every replica may
// run the terminal loop so the instance holding a node stream can make
// progress; PostgreSQL fences and deterministic operations serialize effects.
type Component struct {
	store      Store
	hub        *runtimeslotnode.ChannelHub
	server     *rootfswriterauthority.Server
	terminal   *runtimeslotreconciler.Worker
	allocation *runtimeslotnomad.Controller
}

// New constructs all node-facing authorities over one verifier, store, and
// outbound channel hub.
func New(config Config) (*Component, error) {
	if config.Store == nil {
		return nil, fmt.Errorf("node authority store is required")
	}
	if config.WriterLeaseTTL == 0 {
		config.WriterLeaseTTL = defaultWriterLeaseTTL
	}
	if config.WriterRenewalGrace == 0 {
		config.WriterRenewalGrace = min(config.WriterLeaseTTL/2, 5*time.Second)
	}
	if config.RuntimeSlotHeartbeatTTL == 0 {
		config.RuntimeSlotHeartbeatTTL = defaultRuntimeSlotHeartbeatTTL
	}

	verifier, err := nodeauth.NewCertificateVerifier(config.Identities)
	if err != nil {
		return nil, fmt.Errorf("create node authority verifier: %w", err)
	}
	writerHandler, err := rootfswriterauthority.NewHandler(rootfswriterauthority.HandlerConfig{
		Verifier: verifier,
		Store:    config.Store,
		LeaseTTL: config.WriterLeaseTTL,
		RenewalPolicy: sandboxstore.RootFSWriterLeaseRenewalPolicy{
			LeaseTTL: config.WriterLeaseTTL, GracePeriod: config.WriterRenewalGrace,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create RootFS writer authority handler: %w", err)
	}
	lifecycleHandler, err := rootfswriterauthority.NewLifecycleHandler(verifier, config.Store, writerHandler)
	if err != nil {
		return nil, fmt.Errorf("create RootFS lifecycle authority handler: %w", err)
	}
	runtimeSlotHandler, err := runtimeslotauthority.NewHandler(runtimeslotauthority.HandlerConfig{
		Verifier: verifier, Store: config.Store, HeartbeatTTL: config.RuntimeSlotHeartbeatTTL,
	})
	if err != nil {
		return nil, fmt.Errorf("create runtime slot authority handler: %w", err)
	}
	hub, err := runtimeslotnode.NewChannelHub(verifier)
	if err != nil {
		return nil, fmt.Errorf("create runtime slot node channel: %w", err)
	}
	terminal, allocation, err := runtimeslotterminal.NewWithAllocation(config.Store, hub, config.Terminal)
	if err != nil {
		_ = hub.Close()
		return nil, err
	}

	mux := http.NewServeMux()
	mux.Handle("/internal/v1/rootfs-writer-grants", http.NotFoundHandler())
	mux.Handle("/internal/v1/rootfs-writer-grants/", lifecycleHandler)
	mux.Handle(strings.TrimSuffix(protocol.PathPrefix, "/"), http.NotFoundHandler())
	mux.Handle(protocol.PathPrefix, runtimeSlotHandler)
	mux.Handle(protocol.NodeChannelPath, hub)
	mux.HandleFunc("/healthz", backlogHealthHandler(config.Store))
	authorized, err := nodeauth.NewCertificateMiddleware(config.Identities, mux)
	if err != nil {
		_ = hub.Close()
		return nil, fmt.Errorf("create node authority certificate middleware: %w", err)
	}
	server, err := rootfswriterauthority.NewServer(rootfswriterauthority.ServerConfig{
		Address: config.Address, CertFile: config.CertFile, KeyFile: config.KeyFile,
		ClientCAFile: config.ClientCAFile, Handler: authorized,
	})
	if err != nil {
		_ = hub.Close()
		return nil, fmt.Errorf("create node authority server: %w", err)
	}
	return &Component{
		store: config.Store, hub: hub, server: server,
		terminal: terminal, allocation: allocation,
	}, nil
}

// RunServer serves the dedicated mTLS endpoint until cancellation and closes
// every hijacked node channel before returning.
func (c *Component) RunServer(ctx context.Context) error {
	if c == nil || c.server == nil || c.hub == nil {
		return fmt.Errorf("node authority is not initialized")
	}
	defer c.hub.Close()
	return c.server.Start(ctx)
}

// Ready closes after the dedicated TCP listener binds successfully.
func (c *Component) Ready() <-chan struct{} {
	if c == nil || c.server == nil {
		return nil
	}
	return c.server.Ready()
}

// TerminalEnabled reports whether this component has a destructive terminal
// worker configured.
func (c *Component) TerminalEnabled() bool {
	return c != nil && c.terminal != nil
}

// NomadAllocationController returns the endpoint-catalog-pinned controller
// shared by planned lifecycle stop and terminal purge.
func (c *Component) NomadAllocationController() *runtimeslotnomad.Controller {
	if c == nil {
		return nil
	}
	return c.allocation
}

// RunTerminal waits for listener readiness, then runs the bounded terminal
// loop. It is safe to invoke on every manager replica.
func (c *Component) RunTerminal(
	ctx context.Context,
	report func(runtimeslotreconciler.WorkerReport),
) error {
	if c == nil || c.server == nil || c.terminal == nil {
		return fmt.Errorf("runtime slot terminal worker is not enabled")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.server.Ready():
	}
	return c.terminal.Run(ctx, report)
}

// NewClaimPlanner binds the request path to the exact same authenticated hub
// used for node registration, network preparation, and terminal cleanup.
func (c *Component) NewClaimPlanner(config ClaimPlannerConfig) (*runtimeslotclaim.Planner, error) {
	if c == nil || c.store == nil || c.hub == nil {
		return nil, fmt.Errorf("node authority is not initialized")
	}
	return runtimeslotclaim.New(runtimeslotclaim.Config{
		Store: c.store, Network: c.hub, Node: c.hub,
		Prober: config.Prober, TokenGenerator: config.TokenGenerator,
		Observer: config.Observer, WriterTokenKey: config.WriterTokenKey,
		ClaimTTL: config.ClaimTTL, SLO: config.SLO, Now: config.Now,
	})
}

func backlogHealthHandler(store Store) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		usage, err := store.GetRootFSCompositeBacklogUsage(request.Context())
		if err != nil {
			http.Error(writer, "composite backlog unavailable", http.StatusServiceUnavailable)
			return
		}
		writer.Header().Set("X-Sandbox0-RootFS-Composite-Bytes", strconv.FormatInt(usage.UsedDescriptorBytes, 10))
		writer.Header().Set("X-Sandbox0-RootFS-Composite-Limit", strconv.FormatInt(usage.MaxDescriptorBytes, 10))
		writer.Header().Set("X-Sandbox0-RootFS-Composite-Generations", strconv.FormatInt(usage.GenerationCount, 10))
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ok\n"))
	}
}
