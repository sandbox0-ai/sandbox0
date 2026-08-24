package runtimeslot

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/containerd/errdefs"
)

const (
	defaultNodeChannelResolveInterval = time.Second
	defaultNodeChannelResolveTimeout  = 2 * time.Second
	defaultNodeChannelMaxEndpoints    = 64
)

// NodeChannelResolver resolves every address currently published for a
// manager authority hostname. Implementations must not return a load-balanced
// virtual IP in addition to the exact manager endpoints.
type NodeChannelResolver interface {
	LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
}

// NodeChannelAgentSetConfig configures exact outbound streams to every manager
// endpoint published for one authority hostname.
type NodeChannelAgentSetConfig struct {
	Agent NodeChannelAgentConfig

	Resolver        NodeChannelResolver
	ResolveInterval time.Duration
	ResolveTimeout  time.Duration
	MaxEndpoints    int
}

// NodeChannelAgentSet continuously resolves an authority hostname and
// maintains one independently authenticated stream to every exact endpoint.
// The HTTPS authority remains unchanged for HTTP Host and TLS verification;
// only the underlying TCP destination is pinned to a resolved endpoint IP.
type NodeChannelAgentSet struct {
	config    NodeChannelAgentSetConfig
	authority *url.URL
	resolver  NodeChannelResolver
}

type nodeChannelAgentChild struct {
	cancel context.CancelFunc
}

// NewNodeChannelAgentSet validates discovery and shared stream policy.
func NewNodeChannelAgentSet(config NodeChannelAgentSetConfig) (*NodeChannelAgentSet, error) {
	validated, err := NewNodeChannelAgent(config.Agent)
	if err != nil {
		return nil, err
	}
	if net.ParseIP(validated.baseURL.Hostname()) != nil {
		return nil, fmt.Errorf("node channel agent set requires an authority hostname, not an IP: %w", errdefs.ErrInvalidArgument)
	}
	if config.ResolveInterval == 0 {
		config.ResolveInterval = defaultNodeChannelResolveInterval
	}
	if config.ResolveInterval < 100*time.Millisecond || config.ResolveInterval > time.Minute {
		return nil, fmt.Errorf("node channel resolve interval must be between 100 milliseconds and one minute: %w", errdefs.ErrInvalidArgument)
	}
	if config.ResolveTimeout == 0 {
		config.ResolveTimeout = defaultNodeChannelResolveTimeout
	}
	if config.ResolveTimeout < 100*time.Millisecond || config.ResolveTimeout > 30*time.Second {
		return nil, fmt.Errorf("node channel resolve timeout must be between 100 milliseconds and 30 seconds: %w", errdefs.ErrInvalidArgument)
	}
	if config.MaxEndpoints == 0 {
		config.MaxEndpoints = defaultNodeChannelMaxEndpoints
	}
	if config.MaxEndpoints < 1 || config.MaxEndpoints > 1024 {
		return nil, fmt.Errorf("node channel endpoint limit must be between one and 1024: %w", errdefs.ErrInvalidArgument)
	}
	resolver := config.Resolver
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	config.Agent.AgentInstanceID = validated.agentInstanceID
	return &NodeChannelAgentSet{config: config, authority: validated.baseURL, resolver: resolver}, nil
}

// Run maintains the complete exact endpoint set until cancellation. DNS
// failures retain the current exact membership snapshot, while a successful
// empty answer removes every stream and never falls back to the authority's
// load-balanced address.
func (s *NodeChannelAgentSet) Run(ctx context.Context) error {
	if s == nil || s.authority == nil || s.resolver == nil {
		return fmt.Errorf("node channel agent set is not initialized: %w", errdefs.ErrUnavailable)
	}
	children := make(map[string]*nodeChannelAgentChild)
	runCtx, cancelRun := context.WithCancel(ctx)
	results := make(chan struct {
		address string
		child   *nodeChannelAgentChild
		err     error
	}, s.config.MaxEndpoints)
	var childrenWG sync.WaitGroup
	stopChildren := func() {
		cancelRun()
		for address, child := range children {
			delete(children, address)
			child.cancel()
		}
		childrenWG.Wait()
	}
	defer stopChildren()

	reconcile := func(addresses []string) error {
		desired := make(map[string]struct{}, len(addresses))
		for _, address := range addresses {
			desired[address] = struct{}{}
			if children[address] != nil {
				continue
			}
			agent, err := newNodeChannelAgent(s.config.Agent, address)
			if err != nil {
				return err
			}
			childCtx, cancel := context.WithCancel(runCtx)
			child := &nodeChannelAgentChild{cancel: cancel}
			children[address] = child
			childrenWG.Add(1)
			go func(address string, child *nodeChannelAgentChild) {
				defer childrenWG.Done()
				err := agent.Run(childCtx)
				result := struct {
					address string
					child   *nodeChannelAgentChild
					err     error
				}{address: address, child: child, err: err}
				select {
				case results <- result:
				case <-runCtx.Done():
				}
			}(address, child)
		}
		for address, child := range children {
			if _, keep := desired[address]; keep {
				continue
			}
			delete(children, address)
			child.cancel()
		}
		return nil
	}

	resolveAndReconcile := func() error {
		resolveCtx, cancel := context.WithTimeout(runCtx, s.config.ResolveTimeout)
		defer cancel()
		addresses, err := s.resolve(resolveCtx)
		if err != nil {
			return err
		}
		return reconcile(addresses)
	}

	// Initial DNS absence is retried rather than replaced by a connection to a
	// virtual Service address. This keeps startup fail-closed during rollouts.
	if err := resolveAndReconcile(); err != nil &&
		(errdefs.IsInvalidArgument(err) || errdefs.IsPermissionDenied(err)) {
		return err
	}
	ticker := time.NewTicker(s.config.ResolveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result := <-results:
			if children[result.address] != result.child {
				continue
			}
			delete(children, result.address)
			if ctx.Err() != nil || errors.Is(result.err, context.Canceled) {
				continue
			}
			if result.err == nil {
				result.err = errdefs.ErrUnavailable
			}
			return fmt.Errorf("node channel endpoint %s stopped: %w", result.address, result.err)
		case <-ticker.C:
			if err := resolveAndReconcile(); err != nil &&
				(errdefs.IsInvalidArgument(err) || errdefs.IsPermissionDenied(err)) {
				return err
			}
		}
	}
}

func (s *NodeChannelAgentSet) resolve(ctx context.Context) ([]string, error) {
	addresses, err := s.resolver.LookupIPAddr(ctx, s.authority.Hostname())
	if err != nil {
		return nil, fmt.Errorf("resolve node channel authority endpoints: %w: %w", err, errdefs.ErrUnavailable)
	}
	port := s.authority.Port()
	if port == "" {
		port = "443"
	}
	unique := make(map[string]struct{}, len(addresses))
	for _, resolved := range addresses {
		ip, ok := netip.AddrFromSlice(resolved.IP)
		if !ok || resolved.Zone != "" || ip.IsUnspecified() || ip.IsMulticast() {
			return nil, fmt.Errorf("resolver returned a non-unicast node channel endpoint: %w", errdefs.ErrInvalidArgument)
		}
		ip = ip.Unmap()
		address := net.JoinHostPort(ip.String(), port)
		if _, err := validateNodeChannelDialAddress(address, s.authority); err != nil {
			return nil, err
		}
		unique[address] = struct{}{}
	}
	if len(unique) > s.config.MaxEndpoints {
		return nil, fmt.Errorf("resolver returned %d node channel endpoints, limit %d: %w",
			len(unique), s.config.MaxEndpoints, errdefs.ErrInvalidArgument)
	}
	result := make([]string, 0, len(unique))
	for address := range unique {
		result = append(result, address)
	}
	sort.Strings(result)
	return result, nil
}
