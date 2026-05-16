package proxy

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	xproxy "golang.org/x/net/proxy"
)

var errEgressProxyEndpointProtected = errors.New("egress proxy endpoint resolves to a protected address")

var allowLocalEgressProxyEndpointsForTest bool

type timeoutDialer struct {
	timeout time.Duration
}

func (d timeoutDialer) Dial(network, address string) (net.Conn, error) {
	return net.DialTimeout(network, address, d.timeout)
}

func (s *Server) dialTCPUpstreamForRequest(req *adapterRequest) (net.Conn, error) {
	if s == nil {
		return nil, fmt.Errorf("server is nil")
	}
	if req == nil || req.DestIP == nil || req.DestPort <= 0 {
		return nil, fmt.Errorf("upstream tcp request is incomplete")
	}
	egressProxy := compiledEgressProxy(req.Compiled)
	if egressProxy == nil {
		return s.dialTCPUpstream(req.DestIP, req.DestPort)
	}
	return s.dialViaSOCKS5EgressProxy(req, egressProxy)
}

func compiledEgressProxy(compiled *policy.CompiledPolicy) *policy.CompiledEgressProxy {
	if compiled == nil || compiled.Egress.Proxy == nil {
		return nil
	}
	if compiled.Egress.Proxy.Type != "socks5" {
		return nil
	}
	return compiled.Egress.Proxy
}

func (s *Server) dialViaSOCKS5EgressProxy(req *adapterRequest, cfg *policy.CompiledEgressProxy) (net.Conn, error) {
	if cfg == nil {
		return nil, fmt.Errorf("egress proxy config is nil")
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.upstreamTimeout())
	defer cancel()

	proxyAddress, err := resolveSOCKS5ProxyDialAddress(ctx, req.Compiled, cfg)
	if err != nil {
		return nil, err
	}
	auth, err := s.resolveSOCKS5ProxyAuth(context.Background(), req, cfg)
	if err != nil {
		return nil, err
	}
	dialer, err := xproxy.SOCKS5("tcp", proxyAddress, auth, timeoutDialer{timeout: s.upstreamTimeout()})
	if err != nil {
		return nil, fmt.Errorf("create socks5 egress proxy dialer: %w", err)
	}
	targetHost := strings.TrimSpace(req.Host)
	if targetHost == "" {
		targetHost = req.DestIP.String()
	}
	target := net.JoinHostPort(targetHost, fmt.Sprintf("%d", req.DestPort))
	conn, err := dialer.Dial("tcp", target)
	if err != nil {
		return nil, fmt.Errorf("dial socks5 egress proxy target %s via %s: %w", target, proxyAddress, err)
	}
	return conn, nil
}

func (s *Server) resolveSOCKS5ProxyAuth(ctx context.Context, req *adapterRequest, cfg *policy.CompiledEgressProxy) (*xproxy.Auth, error) {
	if cfg == nil || cfg.CredentialRef == "" {
		return nil, nil
	}
	if req == nil || req.Compiled == nil {
		return nil, fmt.Errorf("egress proxy credential resolution requires compiled policy")
	}
	if s == nil || s.authResolver == nil {
		return nil, errEgressAuthResolverUnconfigured
	}
	key := egressAuthCacheKey{
		SandboxID:       req.Compiled.SandboxID,
		AuthRef:         cfg.CredentialRef,
		Destination:     egressProxyDestination(req),
		DestinationPort: req.DestPort,
		Transport:       "tcp",
		Protocol:        "socks5",
	}
	if s.authCache != nil {
		if cached, ok := s.authCache.Get(key); ok {
			material, err := resolveUsernamePasswordForAdapter(&egressAuthContext{Resolved: cached}, true)
			if err != nil {
				return nil, fmt.Errorf("resolve cached egress proxy credentials: %w", err)
			}
			return &xproxy.Auth{User: material.Username, Password: material.Password}, nil
		}
	}
	resp, err := s.authResolver.Resolve(ctx, &egressauth.ResolveRequest{
		SandboxID:       req.Compiled.SandboxID,
		TeamID:          req.Compiled.TeamID,
		AuthRef:         cfg.CredentialRef,
		Destination:     key.Destination,
		DestinationPort: req.DestPort,
		Transport:       "tcp",
		Protocol:        "socks5",
	})
	if err != nil {
		return nil, fmt.Errorf("resolve egress proxy credentials for %q: %w", cfg.CredentialRef, err)
	}
	if resp == nil {
		return nil, fmt.Errorf("egress proxy credentials for %q are unavailable", cfg.CredentialRef)
	}
	if s.authCache != nil {
		s.authCache.Put(key, resp)
	}
	material, err := resolveUsernamePasswordForAdapter(&egressAuthContext{Resolved: resp}, true)
	if err != nil {
		return nil, fmt.Errorf("resolve egress proxy credentials for %q: %w", cfg.CredentialRef, err)
	}
	return &xproxy.Auth{User: material.Username, Password: material.Password}, nil
}

func egressProxyDestination(req *adapterRequest) string {
	if req == nil {
		return ""
	}
	if host := strings.TrimSpace(req.Host); host != "" {
		return host
	}
	if req.DestIP != nil {
		return req.DestIP.String()
	}
	return ""
}

func validateSOCKS5ProxyEndpoint(ctx context.Context, compiled *policy.CompiledPolicy, host string) error {
	host = strings.TrimSpace(host)
	if host == "" {
		return fmt.Errorf("egress proxy host is required")
	}
	ips, err := resolveProxyEndpointIPs(ctx, host)
	if err != nil {
		return fmt.Errorf("resolve egress proxy host %q: %w", host, err)
	}
	for _, ip := range ips {
		if isProtectedProxyEndpointIP(compiled, ip) {
			return fmt.Errorf("%w: %s resolves to %s", errEgressProxyEndpointProtected, host, ip)
		}
	}
	return nil
}

func resolveSOCKS5ProxyDialAddress(ctx context.Context, compiled *policy.CompiledPolicy, cfg *policy.CompiledEgressProxy) (string, error) {
	if cfg == nil {
		return "", fmt.Errorf("egress proxy config is nil")
	}
	ips, err := resolveProxyEndpointIPs(ctx, cfg.Host)
	if err != nil {
		return "", fmt.Errorf("resolve egress proxy host %q: %w", cfg.Host, err)
	}
	var protected []net.IP
	for _, ip := range ips {
		if !isProtectedProxyEndpointIP(compiled, ip) {
			return net.JoinHostPort(ip.String(), fmt.Sprintf("%d", cfg.Port)), nil
		}
		protected = append(protected, ip)
	}
	if len(protected) > 0 {
		return "", fmt.Errorf("%w: %s resolves to %s", errEgressProxyEndpointProtected, cfg.Host, protected[0])
	}
	return "", fmt.Errorf("%w: %s has no usable addresses", errEgressProxyEndpointProtected, cfg.Host)
}

func resolveProxyEndpointIPs(ctx context.Context, host string) ([]net.IP, error) {
	if ip := net.ParseIP(host); ip != nil {
		return []net.IP{ip}, nil
	}
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	ips := make([]net.IP, 0, len(addrs))
	for _, addr := range addrs {
		if addr.IP != nil {
			ips = append(ips, addr.IP)
		}
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("no A or AAAA records")
	}
	return ips, nil
}

func isProtectedProxyEndpointIP(compiled *policy.CompiledPolicy, ip net.IP) bool {
	if ip == nil {
		return true
	}
	if !allowLocalEgressProxyEndpointsForTest && (ip.IsUnspecified() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast()) {
		return true
	}
	if compiled != nil && compiled.Platform != nil {
		for _, cidr := range compiled.Platform.DeniedCIDRs {
			if cidr != nil && cidr.Contains(ip) {
				return true
			}
		}
	}
	return false
}

func (s *Server) upstreamTimeout() time.Duration {
	if s != nil && s.cfg != nil && s.cfg.ProxyUpstreamTimeout.Duration > 0 {
		return s.cfg.ProxyUpstreamTimeout.Duration
	}
	return 30 * time.Second
}
