package network

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

// TCPProxy handles TCP connections with domain filtering.
type TCPProxy struct {
	mu sync.RWMutex

	listenAddr  string
	port        int32
	dnsResolver *DNSResolver
	logger      *zap.Logger

	// Whitelist
	allowDomains *DomainMatcher
	allowIPs     *IPNetSet

	// Server
	listener net.Listener
	running  bool
	ctx      context.Context
	cancel   context.CancelFunc

	// Connection tracking
	connections sync.Map
}

// NewTCPProxy creates a new TCP proxy.
func NewTCPProxy(port int32, dnsResolver *DNSResolver, logger *zap.Logger) *TCPProxy {
	return &TCPProxy{
		listenAddr:  fmt.Sprintf("127.0.0.1:%d", port),
		port:        port,
		dnsResolver: dnsResolver,
		logger:      logger,
		allowIPs:    NewIPNetSet(),
	}
}

// Start starts the TCP proxy.
func (p *TCPProxy) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.running {
		return nil
	}

	listener, err := net.Listen("tcp", p.listenAddr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	p.listener = listener
	p.ctx, p.cancel = context.WithCancel(context.Background())
	p.running = true

	go p.acceptLoop()

	p.logger.Info("TCP proxy started",
		zap.String("addr", p.listenAddr),
	)

	return nil
}

// Stop stops the TCP proxy.
func (p *TCPProxy) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.running {
		return nil
	}

	p.cancel()

	if p.listener != nil {
		p.listener.Close()
	}

	// Close all active connections
	p.connections.Range(func(key, value any) bool {
		if conn, ok := value.(net.Conn); ok {
			conn.Close()
		}
		return true
	})

	p.running = false
	p.logger.Info("TCP proxy stopped")

	return nil
}

// SetAllowDomains sets the allowed domains.
func (p *TCPProxy) SetAllowDomains(domains []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.allowDomains = NewDomainMatcher(domains)
}

// SetAllowIPs sets the allowed IPs.
func (p *TCPProxy) SetAllowIPs(ips *IPNetSet) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.allowIPs = ips
}

func (p *TCPProxy) acceptLoop() {
	for {
		select {
		case <-p.ctx.Done():
			return
		default:
		}

		conn, err := p.listener.Accept()
		if err != nil {
			if p.ctx.Err() != nil {
				return
			}
			p.logger.Error("Accept error", zap.Error(err))
			continue
		}

		go p.handleConnection(conn)
	}
}

func (p *TCPProxy) handleConnection(clientConn net.Conn) {
	defer clientConn.Close()

	connID := fmt.Sprintf("%s-%d", clientConn.RemoteAddr().String(), time.Now().UnixNano())
	p.connections.Store(connID, clientConn)
	defer p.connections.Delete(connID)

	// Read SOCKS5 handshake or use transparent proxy
	// For simplicity, we'll implement a simple CONNECT-style proxy

	// Read target address from the connection
	// In a real implementation, this would parse SOCKS5 protocol
	// or use the original destination from iptables REDIRECT

	buf := make([]byte, 1024)
	n, err := clientConn.Read(buf)
	if err != nil {
		return
	}

	// Parse the target from the first packet
	// This is a simplified implementation
	targetAddr := string(buf[:n])
	targetAddr = strings.TrimSpace(targetAddr)

	if !p.isAllowed(targetAddr) {
		p.logger.Info("Connection blocked",
			zap.String("target", targetAddr),
		)
		return
	}

	// Connect to target
	targetConn, err := net.DialTimeout("tcp", targetAddr, 10*time.Second)
	if err != nil {
		p.logger.Error("Failed to connect to target",
			zap.String("target", targetAddr),
			zap.Error(err),
		)
		return
	}
	defer targetConn.Close()

	// Relay data between client and target
	p.relay(clientConn, targetConn)
}

func (p *TCPProxy) isAllowed(addr string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}

	// Check if it's an IP address
	ip := net.ParseIP(host)
	if ip != nil {
		// Block private IPs
		if IsPrivateIP(ip) {
			return false
		}

		// Check IP whitelist
		if p.allowIPs != nil && !p.allowIPs.Contains(ip) {
			return false
		}

		return true
	}

	// It's a domain name
	// Check domain whitelist
	if p.allowDomains != nil && !p.allowDomains.Match(host) {
		return false
	}

	// Resolve domain using our own DNS resolver (anti-spoofing)
	ips, err := p.dnsResolver.Resolve(host)
	if err != nil {
		p.logger.Error("DNS resolution failed",
			zap.String("domain", host),
			zap.Error(err),
		)
		return false
	}

	// Check all resolved IPs
	for _, ipStr := range ips {
		ip := net.ParseIP(ipStr)
		if ip == nil {
			continue
		}

		// Block private IPs
		if IsPrivateIP(ip) {
			return false
		}

		// Check IP whitelist
		if p.allowIPs != nil && !p.allowIPs.Contains(ip) {
			return false
		}
	}

	return true
}

func (p *TCPProxy) relay(client, target net.Conn) {
	ctx, cancel := context.WithCancel(p.ctx)
	defer cancel()

	// Client -> Target
	go func() {
		io.Copy(target, client)
		cancel()
	}()

	// Target -> Client
	go func() {
		io.Copy(client, target)
		cancel()
	}()

	<-ctx.Done()
}

// DomainMatcher matches domains against patterns.
type DomainMatcher struct {
	mu       sync.RWMutex
	exact    map[string]bool
	wildcard []string
	allowAll bool
}

// NewDomainMatcher creates a new domain matcher.
func NewDomainMatcher(domains []string) *DomainMatcher {
	dm := &DomainMatcher{
		exact: make(map[string]bool),
	}

	for _, d := range domains {
		d = strings.ToLower(strings.TrimSpace(d))

		if d == "*" {
			dm.allowAll = true
			continue
		}

		if strings.HasPrefix(d, "*.") {
			// Wildcard domain
			dm.wildcard = append(dm.wildcard, d[2:])
		} else {
			// Exact domain
			dm.exact[d] = true
		}
	}

	return dm
}

// Match checks if a domain matches any pattern.
func (dm *DomainMatcher) Match(domain string) bool {
	if dm == nil {
		return false
	}

	dm.mu.RLock()
	defer dm.mu.RUnlock()

	if dm.allowAll {
		return true
	}

	domain = strings.ToLower(domain)

	// Check exact match
	if dm.exact[domain] {
		return true
	}

	// Check wildcard match
	for _, suffix := range dm.wildcard {
		if domain == suffix || strings.HasSuffix(domain, "."+suffix) {
			return true
		}
	}

	return false
}
