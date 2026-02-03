// Package proxy implements the L7 transparent proxy for netd.
// It handles HTTP/HTTPS traffic for domain-based filtering and SNI inspection.
package proxy

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/infra/netd/pkg/metrics"
	"github.com/sandbox0-ai/infra/netd/pkg/watcher"
	"go.uber.org/zap"
)

// Proxy is the L7 transparent proxy
type Proxy struct {
	logger        *zap.Logger
	watcher       *watcher.Watcher
	httpListener  net.Listener
	httpsListener net.Listener
	httpPort      int
	httpsPort     int
	listenAddr    string
	dnsResolvers  []string
	dialTimeout   time.Duration
	dnsTimeout    time.Duration
	headerLimit   int64
	failClosed    bool
	httpTransport *http.Transport
	httpClient    *http.Client
	auditPath     string
	auditMaxBytes int64
	auditBackups  int
	auditFile     *os.File
	auditMu       sync.Mutex

	// Stats
	mu              sync.RWMutex
	connectionStats map[string]*ConnectionStats

	// Shutdown
	ctx    context.Context
	cancel context.CancelFunc
}

// ConnectionStats tracks connection statistics per sandbox
type ConnectionStats struct {
	SandboxID       string
	TotalBytes      int64
	TotalPackets    int64
	ConnectionCount int64
	AllowedCount    int64
	DeniedCount     int64
	LastUpdated     time.Time
}

// AuditEntry represents an audit log entry
type AuditEntry struct {
	Timestamp time.Time `json:"timestamp"`
	SandboxID string    `json:"sandbox_id"`
	TeamID    string    `json:"team_id"`
	SourceIP  string    `json:"source_ip"`
	DestHost  string    `json:"dest_host"`
	DestPort  int       `json:"dest_port"`
	Protocol  string    `json:"protocol"`
	Decision  string    `json:"decision"` // allow, deny
	Reason    string    `json:"reason"`
	BytesSent int64     `json:"bytes_sent"`
	BytesRecv int64     `json:"bytes_recv"`
	Duration  float64   `json:"duration_ms"`
	Error     string    `json:"error,omitempty"`
}

// NewProxy creates a new L7 proxy
func NewProxy(
	logger *zap.Logger,
	watcher *watcher.Watcher,
	listenAddr string,
	httpPort int,
	httpsPort int,
	dnsResolvers []string,
	dialTimeout time.Duration,
	dnsTimeout time.Duration,
	headerLimit int64,
	failClosed bool,
	maxIdleConns int,
	maxIdleConnsPerHost int,
	idleConnTimeout time.Duration,
	responseHeaderTimeout time.Duration,
	auditPath string,
	auditMaxBytes int64,
	auditBackups int,
) *Proxy {
	ctx, cancel := context.WithCancel(context.Background())
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: dialTimeout, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     false,
		MaxIdleConns:          maxIdleConns,
		MaxIdleConnsPerHost:   maxIdleConnsPerHost,
		IdleConnTimeout:       idleConnTimeout,
		ResponseHeaderTimeout: responseHeaderTimeout,
	}
	client := &http.Client{Transport: transport}

	p := &Proxy{
		logger:          logger,
		watcher:         watcher,
		listenAddr:      listenAddr,
		httpPort:        httpPort,
		httpsPort:       httpsPort,
		dnsResolvers:    dnsResolvers,
		dialTimeout:     dialTimeout,
		dnsTimeout:      dnsTimeout,
		headerLimit:     headerLimit,
		failClosed:      failClosed,
		httpTransport:   transport,
		httpClient:      client,
		auditPath:       auditPath,
		auditMaxBytes:   auditMaxBytes,
		auditBackups:    auditBackups,
		connectionStats: make(map[string]*ConnectionStats),
		ctx:             ctx,
		cancel:          cancel,
	}
	p.openAuditLog()
	return p
}

// Start starts the proxy listeners
func (p *Proxy) Start(ctx context.Context) error {
	// Start HTTP proxy
	httpAddr := fmt.Sprintf("%s:%d", p.listenAddr, p.httpPort)
	var err error
	p.httpListener, err = net.Listen("tcp", httpAddr)
	if err != nil {
		return fmt.Errorf("listen HTTP proxy: %w", err)
	}

	// Start HTTPS proxy
	httpsAddr := fmt.Sprintf("%s:%d", p.listenAddr, p.httpsPort)
	p.httpsListener, err = net.Listen("tcp", httpsAddr)
	if err != nil {
		p.httpListener.Close()
		return fmt.Errorf("listen HTTPS proxy: %w", err)
	}

	p.logger.Info("Proxy started",
		zap.String("httpAddr", httpAddr),
		zap.String("httpsAddr", httpsAddr),
	)

	// Handle HTTP connections
	go p.acceptLoop(p.httpListener, false)

	// Handle HTTPS connections
	go p.acceptLoop(p.httpsListener, true)

	return nil
}

// acceptLoop accepts connections in a loop
func (p *Proxy) acceptLoop(listener net.Listener, isTLS bool) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-p.ctx.Done():
				return
			default:
				p.logger.Error("Accept error", zap.Error(err))
				continue
			}
		}

		go p.handleConnection(conn, isTLS)
	}
}

// handleConnection handles a single proxied connection
func (p *Proxy) handleConnection(clientConn net.Conn, isTLS bool) {
	defer clientConn.Close()

	startTime := time.Now()

	// Get client IP (this is the pod IP due to transparent proxy)
	clientAddr := clientConn.RemoteAddr().(*net.TCPAddr)
	clientIP := clientAddr.IP.String()

	// Look up sandbox info
	sandboxInfo := p.watcher.GetSandboxByIP(clientIP)
	if sandboxInfo == nil {
		p.logger.Warn("Connection from unknown IP",
			zap.String("clientIP", clientIP),
		)
		// Fail closed - deny unknown traffic
		return
	}

	var destHost string
	var destPort int
	var bytesSent, bytesRecv int64
	var decision, reason string
	var connErr error
	protocol := "http"

	if isTLS {
		protocol = "https"
		destHost, destPort, bytesSent, bytesRecv, decision, reason, connErr = p.handleTLSConnection(clientConn, sandboxInfo)
	} else {
		destHost, destPort, bytesSent, bytesRecv, decision, reason, connErr = p.handleHTTPConnection(clientConn, sandboxInfo)
	}

	// Log audit entry
	entry := &AuditEntry{
		Timestamp: startTime,
		SandboxID: sandboxInfo.SandboxID,
		TeamID:    sandboxInfo.TeamID,
		SourceIP:  clientIP,
		DestHost:  destHost,
		DestPort:  destPort,
		Protocol:  protocol,
		Decision:  decision,
		Reason:    reason,
		BytesSent: bytesSent,
		BytesRecv: bytesRecv,
		Duration:  float64(time.Since(startTime).Milliseconds()),
	}
	if connErr != nil {
		entry.Error = connErr.Error()
	}

	p.logAudit(entry)
	p.updateStats(sandboxInfo.SandboxID, bytesSent+bytesRecv, decision == "allow")
	metrics.RecordConnection(sandboxInfo.SandboxID, sandboxInfo.TeamID, "egress", decision)
	metrics.RecordProxyRequest(sandboxInfo.SandboxID, entry.Protocol, decision, float64(time.Since(startTime).Seconds()))
	if bytesSent+bytesRecv > 0 {
		metrics.RecordBytes(sandboxInfo.SandboxID, sandboxInfo.TeamID, "egress", bytesSent+bytesRecv)
	}
}

// handleTLSConnection handles a TLS connection with SNI inspection
func (p *Proxy) handleTLSConnection(
	clientConn net.Conn,
	sandboxInfo *watcher.SandboxInfo,
) (destHost string, destPort int, bytesSent, bytesRecv int64, decision, reason string, err error) {
	destPort = 443

	// Peek the TLS Client Hello to extract SNI
	if err := clientConn.SetReadDeadline(time.Now().Add(p.dialTimeout)); err == nil {
		defer clientConn.SetReadDeadline(time.Time{})
	}
	clientHello, clientReader, err := peekTLSClientHello(clientConn)
	if err != nil {
		return "", destPort, 0, 0, "deny", "failed to read TLS ClientHello", err
	}

	destHost = clientHello.ServerName
	if destHost == "" {
		return "", destPort, 0, 0, "deny", "no SNI in TLS ClientHello", nil
	}

	// Check policy
	decision, reason = p.checkPolicy(sandboxInfo.SandboxID, destHost, destPort)
	if decision == "deny" {
		return destHost, destPort, 0, 0, decision, reason, nil
	}

	// Resolve destination IP (with DNS rebinding protection)
	destIP, err := p.resolveWithProtection(destHost)
	if err != nil {
		return destHost, destPort, 0, 0, "deny", "DNS resolution failed: " + err.Error(), err
	}

	// Connect to upstream
	upstreamAddr := net.JoinHostPort(destIP, strconv.Itoa(destPort))
	upstreamConn, err := net.DialTimeout("tcp", upstreamAddr, p.dialTimeout)
	if err != nil {
		return destHost, destPort, 0, 0, "deny", "upstream connection failed", err
	}
	defer upstreamConn.Close()

	// Relay traffic bidirectionally
	bytesSent, bytesRecv = relay(clientReader, clientConn, upstreamConn)

	return destHost, destPort, bytesSent, bytesRecv, "allow", "policy allowed", nil
}

// handleHTTPConnection handles an HTTP connection with Host header inspection
func (p *Proxy) handleHTTPConnection(
	clientConn net.Conn,
	sandboxInfo *watcher.SandboxInfo,
) (destHost string, destPort int, bytesSent, bytesRecv int64, decision, reason string, err error) {
	destPort = 80

	reader := bufio.NewReader(clientConn)
	for {
		_ = clientConn.SetReadDeadline(time.Now().Add(p.dialTimeout))
		req, reqErr := http.ReadRequest(reader)
		_ = clientConn.SetReadDeadline(time.Time{})
		if reqErr != nil {
			if reqErr == io.EOF {
				return destHost, destPort, 0, 0, "allow", "connection closed", nil
			}
			return destHost, destPort, 0, 0, "deny", "failed to read HTTP request", reqErr
		}

		if p.headerLimit > 0 && estimateHeaderSize(req) > p.headerLimit {
			resp := &http.Response{
				StatusCode: http.StatusRequestHeaderFieldsTooLarge,
				Status:     "431 Request Header Fields Too Large",
				Proto:      "HTTP/1.1",
				ProtoMajor: 1,
				ProtoMinor: 1,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("Request headers too large")),
				Close:      true,
			}
			resp.Write(clientConn)
			return destHost, destPort, 0, 0, "deny", "request headers too large", nil
		}

		if strings.EqualFold(req.Method, "CONNECT") {
			resp := &http.Response{
				StatusCode: http.StatusForbidden,
				Status:     "403 Forbidden",
				Proto:      "HTTP/1.1",
				ProtoMajor: 1,
				ProtoMinor: 1,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("CONNECT not allowed")),
				Close:      true,
			}
			resp.Write(clientConn)
			return destHost, destPort, 0, 0, "deny", "CONNECT not allowed", nil
		}

		destHost = req.Host
		destPort = 80
		if strings.Contains(destHost, ":") {
			host, port, _ := net.SplitHostPort(destHost)
			destHost = host
			fmt.Sscanf(port, "%d", &destPort)
		}

		if destHost == "" {
			resp := &http.Response{
				StatusCode: http.StatusForbidden,
				Status:     "403 Forbidden",
				Proto:      "HTTP/1.1",
				ProtoMajor: 1,
				ProtoMinor: 1,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("Missing Host header")),
				Close:      true,
			}
			resp.Write(clientConn)
			return destHost, destPort, 0, 0, "deny", "no Host header in HTTP request", nil
		}

		// Check policy
		decision, reason = p.checkPolicy(sandboxInfo.SandboxID, destHost, destPort)
		if decision == "deny" {
			resp := &http.Response{
				StatusCode: http.StatusForbidden,
				Status:     "403 Forbidden",
				Proto:      "HTTP/1.1",
				ProtoMajor: 1,
				ProtoMinor: 1,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("Access denied by network policy")),
				Close:      true,
			}
			resp.Write(clientConn)
			return destHost, destPort, 0, 0, decision, reason, nil
		}

		// Resolve destination IP (with DNS rebinding protection)
		_, resolveErr := p.resolveWithProtection(destHost)
		if resolveErr != nil {
			return destHost, destPort, 0, 0, "deny", "DNS resolution failed: " + resolveErr.Error(), resolveErr
		}

		req.RequestURI = ""
		req.Header.Del("Proxy-Connection")
		if req.URL != nil {
			req.URL.Scheme = "http"
			req.URL.Host = net.JoinHostPort(destHost, strconv.Itoa(destPort))
		}
		if req.URL != nil && req.URL.Host == "" {
			req.URL.Host = net.JoinHostPort(destHost, strconv.Itoa(destPort))
		}

		resp, respErr := p.httpClient.Do(req)
		if respErr != nil {
			if p.failClosed {
				return destHost, destPort, 0, 0, "deny", "upstream request failed", respErr
			}
			return destHost, destPort, 0, 0, "allow", "upstream request failed, fail open", respErr
		}
		defer resp.Body.Close()

		if err := resp.Write(clientConn); err != nil {
			return destHost, destPort, 0, 0, "deny", "failed to write response", err
		}

		if req.Close || resp.Close {
			return destHost, destPort, 0, 0, "allow", "policy allowed", nil
		}
	}
}

// checkPolicy checks if the destination is allowed by policy
func (p *Proxy) checkPolicy(sandboxID, destHost string, destPort int) (decision, reason string) {
	start := time.Now()
	defer metrics.RecordPolicyEvaluation(sandboxID, time.Since(start).Seconds())

	policy := p.watcher.GetNetworkPolicy(sandboxID)
	if policy == nil {
		// No policy = fail closed unless configured otherwise
		if p.failClosed {
			return "deny", "no network policy found"
		}
		return "allow", "no network policy, fail open"
	}

	if policy.Egress == nil {
		// No egress rules = default deny unless configured otherwise
		if p.failClosed {
			return "deny", "no egress policy, default deny"
		}
		return "allow", "no egress policy, fail open"
	}

	egress := policy.Egress

	// Check denied domains first
	for _, domain := range egress.DeniedDomains {
		if matchDomain(destHost, domain) {
			return "deny", "domain in deny list: " + domain
		}
	}

	// Check allowed domains
	for _, domain := range egress.AllowedDomains {
		if matchDomain(destHost, domain) {
			return "allow", "domain in allow list: " + domain
		}
	}

	// Default action
	if egress.DefaultAction == "allow" {
		return "allow", "default allow"
	}

	return "deny", "no matching rule, default deny"
}

// resolveWithProtection resolves a hostname with DNS rebinding protection
func (p *Proxy) resolveWithProtection(hostname string) (string, error) {
	start := time.Now()
	// Use custom resolver if configured
	var resolver *net.Resolver
	if len(p.dnsResolvers) > 0 {
		resolver = &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
				d := net.Dialer{Timeout: p.dnsTimeout}
				return d.DialContext(ctx, "udp", p.dnsResolvers[0])
			},
		}
	} else {
		resolver = net.DefaultResolver
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.dnsTimeout)
	defer cancel()

	ips, err := resolver.LookupIPAddr(ctx, hostname)
	if err != nil {
		metrics.RecordDNSResolution("error", time.Since(start).Seconds())
		return "", err
	}

	if len(ips) == 0 {
		metrics.RecordDNSResolution("error", time.Since(start).Seconds())
		return "", fmt.Errorf("no IP addresses found for %s", hostname)
	}

	// Check for DNS rebinding - ensure resolved IP is not private/internal
	for _, ip := range ips {
		if isInternalIP(ip.IP) {
			metrics.RecordDNSResolution("rebinding_blocked", time.Since(start).Seconds())
			return "", fmt.Errorf("DNS rebinding detected: %s resolved to internal IP %s", hostname, ip.IP)
		}
	}

	metrics.RecordDNSResolution("success", time.Since(start).Seconds())
	return ips[0].IP.String(), nil
}

// isInternalIP checks if an IP is internal/private
func isInternalIP(ip net.IP) bool {
	for _, cidr := range v1alpha1.PlatformDeniedCIDRs {
		_, network, _ := net.ParseCIDR(cidr)
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

// matchDomain checks if host matches a domain pattern (supports wildcards)
func matchDomain(host, pattern string) bool {
	if pattern == host {
		return true
	}
	if strings.HasPrefix(pattern, "*.") {
		suffix := pattern[1:] // Remove *
		return strings.HasSuffix(host, suffix)
	}
	return false
}

// logAudit logs an audit entry
func (p *Proxy) logAudit(entry *AuditEntry) {
	level := zap.InfoLevel
	if entry.Decision == "deny" {
		level = zap.WarnLevel
	}

	p.logger.Log(level, "proxy audit",
		zap.String("sandbox_id", entry.SandboxID),
		zap.String("team_id", entry.TeamID),
		zap.String("source_ip", entry.SourceIP),
		zap.String("dest_host", entry.DestHost),
		zap.Int("dest_port", entry.DestPort),
		zap.String("decision", entry.Decision),
		zap.String("reason", entry.Reason),
		zap.Int64("bytes_sent", entry.BytesSent),
		zap.Int64("bytes_recv", entry.BytesRecv),
		zap.Float64("duration_ms", entry.Duration),
		zap.String("error", entry.Error),
	)
	p.writeAuditEntry(entry)
}

// updateStats updates connection statistics
func (p *Proxy) updateStats(sandboxID string, bytes int64, allowed bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	stats, ok := p.connectionStats[sandboxID]
	if !ok {
		stats = &ConnectionStats{SandboxID: sandboxID}
		p.connectionStats[sandboxID] = stats
	}

	stats.TotalBytes += bytes
	stats.ConnectionCount++
	stats.LastUpdated = time.Now()

	if allowed {
		stats.AllowedCount++
	} else {
		stats.DeniedCount++
	}
}

// GetStats returns connection statistics for a sandbox
func (p *Proxy) GetStats(sandboxID string) *ConnectionStats {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.connectionStats[sandboxID]
}

// Stop stops the proxy
func (p *Proxy) Stop() error {
	p.cancel()

	if p.httpListener != nil {
		p.httpListener.Close()
	}
	if p.httpsListener != nil {
		p.httpsListener.Close()
	}

	return nil
}
