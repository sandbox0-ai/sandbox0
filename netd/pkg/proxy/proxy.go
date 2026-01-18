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
	"strconv"
	"strings"
	"sync"
	"time"

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
) *Proxy {
	ctx, cancel := context.WithCancel(context.Background())
	return &Proxy{
		logger:          logger,
		watcher:         watcher,
		listenAddr:      listenAddr,
		httpPort:        httpPort,
		httpsPort:       httpsPort,
		dnsResolvers:    dnsResolvers,
		connectionStats: make(map[string]*ConnectionStats),
		ctx:             ctx,
		cancel:          cancel,
	}
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

	if isTLS {
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
		Protocol:  "tcp",
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
}

// handleTLSConnection handles a TLS connection with SNI inspection
func (p *Proxy) handleTLSConnection(
	clientConn net.Conn,
	sandboxInfo *watcher.SandboxInfo,
) (destHost string, destPort int, bytesSent, bytesRecv int64, decision, reason string, err error) {
	destPort = 443

	// Peek the TLS Client Hello to extract SNI
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
	upstreamConn, err := net.DialTimeout("tcp", upstreamAddr, 10*time.Second)
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

	// Read HTTP request to get Host header
	req, err := http.ReadRequest(bufio.NewReader(clientConn))
	if err != nil {
		return "", destPort, 0, 0, "deny", "failed to read HTTP request", err
	}

	destHost = req.Host
	if strings.Contains(destHost, ":") {
		host, port, _ := net.SplitHostPort(destHost)
		destHost = host
		fmt.Sscanf(port, "%d", &destPort)
	}

	if destHost == "" {
		return "", destPort, 0, 0, "deny", "no Host header in HTTP request", nil
	}

	// Check policy
	decision, reason = p.checkPolicy(sandboxInfo.SandboxID, destHost, destPort)
	if decision == "deny" {
		// Send 403 response
		resp := &http.Response{
			StatusCode: http.StatusForbidden,
			Status:     "403 Forbidden",
			Proto:      "HTTP/1.1",
			ProtoMajor: 1,
			ProtoMinor: 1,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("Access denied by network policy")),
		}
		resp.Write(clientConn)
		return destHost, destPort, 0, 0, decision, reason, nil
	}

	// Resolve destination IP (with DNS rebinding protection)
	destIP, err := p.resolveWithProtection(destHost)
	if err != nil {
		return destHost, destPort, 0, 0, "deny", "DNS resolution failed: " + err.Error(), err
	}

	// Connect to upstream
	upstreamAddr := net.JoinHostPort(destIP, strconv.Itoa(destPort))
	upstreamConn, err := net.DialTimeout("tcp", upstreamAddr, 10*time.Second)
	if err != nil {
		return destHost, destPort, 0, 0, "deny", "upstream connection failed", err
	}
	defer upstreamConn.Close()

	// Forward the request
	if err := req.Write(upstreamConn); err != nil {
		return destHost, destPort, 0, 0, "deny", "failed to forward request", err
	}

	// Relay response
	bytesSent, bytesRecv = relay(clientConn, clientConn, upstreamConn)

	return destHost, destPort, bytesSent, bytesRecv, "allow", "policy allowed", nil
}

// checkPolicy checks if the destination is allowed by policy
func (p *Proxy) checkPolicy(sandboxID, destHost string, destPort int) (decision, reason string) {
	policy := p.watcher.GetNetworkPolicy(sandboxID)
	if policy == nil {
		// No policy = fail closed
		return "deny", "no network policy found"
	}

	if policy.Egress == nil {
		// No egress rules = default deny
		return "deny", "no egress policy, default deny"
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
	// Use custom resolver if configured
	var resolver *net.Resolver
	if len(p.dnsResolvers) > 0 {
		resolver = &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
				d := net.Dialer{Timeout: 5 * time.Second}
				return d.DialContext(ctx, "udp", p.dnsResolvers[0])
			},
		}
	} else {
		resolver = net.DefaultResolver
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ips, err := resolver.LookupIPAddr(ctx, hostname)
	if err != nil {
		return "", err
	}

	if len(ips) == 0 {
		return "", fmt.Errorf("no IP addresses found for %s", hostname)
	}

	// Check for DNS rebinding - ensure resolved IP is not private/internal
	for _, ip := range ips {
		if isInternalIP(ip.IP) {
			return "", fmt.Errorf("DNS rebinding detected: %s resolved to internal IP %s", hostname, ip.IP)
		}
	}

	return ips[0].IP.String(), nil
}

// isInternalIP checks if an IP is internal/private
func isInternalIP(ip net.IP) bool {
	privateRanges := []string{
		"10.0.0.0/8",
		"127.0.0.0/8",
		"169.254.0.0/16",
		"169.254.169.254/32", // Cloud metadata
		"172.16.0.0/12",
		"192.168.0.0/16",
		"fc00::/7",
		"fe80::/10",
	}

	for _, cidr := range privateRanges {
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

// TLS ClientHello parsing

type tlsClientHello struct {
	ServerName string
}

// peekTLSClientHello reads the TLS ClientHello and returns the SNI
func peekTLSClientHello(conn net.Conn) (*tlsClientHello, io.Reader, error) {
	// Read enough bytes to parse ClientHello
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, nil, err
	}

	data := buf[:n]

	// Parse TLS record header
	if len(data) < 5 {
		return nil, nil, fmt.Errorf("TLS record too short")
	}

	// Check content type (22 = Handshake)
	if data[0] != 22 {
		return nil, nil, fmt.Errorf("not a TLS handshake record")
	}

	// Extract SNI from ClientHello
	sni := extractSNI(data)

	// Create a reader that includes the already-read bytes
	reader := io.MultiReader(strings.NewReader(string(data)), conn)

	return &tlsClientHello{ServerName: sni}, reader, nil
}

// extractSNI extracts the SNI from a TLS ClientHello
func extractSNI(data []byte) string {
	// This is a simplified SNI extraction
	// In production, use a proper TLS parser

	// Look for SNI extension (type 0x0000)
	sniMarker := []byte{0x00, 0x00} // SNI extension type
	idx := 0
	for idx < len(data)-5 {
		if data[idx] == sniMarker[0] && data[idx+1] == sniMarker[1] {
			// Found potential SNI extension
			// Skip extension type (2 bytes) and length (2 bytes)
			idx += 4
			if idx >= len(data)-3 {
				break
			}
			// Read SNI list length
			sniListLen := int(data[idx])<<8 | int(data[idx+1])
			idx += 2
			if idx >= len(data)-3 || sniListLen == 0 {
				break
			}
			// Read SNI type (should be 0 for hostname)
			if data[idx] != 0 {
				idx++
				continue
			}
			idx++
			// Read hostname length
			if idx >= len(data)-2 {
				break
			}
			hostnameLen := int(data[idx])<<8 | int(data[idx+1])
			idx += 2
			if idx+hostnameLen > len(data) {
				break
			}
			return string(data[idx : idx+hostnameLen])
		}
		idx++
	}
	return ""
}

// relay copies data between two connections bidirectionally
func relay(clientReader io.Reader, clientConn, upstreamConn net.Conn) (bytesSent, bytesRecv int64) {
	var wg sync.WaitGroup
	wg.Add(2)

	// Client -> Upstream
	go func() {
		defer wg.Done()
		bytesSent, _ = io.Copy(upstreamConn, clientReader)
	}()

	// Upstream -> Client
	go func() {
		defer wg.Done()
		bytesRecv, _ = io.Copy(clientConn, upstreamConn)
	}()

	wg.Wait()
	return
}

// limitedReader wraps a reader with a size limit
type limitedReader struct {
	r io.Reader
	n int64
}

func newLimitedReader(r io.Reader) *limitedReader {
	return &limitedReader{r: r, n: 64 * 1024} // 64KB limit for HTTP headers
}

func (l *limitedReader) Read(p []byte) (n int, err error) {
	if l.n <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > l.n {
		p = p[0:l.n]
	}
	n, err = l.r.Read(p)
	l.n -= int64(n)
	return
}

// bufReader implements io.Reader for already-buffered data
type bufReader struct {
	buf    []byte
	offset int
}

func (b *bufReader) Read(p []byte) (n int, err error) {
	if b.offset >= len(b.buf) {
		return 0, io.EOF
	}
	n = copy(p, b.buf[b.offset:])
	b.offset += n
	return n, nil
}
