package proxy

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
)

type proxyAdapter interface {
	Name() string
	Transport() string
	Protocol() string
	Capability() adapterCapability
	Handle(*adapterRequest) error
}

type adapterCapability string

const (
	adapterCapabilityPassThrough adapterCapability = "pass-through"
	adapterCapabilityInspect     adapterCapability = "inspect"
	adapterCapabilityTerminate   adapterCapability = "terminate"
)

type adapterRequest struct {
	Server     *Server
	Compiled   *policy.CompiledPolicy
	Audit      *flowAudit
	EgressAuth *egressAuthContext
	UDPSession *udpSession
	SrcIP      string
	DestIP     net.IP
	DestPort   int
	Host       string
	Conn       net.Conn
	Prefix     io.Reader
	UDPConn    *net.UDPConn
	UDPSource  *net.UDPAddr
	UDPPayload []byte
}

type httpAdapter struct{}

func (a *httpAdapter) Name() string      { return "http" }
func (a *httpAdapter) Transport() string { return "tcp" }
func (a *httpAdapter) Protocol() string  { return "http" }
func (a *httpAdapter) Capability() adapterCapability {
	return adapterCapabilityInspect
}

func (a *httpAdapter) Handle(req *adapterRequest) error {
	if req == nil || req.Server == nil || req.Conn == nil {
		return fmt.Errorf("http adapter requires connection")
	}
	req.Server.recordFlow(req.SrcIP, req.DestIP, req.DestPort, "tcp", remotePort(req.Conn.RemoteAddr()))
	if req.EgressAuth != nil && req.EgressAuth.Rule != nil {
		if err := prepareHTTPHeaderDirectives(req.EgressAuth, "http", true); err != nil {
			if req.EgressAuth.ShouldBypass() {
				return req.Server.proxyHTTPRequest(req)
			}
			if errors.Is(err, errEgressAuthDirectiveUnsupported) {
				_ = writeHTTPProxyError(req.Conn, http.StatusServiceUnavailable, "egress auth directives unsupported")
				return fmt.Errorf("egress auth directives unsupported for %q", req.EgressAuth.Rule.AuthRef)
			}
		}
		if req.EgressAuth.ShouldBypass() {
			return req.Server.proxyHTTPRequest(req)
		}
		if req.EgressAuth.ResolveError != nil {
			_ = writeHTTPProxyError(req.Conn, http.StatusServiceUnavailable, "egress auth resolution failed")
			return fmt.Errorf("resolve egress auth for %q: %w", req.EgressAuth.Rule.AuthRef, req.EgressAuth.ResolveError)
		}
		if req.EgressAuth.Resolved == nil || len(req.EgressAuth.ResolvedHeaders) == 0 {
			_ = writeHTTPProxyError(req.Conn, http.StatusServiceUnavailable, "egress auth material unavailable")
			return fmt.Errorf("egress auth material missing for %q", req.EgressAuth.Rule.AuthRef)
		}
		return req.Server.proxyHTTPRequest(req)
	}
	return req.Server.relayTCPConn(req.Conn, req.Prefix, req.DestIP, req.DestPort, req.Compiled, req.Audit)
}

func parseBufferedHTTPRequest(headerBytes []byte) (*http.Request, error) {
	reader := bufio.NewReader(bytes.NewReader(headerBytes))
	req, err := http.ReadRequest(reader)
	if err != nil {
		return nil, err
	}
	return req, nil
}

func injectHTTPHeaders(req *http.Request, headers map[string]string) {
	if req == nil || len(headers) == 0 {
		return
	}
	for key, value := range headers {
		key = http.CanonicalHeaderKey(strings.TrimSpace(key))
		if key == "" {
			continue
		}
		req.Header.Set(key, value)
	}
}

func writeHTTPProxyError(conn net.Conn, statusCode int, message string) error {
	if conn == nil {
		return nil
	}
	resp := &http.Response{
		StatusCode:    statusCode,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		ContentLength: int64(len(message)),
		Body:          io.NopCloser(strings.NewReader(message)),
		Header:        make(http.Header),
		Close:         true,
	}
	resp.Header.Set("Content-Type", "text/plain; charset=utf-8")
	resp.Header.Set("Connection", "close")
	return resp.Write(conn)
}

type tlsAdapter struct{}

func (a *tlsAdapter) Name() string      { return "tls" }
func (a *tlsAdapter) Transport() string { return "tcp" }
func (a *tlsAdapter) Protocol() string  { return "tls" }
func (a *tlsAdapter) Capability() adapterCapability {
	return adapterCapabilityTerminate
}

func (a *tlsAdapter) Handle(req *adapterRequest) error {
	if req == nil || req.Server == nil || req.Conn == nil {
		return fmt.Errorf("tls adapter requires connection")
	}
	req.Server.recordFlow(req.SrcIP, req.DestIP, req.DestPort, "tcp", remotePort(req.Conn.RemoteAddr()))
	if req.EgressAuth != nil && req.EgressAuth.Rule != nil {
		if req.EgressAuth.Rule.Protocol == v1alpha1.EgressAuthProtocolTLS {
			if err := prepareTLSClientCertificateDirectives(req.EgressAuth, "tls", tlsTerminationRequired(req)); err != nil {
				if req.EgressAuth.ShouldBypass() {
					return req.Server.relayTCPConn(req.Conn, req.Prefix, req.DestIP, req.DestPort, req.Compiled, req.Audit)
				}
				return fmt.Errorf("prepare tls client certificate directives for %q: %w", req.EgressAuth.Rule.AuthRef, err)
			}
			if req.EgressAuth.ShouldBypass() {
				return req.Server.relayTCPConn(req.Conn, req.Prefix, req.DestIP, req.DestPort, req.Compiled, req.Audit)
			}
			if req.EgressAuth.ResolveError != nil {
				return fmt.Errorf("resolve egress auth for %q: %w", req.EgressAuth.Rule.AuthRef, req.EgressAuth.ResolveError)
			}
			if req.EgressAuth.ResolvedTLSClientCertificate == nil {
				return fmt.Errorf("egress auth material missing for %q", req.EgressAuth.Rule.AuthRef)
			}
			return req.Server.proxyTLSStream(req)
		}
		if err := prepareHTTPHeaderDirectives(req.EgressAuth, "tls", tlsTerminationRequired(req)); err != nil {
			if req.EgressAuth.ShouldBypass() {
				return req.Server.relayTCPConn(req.Conn, req.Prefix, req.DestIP, req.DestPort, req.Compiled, req.Audit)
			}
			if errors.Is(err, errEgressAuthDirectiveUnsupported) {
				return fmt.Errorf("egress auth directives unsupported for %q", req.EgressAuth.Rule.AuthRef)
			}
		}
	}
	if tlsTerminationRequired(req) {
		if req.EgressAuth != nil && req.EgressAuth.ShouldBypass() {
			return req.Server.relayTCPConn(req.Conn, req.Prefix, req.DestIP, req.DestPort, req.Compiled, req.Audit)
		}
		return req.Server.proxyHTTPSRequest(req)
	}
	if req.EgressAuth != nil && req.EgressAuth.Rule != nil && req.EgressAuth.Rule.Protocol == v1alpha1.EgressAuthProtocolHTTPS && req.EgressAuth.Rule.TLSMode == "" {
		return req.Server.relayTCPConn(req.Conn, req.Prefix, req.DestIP, req.DestPort, req.Compiled, req.Audit)
	}
	return req.Server.relayTCPConn(req.Conn, req.Prefix, req.DestIP, req.DestPort, req.Compiled, req.Audit)
}

type sshAdapter struct{}

func (a *sshAdapter) Name() string      { return "ssh" }
func (a *sshAdapter) Transport() string { return "tcp" }
func (a *sshAdapter) Protocol() string  { return "ssh" }
func (a *sshAdapter) Capability() adapterCapability {
	return adapterCapabilityPassThrough
}

func (a *sshAdapter) Handle(req *adapterRequest) error {
	if req == nil || req.Server == nil || req.Conn == nil {
		return fmt.Errorf("ssh adapter requires connection")
	}
	req.Server.recordFlow(req.SrcIP, req.DestIP, req.DestPort, "tcp", remotePort(req.Conn.RemoteAddr()))
	return req.Server.relayTCPConn(req.Conn, req.Prefix, req.DestIP, req.DestPort, req.Compiled, req.Audit)
}

type postgresAdapter struct{}

func (a *postgresAdapter) Name() string      { return "postgres" }
func (a *postgresAdapter) Transport() string { return "tcp" }
func (a *postgresAdapter) Protocol() string  { return "postgres" }
func (a *postgresAdapter) Capability() adapterCapability {
	return adapterCapabilityPassThrough
}

func (a *postgresAdapter) Handle(req *adapterRequest) error {
	if req == nil || req.Server == nil || req.Conn == nil {
		return fmt.Errorf("postgres adapter requires connection")
	}
	req.Server.recordFlow(req.SrcIP, req.DestIP, req.DestPort, "tcp", remotePort(req.Conn.RemoteAddr()))
	return req.Server.relayTCPConn(req.Conn, req.Prefix, req.DestIP, req.DestPort, req.Compiled, req.Audit)
}

type socks5Adapter struct{}

func (a *socks5Adapter) Name() string      { return "socks5" }
func (a *socks5Adapter) Transport() string { return "tcp" }
func (a *socks5Adapter) Protocol() string  { return "socks5" }
func (a *socks5Adapter) Capability() adapterCapability {
	return adapterCapabilityPassThrough
}

func (a *socks5Adapter) Handle(req *adapterRequest) error {
	if req == nil || req.Server == nil || req.Conn == nil {
		return fmt.Errorf("socks5 adapter requires connection")
	}
	req.Server.recordFlow(req.SrcIP, req.DestIP, req.DestPort, "tcp", remotePort(req.Conn.RemoteAddr()))
	return req.Server.relayTCPConn(req.Conn, req.Prefix, req.DestIP, req.DestPort, req.Compiled, req.Audit)
}

type amqpAdapter struct{}

func (a *amqpAdapter) Name() string      { return "amqp" }
func (a *amqpAdapter) Transport() string { return "tcp" }
func (a *amqpAdapter) Protocol() string  { return "amqp" }
func (a *amqpAdapter) Capability() adapterCapability {
	return adapterCapabilityPassThrough
}

func (a *amqpAdapter) Handle(req *adapterRequest) error {
	if req == nil || req.Server == nil || req.Conn == nil {
		return fmt.Errorf("amqp adapter requires connection")
	}
	req.Server.recordFlow(req.SrcIP, req.DestIP, req.DestPort, "tcp", remotePort(req.Conn.RemoteAddr()))
	return req.Server.relayTCPConn(req.Conn, req.Prefix, req.DestIP, req.DestPort, req.Compiled, req.Audit)
}

type dnsAdapter struct{}

func (a *dnsAdapter) Name() string      { return "dns" }
func (a *dnsAdapter) Transport() string { return "tcp" }
func (a *dnsAdapter) Protocol() string  { return "dns" }
func (a *dnsAdapter) Capability() adapterCapability {
	return adapterCapabilityPassThrough
}

func (a *dnsAdapter) Handle(req *adapterRequest) error {
	if req == nil || req.Server == nil || req.Conn == nil {
		return fmt.Errorf("dns adapter requires connection")
	}
	req.Server.recordFlow(req.SrcIP, req.DestIP, req.DestPort, "tcp", remotePort(req.Conn.RemoteAddr()))
	return req.Server.relayTCPConn(req.Conn, req.Prefix, req.DestIP, req.DestPort, req.Compiled, req.Audit)
}

type mqttAdapter struct{}

func (a *mqttAdapter) Name() string      { return "mqtt" }
func (a *mqttAdapter) Transport() string { return "tcp" }
func (a *mqttAdapter) Protocol() string  { return "mqtt" }
func (a *mqttAdapter) Capability() adapterCapability {
	return adapterCapabilityPassThrough
}

func (a *mqttAdapter) Handle(req *adapterRequest) error {
	if req == nil || req.Server == nil || req.Conn == nil {
		return fmt.Errorf("mqtt adapter requires connection")
	}
	req.Server.recordFlow(req.SrcIP, req.DestIP, req.DestPort, "tcp", remotePort(req.Conn.RemoteAddr()))
	return req.Server.relayTCPConn(req.Conn, req.Prefix, req.DestIP, req.DestPort, req.Compiled, req.Audit)
}

type mongodbAdapter struct{}

func (a *mongodbAdapter) Name() string      { return "mongodb" }
func (a *mongodbAdapter) Transport() string { return "tcp" }
func (a *mongodbAdapter) Protocol() string  { return "mongodb" }
func (a *mongodbAdapter) Capability() adapterCapability {
	return adapterCapabilityPassThrough
}

func (a *mongodbAdapter) Handle(req *adapterRequest) error {
	if req == nil || req.Server == nil || req.Conn == nil {
		return fmt.Errorf("mongodb adapter requires connection")
	}
	req.Server.recordFlow(req.SrcIP, req.DestIP, req.DestPort, "tcp", remotePort(req.Conn.RemoteAddr()))
	return req.Server.relayTCPConn(req.Conn, req.Prefix, req.DestIP, req.DestPort, req.Compiled, req.Audit)
}

type redisAdapter struct{}

func (a *redisAdapter) Name() string      { return "redis" }
func (a *redisAdapter) Transport() string { return "tcp" }
func (a *redisAdapter) Protocol() string  { return "redis" }
func (a *redisAdapter) Capability() adapterCapability {
	return adapterCapabilityPassThrough
}

func (a *redisAdapter) Handle(req *adapterRequest) error {
	if req == nil || req.Server == nil || req.Conn == nil {
		return fmt.Errorf("redis adapter requires connection")
	}
	req.Server.recordFlow(req.SrcIP, req.DestIP, req.DestPort, "tcp", remotePort(req.Conn.RemoteAddr()))
	return req.Server.relayTCPConn(req.Conn, req.Prefix, req.DestIP, req.DestPort, req.Compiled, req.Audit)
}

type udpAdapter struct{}

func (a *udpAdapter) Name() string      { return "udp" }
func (a *udpAdapter) Transport() string { return "udp" }
func (a *udpAdapter) Protocol() string  { return "udp" }
func (a *udpAdapter) Capability() adapterCapability {
	return adapterCapabilityPassThrough
}

func (a *udpAdapter) Handle(req *adapterRequest) error {
	if req == nil || req.Server == nil || req.UDPConn == nil || req.UDPSource == nil {
		return fmt.Errorf("udp adapter requires source datagram")
	}
	req.Server.recordFlow(req.SrcIP, req.DestIP, req.DestPort, "udp", req.UDPSource.Port)
	return req.Server.forwardUDPDatagram(req.UDPConn, req.UDPSource, req.UDPPayload, req.DestIP, req.DestPort, req.Compiled, req.Audit)
}

type tcpPassThroughAdapter struct{}

func (a *tcpPassThroughAdapter) Name() string      { return "tcp-pass-through" }
func (a *tcpPassThroughAdapter) Transport() string { return "tcp" }
func (a *tcpPassThroughAdapter) Protocol() string  { return "unknown" }
func (a *tcpPassThroughAdapter) Capability() adapterCapability {
	return adapterCapabilityPassThrough
}

func (a *tcpPassThroughAdapter) Handle(req *adapterRequest) error {
	if req == nil || req.Server == nil || req.Conn == nil {
		return fmt.Errorf("tcp fallback adapter requires connection")
	}
	req.Server.recordFlow(req.SrcIP, req.DestIP, req.DestPort, "tcp", remotePort(req.Conn.RemoteAddr()))
	return req.Server.relayTCPConn(req.Conn, req.Prefix, req.DestIP, req.DestPort, req.Compiled, req.Audit)
}

type udpPassThroughAdapter struct{}

func (a *udpPassThroughAdapter) Name() string      { return "udp-pass-through" }
func (a *udpPassThroughAdapter) Transport() string { return "udp" }
func (a *udpPassThroughAdapter) Protocol() string  { return "unknown" }
func (a *udpPassThroughAdapter) Capability() adapterCapability {
	return adapterCapabilityPassThrough
}

func (a *udpPassThroughAdapter) Handle(req *adapterRequest) error {
	if req == nil || req.Server == nil || req.UDPConn == nil || req.UDPSource == nil {
		return fmt.Errorf("udp fallback adapter requires source datagram")
	}
	req.Server.recordFlow(req.SrcIP, req.DestIP, req.DestPort, "udp", req.UDPSource.Port)
	return req.Server.forwardUDPDatagram(req.UDPConn, req.UDPSource, req.UDPPayload, req.DestIP, req.DestPort, req.Compiled, req.Audit)
}
