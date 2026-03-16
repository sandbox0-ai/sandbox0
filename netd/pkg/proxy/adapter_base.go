package proxy

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"

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
	Server      *Server
	Compiled    *policy.CompiledPolicy
	SrcIP       string
	DestIP      net.IP
	DestPort    int
	Host        string
	Conn        net.Conn
	Prefix      io.Reader
	HTTPRequest *http.Request
	HTTPReader  *bufio.Reader
	UDPSource   *net.UDPAddr
	UDPPayload  []byte
}

type httpAdapter struct{}

func (a *httpAdapter) Name() string      { return "http" }
func (a *httpAdapter) Transport() string { return "tcp" }
func (a *httpAdapter) Protocol() string  { return "http" }
func (a *httpAdapter) Capability() adapterCapability {
	return adapterCapabilityInspect
}

func (a *httpAdapter) Handle(req *adapterRequest) error {
	if req == nil || req.Server == nil || req.Conn == nil || req.HTTPRequest == nil || req.HTTPReader == nil {
		return fmt.Errorf("http adapter requires request, reader, and connection")
	}
	req.Server.recordFlow(req.SrcIP, req.DestIP, req.DestPort, "tcp", remotePort(req.Conn.RemoteAddr()))

	upstream, err := net.DialTimeout("tcp", net.JoinHostPort(req.DestIP.String(), fmt.Sprintf("%d", req.DestPort)), req.Server.cfg.ProxyUpstreamTimeout.Duration)
	if err != nil {
		return err
	}
	upstream = &countingConn{Conn: upstream}
	defer upstream.Close()

	if err := req.HTTPRequest.Write(upstream); err != nil {
		return err
	}
	if counter, ok := upstream.(*countingConn); ok {
		req.Server.recordEgressBytes(req.Compiled, counter.written)
		counter.written = 0
	}
	req.Server.pipe(req.Conn, upstream, req.HTTPReader, req.Compiled)
	return nil
}

type tlsAdapter struct{}

func (a *tlsAdapter) Name() string      { return "tls" }
func (a *tlsAdapter) Transport() string { return "tcp" }
func (a *tlsAdapter) Protocol() string  { return "tls" }
func (a *tlsAdapter) Capability() adapterCapability {
	return adapterCapabilityPassThrough
}

func (a *tlsAdapter) Handle(req *adapterRequest) error {
	if req == nil || req.Server == nil || req.Conn == nil {
		return fmt.Errorf("tls adapter requires connection")
	}
	req.Server.recordFlow(req.SrcIP, req.DestIP, req.DestPort, "tcp", remotePort(req.Conn.RemoteAddr()))
	return req.Server.relayTCPConn(req.Conn, req.Prefix, req.DestIP, req.DestPort, req.Compiled)
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
	return req.Server.relayTCPConn(req.Conn, req.Prefix, req.DestIP, req.DestPort, req.Compiled)
}

type udpAdapter struct{}

func (a *udpAdapter) Name() string      { return "udp" }
func (a *udpAdapter) Transport() string { return "udp" }
func (a *udpAdapter) Protocol() string  { return "udp" }
func (a *udpAdapter) Capability() adapterCapability {
	return adapterCapabilityPassThrough
}

func (a *udpAdapter) Handle(req *adapterRequest) error {
	if req == nil || req.Server == nil || req.UDPSource == nil {
		return fmt.Errorf("udp adapter requires source datagram")
	}
	req.Server.recordFlow(req.SrcIP, req.DestIP, req.DestPort, "udp", req.UDPSource.Port)
	return req.Server.forwardUDPDatagram(req.UDPSource, req.UDPPayload, req.DestIP, req.DestPort, req.Compiled)
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
	return req.Server.relayTCPConn(req.Conn, req.Prefix, req.DestIP, req.DestPort, req.Compiled)
}

type udpPassThroughAdapter struct{}

func (a *udpPassThroughAdapter) Name() string      { return "udp-pass-through" }
func (a *udpPassThroughAdapter) Transport() string { return "udp" }
func (a *udpPassThroughAdapter) Protocol() string  { return "unknown" }
func (a *udpPassThroughAdapter) Capability() adapterCapability {
	return adapterCapabilityPassThrough
}

func (a *udpPassThroughAdapter) Handle(req *adapterRequest) error {
	if req == nil || req.Server == nil || req.UDPSource == nil {
		return fmt.Errorf("udp fallback adapter requires source datagram")
	}
	req.Server.recordFlow(req.SrcIP, req.DestIP, req.DestPort, "udp", req.UDPSource.Port)
	return req.Server.forwardUDPDatagram(req.UDPSource, req.UDPPayload, req.DestIP, req.DestPort, req.Compiled)
}
