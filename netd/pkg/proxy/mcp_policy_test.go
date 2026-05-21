package proxy

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestHTTPAdapterDeniesBlockedMCPToolCall(t *testing.T) {
	upstreamHit := make(chan struct{}, 1)
	upstream := httptestServer(t, func(w http.ResponseWriter, r *http.Request) {
		upstreamHit <- struct{}{}
		w.WriteHeader(http.StatusTeapot)
	})
	defer upstream.Close()

	addr := upstream.Listener.Addr().(*net.TCPAddr)
	proxyListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen proxy: %v", err)
	}
	defer proxyListener.Close()

	compiled := compileMCPProxyTestPolicy(t)
	server := &Server{
		cfg:    &config.NetdConfig{ProxyUpstreamTimeout: metav1.Duration{Duration: 2 * time.Second}},
		logger: zap.NewNop(),
	}

	done := make(chan error, 1)
	go func() {
		conn, acceptErr := proxyListener.Accept()
		if acceptErr != nil {
			done <- acceptErr
			return
		}
		defer conn.Close()
		rawBody := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"run_command","arguments":{"cmd":"id"}}}`
		rawReq := "POST /mcp HTTP/1.1\r\nHost: mcp.example.com\r\nContent-Length: " + intString(len(rawBody)) + "\r\n\r\n" + rawBody
		req := &adapterRequest{
			Server:   server,
			Compiled: compiled,
			SrcIP:    "10.0.0.2",
			DestIP:   addr.IP,
			DestPort: addr.Port,
			Host:     "mcp.example.com",
			Conn:     conn,
			Prefix:   bytes.NewReader([]byte(rawReq)),
		}
		done <- (&httpAdapter{}).Handle(req)
	}()

	clientConn, err := net.Dial("tcp4", proxyListener.Addr().String())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer clientConn.Close()
	resp, err := http.ReadResponse(bufio.NewReader(clientConn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	var rpcResp map[string]any
	if err := json.Unmarshal(body, &rpcResp); err != nil {
		t.Fatalf("decode json-rpc response: %v", err)
	}
	if rpcResp["error"] == nil {
		t.Fatalf("expected json-rpc error response, got %s", body)
	}
	if err := <-done; !errors.Is(err, errProtocolPolicyDenied) {
		t.Fatalf("adapter error = %v, want protocol policy denied", err)
	}
	select {
	case <-upstreamHit:
		t.Fatal("blocked MCP tool call reached upstream")
	default:
	}
}

func TestHTTPAdapterAllowsMCPToolCall(t *testing.T) {
	requestBody := make(chan string, 1)
	upstream := httptestServer(t, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		requestBody <- string(body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"content":[]}}`))
	})
	defer upstream.Close()

	addr := upstream.Listener.Addr().(*net.TCPAddr)
	proxyListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen proxy: %v", err)
	}
	defer proxyListener.Close()

	compiled := compileMCPProxyTestPolicy(t)
	server := &Server{
		cfg:    &config.NetdConfig{ProxyUpstreamTimeout: metav1.Duration{Duration: 2 * time.Second}},
		logger: zap.NewNop(),
	}

	done := make(chan error, 1)
	go func() {
		conn, acceptErr := proxyListener.Accept()
		if acceptErr != nil {
			done <- acceptErr
			return
		}
		defer conn.Close()
		rawBody := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"read_file","arguments":{"path":"/tmp/a"}}}`
		rawReq := "POST /mcp HTTP/1.1\r\nHost: mcp.example.com\r\nContent-Length: " + intString(len(rawBody)) + "\r\n\r\n" + rawBody
		req := &adapterRequest{
			Server:   server,
			Compiled: compiled,
			SrcIP:    "10.0.0.2",
			DestIP:   addr.IP,
			DestPort: addr.Port,
			Host:     "mcp.example.com",
			Conn:     conn,
			Prefix:   bytes.NewReader([]byte(rawReq)),
		}
		done <- (&httpAdapter{}).Handle(req)
	}()

	clientConn, err := net.Dial("tcp4", proxyListener.Addr().String())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer clientConn.Close()
	resp, err := http.ReadResponse(bufio.NewReader(clientConn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err := <-done; err != nil {
		t.Fatalf("adapter handle: %v", err)
	}
	if got := <-requestBody; !bytes.Contains([]byte(got), []byte(`"read_file"`)) {
		t.Fatalf("upstream body = %q", got)
	}
}

func TestMCPPolicyDeniesUnknownLengthBody(t *testing.T) {
	compiled := compileMCPProxyTestPolicy(t)
	server := &Server{logger: zap.NewNop()}
	rawBody := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"run_command"}}`
	httpReq, err := http.NewRequest(http.MethodPost, "http://mcp.example.com/mcp", bytes.NewReader([]byte(rawBody)))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	httpReq.ContentLength = -1
	httpReq.TransferEncoding = []string{"chunked"}
	req := &adapterRequest{
		Server:   server,
		Compiled: compiled,
		Host:     "mcp.example.com",
		DestPort: 80,
	}
	var status int
	var body []byte
	err = server.enforceMCPPolicyForHTTPRequest(req, httpReq, func(gotStatus int, gotBody []byte) error {
		status = gotStatus
		body = append([]byte(nil), gotBody...)
		return nil
	})
	if !errors.Is(err, errProtocolPolicyDenied) {
		t.Fatalf("error = %v, want protocol policy denied", err)
	}
	if status != http.StatusOK {
		t.Fatalf("status = %d, want %d", status, http.StatusOK)
	}
	if !bytes.Contains(body, []byte("unsupported_streaming_body")) {
		t.Fatalf("response body = %s, want unsupported_streaming_body", body)
	}
}

func compileMCPProxyTestPolicy(t *testing.T) *policy.CompiledPolicy {
	t.Helper()
	compiled, err := policy.CompileNetworkPolicy(&v1alpha1.NetworkPolicySpec{
		Egress: &v1alpha1.NetworkEgressPolicy{
			ProtocolRules: []v1alpha1.ProtocolRule{{
				Name:     "docs-mcp",
				Protocol: v1alpha1.ProtocolRuleProtocolMCP,
				Domains:  []string{"mcp.example.com"},
				HTTPMatch: &v1alpha1.HTTPMatch{
					Methods: []string{http.MethodPost},
					Paths:   []string{"/mcp"},
				},
				MCP: &v1alpha1.MCPProtocolRule{
					Tools: &v1alpha1.MCPToolPolicy{
						Allowed: []string{"read_file"},
						Denied:  []string{"run_command"},
					},
				},
			}},
		},
	})
	if err != nil {
		t.Fatalf("compile policy: %v", err)
	}
	return compiled
}

func intString(value int) string {
	return strconv.Itoa(value)
}
