package proxy

import (
	"bufio"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRewriteMQTTConnectPacketInjectsUsernamePassword(t *testing.T) {
	rewritten, err := rewriteMQTTConnectPacket(buildMQTTConnectPacket(), "alice", "secret")
	if err != nil {
		t.Fatalf("rewrite mqtt connect packet: %v", err)
	}

	username, password, err := parseMQTTCredentials(rewritten)
	if err != nil {
		t.Fatalf("parse mqtt credentials: %v", err)
	}
	if username != "alice" {
		t.Fatalf("username = %q, want alice", username)
	}
	if password != "secret" {
		t.Fatalf("password = %q, want secret", password)
	}
}

func TestProxySOCKS5SessionInjectsUsernamePassword(t *testing.T) {
	upstreamListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen upstream: %v", err)
	}
	defer upstreamListener.Close()

	upstreamDone := make(chan error, 1)
	go func() {
		conn, err := upstreamListener.Accept()
		if err != nil {
			upstreamDone <- err
			return
		}
		defer conn.Close()

		reader := bufio.NewReader(conn)
		methods, _, err := readSOCKS5Greeting(reader)
		if err != nil {
			upstreamDone <- err
			return
		}
		if !socks5MethodsContain(methods, socks5MethodUsernamePassword) {
			upstreamDone <- io.ErrUnexpectedEOF
			return
		}
		if _, err := conn.Write([]byte{socks5Version, socks5MethodUsernamePassword}); err != nil {
			upstreamDone <- err
			return
		}
		authPacket, err := readSOCKS5UsernamePasswordRequest(reader)
		if err != nil {
			upstreamDone <- err
			return
		}
		username, password, err := parseSOCKS5UsernamePasswordRequest(authPacket)
		if err != nil {
			upstreamDone <- err
			return
		}
		if username != "alice" || password != "secret" {
			upstreamDone <- io.ErrUnexpectedEOF
			return
		}
		if _, err := conn.Write([]byte{socks5UserPassAuthVersion, 0x00}); err != nil {
			upstreamDone <- err
			return
		}
		payload := make([]byte, 4)
		if _, err := io.ReadFull(reader, payload); err != nil {
			upstreamDone <- err
			return
		}
		if string(payload) != "PING" {
			upstreamDone <- io.ErrUnexpectedEOF
			return
		}
		_, err = conn.Write([]byte("PONG"))
		upstreamDone <- err
	}()

	clientConn, proxyConn := net.Pipe()
	defer proxyConn.Close()

	server := &Server{
		cfg: &config.NetdConfig{
			ProxyUpstreamTimeout: metav1.Duration{Duration: time.Second},
		},
	}

	proxyDone := make(chan error, 1)
	go func() {
		proxyDone <- server.proxySOCKS5Session(&adapterRequest{
			Server:   server,
			Conn:     proxyConn,
			DestIP:   net.ParseIP("127.0.0.1"),
			DestPort: upstreamListener.Addr().(*net.TCPAddr).Port,
			Compiled: &policy.CompiledPolicy{},
			EgressAuth: &egressAuthContext{
				ResolvedUsernamePassword: &resolvedUsernamePassword{
					Username: "alice",
					Password: "secret",
				},
			},
		})
	}()

	if _, err := clientConn.Write([]byte{socks5Version, 0x02, socks5MethodNoAuth, socks5MethodUsernamePassword}); err != nil {
		t.Fatalf("write greeting: %v", err)
	}
	methodSelection := make([]byte, 2)
	if _, err := io.ReadFull(clientConn, methodSelection); err != nil {
		t.Fatalf("read method selection: %v", err)
	}
	if string(methodSelection) != string([]byte{socks5Version, socks5MethodUsernamePassword}) {
		t.Fatalf("method selection = %v", methodSelection)
	}
	if _, err := clientConn.Write([]byte{socks5UserPassAuthVersion, 0x03, 'b', 'a', 'd', 0x03, 'o', 'l', 'd'}); err != nil {
		t.Fatalf("write auth packet: %v", err)
	}
	authResponse := make([]byte, 2)
	if _, err := io.ReadFull(clientConn, authResponse); err != nil {
		t.Fatalf("read auth response: %v", err)
	}
	if string(authResponse) != string([]byte{socks5UserPassAuthVersion, 0x00}) {
		t.Fatalf("auth response = %v", authResponse)
	}
	if _, err := clientConn.Write([]byte("PING")); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	reply := make([]byte, 4)
	if _, err := io.ReadFull(clientConn, reply); err != nil {
		t.Fatalf("read reply: %v", err)
	}
	if string(reply) != "PONG" {
		t.Fatalf("reply = %q, want PONG", reply)
	}
	if err := clientConn.Close(); err != nil {
		t.Fatalf("close client conn: %v", err)
	}

	if err := <-proxyDone; err != nil {
		t.Fatalf("proxy socks5 session: %v", err)
	}
	if err := <-upstreamDone; err != nil {
		t.Fatalf("upstream socks5 session: %v", err)
	}
}

func TestProxyMQTTSessionInjectsUsernamePassword(t *testing.T) {
	upstreamListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen upstream: %v", err)
	}
	defer upstreamListener.Close()

	upstreamDone := make(chan error, 1)
	go func() {
		conn, err := upstreamListener.Accept()
		if err != nil {
			upstreamDone <- err
			return
		}
		defer conn.Close()

		reader := bufio.NewReader(conn)
		packet, err := readMQTTConnectPacket(reader)
		if err != nil {
			upstreamDone <- err
			return
		}
		username, password, err := parseMQTTCredentials(packet)
		if err != nil {
			upstreamDone <- err
			return
		}
		if username != "alice" || password != "secret" {
			upstreamDone <- io.ErrUnexpectedEOF
			return
		}
		payload := make([]byte, 4)
		if _, err := io.ReadFull(reader, payload); err != nil {
			upstreamDone <- err
			return
		}
		if string(payload) != "PING" {
			upstreamDone <- io.ErrUnexpectedEOF
			return
		}
		_, err = conn.Write([]byte("PONG"))
		upstreamDone <- err
	}()

	clientConn, proxyConn := net.Pipe()
	defer proxyConn.Close()

	server := &Server{
		cfg: &config.NetdConfig{
			ProxyUpstreamTimeout: metav1.Duration{Duration: time.Second},
		},
	}

	proxyDone := make(chan error, 1)
	go func() {
		proxyDone <- server.proxyMQTTSession(&adapterRequest{
			Server:   server,
			Conn:     proxyConn,
			DestIP:   net.ParseIP("127.0.0.1"),
			DestPort: upstreamListener.Addr().(*net.TCPAddr).Port,
			Compiled: &policy.CompiledPolicy{},
			EgressAuth: &egressAuthContext{
				ResolvedUsernamePassword: &resolvedUsernamePassword{
					Username: "alice",
					Password: "secret",
				},
			},
		})
	}()

	if _, err := clientConn.Write(buildMQTTConnectPacket()); err != nil {
		t.Fatalf("write connect packet: %v", err)
	}
	if _, err := clientConn.Write([]byte("PING")); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	reply := make([]byte, 4)
	if _, err := io.ReadFull(clientConn, reply); err != nil {
		t.Fatalf("read reply: %v", err)
	}
	if string(reply) != "PONG" {
		t.Fatalf("reply = %q, want PONG", reply)
	}
	if err := clientConn.Close(); err != nil {
		t.Fatalf("close client conn: %v", err)
	}

	if err := <-proxyDone; err != nil {
		t.Fatalf("proxy mqtt session: %v", err)
	}
	if err := <-upstreamDone; err != nil {
		t.Fatalf("upstream mqtt session: %v", err)
	}
}

func TestProxyRedisSessionInjectsUsernamePassword(t *testing.T) {
	upstreamListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen upstream: %v", err)
	}
	defer upstreamListener.Close()

	upstreamDone := make(chan error, 1)
	go func() {
		conn, err := upstreamListener.Accept()
		if err != nil {
			upstreamDone <- err
			return
		}
		defer conn.Close()

		reader := bufio.NewReader(conn)
		authFrame, err := readRESPFrame(reader)
		if err != nil {
			upstreamDone <- err
			return
		}
		values, err := parseRESPArrayStrings(authFrame)
		if err != nil {
			upstreamDone <- err
			return
		}
		if len(values) != 3 || values[0] != "AUTH" || values[1] != "alice" || values[2] != "secret" {
			upstreamDone <- io.ErrUnexpectedEOF
			return
		}
		if _, err := conn.Write([]byte("+OK\r\n")); err != nil {
			upstreamDone <- err
			return
		}

		firstCommand, err := readRESPFrame(reader)
		if err != nil {
			upstreamDone <- err
			return
		}
		firstValues, err := parseRESPArrayStrings(firstCommand)
		if err != nil {
			upstreamDone <- err
			return
		}
		if len(firstValues) != 1 || firstValues[0] != "PING" {
			upstreamDone <- io.ErrUnexpectedEOF
			return
		}
		if _, err := conn.Write([]byte("+PONG\r\n")); err != nil {
			upstreamDone <- err
			return
		}
		upstreamDone <- nil
	}()

	clientConn, proxyConn := net.Pipe()
	defer proxyConn.Close()

	server := &Server{
		cfg: &config.NetdConfig{
			ProxyUpstreamTimeout: metav1.Duration{Duration: time.Second},
		},
	}

	proxyDone := make(chan error, 1)
	go func() {
		proxyDone <- server.proxyRedisSession(&adapterRequest{
			Server:   server,
			Conn:     proxyConn,
			DestIP:   net.ParseIP("127.0.0.1"),
			DestPort: upstreamListener.Addr().(*net.TCPAddr).Port,
			Compiled: &policy.CompiledPolicy{},
			EgressAuth: &egressAuthContext{
				ResolvedUsernamePassword: &resolvedUsernamePassword{
					Username: "alice",
					Password: "secret",
				},
			},
		})
	}()

	if _, err := clientConn.Write(buildRESPArray([]string{"PING"})); err != nil {
		t.Fatalf("write redis command: %v", err)
	}
	reply := make([]byte, len("+PONG\r\n"))
	if _, err := io.ReadFull(clientConn, reply); err != nil {
		t.Fatalf("read redis reply: %v", err)
	}
	if string(reply) != "+PONG\r\n" {
		t.Fatalf("reply = %q, want +PONG\\r\\n", reply)
	}
	if err := clientConn.Close(); err != nil {
		t.Fatalf("close client conn: %v", err)
	}

	if err := <-proxyDone; err != nil {
		t.Fatalf("proxy redis session: %v", err)
	}
	if err := <-upstreamDone; err != nil {
		t.Fatalf("upstream redis session: %v", err)
	}
}

func parseSOCKS5UsernamePasswordRequest(packet []byte) (string, string, error) {
	if len(packet) < 3 || packet[0] != socks5UserPassAuthVersion {
		return "", "", io.ErrUnexpectedEOF
	}
	usernameLen := int(packet[1])
	if len(packet) < 2+usernameLen+1 {
		return "", "", io.ErrUnexpectedEOF
	}
	username := string(packet[2 : 2+usernameLen])
	passwordLenOffset := 2 + usernameLen
	passwordLen := int(packet[passwordLenOffset])
	if len(packet) != passwordLenOffset+1+passwordLen {
		return "", "", io.ErrUnexpectedEOF
	}
	password := string(packet[passwordLenOffset+1:])
	return username, password, nil
}

func parseMQTTCredentials(packet []byte) (string, string, error) {
	remainingLength, consumed, ok, needMore := parseMQTTRemainingLength(packet[1:])
	if !ok || needMore {
		return "", "", io.ErrUnexpectedEOF
	}
	body := packet[1+consumed:]
	if len(body) != remainingLength {
		return "", "", io.ErrUnexpectedEOF
	}
	_, rest, ok := readMQTTLengthPrefixed(body)
	if !ok || len(rest) < 4 {
		return "", "", io.ErrUnexpectedEOF
	}
	protocolLevel := rest[0]
	connectFlags := rest[1]
	rest = rest[4:]
	if protocolLevel == 5 {
		propertiesLength, propertiesConsumed, ok, needMore := parseMQTTRemainingLength(rest)
		if !ok || needMore || len(rest) < propertiesConsumed+propertiesLength {
			return "", "", io.ErrUnexpectedEOF
		}
		rest = rest[propertiesConsumed+propertiesLength:]
	}
	_, rest, ok = readMQTTLengthPrefixed(rest)
	if !ok {
		return "", "", io.ErrUnexpectedEOF
	}
	if connectFlags&mqttConnectFlagWillFlag != 0 {
		if protocolLevel == 5 {
			willLength, willConsumed, ok, needMore := parseMQTTRemainingLength(rest)
			if !ok || needMore || len(rest) < willConsumed+willLength {
				return "", "", io.ErrUnexpectedEOF
			}
			rest = rest[willConsumed+willLength:]
		}
		if _, rest, ok = readMQTTLengthPrefixed(rest); !ok {
			return "", "", io.ErrUnexpectedEOF
		}
		if _, rest, ok = readMQTTLengthPrefixed(rest); !ok {
			return "", "", io.ErrUnexpectedEOF
		}
	}
	usernameBytes, rest, ok := readMQTTLengthPrefixed(rest)
	if !ok {
		return "", "", io.ErrUnexpectedEOF
	}
	passwordBytes, rest, ok := readMQTTLengthPrefixed(rest)
	if !ok || len(rest) != 0 {
		return "", "", io.ErrUnexpectedEOF
	}
	return string(usernameBytes), string(passwordBytes), nil
}
