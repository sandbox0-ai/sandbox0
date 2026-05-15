package proxy

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
)

const (
	socks5Version                     = 0x05
	socks5MethodNoAuth                = 0x00
	socks5MethodUsernamePassword      = 0x02
	socks5MethodNoAcceptable          = 0xff
	socks5UserPassAuthVersion         = 0x01
	mqttPacketTypeConnect             = 0x10
	mqttConnectFlagUsername      byte = 0x80
	mqttConnectFlagPassword      byte = 0x40
	mqttConnectFlagWillFlag      byte = 0x04
)

func (s *Server) proxySOCKS5Session(req *adapterRequest) error {
	if s == nil {
		return fmt.Errorf("server is nil")
	}
	if req == nil || req.Conn == nil || req.DestIP == nil || req.DestPort <= 0 {
		return fmt.Errorf("socks5 proxy request is incomplete")
	}
	if req.EgressAuth == nil || req.EgressAuth.ResolvedUsernamePassword == nil {
		return fmt.Errorf("socks5 egress auth material is missing")
	}

	reader := bufio.NewReader(multiReader(req.Prefix, req.Conn))
	upstream, err := s.dialUpstreamTCP(req)
	if err != nil {
		return err
	}
	defer upstream.Close()

	clientMethods, greeting, err := readSOCKS5Greeting(reader)
	if err != nil {
		return fmt.Errorf("read socks5 greeting: %w", err)
	}
	rewrittenGreeting, clientOfferedUserPass := ensureSOCKS5Method(greeting, clientMethods, socks5MethodUsernamePassword)
	clientOfferedNoAuth := socks5MethodsContain(clientMethods, socks5MethodNoAuth)
	if err := writeCounted(req, upstream, rewrittenGreeting, true); err != nil {
		return fmt.Errorf("write socks5 greeting upstream: %w", err)
	}

	methodSelection := make([]byte, 2)
	if _, err := io.ReadFull(upstream, methodSelection); err != nil {
		return fmt.Errorf("read socks5 method selection: %w", err)
	}

	switch methodSelection[1] {
	case socks5MethodUsernamePassword:
		if !clientOfferedUserPass && !clientOfferedNoAuth {
			applyEgressAuthFailurePolicy(req.EgressAuth, "socks5", "client_auth_incompatible")
			if req.EgressAuth.ShouldBypass() {
				if err := writeCounted(req, req.Conn, methodSelection, false); err != nil {
					return fmt.Errorf("write socks5 method selection to client: %w", err)
				}
				return s.pipeWithReader(req.Conn, upstream, reader, req.Compiled, req.Audit)
			}
			return fmt.Errorf("socks5 client does not support an injectable auth method")
		}
		if clientOfferedUserPass {
			if err := writeCounted(req, req.Conn, methodSelection, false); err != nil {
				return fmt.Errorf("write socks5 method selection to client: %w", err)
			}
			if _, err := readSOCKS5UsernamePasswordRequest(reader); err != nil {
				return fmt.Errorf("read downstream socks5 auth request: %w", err)
			}
		}

		authRequest, err := buildSOCKS5UsernamePasswordRequest(req.EgressAuth.ResolvedUsernamePassword)
		if err != nil {
			return err
		}
		if err := writeCounted(req, upstream, authRequest, true); err != nil {
			return fmt.Errorf("write socks5 auth request upstream: %w", err)
		}

		authResponse := make([]byte, 2)
		if _, err := io.ReadFull(upstream, authResponse); err != nil {
			return fmt.Errorf("read socks5 auth response: %w", err)
		}
		if clientOfferedUserPass {
			if err := writeCounted(req, req.Conn, authResponse, false); err != nil {
				return fmt.Errorf("write socks5 auth response to client: %w", err)
			}
		} else {
			selection := []byte{socks5Version, socks5MethodNoAuth}
			if authResponse[1] != 0x00 {
				selection[1] = socks5MethodNoAcceptable
			}
			if err := writeCounted(req, req.Conn, selection, false); err != nil {
				return fmt.Errorf("write socks5 shim method selection to client: %w", err)
			}
		}
		if authResponse[1] != 0x00 {
			return fmt.Errorf("socks5 upstream rejected injected credentials")
		}
	default:
		if err := writeCounted(req, req.Conn, methodSelection, false); err != nil {
			return fmt.Errorf("write socks5 method selection to client: %w", err)
		}
		applyEgressAuthFailurePolicy(req.EgressAuth, "socks5", "upstream_auth_unavailable")
		if req.EgressAuth.ShouldBypass() {
			return s.pipeWithReader(req.Conn, upstream, reader, req.Compiled, req.Audit)
		}
		return fmt.Errorf("socks5 upstream selected unsupported auth method %d", methodSelection[1])
	}

	return s.pipeWithReader(req.Conn, upstream, reader, req.Compiled, req.Audit)
}

func (s *Server) proxyMQTTSession(req *adapterRequest) error {
	if s == nil {
		return fmt.Errorf("server is nil")
	}
	if req == nil || req.Conn == nil || req.DestIP == nil || req.DestPort <= 0 {
		return fmt.Errorf("mqtt proxy request is incomplete")
	}
	if req.EgressAuth == nil || req.EgressAuth.ResolvedUsernamePassword == nil {
		return fmt.Errorf("mqtt egress auth material is missing")
	}

	reader := bufio.NewReader(multiReader(req.Prefix, req.Conn))
	packet, err := readMQTTConnectPacket(reader)
	if err != nil {
		return fmt.Errorf("read mqtt connect packet: %w", err)
	}
	rewritten, err := rewriteMQTTConnectPacket(packet, req.EgressAuth.ResolvedUsernamePassword.Username, req.EgressAuth.ResolvedUsernamePassword.Password)
	if err != nil {
		return fmt.Errorf("rewrite mqtt connect packet: %w", err)
	}

	upstream, err := s.dialUpstreamTCP(req)
	if err != nil {
		return err
	}
	defer upstream.Close()

	if err := writeCounted(req, upstream, rewritten, true); err != nil {
		return fmt.Errorf("write rewritten mqtt connect packet upstream: %w", err)
	}
	return s.pipeWithReader(req.Conn, upstream, reader, req.Compiled, req.Audit)
}

func (s *Server) proxyRedisSession(req *adapterRequest) error {
	if s == nil {
		return fmt.Errorf("server is nil")
	}
	if req == nil || req.Conn == nil || req.DestIP == nil || req.DestPort <= 0 {
		return fmt.Errorf("redis proxy request is incomplete")
	}
	if req.EgressAuth == nil || req.EgressAuth.ResolvedUsernamePassword == nil {
		return fmt.Errorf("redis egress auth material is missing")
	}

	reader := bufio.NewReader(multiReader(req.Prefix, req.Conn))
	firstFrame, err := readRESPFrame(reader)
	if err != nil {
		return fmt.Errorf("read redis first command: %w", err)
	}

	upstream, err := s.dialUpstreamTCP(req)
	if err != nil {
		return err
	}
	defer upstream.Close()
	upstreamReader := bufio.NewReader(upstream)

	authFrames := [][]byte{buildRedisAUTHCommand(req.EgressAuth.ResolvedUsernamePassword.Username, req.EgressAuth.ResolvedUsernamePassword.Password)}
	if strings.EqualFold(req.EgressAuth.ResolvedUsernamePassword.Username, "default") {
		authFrames = append(authFrames, buildRedisAUTHPasswordOnlyCommand(req.EgressAuth.ResolvedUsernamePassword.Password))
	}

	authSucceeded := false
	for _, authFrame := range authFrames {
		if err := writeCounted(req, upstream, authFrame, true); err != nil {
			return fmt.Errorf("write redis auth command upstream: %w", err)
		}
		reply, err := readRESPFrame(upstreamReader)
		if err != nil {
			return fmt.Errorf("read redis auth reply: %w", err)
		}
		if isRedisOKReply(reply) {
			authSucceeded = true
			break
		}
	}
	if !authSucceeded {
		applyEgressAuthFailurePolicy(req.EgressAuth, "redis", "upstream_auth_rejected")
		if !req.EgressAuth.ShouldBypass() {
			return fmt.Errorf("redis upstream rejected injected credentials")
		}
	}

	if authSucceeded && isRedisAUTHCommand(firstFrame) {
		return s.pipeWithReader(req.Conn, upstream, reader, req.Compiled, req.Audit)
	}
	if err := writeCounted(req, upstream, firstFrame, true); err != nil {
		return fmt.Errorf("write redis first command upstream: %w", err)
	}
	return s.pipeWithReader(req.Conn, upstream, reader, req.Compiled, req.Audit)
}

func (s *Server) dialUpstreamTCP(req *adapterRequest) (*countingConn, error) {
	if s == nil || req == nil || req.DestIP == nil || req.DestPort <= 0 {
		return nil, fmt.Errorf("upstream tcp request is incomplete")
	}
	upstream, err := s.dialTCPUpstreamForRequest(req)
	if err != nil {
		return nil, err
	}
	return &countingConn{Conn: upstream}, nil
}

func multiReader(prefix io.Reader, conn net.Conn) io.Reader {
	if prefix == nil {
		return conn
	}
	return io.MultiReader(prefix, conn)
}

func writeCounted(req *adapterRequest, conn net.Conn, payload []byte, upstream bool) error {
	if len(payload) == 0 {
		return nil
	}
	before := int64(0)
	if counter, ok := conn.(*countingConn); ok {
		before = counter.WrittenBytes()
	}
	n, err := conn.Write(payload)
	if upstream {
		if counter, ok := conn.(*countingConn); ok {
			req.Server.recordEgressBytes(req.Compiled, counter.WrittenBytes()-before, req.Audit)
		}
	} else {
		req.Server.recordIngressBytes(req.Compiled, int64(n), req.Audit)
	}
	if err != nil {
		return err
	}
	if n != len(payload) {
		return io.ErrShortWrite
	}
	return nil
}

func readSOCKS5Greeting(reader *bufio.Reader) ([]byte, []byte, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, nil, err
	}
	if header[0] != socks5Version {
		return nil, nil, fmt.Errorf("unexpected socks5 version %d", header[0])
	}
	methods := make([]byte, int(header[1]))
	if _, err := io.ReadFull(reader, methods); err != nil {
		return nil, nil, err
	}
	greeting := append(append([]byte(nil), header...), methods...)
	return methods, greeting, nil
}

func ensureSOCKS5Method(greeting, methods []byte, method byte) ([]byte, bool) {
	if socks5MethodsContain(methods, method) {
		return append([]byte(nil), greeting...), true
	}
	rewritten := append([]byte(nil), greeting...)
	rewritten[1]++
	rewritten = append(rewritten, method)
	return rewritten, false
}

func socks5MethodsContain(methods []byte, method byte) bool {
	for _, candidate := range methods {
		if candidate == method {
			return true
		}
	}
	return false
}

func readSOCKS5UsernamePasswordRequest(reader *bufio.Reader) ([]byte, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, err
	}
	if header[0] != socks5UserPassAuthVersion {
		return nil, fmt.Errorf("unexpected socks5 auth version %d", header[0])
	}
	username := make([]byte, int(header[1]))
	if _, err := io.ReadFull(reader, username); err != nil {
		return nil, err
	}
	passwordLen, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	password := make([]byte, int(passwordLen))
	if _, err := io.ReadFull(reader, password); err != nil {
		return nil, err
	}
	packet := append(append(append([]byte(nil), header...), username...), passwordLen)
	packet = append(packet, password...)
	return packet, nil
}

func buildSOCKS5UsernamePasswordRequest(material *resolvedUsernamePassword) ([]byte, error) {
	if material == nil {
		return nil, fmt.Errorf("socks5 username/password material is required")
	}
	username := []byte(material.Username)
	password := []byte(material.Password)
	if len(username) == 0 || len(username) > 255 {
		return nil, fmt.Errorf("socks5 username length must be between 1 and 255")
	}
	if len(password) == 0 || len(password) > 255 {
		return nil, fmt.Errorf("socks5 password length must be between 1 and 255")
	}
	packet := []byte{socks5UserPassAuthVersion, byte(len(username))}
	packet = append(packet, username...)
	packet = append(packet, byte(len(password)))
	packet = append(packet, password...)
	return packet, nil
}

func readMQTTConnectPacket(reader *bufio.Reader) ([]byte, error) {
	firstByte, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	lengthBytes := make([]byte, 0, 4)
	for i := 0; i < 4; i++ {
		b, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}
		lengthBytes = append(lengthBytes, b)
		if b&0x80 == 0 {
			break
		}
	}
	remainingLength, consumed, ok, needMore := parseMQTTRemainingLength(lengthBytes)
	if !ok || needMore || consumed != len(lengthBytes) {
		return nil, fmt.Errorf("invalid mqtt remaining length")
	}
	body := make([]byte, remainingLength)
	if _, err := io.ReadFull(reader, body); err != nil {
		return nil, err
	}
	packet := append([]byte{firstByte}, lengthBytes...)
	packet = append(packet, body...)
	return packet, nil
}

func rewriteMQTTConnectPacket(packet []byte, username, password string) ([]byte, error) {
	if len(packet) < 2 || packet[0] != mqttPacketTypeConnect {
		return nil, fmt.Errorf("mqtt packet is not CONNECT")
	}
	remainingLength, consumed, ok, needMore := parseMQTTRemainingLength(packet[1:])
	if !ok || needMore {
		return nil, fmt.Errorf("mqtt remaining length is invalid")
	}
	offset := 1 + consumed
	if len(packet[offset:]) != remainingLength {
		return nil, fmt.Errorf("mqtt packet length does not match remaining length")
	}
	body := packet[offset:]

	protocolName, rest, ok := readMQTTLengthPrefixed(body)
	if !ok || len(rest) < 4 {
		return nil, fmt.Errorf("mqtt connect variable header is incomplete")
	}
	protocolLevel := rest[0]
	connectFlags := rest[1]
	keepAlive := append([]byte(nil), rest[2:4]...)
	rest = rest[4:]

	propertiesField := []byte(nil)
	if protocolLevel == 5 {
		propLength, propConsumed, ok, needMore := parseMQTTRemainingLength(rest)
		if !ok || needMore || len(rest) < propConsumed+propLength {
			return nil, fmt.Errorf("mqtt connect properties are invalid")
		}
		propertiesField = append([]byte(nil), rest[:propConsumed+propLength]...)
		rest = rest[propConsumed+propLength:]
	}

	clientID, rest, ok := readMQTTLengthPrefixed(rest)
	if !ok {
		return nil, fmt.Errorf("mqtt client id is invalid")
	}

	willFields := []byte(nil)
	if connectFlags&mqttConnectFlagWillFlag != 0 {
		willStart := rest
		if protocolLevel == 5 {
			willLength, willConsumed, ok, needMore := parseMQTTRemainingLength(rest)
			if !ok || needMore || len(rest) < willConsumed+willLength {
				return nil, fmt.Errorf("mqtt will properties are invalid")
			}
			rest = rest[willConsumed+willLength:]
		}
		var okTopic bool
		_, rest, okTopic = readMQTTLengthPrefixed(rest)
		if !okTopic {
			return nil, fmt.Errorf("mqtt will topic is invalid")
		}
		var okPayload bool
		_, rest, okPayload = readMQTTLengthPrefixed(rest)
		if !okPayload {
			return nil, fmt.Errorf("mqtt will payload is invalid")
		}
		willFields = append([]byte(nil), willStart[:len(willStart)-len(rest)]...)
	}

	if connectFlags&mqttConnectFlagUsername != 0 {
		if _, rest, ok = readMQTTLengthPrefixed(rest); !ok {
			return nil, fmt.Errorf("mqtt username field is invalid")
		}
	}
	if connectFlags&mqttConnectFlagPassword != 0 {
		if _, rest, ok = readMQTTLengthPrefixed(rest); !ok {
			return nil, fmt.Errorf("mqtt password field is invalid")
		}
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("mqtt connect payload has trailing bytes")
	}

	rewrittenBody := make([]byte, 0, len(body)+len(username)+len(password)+8)
	var err error
	if rewrittenBody, err = appendMQTTLengthPrefixed(rewrittenBody, protocolName); err != nil {
		return nil, err
	}
	rewrittenBody = append(rewrittenBody, protocolLevel, connectFlags|mqttConnectFlagUsername|mqttConnectFlagPassword)
	rewrittenBody = append(rewrittenBody, keepAlive...)
	rewrittenBody = append(rewrittenBody, propertiesField...)
	if rewrittenBody, err = appendMQTTLengthPrefixed(rewrittenBody, clientID); err != nil {
		return nil, err
	}
	rewrittenBody = append(rewrittenBody, willFields...)
	if rewrittenBody, err = appendMQTTLengthPrefixed(rewrittenBody, []byte(username)); err != nil {
		return nil, err
	}
	if rewrittenBody, err = appendMQTTLengthPrefixed(rewrittenBody, []byte(password)); err != nil {
		return nil, err
	}

	encodedRemainingLength, err := encodeMQTTRemainingLength(len(rewrittenBody))
	if err != nil {
		return nil, err
	}
	rewritten := append([]byte{mqttPacketTypeConnect}, encodedRemainingLength...)
	rewritten = append(rewritten, rewrittenBody...)
	return rewritten, nil
}

func readMQTTLengthPrefixed(data []byte) ([]byte, []byte, bool) {
	if len(data) < 2 {
		return nil, nil, false
	}
	length := int(data[0])<<8 | int(data[1])
	if len(data) < 2+length {
		return nil, nil, false
	}
	return append([]byte(nil), data[2:2+length]...), data[2+length:], true
}

func appendMQTTLengthPrefixed(dst []byte, value []byte) ([]byte, error) {
	if len(value) > 65535 {
		return nil, fmt.Errorf("mqtt field length exceeds 65535 bytes")
	}
	dst = append(dst, byte(len(value)>>8), byte(len(value)))
	dst = append(dst, value...)
	return dst, nil
}

func encodeMQTTRemainingLength(value int) ([]byte, error) {
	if value < 0 || value > 268435455 {
		return nil, fmt.Errorf("mqtt remaining length %d is out of range", value)
	}
	if value == 0 {
		return []byte{0}, nil
	}
	out := make([]byte, 0, 4)
	for value > 0 {
		encoded := byte(value % 128)
		value /= 128
		if value > 0 {
			encoded |= 0x80
		}
		out = append(out, encoded)
	}
	return out, nil
}

func readRESPFrame(reader *bufio.Reader) ([]byte, error) {
	if reader == nil {
		return nil, fmt.Errorf("resp reader is nil")
	}
	var frame bytes.Buffer
	if err := appendRESPFrame(&frame, reader); err != nil {
		return nil, err
	}
	return frame.Bytes(), nil
}

func appendRESPFrame(frame *bytes.Buffer, reader *bufio.Reader) error {
	prefix, err := reader.ReadByte()
	if err != nil {
		return err
	}
	if err := frame.WriteByte(prefix); err != nil {
		return err
	}

	switch prefix {
	case '+', '-', ':':
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return err
		}
		_, err = frame.Write(line)
		return err
	case '$':
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return err
		}
		if _, err := frame.Write(line); err != nil {
			return err
		}
		length, err := parseRESPIntegerLine(line)
		if err != nil {
			return err
		}
		if length < 0 {
			return nil
		}
		payload := make([]byte, length+2)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return err
		}
		_, err = frame.Write(payload)
		return err
	case '*':
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return err
		}
		if _, err := frame.Write(line); err != nil {
			return err
		}
		count, err := parseRESPIntegerLine(line)
		if err != nil {
			return err
		}
		if count < 0 {
			return nil
		}
		for i := 0; i < count; i++ {
			if err := appendRESPFrame(frame, reader); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported redis resp type %q", prefix)
	}
}

func parseRESPIntegerLine(line []byte) (int, error) {
	value := strings.TrimSuffix(string(line), "\r\n")
	return strconv.Atoi(value)
}

func parseRESPArrayStrings(frame []byte) ([]string, error) {
	reader := bufio.NewReader(bytes.NewReader(frame))
	prefix, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	if prefix != '*' {
		return nil, fmt.Errorf("redis command is not an array")
	}
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	count, err := parseRESPIntegerLine(line)
	if err != nil {
		return nil, err
	}
	values := make([]string, 0, count)
	for i := 0; i < count; i++ {
		itemPrefix, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}
		switch itemPrefix {
		case '$':
			lengthLine, err := reader.ReadBytes('\n')
			if err != nil {
				return nil, err
			}
			length, err := parseRESPIntegerLine(lengthLine)
			if err != nil {
				return nil, err
			}
			if length < 0 {
				values = append(values, "")
				continue
			}
			payload := make([]byte, length+2)
			if _, err := io.ReadFull(reader, payload); err != nil {
				return nil, err
			}
			values = append(values, string(payload[:length]))
		case '+':
			simpleLine, err := reader.ReadBytes('\n')
			if err != nil {
				return nil, err
			}
			values = append(values, strings.TrimSuffix(string(simpleLine), "\r\n"))
		default:
			return nil, fmt.Errorf("unsupported redis array item type %q", itemPrefix)
		}
	}
	return values, nil
}

func buildRedisAUTHCommand(username, password string) []byte {
	return buildRESPArray([]string{"AUTH", username, password})
}

func buildRedisAUTHPasswordOnlyCommand(password string) []byte {
	return buildRESPArray([]string{"AUTH", password})
}

func buildRESPArray(values []string) []byte {
	var frame bytes.Buffer
	frame.WriteString(fmt.Sprintf("*%d\r\n", len(values)))
	for _, value := range values {
		frame.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value))
	}
	return frame.Bytes()
}

func isRedisAUTHCommand(frame []byte) bool {
	values, err := parseRESPArrayStrings(frame)
	if err != nil || len(values) == 0 {
		return false
	}
	return strings.EqualFold(values[0], "AUTH")
}

func isRedisOKReply(frame []byte) bool {
	return bytes.Equal(frame, []byte("+OK\r\n"))
}
