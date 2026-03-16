package proxy

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
)

const (
	defaultTCPHeaderLimit          = 64 * 1024
	defaultTCPFirstByteTimeout     = 500 * time.Millisecond
	defaultTCPClassificationWindow = 5 * time.Second
)

type classificationResult struct {
	Classification trafficClassification
	Host           string
	Error          error
	Apply          func(*adapterRequest)
}

type tcpClassifierDecision uint8

const (
	tcpClassifierNoMatch tcpClassifierDecision = iota
	tcpClassifierNeedMore
	tcpClassifierMatched
)

type tcpClassifier interface {
	Name() string
	Classify(*tcpClassifyContext) (*classificationResult, tcpClassifierDecision)
}

type udpClassifier interface {
	Name() string
	Classify(*udpClassifyContext) (*classificationResult, bool)
}

type tcpClassifyContext struct {
	Compiled         *policy.CompiledPolicy
	SrcIP            string
	OrigIP           net.IP
	OrigPort         int
	Conn             net.Conn
	HeaderLimit      int
	FirstByteTimeout time.Duration
	ReadTimeout      time.Duration

	Peeked       []byte
	ReadErr      error
	ReadDone     bool
	LimitReached bool
	TimedOut     bool
}

type udpClassifyContext struct {
	Compiled    *policy.CompiledPolicy
	SrcIP       string
	SrcAddr     *net.UDPAddr
	DestIP      net.IP
	DestPort    int
	Payload     []byte
	Reassembler *quicReassembler
}

type httpRequestClassifier struct{}

func (c *httpRequestClassifier) Name() string { return "http-request" }

func (c *httpRequestClassifier) Classify(ctx *tcpClassifyContext) (*classificationResult, tcpClassifierDecision) {
	if ctx == nil || ctx.Conn == nil {
		return nil, tcpClassifierNoMatch
	}
	req, decision := parseHTTPRequest(ctx.Buffered(), ctx.IsComplete(), ctx.headerLimit())
	if decision != tcpClassifierMatched {
		return nil, decision
	}
	host := normalizeHost(req.Host)
	raw := append([]byte(nil), ctx.Buffered()...)
	if host == "" {
		return &classificationResult{
			Classification: classifyUnknownTraffic("tcp", "http", ctx.OrigIP, ctx.OrigPort, "missing_host"),
			Apply: func(req *adapterRequest) {
				req.Prefix = bytes.NewReader(raw)
			},
		}, tcpClassifierMatched
	}
	return &classificationResult{
		Classification: classifyKnownTraffic("tcp", "http", ctx.OrigIP, ctx.OrigPort, host),
		Host:           host,
		Apply: func(req *adapterRequest) {
			req.Prefix = bytes.NewReader(raw)
		},
	}, tcpClassifierMatched
}

type sshBannerClassifier struct{}

func (c *sshBannerClassifier) Name() string { return "ssh-banner" }

func (c *sshBannerClassifier) Classify(ctx *tcpClassifyContext) (*classificationResult, tcpClassifierDecision) {
	if ctx == nil || ctx.Conn == nil {
		return nil, tcpClassifierNoMatch
	}
	banner, decision := parseSSHBannerClassification(ctx.Buffered(), ctx.IsComplete(), ctx.headerLimit())
	if decision != tcpClassifierMatched || banner == "" {
		return nil, decision
	}
	raw := append([]byte(nil), ctx.Buffered()...)
	return &classificationResult{
		Classification: classifyKnownTraffic("tcp", "ssh", ctx.OrigIP, ctx.OrigPort, ""),
		Apply: func(req *adapterRequest) {
			req.Prefix = bytes.NewReader(raw)
		},
	}, tcpClassifierMatched
}

type tlsClientHelloClassifier struct{}

func (c *tlsClientHelloClassifier) Name() string { return "tls-clienthello" }

func (c *tlsClientHelloClassifier) Classify(ctx *tcpClassifyContext) (*classificationResult, tcpClassifierDecision) {
	if ctx == nil || ctx.Conn == nil {
		return nil, tcpClassifierNoMatch
	}
	serverName, decision := parseSNIFromTLS(ctx.Buffered(), ctx.IsComplete())
	if decision != tcpClassifierMatched {
		return nil, decision
	}
	host := normalizeHost(serverName)
	raw := append([]byte(nil), ctx.Buffered()...)
	if host == "" {
		return &classificationResult{
			Classification: classifyUnknownTraffic("tcp", "tls", ctx.OrigIP, ctx.OrigPort, "missing_sni"),
			Apply: func(req *adapterRequest) {
				req.Prefix = bytes.NewReader(raw)
			},
		}, tcpClassifierMatched
	}
	return &classificationResult{
		Classification: classifyKnownTraffic("tcp", "tls", ctx.OrigIP, ctx.OrigPort, host),
		Host:           host,
		Apply: func(req *adapterRequest) {
			req.Prefix = bytes.NewReader(raw)
		},
	}, tcpClassifierMatched
}

type tcpUnknownClassifier struct{}

func (c *tcpUnknownClassifier) Name() string { return "tcp-unknown" }

func (c *tcpUnknownClassifier) Classify(ctx *tcpClassifyContext) (*classificationResult, tcpClassifierDecision) {
	if ctx == nil || ctx.Conn == nil {
		return nil, tcpClassifierNoMatch
	}
	if len(ctx.Buffered()) == 0 && !ctx.IsComplete() {
		return nil, tcpClassifierNeedMore
	}
	reason := "unclassified"
	switch {
	case len(ctx.Buffered()) == 0 && ctx.TimedOut:
		reason = "client_idle"
	case ctx.TimedOut:
		reason = "classification_timeout"
	case len(ctx.Buffered()) == 0 && ctx.ReadErr != nil:
		reason = "read_failed"
	case ctx.LimitReached:
		reason = "header_limit_exceeded"
	}
	raw := append([]byte(nil), ctx.Buffered()...)
	result := &classificationResult{
		Classification: classifyUnknownTraffic("tcp", "unknown", ctx.OrigIP, ctx.OrigPort, reason),
	}
	if len(raw) > 0 {
		result.Apply = func(req *adapterRequest) {
			req.Prefix = bytes.NewReader(raw)
		}
	}
	if len(raw) == 0 && ctx.ReadErr != nil && !errors.Is(ctx.ReadErr, io.EOF) {
		result.Error = ctx.ReadErr
	}
	return result, tcpClassifierMatched
}

type udpMissingOriginalDstClassifier struct{}

func (c *udpMissingOriginalDstClassifier) Name() string { return "udp-missing-original-dst" }

func (c *udpMissingOriginalDstClassifier) Classify(ctx *udpClassifyContext) (*classificationResult, bool) {
	if ctx == nil || (ctx.DestIP != nil && ctx.DestPort > 0) {
		return nil, false
	}
	return &classificationResult{
		Classification: classifyUnknownTraffic("udp", "unknown", ctx.DestIP, ctx.DestPort, "missing_original_dst"),
	}, true
}

type udpSNIClassifier struct{}

func (c *udpSNIClassifier) Name() string { return "udp-sni" }

func (c *udpSNIClassifier) Classify(ctx *udpClassifyContext) (*classificationResult, bool) {
	if ctx == nil || ctx.DestIP == nil || ctx.DestPort <= 0 || !policy.HasDomainRules(ctx.Compiled) {
		return nil, false
	}
	sni := ""
	if ctx.Reassembler != nil {
		sni = ctx.Reassembler.ParseSNI(ctx.Payload, ctx.SrcIP, ctx.DestIP.String())
	}
	if sni == "" {
		return &classificationResult{
			Classification: classifyUnknownTraffic("udp", "udp", ctx.DestIP, ctx.DestPort, "missing_sni"),
		}, true
	}
	return &classificationResult{
		Classification: classifyKnownTraffic("udp", "udp", ctx.DestIP, ctx.DestPort, sni),
		Host:           sni,
	}, true
}

type udpGenericClassifier struct{}

func (c *udpGenericClassifier) Name() string { return "udp-generic" }

func (c *udpGenericClassifier) Classify(ctx *udpClassifyContext) (*classificationResult, bool) {
	if ctx == nil || ctx.DestIP == nil || ctx.DestPort <= 0 {
		return nil, false
	}
	return &classificationResult{
		Classification: classifyKnownTraffic("udp", "udp", ctx.DestIP, ctx.DestPort, ""),
	}, true
}

func defaultTCPClassifiers() []tcpClassifier {
	return []tcpClassifier{
		&httpRequestClassifier{},
		&tlsClientHelloClassifier{},
		&postgresStartupClassifier{},
		&sshBannerClassifier{},
		&tcpUnknownClassifier{},
	}
}

func defaultUDPClassifiers() []udpClassifier {
	return []udpClassifier{
		&udpMissingOriginalDstClassifier{},
		&udpSNIClassifier{},
		&udpGenericClassifier{},
	}
}

func classifyTCP(classifiers []tcpClassifier, ctx *tcpClassifyContext) (*classificationResult, error) {
	if ctx == nil {
		return nil, fmt.Errorf("tcp classify context is nil")
	}
	defer ctx.clearReadDeadline()
	for {
		needMore := false
		var fallback *classificationResult
		for _, classifier := range classifiers {
			result, decision := classifier.Classify(ctx)
			switch decision {
			case tcpClassifierMatched:
				if result != nil && result.Classification.Protocol == "unknown" && needMore && !ctx.IsComplete() {
					fallback = result
					continue
				}
				return result, nil
			case tcpClassifierNeedMore:
				needMore = true
			}
		}
		if fallback != nil && (!needMore || ctx.IsComplete()) {
			return fallback, nil
		}
		if !needMore {
			return nil, fmt.Errorf("no tcp classifier matched")
		}
		if ctx.IsComplete() {
			return nil, fmt.Errorf("no tcp classifier matched")
		}
		n, err := ctx.readMore()
		if n > 0 {
			continue
		}
		if err != nil || ctx.IsComplete() {
			continue
		}
		return nil, fmt.Errorf("tcp classifier could not make progress")
	}
}

func classifyUDP(classifiers []udpClassifier, ctx *udpClassifyContext) (*classificationResult, error) {
	for _, classifier := range classifiers {
		result, matched := classifier.Classify(ctx)
		if matched {
			return result, nil
		}
	}
	return nil, fmt.Errorf("no udp classifier matched")
}

func (ctx *tcpClassifyContext) Buffered() []byte {
	if ctx == nil {
		return nil
	}
	return ctx.Peeked
}

func (ctx *tcpClassifyContext) IsComplete() bool {
	if ctx == nil {
		return true
	}
	return ctx.ReadDone || ctx.LimitReached
}

func (ctx *tcpClassifyContext) headerLimit() int {
	if ctx == nil || ctx.HeaderLimit <= 0 {
		return defaultTCPHeaderLimit
	}
	return ctx.HeaderLimit
}

func (ctx *tcpClassifyContext) classificationTimeout() time.Duration {
	if ctx == nil || ctx.ReadTimeout <= 0 {
		return defaultTCPClassificationWindow
	}
	return ctx.ReadTimeout
}

func (ctx *tcpClassifyContext) firstByteTimeout() time.Duration {
	if ctx == nil || ctx.FirstByteTimeout <= 0 {
		return defaultTCPFirstByteTimeout
	}
	return ctx.FirstByteTimeout
}

func (ctx *tcpClassifyContext) nextReadTimeout() time.Duration {
	if ctx == nil {
		return defaultTCPClassificationWindow
	}
	if len(ctx.Peeked) == 0 {
		return ctx.firstByteTimeout()
	}
	return ctx.classificationTimeout()
}

func (ctx *tcpClassifyContext) readMore() (int, error) {
	if ctx == nil || ctx.Conn == nil {
		return 0, fmt.Errorf("tcp classify connection is nil")
	}
	if ctx.IsComplete() {
		return 0, ctx.ReadErr
	}
	remaining := ctx.headerLimit() - len(ctx.Peeked)
	if remaining <= 0 {
		ctx.LimitReached = true
		ctx.ReadDone = true
		return 0, nil
	}
	_ = ctx.Conn.SetReadDeadline(time.Now().Add(ctx.nextReadTimeout()))
	buf := make([]byte, remaining)
	n, err := ctx.Conn.Read(buf)
	if n > 0 {
		ctx.Peeked = append(ctx.Peeked, buf[:n]...)
	}
	if len(ctx.Peeked) >= ctx.headerLimit() {
		ctx.LimitReached = true
		ctx.ReadDone = true
	}
	if err != nil {
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			ctx.TimedOut = true
			ctx.ReadDone = true
			return n, nil
		}
		ctx.ReadErr = err
		ctx.ReadDone = true
	}
	return n, err
}

func (ctx *tcpClassifyContext) clearReadDeadline() {
	if ctx == nil || ctx.Conn == nil {
		return
	}
	_ = ctx.Conn.SetReadDeadline(time.Time{})
}

func parseHTTPRequest(data []byte, complete bool, limit int) (*http.Request, tcpClassifierDecision) {
	if len(data) == 0 {
		if complete {
			return nil, tcpClassifierNoMatch
		}
		return nil, tcpClassifierNeedMore
	}
	if !looksLikeHTTPRequestPrefix(data) {
		return nil, tcpClassifierNoMatch
	}
	headerEnd := findHTTPHeaderTerminator(data)
	if headerEnd < 0 {
		if !complete && len(data) < limit {
			return nil, tcpClassifierNeedMore
		}
		return nil, tcpClassifierNoMatch
	}
	reader := bufio.NewReader(bytes.NewReader(data[:headerEnd]))
	req, err := http.ReadRequest(reader)
	if err != nil {
		return nil, tcpClassifierNoMatch
	}
	return req, tcpClassifierMatched
}

func findHTTPHeaderTerminator(data []byte) int {
	if index := bytes.Index(data, []byte("\r\n\r\n")); index >= 0 {
		return index + 4
	}
	if index := bytes.Index(data, []byte("\n\n")); index >= 0 {
		return index + 2
	}
	return -1
}

func looksLikeHTTPRequestPrefix(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	line := data
	if index := bytes.IndexByte(line, '\n'); index >= 0 {
		line = line[:index]
	}
	line = bytes.TrimRight(line, "\r")
	parts := bytes.SplitN(line, []byte(" "), 3)
	if len(parts) == 0 || !isHTTPMethodToken(parts[0]) {
		return false
	}
	if len(parts) == 1 {
		return true
	}
	if len(parts) == 2 {
		return len(parts[1]) > 0
	}
	return len(parts[1]) > 0 && bytes.HasPrefix(parts[2], []byte("HTTP/"))
}

func isHTTPMethodToken(method []byte) bool {
	if len(method) == 0 {
		return false
	}
	for _, b := range method {
		if b >= 'A' && b <= 'Z' {
			continue
		}
		switch b {
		case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
			continue
		default:
			return false
		}
	}
	return true
}

type clientHelloInfo struct {
	ServerName string
}

func parseSNIFromTLS(data []byte, complete bool) (string, tcpClassifierDecision) {
	info, decision := parseTLSClientHello(data, complete)
	if decision != tcpClassifierMatched || info == nil {
		return "", decision
	}
	return info.ServerName, tcpClassifierMatched
}

func parseTLSClientHello(data []byte, complete bool) (*clientHelloInfo, tcpClassifierDecision) {
	if len(data) == 0 {
		if complete {
			return nil, tcpClassifierNoMatch
		}
		return nil, tcpClassifierNeedMore
	}
	if data[0] != 0x16 {
		return nil, tcpClassifierNoMatch
	}
	if len(data) < 5 {
		if complete {
			return nil, tcpClassifierNoMatch
		}
		return nil, tcpClassifierNeedMore
	}
	recordLen := int(data[3])<<8 | int(data[4])
	recordEnd := 5 + recordLen
	if recordEnd > len(data) {
		if complete {
			return nil, tcpClassifierNoMatch
		}
		return nil, tcpClassifierNeedMore
	}
	if recordEnd <= 9 || data[5] != 0x01 {
		return nil, tcpClassifierNoMatch
	}
	offset := 9
	if offset+2+32 > recordEnd {
		if complete {
			return nil, tcpClassifierNoMatch
		}
		return nil, tcpClassifierNeedMore
	}
	offset += 2 + 32
	if offset >= recordEnd {
		if complete {
			return nil, tcpClassifierNoMatch
		}
		return nil, tcpClassifierNeedMore
	}
	sessionLen := int(data[offset])
	offset++
	if offset+sessionLen > recordEnd {
		if complete {
			return nil, tcpClassifierNoMatch
		}
		return nil, tcpClassifierNeedMore
	}
	offset += sessionLen
	if offset+2 > recordEnd {
		if complete {
			return nil, tcpClassifierNoMatch
		}
		return nil, tcpClassifierNeedMore
	}
	cipherLen := int(data[offset])<<8 | int(data[offset+1])
	offset += 2
	if offset+cipherLen > recordEnd {
		if complete {
			return nil, tcpClassifierNoMatch
		}
		return nil, tcpClassifierNeedMore
	}
	offset += cipherLen
	if offset >= recordEnd {
		if complete {
			return nil, tcpClassifierNoMatch
		}
		return nil, tcpClassifierNeedMore
	}
	compLen := int(data[offset])
	offset++
	if offset+compLen > recordEnd {
		if complete {
			return nil, tcpClassifierNoMatch
		}
		return nil, tcpClassifierNeedMore
	}
	offset += compLen
	if offset == recordEnd {
		return &clientHelloInfo{}, tcpClassifierMatched
	}
	if offset+2 > recordEnd {
		if complete {
			return nil, tcpClassifierNoMatch
		}
		return nil, tcpClassifierNeedMore
	}
	extLen := int(data[offset])<<8 | int(data[offset+1])
	offset += 2
	if offset+extLen > recordEnd {
		if complete {
			return nil, tcpClassifierNoMatch
		}
		return nil, tcpClassifierNeedMore
	}
	end := offset + extLen
	for offset+4 <= end {
		extType := int(data[offset])<<8 | int(data[offset+1])
		extSize := int(data[offset+2])<<8 | int(data[offset+3])
		offset += 4
		if offset+extSize > end {
			if complete {
				return nil, tcpClassifierNoMatch
			}
			return nil, tcpClassifierNeedMore
		}
		if extType != 0x00 {
			offset += extSize
			continue
		}
		if offset+2 > end {
			if complete {
				return nil, tcpClassifierNoMatch
			}
			return nil, tcpClassifierNeedMore
		}
		listLen := int(data[offset])<<8 | int(data[offset+1])
		offset += 2
		listEnd := offset + listLen
		if listEnd > end {
			if complete {
				return nil, tcpClassifierNoMatch
			}
			return nil, tcpClassifierNeedMore
		}
		for offset+3 <= listEnd {
			nameType := data[offset]
			nameLen := int(data[offset+1])<<8 | int(data[offset+2])
			offset += 3
			if offset+nameLen > listEnd {
				if complete {
					return nil, tcpClassifierNoMatch
				}
				return nil, tcpClassifierNeedMore
			}
			if nameType == 0 {
				return &clientHelloInfo{ServerName: string(data[offset : offset+nameLen])}, tcpClassifierMatched
			}
			offset += nameLen
		}
		return &clientHelloInfo{}, tcpClassifierMatched
	}
	return &clientHelloInfo{}, tcpClassifierMatched
}

func parseSSHBannerClassification(data []byte, complete bool, limit int) (string, tcpClassifierDecision) {
	if len(data) == 0 {
		if complete {
			return "", tcpClassifierNoMatch
		}
		return "", tcpClassifierNeedMore
	}
	prefix := []byte("SSH-")
	if len(data) < len(prefix) {
		if bytes.HasPrefix(prefix, data) && !complete && len(data) < limit {
			return "", tcpClassifierNeedMore
		}
		return "", tcpClassifierNoMatch
	}
	if !bytes.HasPrefix(data, prefix) {
		return "", tcpClassifierNoMatch
	}
	line, found := cutSSHLine(data)
	if !found {
		if !complete && len(data) < limit {
			return "", tcpClassifierNeedMore
		}
		return "", tcpClassifierNoMatch
	}
	line = bytes.TrimRight(line, "\r")
	if len(line) == 0 {
		return "", tcpClassifierNoMatch
	}
	banner := string(line)
	if !bytes.HasPrefix(line, []byte("SSH-")) {
		return "", tcpClassifierNoMatch
	}
	return banner, tcpClassifierMatched
}

func cutSSHLine(data []byte) ([]byte, bool) {
	if index := bytes.IndexByte(data, '\n'); index >= 0 {
		return data[:index], true
	}
	return nil, false
}

func looksLikePostgresStartup(data []byte, complete bool) tcpClassifierDecision {
	if len(data) < 8 {
		if complete {
			return tcpClassifierNoMatch
		}
		return tcpClassifierNeedMore
	}
	length := binary.BigEndian.Uint32(data[:4])
	if length < 8 || length > uint32(defaultTCPHeaderLimit) {
		return tcpClassifierNoMatch
	}
	if int(length) > len(data) {
		if complete {
			return tcpClassifierNoMatch
		}
		return tcpClassifierNeedMore
	}
	code := binary.BigEndian.Uint32(data[4:8])
	switch code {
	case 80877102, 80877103, 80877104:
		return tcpClassifierMatched
	}
	if code>>16 == 3 {
		return tcpClassifierMatched
	}
	return tcpClassifierNoMatch
}

type postgresStartupClassifier struct{}

func (c *postgresStartupClassifier) Name() string { return "postgres-startup" }

func (c *postgresStartupClassifier) Classify(ctx *tcpClassifyContext) (*classificationResult, tcpClassifierDecision) {
	if ctx == nil || ctx.Conn == nil {
		return nil, tcpClassifierNoMatch
	}
	decision := looksLikePostgresStartup(ctx.Buffered(), ctx.IsComplete())
	if decision != tcpClassifierMatched {
		return nil, decision
	}
	raw := append([]byte(nil), ctx.Buffered()...)
	return &classificationResult{
		Classification: classifyKnownTraffic("tcp", "postgres", ctx.OrigIP, ctx.OrigPort, ""),
		Apply: func(req *adapterRequest) {
			req.Prefix = bytes.NewReader(raw)
		},
	}, tcpClassifierMatched
}
