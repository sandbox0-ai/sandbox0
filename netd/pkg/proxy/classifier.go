package proxy

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
)

type classificationResult struct {
	Classification trafficClassification
	Host           string
	Error          error
	Apply          func(*adapterRequest)
}

type tcpClassifier interface {
	Name() string
	Classify(*tcpClassifyContext) (*classificationResult, bool)
}

type udpClassifier interface {
	Name() string
	Classify(*udpClassifyContext) (*classificationResult, bool)
}

type tcpClassifyContext struct {
	Compiled    *policy.CompiledPolicy
	SrcIP       string
	OrigIP      net.IP
	OrigPort    int
	Conn        net.Conn
	HeaderLimit int

	TLSHello *clientHelloInfo
	Peeked   []byte
	PeekDone bool
	ReadErr  error
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

func (c *httpRequestClassifier) Classify(ctx *tcpClassifyContext) (*classificationResult, bool) {
	if ctx == nil || ctx.Conn == nil {
		return nil, false
	}
	raw, err := ctx.ensurePeek(ctx.HeaderLimit)
	if err != nil {
		return nil, false
	}
	req, ok := parseHTTPRequest(raw)
	if !ok || req == nil {
		return nil, false
	}
	host := normalizeHost(req.Host)
	if host == "" {
		return &classificationResult{
			Classification: classifyUnknownTraffic("tcp", "http", ctx.OrigIP, ctx.OrigPort, "missing_host"),
			Apply: func(req *adapterRequest) {
				req.Prefix = bytes.NewReader(raw)
			},
		}, true
	}
	return &classificationResult{
		Classification: classifyKnownTraffic("tcp", "http", ctx.OrigIP, ctx.OrigPort, host),
		Host:           host,
		Apply: func(req *adapterRequest) {
			req.Prefix = bytes.NewReader(raw)
		},
	}, true
}

type sshBannerClassifier struct{}

func (c *sshBannerClassifier) Name() string { return "ssh-banner" }

func (c *sshBannerClassifier) Classify(ctx *tcpClassifyContext) (*classificationResult, bool) {
	if ctx == nil || ctx.Conn == nil {
		return nil, false
	}
	raw, err := ctx.ensurePeek(ctx.HeaderLimit)
	if err != nil {
		return nil, false
	}
	if parseSSHBanner(raw) == "" {
		return nil, false
	}
	return &classificationResult{
		Classification: classifyKnownTraffic("tcp", "ssh", ctx.OrigIP, ctx.OrigPort, ""),
		Apply: func(req *adapterRequest) {
			req.Prefix = bytes.NewReader(raw)
		},
	}, true
}

type tlsClientHelloClassifier struct{}

func (c *tlsClientHelloClassifier) Name() string { return "tls-clienthello" }

func (c *tlsClientHelloClassifier) Classify(ctx *tcpClassifyContext) (*classificationResult, bool) {
	if ctx == nil || ctx.Conn == nil {
		return nil, false
	}
	if ctx.TLSHello == nil && ctx.ReadErr == nil {
		raw, err := ctx.ensurePeek(ctx.HeaderLimit)
		if err != nil {
			ctx.ReadErr = err
			return nil, false
		}
		serverName, ok := parseSNIFromTLS(raw)
		if !ok {
			return nil, false
		}
		ctx.TLSHello = &clientHelloInfo{
			ServerName: serverName,
			Raw:        raw,
		}
	}
	if ctx.TLSHello == nil {
		return nil, false
	}
	host := normalizeHost(ctx.TLSHello.ServerName)
	if host == "" {
		return &classificationResult{
			Classification: classifyUnknownTraffic("tcp", "tls", ctx.OrigIP, ctx.OrigPort, "missing_sni"),
			Apply: func(req *adapterRequest) {
				req.Prefix = bytes.NewReader(ctx.TLSHello.Raw)
			},
		}, true
	}
	return &classificationResult{
		Classification: classifyKnownTraffic("tcp", "tls", ctx.OrigIP, ctx.OrigPort, host),
		Host:           host,
		Apply: func(req *adapterRequest) {
			req.Prefix = bytes.NewReader(ctx.TLSHello.Raw)
		},
	}, true
}

type tcpUnknownClassifier struct{}

func (c *tcpUnknownClassifier) Name() string { return "tcp-unknown" }

func (c *tcpUnknownClassifier) Classify(ctx *tcpClassifyContext) (*classificationResult, bool) {
	if ctx == nil || ctx.Conn == nil {
		return nil, false
	}
	raw, err := ctx.ensurePeek(ctx.HeaderLimit)
	if err != nil && len(raw) == 0 {
		return &classificationResult{
			Classification: classifyUnknownTraffic("tcp", "unknown", ctx.OrigIP, ctx.OrigPort, "read_failed"),
			Error:          err,
		}, true
	}
	result := &classificationResult{
		Classification: classifyUnknownTraffic("tcp", "unknown", ctx.OrigIP, ctx.OrigPort, "unclassified"),
	}
	if len(raw) > 0 {
		result.Apply = func(req *adapterRequest) {
			req.Prefix = bytes.NewReader(raw)
		}
	}
	return result, true
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
	for _, classifier := range classifiers {
		result, matched := classifier.Classify(ctx)
		if matched {
			return result, nil
		}
	}
	return nil, fmt.Errorf("no tcp classifier matched")
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

func (ctx *tcpClassifyContext) ensurePeek(limit int) ([]byte, error) {
	if ctx.PeekDone {
		return ctx.Peeked, ctx.ReadErr
	}
	ctx.PeekDone = true
	if limit <= 0 {
		limit = 64 * 1024
	}
	_ = ctx.Conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	defer ctx.Conn.SetReadDeadline(time.Time{})

	buf := make([]byte, limit)
	n, err := ctx.Conn.Read(buf)
	if err != nil {
		ctx.ReadErr = err
		return nil, err
	}
	ctx.Peeked = append([]byte(nil), buf[:n]...)
	return ctx.Peeked, nil
}

func parseHTTPRequest(data []byte) (*http.Request, bool) {
	if len(data) == 0 {
		return nil, false
	}
	reader := bufio.NewReader(bytes.NewReader(data))
	req, err := http.ReadRequest(reader)
	if err != nil {
		return nil, false
	}
	return req, true
}

type clientHelloInfo struct {
	ServerName string
	Raw        []byte
}

func parseSNIFromTLS(data []byte) (string, bool) {
	if len(data) < 5 {
		return "", false
	}
	if data[0] != 0x16 {
		return "", false
	}
	recordLen := int(data[3])<<8 | int(data[4])
	if recordLen+5 > len(data) {
		return "", false
	}
	offset := 5
	if data[offset] != 0x01 {
		return "", false
	}
	offset += 4
	if offset+2 > len(data) {
		return "", false
	}
	offset += 2 + 32
	if offset >= len(data) {
		return "", false
	}
	sessionLen := int(data[offset])
	offset += 1 + sessionLen
	if offset+2 > len(data) {
		return "", false
	}
	cipherLen := int(data[offset])<<8 | int(data[offset+1])
	offset += 2 + cipherLen
	if offset >= len(data) {
		return "", false
	}
	compLen := int(data[offset])
	offset += 1 + compLen
	if offset+2 > len(data) {
		return "", false
	}
	extLen := int(data[offset])<<8 | int(data[offset+1])
	offset += 2
	end := offset + extLen
	if end > len(data) {
		return "", false
	}
	for offset+4 <= end {
		extType := int(data[offset])<<8 | int(data[offset+1])
		extSize := int(data[offset+2])<<8 | int(data[offset+3])
		offset += 4
		if offset+extSize > end {
			return "", false
		}
		if extType == 0x00 {
			if offset+2 > end {
				return "", false
			}
			listLen := int(data[offset])<<8 | int(data[offset+1])
			offset += 2
			listEnd := offset + listLen
			for offset+3 <= listEnd {
				nameType := data[offset]
				nameLen := int(data[offset+1])<<8 | int(data[offset+2])
				offset += 3
				if nameType == 0 && offset+nameLen <= listEnd {
					return string(data[offset : offset+nameLen]), true
				}
				offset += nameLen
			}
			return "", true
		}
		offset += extSize
	}
	return "", true
}

func looksLikePostgresStartup(data []byte) bool {
	if len(data) < 8 {
		return false
	}
	length := binary.BigEndian.Uint32(data[:4])
	if length < 8 || length > 64*1024 {
		return false
	}
	code := binary.BigEndian.Uint32(data[4:8])
	switch code {
	case 80877102, 80877103, 80877104:
		return true
	}
	return code>>16 == 3
}

type postgresStartupClassifier struct{}

func (c *postgresStartupClassifier) Name() string { return "postgres-startup" }

func (c *postgresStartupClassifier) Classify(ctx *tcpClassifyContext) (*classificationResult, bool) {
	if ctx == nil || ctx.Conn == nil {
		return nil, false
	}
	raw, err := ctx.ensurePeek(ctx.HeaderLimit)
	if err != nil {
		return nil, false
	}
	if !looksLikePostgresStartup(raw) {
		return nil, false
	}
	return &classificationResult{
		Classification: classifyKnownTraffic("tcp", "postgres", ctx.OrigIP, ctx.OrigPort, ""),
		Apply: func(req *adapterRequest) {
			req.Prefix = bytes.NewReader(raw)
		},
	}, true
}
