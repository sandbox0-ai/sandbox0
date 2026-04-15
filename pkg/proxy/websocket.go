package proxy

import (
	"bufio"
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// WebSocketProxy handles WebSocket connections proxying
type WebSocketProxy struct {
	logger *zap.Logger
	// requestModifiers are applied before proxying.
	requestModifiers []RequestModifier
}

// NewWebSocketProxy creates a new WebSocket proxy
func NewWebSocketProxy(logger *zap.Logger, opts ...Option) *WebSocketProxy {
	parsedOpts := collectOptions(opts...)
	return &WebSocketProxy{
		logger:           logger,
		requestModifiers: parsedOpts.requestModifiers,
	}
}

// Proxy creates a WebSocket proxy handler
func (p *WebSocketProxy) Proxy(targetURL *url.URL) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Check if this is a WebSocket upgrade request
		if !IsWebSocketUpgrade(c.Request) {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "expected WebSocket upgrade request",
			})
			return
		}
		if err := DisableResponseDeadlines(c.Writer); err != nil {
			p.logger.Debug("Failed to disable WebSocket response deadlines", zap.Error(err))
		}

		hijacker, ok := c.Writer.(http.Hijacker)
		if !ok {
			p.logger.Error("ResponseWriter does not support hijacking")
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "websocket hijacking not supported",
			})
			return
		}

		outReq := c.Request.Clone(c.Request.Context())
		outReq.URL.Path = c.Request.URL.Path
		outReq.URL.RawQuery = c.Request.URL.RawQuery
		outReq.Host = targetURL.Host
		outReq.RequestURI = ""

		applyRequestModifiers(outReq, p.requestModifiers)

		upstreamConn, err := dialWebSocketUpstream(outReq.Context(), targetURL)
		if err != nil {
			p.logger.Error("Failed to connect to upstream WebSocket",
				zap.String("target", targetURL.Host),
				zap.Error(err),
			)
			c.JSON(http.StatusBadGateway, gin.H{
				"error": "upstream websocket unavailable",
			})
			return
		}
		defer upstreamConn.Close()

		if err := writeUpstreamHandshake(upstreamConn, outReq); err != nil {
			p.logger.Error("Failed to write upstream WebSocket handshake", zap.Error(err))
			c.JSON(http.StatusBadGateway, gin.H{
				"error": "upstream websocket unavailable",
			})
			return
		}

		upstreamReader := bufio.NewReader(upstreamConn)
		resp, err := http.ReadResponse(upstreamReader, outReq)
		if err != nil {
			p.logger.Error("Failed to read upstream WebSocket handshake", zap.Error(err))
			c.JSON(http.StatusBadGateway, gin.H{
				"error": "upstream websocket unavailable",
			})
			return
		}

		if resp.StatusCode != http.StatusSwitchingProtocols {
			defer resp.Body.Close()
			copyHeaders(c.Writer.Header(), resp.Header)
			c.Writer.WriteHeader(resp.StatusCode)
			_, _ = io.Copy(c.Writer, resp.Body)
			return
		}

		downstreamConn, _, err := hijacker.Hijack()
		if err != nil {
			p.logger.Error("Failed to hijack downstream connection", zap.Error(err))
			return
		}
		defer downstreamConn.Close()

		if err := resp.Write(downstreamConn); err != nil {
			p.logger.Error("Failed to write downstream WebSocket handshake", zap.Error(err))
			return
		}

		errChan := make(chan error, 2)
		go func() { _, err := io.Copy(upstreamConn, downstreamConn); errChan <- err }()
		go func() { _, err := io.Copy(downstreamConn, upstreamReader); errChan <- err }()
		<-errChan
	}
}

// IsWebSocketUpgrade checks if the request is a WebSocket upgrade request.
func IsWebSocketUpgrade(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket") &&
		strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade")
}

func isWebSocketUpgrade(r *http.Request) bool {
	return IsWebSocketUpgrade(r)
}

func dialWebSocketUpstream(ctx context.Context, targetURL *url.URL) (net.Conn, error) {
	dialer := &net.Dialer{}
	switch strings.ToLower(targetURL.Scheme) {
	case "https", "wss":
		rawConn, err := dialer.DialContext(ctx, "tcp", targetURL.Host)
		if err != nil {
			return nil, err
		}
		tlsConn := tls.Client(rawConn, &tls.Config{
			ServerName: targetURL.Hostname(),
		})
		if err := tlsConn.Handshake(); err != nil {
			rawConn.Close()
			return nil, err
		}
		return tlsConn, nil
	default:
		return dialer.DialContext(ctx, "tcp", targetURL.Host)
	}
}

func writeUpstreamHandshake(conn net.Conn, req *http.Request) error {
	writer := bufio.NewWriter(conn)
	if err := req.Write(writer); err != nil {
		return err
	}
	return writer.Flush()
}

func copyHeaders(dst, src http.Header) {
	for key, values := range src {
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}
