package proxy

import (
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// WebSocketProxy handles WebSocket connections proxying
type WebSocketProxy struct {
	logger *zap.Logger
}

// NewWebSocketProxy creates a new WebSocket proxy
func NewWebSocketProxy(logger *zap.Logger) *WebSocketProxy {
	return &WebSocketProxy{
		logger: logger,
	}
}

// Proxy creates a WebSocket proxy handler
func (p *WebSocketProxy) Proxy(targetURL *url.URL) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Check if this is a WebSocket upgrade request
		if !isWebSocketUpgrade(c.Request) {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "expected WebSocket upgrade request",
			})
			return
		}

		// Create target URL
		wsURL := *targetURL
		if wsURL.Scheme == "http" {
			wsURL.Scheme = "ws"
		} else if wsURL.Scheme == "https" {
			wsURL.Scheme = "wss"
		}
		wsURL.Path = c.Request.URL.Path
		wsURL.RawQuery = c.Request.URL.RawQuery

		// Hijack the connection
		hijacker, ok := c.Writer.(http.Hijacker)
		if !ok {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "WebSocket hijacking not supported",
			})
			return
		}

		clientConn, _, err := hijacker.Hijack()
		if err != nil {
			p.logger.Error("Failed to hijack connection", zap.Error(err))
			return
		}
		defer clientConn.Close()

		// Connect to upstream
		upstreamConn, err := dialWebSocket(&wsURL, c.Request.Header)
		if err != nil {
			p.logger.Error("Failed to connect to upstream WebSocket",
				zap.String("target", wsURL.String()),
				zap.Error(err),
			)
			clientConn.Write([]byte("HTTP/1.1 502 Bad Gateway\r\n\r\n"))
			return
		}
		defer upstreamConn.Close()

		// Proxy bidirectionally
		errChan := make(chan error, 2)
		go func() {
			_, err := io.Copy(upstreamConn, clientConn)
			errChan <- err
		}()
		go func() {
			_, err := io.Copy(clientConn, upstreamConn)
			errChan <- err
		}()

		// Wait for either direction to close
		<-errChan
	}
}

// isWebSocketUpgrade checks if the request is a WebSocket upgrade request
func isWebSocketUpgrade(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket") &&
		strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade")
}

// dialWebSocket establishes a WebSocket connection to the target
func dialWebSocket(target *url.URL, headers http.Header) (io.ReadWriteCloser, error) {
	// Create a raw TCP connection and perform WebSocket handshake
	// For production, use a proper WebSocket library like gorilla/websocket

	// Determine target address
	host := target.Host
	if target.Port() == "" {
		if target.Scheme == "ws" || target.Scheme == "http" {
			host = host + ":80"
		} else {
			host = host + ":443"
		}
	}

	// For simplicity, use net.Dial for ws:// connections
	// In production, use gorilla/websocket Dialer
	dialer := &http.Client{}

	// Convert ws:// to http:// for the upgrade request
	httpURL := *target
	if httpURL.Scheme == "ws" {
		httpURL.Scheme = "http"
	} else if httpURL.Scheme == "wss" {
		httpURL.Scheme = "https"
	}

	req, err := http.NewRequest("GET", httpURL.String(), nil)
	if err != nil {
		return nil, err
	}

	// Copy relevant headers
	req.Header = make(http.Header)
	for _, h := range []string{"Sec-WebSocket-Key", "Sec-WebSocket-Version", "Sec-WebSocket-Protocol", "Sec-WebSocket-Extensions"} {
		if v := headers.Get(h); v != "" {
			req.Header.Set(h, v)
		}
	}
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "Upgrade")

	// For production use, this should use a proper WebSocket dialer
	// This is a simplified implementation
	resp, err := dialer.Do(req)
	if err != nil {
		return nil, err
	}

	return &websocketConn{body: resp.Body}, nil
}

// websocketConn wraps an io.ReadCloser to implement ReadWriteCloser
type websocketConn struct {
	body io.ReadCloser
}

func (w *websocketConn) Read(p []byte) (n int, err error) {
	return w.body.Read(p)
}

func (w *websocketConn) Write(p []byte) (n int, err error) {
	// This is a placeholder - proper WebSocket implementation needed
	return len(p), nil
}

func (w *websocketConn) Close() error {
	return w.body.Close()
}
