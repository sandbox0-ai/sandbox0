package sandboxfunction

import (
	"crypto/sha256"
	"encoding/hex"
)

const (
	RuntimePython = "python"

	SourceTypeInline = "inline"

	ProtocolHTTP      = "http"
	ProtocolStream    = "stream"
	ProtocolWebSocket = "websocket"

	StreamFrameStart = "start"
	StreamFrameChunk = "chunk"
	StreamFrameError = "error"

	WebSocketFrameMessage = "message"
	WebSocketFrameClose   = "close"
	WebSocketMessageText  = "text"
	WebSocketMessageBytes = "binary"

	DefaultFilename = "main.py"
	DefaultHandler  = "handler"

	DefaultTimeoutMS = 30000

	MaxInlineSourceBytes = 256 << 10
	MaxHTTPRequestBytes  = 8 << 20

	// DefaultServicePort is the internal public-exposure routing port used for
	// function services. The actual procd address is still resolved from the
	// sandbox runtime.
	DefaultServicePort = 49983
)

// Source describes function code carried by cluster-gateway to procd.
type Source struct {
	Type string `json:"type"`
	// Filename is accepted only for gateway/procd rollout compatibility.
	// Function source is always materialized as DefaultFilename.
	Filename string `json:"filename,omitempty"`
	Code     string `json:"code,omitempty"`
	Digest   string `json:"digest,omitempty"`
}

// HTTPRequest is the public HTTP request normalized by cluster-gateway.
type HTTPRequest struct {
	Method     string              `json:"method,omitempty"`
	Path       string              `json:"path,omitempty"`
	RawQuery   string              `json:"raw_query,omitempty"`
	Headers    map[string][]string `json:"headers,omitempty"`
	BodyBase64 string              `json:"body_base64,omitempty"`
}

// ExecuteRequest asks procd to execute function code inside the sandbox.
type ExecuteRequest struct {
	ServiceID      string            `json:"service_id,omitempty"`
	RouteID        string            `json:"route_id,omitempty"`
	Runtime        string            `json:"runtime"`
	Handler        string            `json:"handler"`
	MaxConcurrency int               `json:"max_concurrency,omitempty"`
	Source         Source            `json:"source"`
	EnvVars        map[string]string `json:"env_vars,omitempty"`
	Request        HTTPRequest       `json:"request"`
	TimeoutMS      int               `json:"timeout_ms,omitempty"`
}

// ExecuteResponse is the HTTP response returned by a function handler.
type ExecuteResponse struct {
	Status     int                 `json:"status"`
	Headers    map[string][]string `json:"headers,omitempty"`
	BodyBase64 string              `json:"body_base64,omitempty"`
}

// StreamFrame is the line-delimited protocol emitted by function stream runners.
type StreamFrame struct {
	Type       string              `json:"type"`
	Status     int                 `json:"status,omitempty"`
	Headers    map[string][]string `json:"headers,omitempty"`
	BodyBase64 string              `json:"body_base64,omitempty"`
	Error      string              `json:"error,omitempty"`
}

// WebSocketFrame is the line-delimited protocol between procd and function WebSocket runners.
type WebSocketFrame struct {
	Type        string `json:"type"`
	MessageType string `json:"message_type,omitempty"`
	Data        string `json:"data,omitempty"`
	DataBase64  string `json:"data_base64,omitempty"`
	Reason      string `json:"reason,omitempty"`
}

func InlineDigest(code string) string {
	sum := sha256.Sum256([]byte(code))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// LegacyInlineDigest returns the digest used by function services before the
// public filename field was removed. It is kept for rolling-upgrade compatibility.
func LegacyInlineDigest(filename, code string) string {
	sum := sha256.Sum256([]byte(filename + "\x00" + code))
	return "sha256:" + hex.EncodeToString(sum[:])
}
