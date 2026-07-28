package proxy

import (
	"net"
	"net/http"

	"github.com/sandbox0-ai/sandbox0/pkg/streaming"
)

// DisableResponseWriteDeadline clears the server-managed write deadline for a
// long-lived response. It must be called before the handler writes headers.
func DisableResponseWriteDeadline(w http.ResponseWriter) error {
	return streaming.DisableResponseWriteDeadline(w)
}

// DisableResponseDeadlines clears server-managed read and write deadlines for
// an upgraded long-lived connection. It must be called before hijacking.
func DisableResponseDeadlines(w http.ResponseWriter) error {
	return streaming.DisableResponseDeadlines(w)
}

// DisableConnectionDeadlines clears read and write deadlines on a hijacked or
// upgraded connection used for long-lived streams.
func DisableConnectionDeadlines(conn net.Conn) error {
	return streaming.DisableConnectionDeadlines(conn)
}

// PrepareStreamingProxyResponse clears downstream server deadlines when a
// proxied request is allowed to outlive the ordinary upstream timeout.
func PrepareStreamingProxyResponse(w http.ResponseWriter, req *http.Request) error {
	if req == nil {
		return DisableResponseWriteDeadline(w)
	}
	if IsWebSocketUpgrade(req) || LongLivedRequest(req.Context()) {
		return DisableResponseDeadlines(w)
	}
	if UpstreamTimeoutDisabled(req.Context()) || StreamingResponseDeadlinesDisabled(req.Context()) {
		return DisableResponseWriteDeadline(w)
	}
	return nil
}
