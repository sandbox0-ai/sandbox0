package http

import (
	"bufio"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

type hijackableRecorder struct {
	*httptest.ResponseRecorder
	hijacked bool
}

func (r *hijackableRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	r.hijacked = true
	serverConn, clientConn := net.Pipe()
	rw := bufio.NewReadWriter(bufio.NewReader(serverConn), bufio.NewWriter(serverConn))
	return clientConn, rw, nil
}

func TestResponseWriterHijack(t *testing.T) {
	base := &hijackableRecorder{ResponseRecorder: httptest.NewRecorder()}
	wrapped := &responseWriter{ResponseWriter: base, statusCode: http.StatusOK}

	conn, rw, err := wrapped.Hijack()
	if err != nil {
		t.Fatalf("Hijack() error = %v", err)
	}
	if conn == nil || rw == nil {
		t.Fatalf("Hijack() returned nil connection or readwriter")
	}
	if !base.hijacked {
		t.Fatalf("Hijack() did not delegate to underlying ResponseWriter")
	}
	_ = conn.Close()
}
