package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRequestShouldBeLongLived(t *testing.T) {
	tests := []struct {
		name string
		req  *http.Request
		want bool
	}{
		{
			name: "sandbox logs follow stream",
			req:  httptest.NewRequest(http.MethodGet, "/api/v1/sandboxes/sb_123/logs?follow=true", nil),
			want: true,
		},
		{
			name: "sandbox logs snapshot",
			req:  httptest.NewRequest(http.MethodGet, "/api/v1/sandboxes/sb_123/logs", nil),
			want: false,
		},
		{
			name: "websocket context stream",
			req: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, "/api/v1/sandboxes/sb_123/contexts/ctx_123/ws", nil)
				req.Header.Set("Connection", "Upgrade")
				req.Header.Set("Upgrade", "websocket")
				return req
			}(),
			want: true,
		},
		{
			name: "non-stream sandbox get",
			req:  httptest.NewRequest(http.MethodGet, "/api/v1/sandboxes/sb_123", nil),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RequestShouldBeLongLived(tt.req); got != tt.want {
				t.Fatalf("RequestShouldBeLongLived() = %v, want %v", got, tt.want)
			}
		})
	}
}
