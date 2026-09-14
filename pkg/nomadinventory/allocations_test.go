package nomadinventory

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestListRejectsAmbiguousAndOversizedResponses(t *testing.T) {
	for _, test := range []string{"oversized", "trailing-data", "duplicate-headers", "http-error"} {
		t.Run(test, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch test {
				case "oversized":
					_, _ = w.Write([]byte("[" + strings.Repeat(" ", 2<<20) + "]"))
				case "trailing-data":
					_, _ = w.Write([]byte("[] {}"))
				case "duplicate-headers":
					w.Header().Add("X-Nomad-NextToken", "first")
					w.Header().Add("X-Nomad-NextToken", "second")
					_, _ = w.Write([]byte("[]"))
				case "http-error":
					http.Error(w, "unavailable", http.StatusServiceUnavailable)
				}
			}))
			defer server.Close()
			address, _ := url.Parse(server.URL)
			records, err := List(context.Background(), server.Client(), address, "node", "default", nil)
			if err == nil || records != nil {
				t.Fatalf("unsafe response accepted: records=%v err=%v", records, err)
			}
		})
	}
}
