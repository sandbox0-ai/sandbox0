package httpclient

import (
	"net/http"
	"time"
)

const DefaultTimeout = 30 * time.Second

// Resolve returns the configured client or a fallback client with a timeout.
func Resolve(configured *http.Client, fallbackTimeout time.Duration) *http.Client {
	if configured != nil {
		return configured
	}
	if fallbackTimeout <= 0 {
		fallbackTimeout = DefaultTimeout
	}
	return &http.Client{Timeout: fallbackTimeout}
}
