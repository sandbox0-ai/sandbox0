package clients

import (
	"net/http"
	"time"
)

// StorageClient is a thin wrapper around the manager storage API.
type StorageClient struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
}

// NewStorageClient creates a manager storage API client with defaults.
func NewStorageClient(baseURL, token string, timeout time.Duration) *StorageClient {
	return &StorageClient{
		BaseURL: baseURL,
		Token:   token,
		HTTP: &http.Client{
			Timeout: timeout,
		},
	}
}
