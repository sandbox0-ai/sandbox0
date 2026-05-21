package functionapi

import (
	"net/http"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/httpclient"
)

const defaultHTTPTimeout = 30 * time.Second

func resolveHTTPClient(httpClient *http.Client) *http.Client {
	return httpclient.Resolve(httpClient, defaultHTTPTimeout)
}
