package objectstore

import (
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	smithymiddleware "github.com/aws/smithy-go/middleware"
)

// RequestAttempt describes one HTTP request sent to an object-storage
// provider. SDK retries therefore produce separate observations.
type RequestAttempt struct {
	Provider   string
	Bucket     string
	Operation  string
	Key        string
	StatusCode int
	ObservedAt time.Time
}

// RequestObserver receives object-storage provider request attempts.
//
// Implementations must return quickly and must not retain object keys after
// extracting the attribution dimensions they need.
type RequestObserver interface {
	ObserveRequestAttempt(RequestAttempt)
}

type requestObservingHTTPClient struct {
	next     aws.HTTPClient
	provider string
	bucket   string
	observer RequestObserver
}

func newRequestObservingHTTPClient(next aws.HTTPClient, provider, bucket string, observer RequestObserver) aws.HTTPClient {
	if observer == nil {
		return next
	}
	if next == nil {
		next = awshttp.NewBuildableClient()
	}
	return &requestObservingHTTPClient{
		next:     next,
		provider: strings.TrimSpace(provider),
		bucket:   strings.TrimSpace(bucket),
		observer: observer,
	}
}

func (c *requestObservingHTTPClient) Do(req *http.Request) (*http.Response, error) {
	resp, err := c.next.Do(req)
	statusCode := 0
	if resp != nil {
		statusCode = resp.StatusCode
	}
	attempt := RequestAttempt{
		Provider:   c.provider,
		Bucket:     c.bucket,
		Operation:  smithymiddleware.GetOperationName(req.Context()),
		Key:        requestObjectKey(req, c.bucket),
		StatusCode: statusCode,
		ObservedAt: time.Now().UTC(),
	}
	notifyRequestObserver(c.observer, attempt)
	return resp, err
}

func requestObjectKey(req *http.Request, bucket string) string {
	if req == nil || req.URL == nil {
		return ""
	}
	operation := smithymiddleware.GetOperationName(req.Context())
	if operation == "ListObjects" || operation == "ListObjectsV2" {
		return strings.TrimLeft(req.URL.Query().Get("prefix"), "/")
	}

	escapedPath := req.URL.EscapedPath()
	decodedPath, err := url.PathUnescape(escapedPath)
	if err != nil {
		decodedPath = req.URL.Path
	}
	decodedPath = strings.TrimLeft(strings.TrimSpace(decodedPath), "/")
	bucket = strings.Trim(strings.TrimSpace(bucket), "/")
	if bucket == "" {
		return decodedPath
	}

	if decodedPath == bucket {
		return ""
	}
	if remainder, ok := strings.CutPrefix(decodedPath, bucket+"/"); ok {
		return remainder
	}
	return decodedPath
}

func notifyRequestObserver(observer RequestObserver, attempt RequestAttempt) {
	if observer == nil {
		return
	}
	// Cost attribution must never change the outcome of the storage request.
	defer func() {
		_ = recover()
	}()
	observer.ObserveRequestAttempt(attempt)
}
