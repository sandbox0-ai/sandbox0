package objectstore

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	smithymiddleware "github.com/aws/smithy-go/middleware"
)

type requestObserverFunc func(RequestAttempt)

func (f requestObserverFunc) ObserveRequestAttempt(attempt RequestAttempt) {
	f(attempt)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) Do(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestRequestObservingHTTPClientRecordsEachProviderAttempt(t *testing.T) {
	var attempts []RequestAttempt
	next := roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusPartialContent,
			Body:       http.NoBody,
			Header:     make(http.Header),
		}, nil
	})
	client := newRequestObservingHTTPClient(next, TypeOSS, "runtime-bucket", requestObserverFunc(func(attempt RequestAttempt) {
		attempts = append(attempts, attempt)
	}))

	ctx := smithymiddleware.WithOperationName(context.Background(), "GetObject")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://oss.example/runtime-bucket/sandboxvolumes/team-1/volume-1/s0fs/segments/a", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	for range 2 {
		if _, err := client.Do(req); err != nil {
			t.Fatalf("Do() error = %v", err)
		}
	}

	if len(attempts) != 2 {
		t.Fatalf("attempt count = %d, want 2", len(attempts))
	}
	for _, attempt := range attempts {
		if attempt.Provider != TypeOSS || attempt.Bucket != "runtime-bucket" {
			t.Fatalf("unexpected provider attribution: %+v", attempt)
		}
		if attempt.Operation != "GetObject" || attempt.StatusCode != http.StatusPartialContent {
			t.Fatalf("unexpected request result: %+v", attempt)
		}
		if attempt.Key != "sandboxvolumes/team-1/volume-1/s0fs/segments/a" {
			t.Fatalf("key = %q", attempt.Key)
		}
		if attempt.ObservedAt.IsZero() {
			t.Fatal("ObservedAt is zero")
		}
	}
}

func TestOSSStoreEmitsSDKRequestAttempt(t *testing.T) {
	var got RequestAttempt
	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		gotPath = req.URL.EscapedPath()
		w.Header().Set("ETag", `"test-etag"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	store, err := Create(Config{
		Type:      TypeOSS,
		Bucket:    "runtime-bucket",
		Region:    "oss-test-1",
		Endpoint:  server.URL,
		AccessKey: "test-access-key",
		SecretKey: "test-secret-key",
		RequestObserver: requestObserverFunc(func(attempt RequestAttempt) {
			got = attempt
		}),
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if err := store.Put("sandboxvolumes/team-1/volume-1/object", strings.NewReader("data")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	if gotPath != "/runtime-bucket/sandboxvolumes/team-1/volume-1/object" {
		t.Fatalf("request path = %q", gotPath)
	}
	if got.Provider != TypeOSS || got.Bucket != "runtime-bucket" ||
		got.Operation != "PutObject" || got.Key != "sandboxvolumes/team-1/volume-1/object" ||
		got.StatusCode != http.StatusOK {
		t.Fatalf("attempt = %+v", got)
	}
}

func TestRequestObservingHTTPClientUsesListPrefix(t *testing.T) {
	var got RequestAttempt
	client := newRequestObservingHTTPClient(
		roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Header: make(http.Header)}, nil
		}),
		TypeOSS,
		"runtime-bucket",
		requestObserverFunc(func(attempt RequestAttempt) { got = attempt }),
	)
	ctx := smithymiddleware.WithOperationName(context.Background(), "ListObjectsV2")
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		"https://oss.example/runtime-bucket?list-type=2&prefix=sandbox-rootfs%2Fteam-2%2Fsandbox-2%2F",
		nil,
	)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	if _, err := client.Do(req); err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	if got.Key != "sandbox-rootfs/team-2/sandbox-2/" {
		t.Fatalf("key = %q", got.Key)
	}
}

func TestRequestObjectKeyPreservesRepeatedBucketSegment(t *testing.T) {
	req, err := http.NewRequest(
		http.MethodGet,
		"https://oss.example/runtime-bucket/sandboxvolumes/team-1/volume-1/runtime-bucket/object",
		nil,
	)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	got := requestObjectKey(req, "runtime-bucket")
	want := "sandboxvolumes/team-1/volume-1/runtime-bucket/object"
	if got != want {
		t.Fatalf("requestObjectKey() = %q, want %q", got, want)
	}
}

func TestRequestObservingHTTPClientPreservesTransportError(t *testing.T) {
	transportErr := errors.New("transport failed")
	var got RequestAttempt
	client := newRequestObservingHTTPClient(
		roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, transportErr
		}),
		TypeOSS,
		"runtime-bucket",
		requestObserverFunc(func(attempt RequestAttempt) { got = attempt }),
	)
	ctx := smithymiddleware.WithOperationName(context.Background(), "PutObject")
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPut,
		"https://oss.example/runtime-bucket/sandboxvolumes/team-1/volume-1/s0fs/heads/current",
		strings.NewReader("data"),
	)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	if _, err := client.Do(req); !errors.Is(err, transportErr) {
		t.Fatalf("Do() error = %v, want %v", err, transportErr)
	}
	if got.StatusCode != 0 || got.Operation != "PutObject" {
		t.Fatalf("attempt = %+v", got)
	}
}

func TestNotifyRequestObserverContainsPanic(t *testing.T) {
	notifyRequestObserver(requestObserverFunc(func(RequestAttempt) {
		panic("boom")
	}), RequestAttempt{})
}
