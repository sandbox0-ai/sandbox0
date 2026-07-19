package resourceguard

import (
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLimitJSONBodyBoundaryAndOneByteOver(t *testing.T) {
	t.Run("boundary", func(t *testing.T) {
		request := httptest.NewRequest("POST", "/", strings.NewReader(`{"v":"x"}`))
		limit := request.ContentLength
		if err := LimitJSONBody(request, "test body", limit); err != nil {
			t.Fatalf("LimitJSONBody() error = %v", err)
		}
		payload, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatalf("ReadAll() error = %v", err)
		}
		if string(payload) != `{"v":"x"}` {
			t.Fatalf("restored body = %q", payload)
		}
	})

	t.Run("one byte over", func(t *testing.T) {
		request := httptest.NewRequest("POST", "/", strings.NewReader(`{"v":"xx"}`))
		err := LimitJSONBody(request, "test body", request.ContentLength-1)
		var tooLarge *TooLargeError
		if !errors.As(err, &tooLarge) {
			t.Fatalf("LimitJSONBody() error = %v, want TooLargeError", err)
		}
		if tooLarge.Actual != request.ContentLength || tooLarge.Limit != request.ContentLength-1 {
			t.Fatalf("TooLargeError = %#v", tooLarge)
		}
	})
}

func TestLimitJSONBodyRejectsOversizedUnknownLengthBody(t *testing.T) {
	request := httptest.NewRequest("POST", "/", nil)
	request.ContentLength = -1
	request.Body = io.NopCloser(strings.NewReader("12345"))

	err := LimitJSONBody(request, "streamed body", 4)
	var tooLarge *TooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("LimitJSONBody() error = %v, want TooLargeError", err)
	}
	if tooLarge.Actual != 5 {
		t.Fatalf("TooLargeError.Actual = %d, want 5", tooLarge.Actual)
	}
}

func TestPrimitiveAndCanonicalJSONLimits(t *testing.T) {
	if err := Bytes("bytes", []byte("1234"), 4); err != nil {
		t.Fatalf("Bytes(boundary) error = %v", err)
	}
	if err := String("string", "12345", 4); !IsTooLarge(err) {
		t.Fatalf("String(over) error = %v, want TooLargeError", err)
	}
	if err := Map("map", 2, 1); !IsTooLarge(err) {
		t.Fatalf("Map(over) error = %v, want TooLargeError", err)
	}
	if err := Slice("slice", 1, 1); err != nil {
		t.Fatalf("Slice(boundary) error = %v", err)
	}

	type value struct {
		Name string `json:"name"`
	}
	boundary, err := CanonicalJSON("value", value{Name: "x"}, int64(len(`{"name":"x"}`)))
	if err != nil {
		t.Fatalf("CanonicalJSON(boundary) error = %v", err)
	}
	if string(boundary) != `{"name":"x"}` {
		t.Fatalf("CanonicalJSON() = %q", boundary)
	}
	_, err = CanonicalJSON("value", value{Name: "xx"}, int64(len(`{"name":"xx"}`)-1))
	if !IsTooLarge(err) {
		t.Fatalf("CanonicalJSON(over) error = %v, want TooLargeError", err)
	}
}

func TestTooLargeErrorDoesNotIncludeInputValue(t *testing.T) {
	secret := "do-not-leak"
	err := String("credential password", secret, 1)
	if err == nil {
		t.Fatal("String() error = nil")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("error leaked secret: %v", err)
	}
}
