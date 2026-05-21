package spec

import (
	"strings"
	"testing"
)

func TestDecodeResponseOrRawEnvelope(t *testing.T) {
	got, err := DecodeResponseOrRaw[struct {
		ID string `json:"id"`
	}](strings.NewReader(`{"success":true,"data":{"id":"ctx-a"}}`))
	if err != nil {
		t.Fatalf("DecodeResponseOrRaw() error = %v", err)
	}
	if got == nil || got.ID != "ctx-a" {
		t.Fatalf("DecodeResponseOrRaw() = %+v, want id ctx-a", got)
	}
}

func TestDecodeResponseOrRawRawBody(t *testing.T) {
	got, err := DecodeResponseOrRaw[struct {
		ID string `json:"id"`
	}](strings.NewReader(`{"id":"ctx-a"}`))
	if err != nil {
		t.Fatalf("DecodeResponseOrRaw() error = %v", err)
	}
	if got == nil || got.ID != "ctx-a" {
		t.Fatalf("DecodeResponseOrRaw() = %+v, want id ctx-a", got)
	}
}

func TestDecodeResponseOrRawEnvelopeError(t *testing.T) {
	_, err := DecodeResponseOrRaw[struct {
		ID string `json:"id"`
	}](strings.NewReader(`{"success":false,"error":{"code":"bad_request","message":"invalid"}}`))
	if err == nil || err.Error() != "invalid" {
		t.Fatalf("DecodeResponseOrRaw() error = %v, want invalid", err)
	}
}

func TestDecodeResponseOrRawEnvelopeMissingData(t *testing.T) {
	_, err := DecodeResponseOrRaw[struct {
		ID string `json:"id"`
	}](strings.NewReader(`{"success":true}`))
	if err == nil {
		t.Fatal("DecodeResponseOrRaw() error = nil, want missing data error")
	}
}

func TestDecodeErrorMessage(t *testing.T) {
	got, ok := DecodeErrorMessage([]byte(`{"success":false,"error":{"code":"bad_request","message":"invalid input"}}`))
	if !ok {
		t.Fatal("DecodeErrorMessage() ok = false")
	}
	if got != "invalid input" {
		t.Fatalf("DecodeErrorMessage() = %q, want invalid input", got)
	}
}

func TestDecodeErrorMessageReturnsFalseForRawBody(t *testing.T) {
	if got, ok := DecodeErrorMessage([]byte(`{"message":"raw error"}`)); ok || got != "" {
		t.Fatalf("DecodeErrorMessage() = %q, %v; want empty false", got, ok)
	}
}
