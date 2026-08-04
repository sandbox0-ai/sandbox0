package spec

import "testing"

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
