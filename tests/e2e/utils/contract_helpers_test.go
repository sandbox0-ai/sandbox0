package utils

import (
	"net/http"
	"testing"
)

func TestValidateBinaryResponseAllowsEmptyPayload(t *testing.T) {
	t.Parallel()

	ValidateResponseExample(
		t,
		http.MethodGet,
		"/api/v1/sandboxvolumes/{id}/files",
		http.StatusOK,
		contentTypeBinary,
		[]byte{},
	)
}

func TestValidateBinaryResponseAllowsRawBytes(t *testing.T) {
	t.Parallel()

	ValidateResponseExample(
		t,
		http.MethodGet,
		"/api/v1/sandboxvolumes/{id}/files",
		http.StatusOK,
		contentTypeBinary,
		[]byte("hello"),
	)
}
