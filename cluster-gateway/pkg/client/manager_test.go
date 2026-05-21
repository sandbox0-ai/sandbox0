package client

import (
	"errors"
	"strings"
	"testing"
)

func TestManagerUnavailableStatusErrorUsesSpecMessage(t *testing.T) {
	err := managerUnavailableStatusError(503, []byte(`{"success":false,"error":{"code":"unavailable","message":"manager is draining"}}`))

	if !errors.Is(err, ErrManagerUnavailable) {
		t.Fatalf("error = %v, want ErrManagerUnavailable", err)
	}
	if !strings.Contains(err.Error(), "manager is draining") {
		t.Fatalf("error = %q, want spec message", err.Error())
	}
}

func TestManagerUnavailableStatusErrorFallsBackToBody(t *testing.T) {
	err := managerUnavailableStatusError(502, []byte(`plain error`))

	if !errors.Is(err, ErrManagerUnavailable) {
		t.Fatalf("error = %v, want ErrManagerUnavailable", err)
	}
	if !strings.Contains(err.Error(), "unexpected status code 502: plain error") {
		t.Fatalf("error = %q, want status and body", err.Error())
	}
}
