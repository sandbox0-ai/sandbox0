package http

import (
	"net/http"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/functionruntime"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

func TestSanitizeFunctionRuntimeClaimHeadersClearsNonFunctionGatewayCaller(t *testing.T) {
	header := http.Header{}
	functionruntime.SetHeaders(header, functionruntime.Metadata{
		OwnerKind:                 functionruntime.OwnerKind,
		FunctionID:                "fn-1",
		FunctionRevisionID:        "rev-1",
		FunctionRuntimeInstanceID: "inst-1",
	})

	sanitizeFunctionRuntimeClaimHeaders(header, &internalauth.Claims{Caller: internalauth.ServiceRegionalGateway})

	if metadata := functionruntime.FromHeaders(header); metadata != nil {
		t.Fatalf("metadata headers were not cleared: %#v", metadata)
	}
}

func TestSanitizeFunctionRuntimeClaimHeadersPreservesFunctionGatewayCaller(t *testing.T) {
	header := http.Header{}
	functionruntime.SetHeaders(header, functionruntime.Metadata{
		OwnerKind:                 functionruntime.OwnerKind,
		FunctionID:                "fn-1",
		FunctionRevisionID:        "rev-1",
		FunctionRuntimeInstanceID: "inst-1",
	})

	sanitizeFunctionRuntimeClaimHeaders(header, &internalauth.Claims{Caller: internalauth.ServiceFunctionGateway})

	metadata := functionruntime.FromHeaders(header)
	if metadata == nil || metadata.FunctionID != "fn-1" {
		t.Fatalf("metadata headers were not preserved: %#v", metadata)
	}
}
