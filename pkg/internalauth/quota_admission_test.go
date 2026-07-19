package internalauth

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/guard"
)

var testQuotaAdmissionVersion = guard.Version{
	EnforcementEpoch: 1,
	RedisGeneration:  1,
}

func TestQuotaAdmissionProofBindsExactQueryAndCanonicalPath(t *testing.T) {
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/sandboxes/a%2Fb?limit=1",
		nil,
	)
	proof, err := NewQuotaAdmissionProof(
		QuotaAdmissionClassEdgeAdmitted,
		request,
		"team-a",
		"operation-a",
		"request-a",
		ServiceRegionalGateway,
		[]coreteamquota.Key{
			coreteamquota.KeyNetworkIngressBytes,
			coreteamquota.KeyAPIRequests,
		},
		testQuotaAdmissionVersion,
	)
	if err != nil {
		t.Fatalf("NewQuotaAdmissionProof() error = %v", err)
	}
	claims := &Claims{
		Caller: ServiceRegionalGateway,
		TeamID: "team-a",
		Audit: &AuditContext{
			OperationID: "operation-a",
			RequestID:   "request-a",
			Origin:      ServiceRegionalGateway,
		},
		QuotaAdmissionProof: proof,
	}
	if !proof.MatchesRequest(claims, request) {
		t.Fatal("exact request did not match")
	}
	changedQuery := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/sandboxes/a%2Fb?limit=200&offset=0",
		nil,
	)
	if proof.MatchesRequest(claims, changedQuery) {
		t.Fatal("query-only change unexpectedly matched")
	}
	if proof.Path != "/api/v1/sandboxes/a%2Fb" {
		t.Fatalf("proof.Path = %q, want escaped path", proof.Path)
	}
	changedMethod := changedQuery.Clone(changedQuery.Context())
	changedMethod.Method = http.MethodDelete
	if proof.MatchesRequest(claims, changedMethod) {
		t.Fatal("method mismatch unexpectedly matched")
	}
	changedPath := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/sandboxes/other",
		nil,
	)
	if proof.MatchesRequest(claims, changedPath) {
		t.Fatal("path mismatch unexpectedly matched")
	}
	claims.ExpiresAt = jwt.NewNumericDate(
		time.UnixMilli(proof.ExpiresAtMS).Add(-time.Second),
	)
	if proof.MatchesRequest(claims, request) {
		t.Fatal("proof outliving its carrying token unexpectedly matched")
	}
}

func TestQuotaAdmissionProofBindsReplayableBodyAndTransportSemantics(t *testing.T) {
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/sandboxes?source=edge",
		bytes.NewReader([]byte("alpha")),
	)
	request.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader([]byte("alpha"))), nil
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Content-Encoding", "identity")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Range", "bytes=0-4")
	request.Header.Set("Idempotency-Key", "create-a")
	request.Header.Set("Last-Event-ID", "event-10")
	proof, err := NewQuotaAdmissionProof(
		QuotaAdmissionClassEdgeAdmitted,
		request,
		"team-a",
		"operation-a",
		"request-a",
		ServiceRegionalGateway,
		[]coreteamquota.Key{coreteamquota.KeyAPIRequests},
		testQuotaAdmissionVersion,
	)
	if err != nil {
		t.Fatalf("NewQuotaAdmissionProof() error = %v", err)
	}
	if proof.BodySHA256 == "" {
		t.Fatal("bounded replayable body was not digested")
	}
	claims := &Claims{
		Caller: ServiceRegionalGateway,
		TeamID: "team-a",
		Audit: &AuditContext{
			OperationID: "operation-a",
			RequestID:   "request-a",
			Origin:      ServiceRegionalGateway,
		},
	}

	exact := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/sandboxes?source=edge",
		bytes.NewReader([]byte("alpha")),
	)
	exact.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader([]byte("alpha"))), nil
	}
	exact.Header = request.Header.Clone()
	if !proof.MatchesRequest(claims, exact) {
		t.Fatal("exact replayable request did not match")
	}
	changedBody := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/sandboxes?source=edge",
		bytes.NewReader([]byte("bravo")),
	)
	changedBody.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader([]byte("bravo"))), nil
	}
	changedBody.Header = request.Header.Clone()
	if proof.MatchesRequest(claims, changedBody) {
		t.Fatal("same-length body change unexpectedly matched")
	}
	for _, test := range []struct {
		name  string
		value string
	}{
		{name: "Content-Encoding", value: "gzip"},
		{name: "Idempotency-Key", value: "create-b"},
		{name: "Last-Event-ID", value: "event-20"},
	} {
		changedHeader := exact.Clone(exact.Context())
		changedHeader.Header = exact.Header.Clone()
		changedHeader.Header.Set(test.name, test.value)
		if proof.MatchesRequest(claims, changedHeader) {
			t.Fatalf("%s change unexpectedly matched", test.name)
		}
	}
}

func TestQuotaAdmissionProofRejectsUnknownKey(t *testing.T) {
	_, err := NewQuotaAdmissionProof(
		QuotaAdmissionClassEdgeAdmitted,
		httptest.NewRequest(http.MethodGet, "/api/v1/sandboxes", nil),
		"team-a",
		"operation-a",
		"request-a",
		ServiceRegionalGateway,
		[]coreteamquota.Key{"future_unknown_key"},
		testQuotaAdmissionVersion,
	)
	if err == nil {
		t.Fatal("unknown key unexpectedly accepted")
	}
	_, err = NewQuotaAdmissionProof(
		QuotaAdmissionClassEdgeAdmitted,
		httptest.NewRequest(http.MethodGet, "/api/v1/sandboxes", nil),
		"team-a",
		"operation-a",
		"request-a",
		ServiceRegionalGateway,
		[]coreteamquota.Key{coreteamquota.KeyAPIRequests},
		guard.Version{},
	)
	if err == nil {
		t.Fatal("missing policy version unexpectedly accepted")
	}
}

func TestQuotaAdmissionProofJSONUsesKeysWithoutLegacyAlias(t *testing.T) {
	proof, err := NewQuotaAdmissionProof(
		QuotaAdmissionClassEdgeAdmitted,
		httptest.NewRequest(http.MethodGet, "/api/v1/sandboxes", nil),
		"team-a",
		"operation-a",
		"request-a",
		ServiceRegionalGateway,
		[]coreteamquota.Key{coreteamquota.KeyAPIRequests},
		testQuotaAdmissionVersion,
	)
	if err != nil {
		t.Fatalf("NewQuotaAdmissionProof() error = %v", err)
	}
	payload, err := json.Marshal(proof)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if _, ok := fields["keys"]; !ok {
		t.Fatalf("proof payload = %s, want keys field", payload)
	}
	if _, ok := fields["dimensions"]; ok {
		t.Fatalf("proof payload = %s, legacy dimensions field is present", payload)
	}

	legacyPayload := []byte(strings.Replace(
		string(payload),
		`"keys"`,
		`"dimensions"`,
		1,
	))
	var legacyProof QuotaAdmissionProof
	if err := json.Unmarshal(legacyPayload, &legacyProof); err != nil {
		t.Fatalf("unmarshal legacy proof: %v", err)
	}
	if err := legacyProof.Validate(); err == nil {
		t.Fatal("legacy dimensions field unexpectedly validated")
	}
}

func TestGeneratorSignsAndClonesQuotaAdmissionProof(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/v1/sandboxes", nil)
	proof, err := NewQuotaAdmissionProof(
		QuotaAdmissionClassEdgeAdmitted,
		request,
		"team-a",
		"operation-a",
		"request-a",
		ServiceRegionalGateway,
		[]coreteamquota.Key{coreteamquota.KeyAPIRequests},
		testQuotaAdmissionVersion,
	)
	if err != nil {
		t.Fatalf("NewQuotaAdmissionProof() error = %v", err)
	}
	generator := NewGenerator(GeneratorConfig{
		Caller:     ServiceRegionalGateway,
		PrivateKey: testPrivateKey,
		TTL:        10 * time.Second,
	})
	token, err := generator.Generate(
		ServiceClusterGateway,
		"team-a",
		"user-a",
		GenerateOptions{
			Audit: &AuditContext{
				OperationID: "operation-a",
				RequestID:   "request-a",
				Origin:      ServiceRegionalGateway,
			},
			QuotaAdmissionProof: proof,
		},
	)
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	proof.Keys[0] = coreteamquota.KeyNetworkEgressBytes

	claims, err := NewValidator(ValidatorConfig{
		Target:    ServiceClusterGateway,
		PublicKey: testPublicKey,
	}).Validate(token)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if got, want := claims.QuotaAdmissionProof.Keys, []coreteamquota.Key{coreteamquota.KeyAPIRequests}; !reflect.DeepEqual(got, want) {
		t.Fatalf("signed keys = %v, want %v", got, want)
	}
	if claims.QuotaAdmissionProof.ProofID == "" ||
		claims.QuotaAdmissionProof.ProofID != proof.ProofID {
		t.Fatalf(
			"signed proof ID = %q, want %q",
			claims.QuotaAdmissionProof.ProofID,
			proof.ProofID,
		)
	}
	if claims.QuotaAdmissionProof.ExpiresAtMS > claims.ExpiresAt.UnixMilli() {
		t.Fatalf(
			"proof expiry = %d, token expiry = %d",
			claims.QuotaAdmissionProof.ExpiresAtMS,
			claims.ExpiresAt.UnixMilli(),
		)
	}
	if got := claims.QuotaAdmissionProof.ExpiresAtMS -
		claims.QuotaAdmissionProof.IssuedAtMS; got > coreteamquota.MaxAdmissionProofLifetime.Milliseconds() {
		t.Fatalf("proof lifetime = %dms, want at most %s", got, coreteamquota.MaxAdmissionProofLifetime)
	}
}

func TestInvalidQuotaProofDoesNotInvalidateAuthentication(t *testing.T) {
	generator := NewGenerator(GeneratorConfig{
		Caller:     ServiceRegionalGateway,
		PrivateKey: testPrivateKey,
	})
	token, err := generator.Generate(
		ServiceClusterGateway,
		"team-a",
		"user-a",
		GenerateOptions{
			Audit: &AuditContext{
				OperationID: "operation-a",
				RequestID:   "request-a",
				Origin:      ServiceRegionalGateway,
			},
			QuotaAdmissionProof: &QuotaAdmissionProof{
				Class:       QuotaAdmissionClassEdgeAdmitted,
				TeamID:      "team-a",
				Method:      http.MethodGet,
				Path:        "/api/v1/sandboxes",
				OperationID: "operation-a",
				RequestID:   "request-a",
				Origin:      ServiceRegionalGateway,
				Keys:        []coreteamquota.Key{"unknown"},
			},
		},
	)
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	claims, err := NewValidator(ValidatorConfig{
		Target:    ServiceClusterGateway,
		PublicKey: testPublicKey,
	}).Validate(token)
	if err != nil {
		t.Fatalf("invalid proof changed authentication result: %v", err)
	}
	if claims.QuotaAdmissionProof.Validate() == nil {
		t.Fatal("invalid signed proof unexpectedly validated")
	}
}
