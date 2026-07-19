package egressauth

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/resourceguard"
)

func TestCredentialSourceScalarBoundaries(t *testing.T) {
	request := &CredentialSourceWriteRequest{
		Name:         strings.Repeat("n", int(MaxCredentialSourceNameBytes)),
		ResolverKind: "static_username_password",
		Spec: CredentialSourceSecretSpec{
			StaticUsernamePassword: &StaticUsernamePasswordSourceSpec{
				Username: strings.Repeat("u", int(MaxCredentialUsernameBytes)),
				Password: strings.Repeat("p", int(MaxCredentialSecretBytes)),
			},
		},
	}
	if err := ValidateCredentialSourceWriteSize(request); err != nil {
		t.Fatalf("ValidateCredentialSourceWriteSize(boundary) error = %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*CredentialSourceWriteRequest)
	}{
		{
			name: "source name one byte over",
			mutate: func(value *CredentialSourceWriteRequest) {
				value.Name += "n"
			},
		},
		{
			name: "username one byte over",
			mutate: func(value *CredentialSourceWriteRequest) {
				value.Spec.StaticUsernamePassword.Username += "u"
			},
		},
		{
			name: "password one byte over",
			mutate: func(value *CredentialSourceWriteRequest) {
				value.Spec.StaticUsernamePassword.Password += "p"
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			copyRequest := *request
			usernamePassword := *request.Spec.StaticUsernamePassword
			copyRequest.Spec.StaticUsernamePassword = &usernamePassword
			tt.mutate(&copyRequest)
			if err := ValidateCredentialSourceWriteSize(&copyRequest); !resourceguard.IsTooLarge(err) {
				t.Fatalf("ValidateCredentialSourceWriteSize() error = %v, want TooLargeError", err)
			}
		})
	}
}

func TestCredentialHeaderCollectionBoundaries(t *testing.T) {
	values := make(map[string]string, MaxCredentialHeaderCount)
	for i := 0; i < MaxCredentialHeaderCount; i++ {
		values[fmt.Sprintf("header-%03d", i)] = "value"
	}
	spec := CredentialSourceSecretSpec{
		StaticHeaders: &StaticHeadersSourceSpec{Values: values},
	}
	if err := ValidateCredentialSourceSpecSize(spec); err != nil {
		t.Fatalf("ValidateCredentialSourceSpecSize(count boundary) error = %v", err)
	}
	values["over"] = "value"
	if err := ValidateCredentialSourceSpecSize(spec); !resourceguard.IsTooLarge(err) {
		t.Fatalf("ValidateCredentialSourceSpecSize(count over) error = %v, want TooLargeError", err)
	}

	aggregateValues := map[string]string{
		"a": strings.Repeat("x", 32767),
		"b": strings.Repeat("x", 32767),
		"c": strings.Repeat("x", 32767),
		"d": strings.Repeat("x", 32767),
	}
	aggregateSpec := CredentialSourceSecretSpec{
		StaticHeaders: &StaticHeadersSourceSpec{Values: aggregateValues},
	}
	if err := ValidateCredentialSourceSpecSize(aggregateSpec); err != nil {
		t.Fatalf("ValidateCredentialSourceSpecSize(aggregate boundary) error = %v", err)
	}
	aggregateValues["a"] += "x"
	if err := ValidateCredentialSourceSpecSize(aggregateSpec); !resourceguard.IsTooLarge(err) {
		t.Fatalf("ValidateCredentialSourceSpecSize(aggregate over) error = %v, want TooLargeError", err)
	}
}

func TestCredentialPEMAndCanonicalPlaintextLimits(t *testing.T) {
	singlePEM := CredentialSourceSecretSpec{
		StaticSSHPrivateKey: &StaticSSHPrivateKeySourceSpec{
			PrivateKeyPEM: strings.Repeat("k", int(MaxCredentialPEMBytes)),
		},
	}
	if err := ValidateCredentialSourceSpecSize(singlePEM); err != nil {
		t.Fatalf("ValidateCredentialSourceSpecSize(PEM boundary) error = %v", err)
	}
	singlePEM.StaticSSHPrivateKey.PrivateKeyPEM += "k"
	if err := ValidateCredentialSourceSpecSize(singlePEM); !resourceguard.IsTooLarge(err) {
		t.Fatalf("ValidateCredentialSourceSpecSize(PEM over) error = %v, want TooLargeError", err)
	}

	canonical := CredentialSourceSecretSpec{
		StaticTLSClientCertificate: &StaticTLSClientCertificateSourceSpec{
			CertificatePEM: strings.Repeat("c", int(MaxCredentialPEMBytes)),
			PrivateKeyPEM:  "k",
		},
	}
	payload, err := json.Marshal(canonical)
	if err != nil {
		t.Fatalf("marshal initial canonical spec: %v", err)
	}
	privateKeyBytes := 1 + int(MaxCredentialSourcePlaintextBytes) - len(payload)
	if privateKeyBytes <= 0 || privateKeyBytes > int(MaxCredentialPEMBytes) {
		t.Fatalf("computed private key length = %d, want within PEM limit", privateKeyBytes)
	}
	canonical.StaticTLSClientCertificate.PrivateKeyPEM = strings.Repeat("k", privateKeyBytes)
	payload, err = CanonicalCredentialSourceSpec(canonical)
	if err != nil {
		t.Fatalf("CanonicalCredentialSourceSpec(boundary) error = %v", err)
	}
	if int64(len(payload)) != MaxCredentialSourcePlaintextBytes {
		t.Fatalf("canonical payload length = %d, want %d", len(payload), MaxCredentialSourcePlaintextBytes)
	}
	canonical.StaticTLSClientCertificate.PrivateKeyPEM += "k"
	if _, err := CanonicalCredentialSourceSpec(canonical); !resourceguard.IsTooLarge(err) {
		t.Fatalf("CanonicalCredentialSourceSpec(one byte over) error = %v, want TooLargeError", err)
	}
}

func TestCredentialEnvelopeBoundaryAndOneByteOver(t *testing.T) {
	if err := ValidateCredentialEnvelope(
		make([]byte, MaxCredentialSourceEnvelopeBytes),
	); err != nil {
		t.Fatalf("ValidateCredentialEnvelope(boundary) error = %v", err)
	}
	if err := ValidateCredentialEnvelope(
		make([]byte, MaxCredentialSourceEnvelopeBytes+1),
	); !resourceguard.IsTooLarge(err) {
		t.Fatalf("ValidateCredentialEnvelope(one byte over) error = %v, want TooLargeError", err)
	}
}

func TestCredentialExternalReferenceBoundaries(t *testing.T) {
	ref := &CredentialSourceExternalRefSpec{
		Provider:   strings.Repeat("p", int(MaxCredentialExternalRefFieldBytes)),
		Connection: strings.Repeat("c", int(MaxCredentialExternalRefFieldBytes)),
		Mount:      strings.Repeat("m", int(MaxCredentialExternalRefFieldBytes)),
		Path:       strings.Repeat("x", int(MaxCredentialExternalRefPathBytes)),
		Version:    strings.Repeat("v", int(MaxCredentialExternalRefFieldBytes)),
		Fields:     map[string]string{"field": "value"},
	}
	if err := ValidateCredentialSourceExternalRefSize(ref); err != nil {
		t.Fatalf("ValidateCredentialSourceExternalRefSize(boundary) error = %v", err)
	}
	ref.Path += "x"
	if err := ValidateCredentialSourceExternalRefSize(ref); !resourceguard.IsTooLarge(err) {
		t.Fatalf("ValidateCredentialSourceExternalRefSize(path over) error = %v, want TooLargeError", err)
	}

	fields := make(map[string]string, MaxCredentialExternalRefFieldCount+1)
	for i := 0; i <= MaxCredentialExternalRefFieldCount; i++ {
		fields[fmt.Sprintf("field-%03d", i)] = "value"
	}
	if err := ValidateCredentialSourceExternalRefSize(
		&CredentialSourceExternalRefSpec{Fields: fields},
	); !resourceguard.IsTooLarge(err) {
		t.Fatalf("ValidateCredentialSourceExternalRefSize(field count over) error = %v, want TooLargeError", err)
	}
}

func TestCredentialBindingBoundaries(t *testing.T) {
	binding := CredentialBinding{}
	base, err := json.Marshal(binding)
	if err != nil {
		t.Fatalf("marshal empty binding: %v", err)
	}
	binding.Ref = strings.Repeat("r", int(MaxCredentialBindingBytes)-len(base))
	record := &BindingRecord{Bindings: []CredentialBinding{binding}}
	if err := ValidateBindingRecordSize(record); err != nil {
		t.Fatalf("ValidateBindingRecordSize(per-binding boundary) error = %v", err)
	}
	binding.Ref += "r"
	record.Bindings[0] = binding
	if err := ValidateBindingRecordSize(record); !resourceguard.IsTooLarge(err) {
		t.Fatalf("ValidateBindingRecordSize(per-binding over) error = %v, want TooLargeError", err)
	}

	record.Bindings = make([]CredentialBinding, MaxCredentialBindingCount)
	if err := ValidateBindingRecordSize(record); err != nil {
		t.Fatalf("ValidateBindingRecordSize(count boundary) error = %v", err)
	}
	record.Bindings = append(record.Bindings, CredentialBinding{})
	if err := ValidateBindingRecordSize(record); !resourceguard.IsTooLarge(err) {
		t.Fatalf("ValidateBindingRecordSize(count over) error = %v, want TooLargeError", err)
	}

	record.Bindings = make([]CredentialBinding, 9)
	for i := range record.Bindings {
		record.Bindings[i].Ref = strings.Repeat("r", 30<<10)
	}
	if err := ValidateBindingRecordSize(record); !resourceguard.IsTooLarge(err) {
		t.Fatalf("ValidateBindingRecordSize(aggregate over) error = %v, want TooLargeError", err)
	}
}

func TestCredentialLimitErrorDoesNotLeakSecret(t *testing.T) {
	secretMarker := "do-not-leak-credential-secret"
	spec := CredentialSourceSecretSpec{
		StaticUsernamePassword: &StaticUsernamePasswordSourceSpec{
			Password: strings.Repeat("p", int(MaxCredentialSecretBytes)) + secretMarker,
		},
	}
	err := ValidateCredentialSourceSpecSize(spec)
	if !resourceguard.IsTooLarge(err) {
		t.Fatalf("ValidateCredentialSourceSpecSize() error = %v, want TooLargeError", err)
	}
	if strings.Contains(err.Error(), secretMarker) {
		t.Fatalf("error leaked secret material: %v", err)
	}
}
