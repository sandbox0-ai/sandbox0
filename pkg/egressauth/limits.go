package egressauth

import (
	"fmt"

	"github.com/sandbox0-ai/sandbox0/pkg/resourceguard"
)

const (
	// MaxCredentialSourceRequestBytes is the public JSON body hard limit.
	MaxCredentialSourceRequestBytes int64 = 512 << 10
	// MaxCredentialSourcePlaintextBytes bounds canonical secret JSON.
	MaxCredentialSourcePlaintextBytes int64 = 256 << 10
	// MaxCredentialSourceEnvelopeBytes bounds encrypted or reference envelopes.
	MaxCredentialSourceEnvelopeBytes int64 = 384 << 10

	MaxCredentialHeaderCount                 = 128
	MaxCredentialHeaderNameBytes             = 1 << 10
	MaxCredentialHeaderValueBytes            = 32 << 10
	MaxCredentialHeadersAggregateBytes int64 = 128 << 10

	MaxCredentialPEMBytes          int64 = 128 << 10
	MaxCredentialPEMAggregateBytes int64 = 256 << 10
	MaxCredentialUsernameBytes     int64 = 1 << 10
	MaxCredentialSecretBytes       int64 = 64 << 10

	MaxCredentialSourceNameBytes        int64 = 256
	MaxCredentialExternalRefFieldBytes  int64 = 1 << 10
	MaxCredentialExternalRefPathBytes   int64 = 8 << 10
	MaxCredentialExternalRefFieldCount        = 128
	MaxCredentialExternalRefFieldsBytes int64 = 128 << 10

	MaxCredentialBindingCount        = 64
	MaxCredentialBindingBytes  int64 = 32 << 10
	MaxCredentialBindingsBytes int64 = 256 << 10
)

// ValidateCredentialSourceWriteSize validates every secret-bearing field
// before encryption, Vault access, or PostgreSQL persistence.
func ValidateCredentialSourceWriteSize(record *CredentialSourceWriteRequest) error {
	if record == nil {
		return fmt.Errorf("credential source record is required")
	}
	if err := resourceguard.String(
		"credential source name",
		record.Name,
		MaxCredentialSourceNameBytes,
	); err != nil {
		return err
	}
	if err := ValidateCredentialSourceSpecSize(record.Spec); err != nil {
		return err
	}
	if err := ValidateCredentialSourceExternalRefSize(record.ExternalRef); err != nil {
		return err
	}
	_, err := resourceguard.CanonicalJSON(
		"credential source request",
		record,
		MaxCredentialSourceRequestBytes,
	)
	return err
}

// ValidateCredentialSourceSpecSize validates one canonical secret spec.
func ValidateCredentialSourceSpecSize(spec CredentialSourceSecretSpec) error {
	if headers := spec.StaticHeaders; headers != nil {
		if err := resourceguard.Map(
			"credential headers",
			len(headers.Values),
			MaxCredentialHeaderCount,
		); err != nil {
			return err
		}
		var total int64
		for name, value := range headers.Values {
			if err := resourceguard.String(
				"credential header name",
				name,
				MaxCredentialHeaderNameBytes,
			); err != nil {
				return err
			}
			if err := resourceguard.String(
				"credential header value",
				value,
				MaxCredentialHeaderValueBytes,
			); err != nil {
				return err
			}
			total += int64(len(name) + len(value))
		}
		if err := resourceguard.Length(
			"credential headers",
			total,
			MaxCredentialHeadersAggregateBytes,
			"bytes",
		); err != nil {
			return err
		}
	}
	if tlsSpec := spec.StaticTLSClientCertificate; tlsSpec != nil {
		fields := []struct {
			name  string
			value string
		}{
			{name: "credential certificate PEM", value: tlsSpec.CertificatePEM},
			{name: "credential private key PEM", value: tlsSpec.PrivateKeyPEM},
			{name: "credential CA PEM", value: tlsSpec.CAPEM},
		}
		var total int64
		for _, field := range fields {
			if err := resourceguard.String(field.name, field.value, MaxCredentialPEMBytes); err != nil {
				return err
			}
			total += int64(len(field.value))
		}
		if err := resourceguard.Length(
			"credential PEM material",
			total,
			MaxCredentialPEMAggregateBytes,
			"bytes",
		); err != nil {
			return err
		}
	}
	if usernamePassword := spec.StaticUsernamePassword; usernamePassword != nil {
		if err := resourceguard.String(
			"credential username",
			usernamePassword.Username,
			MaxCredentialUsernameBytes,
		); err != nil {
			return err
		}
		if err := resourceguard.String(
			"credential password",
			usernamePassword.Password,
			MaxCredentialSecretBytes,
		); err != nil {
			return err
		}
	}
	if sshSpec := spec.StaticSSHPrivateKey; sshSpec != nil {
		if err := resourceguard.String(
			"credential SSH private key PEM",
			sshSpec.PrivateKeyPEM,
			MaxCredentialPEMBytes,
		); err != nil {
			return err
		}
		if err := resourceguard.String(
			"credential SSH passphrase",
			sshSpec.Passphrase,
			MaxCredentialSecretBytes,
		); err != nil {
			return err
		}
	}
	_, err := resourceguard.CanonicalJSON(
		"credential source plaintext",
		spec,
		MaxCredentialSourcePlaintextBytes,
	)
	return err
}

// ValidateCredentialSourceExternalRefSize bounds internal-only external
// reference fields and mappings.
func ValidateCredentialSourceExternalRefSize(ref *CredentialSourceExternalRefSpec) error {
	if ref == nil {
		return nil
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "credential external provider", value: ref.Provider},
		{name: "credential external connection", value: ref.Connection},
		{name: "credential external mount", value: ref.Mount},
		{name: "credential external version", value: ref.Version},
	} {
		if err := resourceguard.String(
			field.name,
			field.value,
			MaxCredentialExternalRefFieldBytes,
		); err != nil {
			return err
		}
	}
	if err := resourceguard.String(
		"credential external path",
		ref.Path,
		MaxCredentialExternalRefPathBytes,
	); err != nil {
		return err
	}
	if err := resourceguard.Map(
		"credential external fields",
		len(ref.Fields),
		MaxCredentialExternalRefFieldCount,
	); err != nil {
		return err
	}
	var total int64
	for name, value := range ref.Fields {
		if err := resourceguard.String(
			"credential external field name",
			name,
			MaxCredentialExternalRefFieldBytes,
		); err != nil {
			return err
		}
		if err := resourceguard.String(
			"credential external field value",
			value,
			MaxCredentialExternalRefFieldBytes,
		); err != nil {
			return err
		}
		total += int64(len(name) + len(value))
	}
	return resourceguard.Length(
		"credential external fields",
		total,
		MaxCredentialExternalRefFieldsBytes,
		"bytes",
	)
}

// CanonicalCredentialSourceSpec returns a size-checked compact JSON payload.
func CanonicalCredentialSourceSpec(spec CredentialSourceSecretSpec) ([]byte, error) {
	if err := ValidateCredentialSourceSpecSize(spec); err != nil {
		return nil, err
	}
	return resourceguard.CanonicalJSON(
		"credential source plaintext",
		spec,
		MaxCredentialSourcePlaintextBytes,
	)
}

// ValidateCredentialEnvelope validates an encrypted or reference payload.
func ValidateCredentialEnvelope(payload []byte) error {
	return resourceguard.Bytes(
		"credential source storage envelope",
		payload,
		MaxCredentialSourceEnvelopeBytes,
	)
}

// ValidateBindingRecordSize bounds materialized bindings before PostgreSQL.
func ValidateBindingRecordSize(record *BindingRecord) error {
	if record == nil {
		return fmt.Errorf("binding record is required")
	}
	if err := resourceguard.Slice(
		"credential bindings",
		len(record.Bindings),
		MaxCredentialBindingCount,
	); err != nil {
		return err
	}
	for i := range record.Bindings {
		if _, err := resourceguard.CanonicalJSON(
			fmt.Sprintf("credential binding %d", i),
			record.Bindings[i],
			MaxCredentialBindingBytes,
		); err != nil {
			return err
		}
	}
	_, err := resourceguard.CanonicalJSON(
		"credential bindings",
		record.Bindings,
		MaxCredentialBindingsBytes,
	)
	return err
}
