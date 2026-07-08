package volume

import (
	"bytes"
	"context"
	"testing"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
)

func TestValidateS3BackendConfigRequiresPerVolumeCredentials(t *testing.T) {
	err := ValidateS3BackendConfig(S3BackendConfig{
		Provider: S3ProviderAWS,
		Bucket:   "sandbox-data",
		Region:   "us-east-1",
	})
	if err == nil || err.Error() != "s3.access_key and s3.secret_key are required" {
		t.Fatalf("ValidateS3BackendConfig() error = %v, want required credentials", err)
	}
}

func TestValidateS3BackendConfigRequiresAWSRegionOrEndpoint(t *testing.T) {
	err := ValidateS3BackendConfig(S3BackendConfig{
		Provider:  S3ProviderAWS,
		Bucket:    "sandbox-data",
		AccessKey: "ak",
		SecretKey: "sk",
	})
	if err == nil || err.Error() != "s3.region or s3.endpoint_url is required for provider aws" {
		t.Fatalf("ValidateS3BackendConfig() error = %v, want region or endpoint error", err)
	}
}

func TestS3ObjectStoreConfigDoesNotInheritStorageProxyCredentials(t *testing.T) {
	got := S3ObjectStoreConfig(S3BackendConfig{
		Provider:  S3ProviderAWS,
		Bucket:    "user-bucket",
		Region:    "us-east-1",
		AccessKey: "user-ak",
		SecretKey: "user-sk",
	}, &config.StorageProxyConfig{
		S3Region:    "platform-region",
		S3Endpoint:  "https://platform.example.com",
		S3AccessKey: "platform-ak",
		S3SecretKey: "platform-sk",
	}, nil)

	if got.Region != "us-east-1" || got.Endpoint != "" {
		t.Fatalf("object store region/endpoint = %q/%q, want user region and no platform endpoint", got.Region, got.Endpoint)
	}
	if got.AccessKey != "user-ak" || got.SecretKey != "user-sk" {
		t.Fatalf("object store credentials = %q/%q, want per-volume credentials", got.AccessKey, got.SecretKey)
	}
}

func TestMarshalEncryptedS3BackendConfigEncryptsCredentials(t *testing.T) {
	codec := testS3BackendCredentialCodec(t)
	raw, err := MarshalEncryptedS3BackendConfig(context.Background(), "team-1", "vol-1", S3BackendConfig{
		Provider:     S3ProviderAWS,
		Bucket:       "user-bucket",
		Region:       "us-east-1",
		AccessKey:    "user-ak",
		SecretKey:    "user-sk",
		SessionToken: "session-token",
	}, codec)
	if err != nil {
		t.Fatalf("MarshalEncryptedS3BackendConfig() error = %v", err)
	}
	if bytes.Contains(raw, []byte("user-ak")) || bytes.Contains(raw, []byte("user-sk")) || bytes.Contains(raw, []byte("session-token")) {
		t.Fatalf("encrypted backend config leaked credentials: %s", raw)
	}
	stored, err := DecodeS3BackendConfig(raw)
	if err != nil {
		t.Fatalf("DecodeS3BackendConfig() error = %v", err)
	}
	if stored.AccessKey != "" || stored.SecretKey != "" || stored.SessionToken != "" {
		t.Fatalf("stored config contains plaintext credentials: %+v", stored)
	}
	if stored.EncryptedCredentials == nil {
		t.Fatal("stored config missing encrypted credentials")
	}
	decoded, err := DecodeS3BackendConfigWithCredentials(context.Background(), "team-1", "vol-1", raw, codec)
	if err != nil {
		t.Fatalf("DecodeS3BackendConfigWithCredentials() error = %v", err)
	}
	if decoded.AccessKey != "user-ak" || decoded.SecretKey != "user-sk" || decoded.SessionToken != "session-token" {
		t.Fatalf("decoded credentials = %q/%q/%q", decoded.AccessKey, decoded.SecretKey, decoded.SessionToken)
	}
}

func TestDecodeS3BackendConfigWithCredentialsRejectsWrongAAD(t *testing.T) {
	codec := testS3BackendCredentialCodec(t)
	raw, err := MarshalEncryptedS3BackendConfig(context.Background(), "team-1", "vol-1", S3BackendConfig{
		Provider:  S3ProviderAWS,
		Bucket:    "user-bucket",
		Region:    "us-east-1",
		AccessKey: "user-ak",
		SecretKey: "user-sk",
	}, codec)
	if err != nil {
		t.Fatalf("MarshalEncryptedS3BackendConfig() error = %v", err)
	}
	if _, err := DecodeS3BackendConfigWithCredentials(context.Background(), "team-2", "vol-1", raw, codec); err == nil {
		t.Fatal("DecodeS3BackendConfigWithCredentials() succeeded with wrong team id")
	}
}

func TestDecodeS3BackendConfigWithCredentialsRejectsPlaintextStoredConfig(t *testing.T) {
	_, err := DecodeS3BackendConfigWithCredentials(context.Background(), "team-1", "vol-1", []byte(`{
		"provider":"aws",
		"bucket":"user-bucket",
		"region":"us-east-1",
		"access_key":"stored-ak",
		"secret_key":"stored-sk"
	}`), testS3BackendCredentialCodec(t))
	if err == nil || err.Error() != "s3 backend encrypted credentials are required" {
		t.Fatalf("DecodeS3BackendConfigWithCredentials() error = %v, want encrypted credentials required", err)
	}
}

func testS3BackendCredentialCodec(t *testing.T) *S3BackendCredentialCodec {
	t.Helper()
	codec, err := NewS3BackendCredentialCodec("test", map[string][]byte{
		"test": []byte("01234567890123456789012345678901"),
	})
	if err != nil {
		t.Fatalf("NewS3BackendCredentialCodec() error = %v", err)
	}
	return codec
}
