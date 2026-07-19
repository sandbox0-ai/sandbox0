package apispec

import (
	"context"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

func TestControlPlaneObjectLimitSchemas(t *testing.T) {
	document := loadObjectLimitOpenAPI(t)

	assertSchemaMaxLength(t, schemaProperty(t, document, "LoginRequest", "email"), 254)
	assertSchemaMaxLength(t, schemaProperty(t, document, "LoginRequest", "password"), 4096)
	assertSchemaMaxLength(t, schemaProperty(t, document, "RegisterRequest", "name"), 250)
	assertSchemaMaxLength(t, schemaProperty(t, document, "RegisterRequest", "home_region_id"), 128)
	assertSchemaMaxLength(t, schemaProperty(t, document, "RefreshRequest", "refresh_token"), 16384)
	assertSchemaMaxLength(t, schemaProperty(t, document, "UpdateUserRequest", "avatar_url"), 8192)
	assertSchemaMaxLength(t, schemaProperty(t, document, "CreateTeamRequest", "name"), 255)
	assertSchemaMaxLength(t, schemaProperty(t, document, "CreateTeamRequest", "slug"), 255)
	assertSchemaMaxLength(t, schemaProperty(t, document, "CreateRegionRequest", "id"), 128)
	assertSchemaMaxLength(
		t,
		schemaProperty(t, document, "CreateRegionRequest", "regional_gateway_url"),
		8192,
	)

	assertSchemaMaxLength(t, schemaProperty(t, document, "CreateAPIKeyRequest", "name"), 128)
	assertSchemaMaxLength(t, schemaProperty(t, document, "CreateSSHPublicKeyRequest", "name"), 128)
	assertSchemaMaxLength(t, schemaProperty(t, document, "CreateSSHPublicKeyRequest", "public_key"), 16384)

	assertSchemaMaxLength(t, schemaProperty(t, document, "CredentialSourceWriteRequest", "name"), 256)
	headerValues := schemaProperty(t, document, "StaticHeadersSourceSpec", "values")
	assertSchemaMaxProperties(t, headerValues, 128)
	if headerValues.AdditionalProperties.Schema == nil ||
		headerValues.AdditionalProperties.Schema.Value == nil {
		t.Fatal("StaticHeadersSourceSpec.values additionalProperties schema is missing")
	}
	assertSchemaMaxLength(t, headerValues.AdditionalProperties.Schema.Value, 32768)
	assertSchemaMaxLength(
		t,
		schemaProperty(t, document, "StaticTLSClientCertificateSourceSpec", "certificatePem"),
		131072,
	)
	assertSchemaMaxLength(
		t,
		schemaProperty(t, document, "StaticUsernamePasswordSourceSpec", "username"),
		1024,
	)
	assertSchemaMaxLength(
		t,
		schemaProperty(t, document, "StaticUsernamePasswordSourceSpec", "password"),
		65536,
	)
	assertSchemaMaxLength(
		t,
		schemaProperty(t, document, "StaticSSHPrivateKeySourceSpec", "passphrase"),
		65536,
	)

	assertSchemaMaxLength(t, schemaProperty(t, document, "SandboxTemplateSpec", "description"), 8192)
	assertSchemaMaxLength(t, schemaProperty(t, document, "SandboxTemplateSpec", "displayName"), 256)
	tags := schemaProperty(t, document, "SandboxTemplateSpec", "tags")
	assertSchemaMaxItems(t, tags, 64)
	if tags.Items == nil || tags.Items.Value == nil {
		t.Fatal("SandboxTemplateSpec.tags item schema is missing")
	}
	assertSchemaMaxLength(t, tags.Items.Value, 128)
	assertStringMapLimits(t, schemaProperty(t, document, "SandboxTemplateSpec", "envVars"), 256, 32768)

	assertSchemaMaxItems(t, schemaProperty(t, document, "ClaimRequest", "mounts"), 256)
	assertStringMapLimits(t, schemaProperty(t, document, "SandboxConfig", "env_vars"), 256, 32768)
	assertSchemaMaxItems(t, schemaProperty(t, document, "SandboxConfig", "services"), 256)
	assertSchemaMaxItems(
		t,
		schemaProperty(t, document, "SandboxNetworkPolicy", "credentialBindings"),
		64,
	)
	assertSchemaMaxItems(t, schemaProperty(t, document, "NetworkEgressPolicy", "trafficRules"), 256)
	assertSchemaMaxItems(t, schemaProperty(t, document, "HTTPMatch", "headers"), 256)
}

func TestCredentialSourcePublicSchemaDoesNotExposeStorageInternals(t *testing.T) {
	document := loadObjectLimitOpenAPI(t)
	schemaRef := document.Components.Schemas["CredentialSourceWriteRequest"]
	if schemaRef == nil || schemaRef.Value == nil {
		t.Fatal("CredentialSourceWriteRequest schema is missing")
	}
	for _, property := range []string{"storageKind", "externalRef"} {
		if _, ok := schemaRef.Value.Properties[property]; ok {
			t.Fatalf("CredentialSourceWriteRequest unexpectedly exposes %q", property)
		}
	}
}

func loadObjectLimitOpenAPI(t *testing.T) *openapi3.T {
	t.Helper()
	loader := openapi3.NewLoader()
	document, err := loader.LoadFromFile("openapi.yaml")
	if err != nil {
		t.Fatalf("load OpenAPI document: %v", err)
	}
	if err := document.Validate(context.Background()); err != nil {
		t.Fatalf("validate OpenAPI document: %v", err)
	}
	return document
}

func schemaProperty(
	t *testing.T,
	document *openapi3.T,
	schemaName, propertyName string,
) *openapi3.Schema {
	t.Helper()
	schemaRef := document.Components.Schemas[schemaName]
	if schemaRef == nil || schemaRef.Value == nil {
		t.Fatalf("%s schema is missing", schemaName)
	}
	propertyRef := schemaRef.Value.Properties[propertyName]
	if propertyRef == nil || propertyRef.Value == nil {
		t.Fatalf("%s.%s schema is missing", schemaName, propertyName)
	}
	return propertyRef.Value
}

func assertSchemaMaxLength(t *testing.T, schema *openapi3.Schema, want uint64) {
	t.Helper()
	if schema.MaxLength == nil || *schema.MaxLength != want {
		t.Fatalf("maxLength = %v, want %d", schema.MaxLength, want)
	}
}

func assertSchemaMaxItems(t *testing.T, schema *openapi3.Schema, want uint64) {
	t.Helper()
	if schema.MaxItems == nil || *schema.MaxItems != want {
		t.Fatalf("maxItems = %v, want %d", schema.MaxItems, want)
	}
}

func assertSchemaMaxProperties(t *testing.T, schema *openapi3.Schema, want uint64) {
	t.Helper()
	if schema.MaxProps == nil || *schema.MaxProps != want {
		t.Fatalf("maxProperties = %v, want %d", schema.MaxProps, want)
	}
}

func assertStringMapLimits(
	t *testing.T,
	schema *openapi3.Schema,
	maxProperties, maxValueLength uint64,
) {
	t.Helper()
	assertSchemaMaxProperties(t, schema, maxProperties)
	if schema.AdditionalProperties.Schema == nil ||
		schema.AdditionalProperties.Schema.Value == nil {
		t.Fatal("additionalProperties schema is missing")
	}
	assertSchemaMaxLength(t, schema.AdditionalProperties.Schema.Value, maxValueLength)
}
