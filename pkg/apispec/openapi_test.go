package apispec

import (
	"slices"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

func loadOpenAPIDocument(t *testing.T) *openapi3.T {
	t.Helper()
	document, err := openapi3.NewLoader().LoadFromFile("openapi.yaml")
	if err != nil {
		t.Fatalf("load OpenAPI document: %v", err)
	}
	return document
}

func TestSandboxExpirationFieldsAreOptionalAndNullable(t *testing.T) {
	document := loadOpenAPIDocument(t)

	for _, schemaName := range []string{"Sandbox", "SandboxSummary", "RefreshResponse"} {
		schemaRef, ok := document.Components.Schemas[schemaName]
		if !ok || schemaRef.Value == nil {
			t.Fatalf("schema %q is missing", schemaName)
		}
		for _, fieldName := range []string{"expires_at", "hard_expires_at"} {
			fieldRef, ok := schemaRef.Value.Properties[fieldName]
			if !ok || fieldRef.Value == nil {
				t.Fatalf("schema %q field %q is missing", schemaName, fieldName)
			}
			if !fieldRef.Value.Nullable {
				t.Errorf("schema %q field %q must be nullable", schemaName, fieldName)
			}
			if slices.Contains(schemaRef.Value.Required, fieldName) {
				t.Errorf("schema %q field %q must be optional", schemaName, fieldName)
			}
		}
	}
}

func TestSandboxRuntimeIdentityIsRuntimeNeutral(t *testing.T) {
	document := loadOpenAPIDocument(t)

	for _, schemaName := range []string{"ClaimResponse", "Sandbox", "SandboxStatus"} {
		schemaRef, ok := document.Components.Schemas[schemaName]
		if !ok || schemaRef.Value == nil {
			t.Fatalf("schema %q is missing", schemaName)
		}
		if _, exists := schemaRef.Value.Properties["pod_name"]; exists {
			t.Errorf("schema %q must not expose Kubernetes pod_name", schemaName)
		}
		if _, exists := schemaRef.Value.Properties["runtime_id"]; !exists {
			t.Errorf("schema %q must expose runtime_id", schemaName)
		}
	}
}

func TestBearerOperationsDeclareUnauthorizedResponse(t *testing.T) {
	document := loadOpenAPIDocument(t)
	for path, item := range document.Paths.Map() {
		for method, operation := range item.Operations() {
			if operation.Security == nil || !usesSecurityScheme(*operation.Security, "bearerAuth") {
				continue
			}
			if operation.Responses.Status(401) == nil {
				t.Errorf("%s %s uses bearerAuth but does not declare a 401 response", method, path)
			}
		}
	}
}

func TestConcreteSuccessResponsesRequireData(t *testing.T) {
	document := loadOpenAPIDocument(t)
	for name, schemaRef := range document.Components.Schemas {
		if !strings.HasPrefix(name, "Success") || name == "SuccessEnvelope" {
			continue
		}
		if schemaRef.Value == nil {
			t.Errorf("schema %q is unresolved", name)
			continue
		}
		found := slices.Contains(schemaRef.Value.Required, "data")
		for _, branch := range schemaRef.Value.AllOf {
			if branch.Value != nil && slices.Contains(branch.Value.Required, "data") {
				found = true
			}
		}
		if !found {
			t.Errorf("schema %q must require data", name)
		}
	}
}

func TestClaimRequestRequiresTemplate(t *testing.T) {
	document := loadOpenAPIDocument(t)
	claim := document.Components.Schemas["ClaimRequest"]
	if claim == nil || claim.Value == nil {
		t.Fatal("ClaimRequest schema is missing")
	}
	if !slices.Contains(claim.Value.Required, "template") {
		t.Fatal("ClaimRequest.template must be required")
	}
}

func usesSecurityScheme(requirements openapi3.SecurityRequirements, scheme string) bool {
	for _, requirement := range requirements {
		if _, ok := requirement[scheme]; ok {
			return true
		}
	}
	return false
}
