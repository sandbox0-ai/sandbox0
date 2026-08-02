package apispec

import (
	"slices"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

func TestSandboxExpirationFieldsAreOptionalAndNullable(t *testing.T) {
	document, err := openapi3.NewLoader().LoadFromFile("openapi.yaml")
	if err != nil {
		t.Fatalf("load OpenAPI document: %v", err)
	}

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
