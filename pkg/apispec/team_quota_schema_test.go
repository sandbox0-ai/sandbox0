package apispec

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

func TestTeamQuotaPolicyWriteRequestSchema(t *testing.T) {
	loader := openapi3.NewLoader()
	document, err := loader.LoadFromFile("openapi.yaml")
	if err != nil {
		t.Fatalf("load OpenAPI document: %v", err)
	}
	if err := document.Validate(context.Background()); err != nil {
		t.Fatalf("validate OpenAPI document: %v", err)
	}
	schemaRef := document.Components.Schemas["TeamQuotaPolicyWriteRequest"]
	if schemaRef == nil || schemaRef.Value == nil {
		t.Fatal("TeamQuotaPolicyWriteRequest schema is missing")
	}
	schema := schemaRef.Value
	if len(schema.Properties) != 0 || len(schema.Required) != 0 {
		t.Fatalf(
			"TeamQuotaPolicyWriteRequest must be a pure union, got properties=%v required=%v",
			schema.Properties,
			schema.Required,
		)
	}
	if len(schema.OneOf) != 3 {
		t.Fatalf("TeamQuotaPolicyWriteRequest oneOf branches = %d, want 3", len(schema.OneOf))
	}
	if schema.Discriminator == nil || schema.Discriminator.PropertyName != "kind" {
		t.Fatalf("TeamQuotaPolicyWriteRequest discriminator = %#v, want kind", schema.Discriminator)
	}
	for _, branch := range schema.OneOf {
		if branch == nil || branch.Value == nil {
			t.Fatal("TeamQuotaPolicyWriteRequest contains an unresolved union branch")
		}
		if branch.Value.AdditionalProperties.Has == nil ||
			*branch.Value.AdditionalProperties.Has {
			t.Fatalf("union branch %q must prohibit additional properties", branch.Ref)
		}
	}

	valid := []map[string]interface{}{
		{"kind": "capacity", "limit": int64(10)},
		{"kind": "concurrency", "limit": int64(10)},
		{"kind": "rate", "tokens": int64(10), "interval_ms": int64(1), "burst": int64(20)},
		{"kind": "rate", "tokens": int64(10), "interval_ms": int64(3_600_000), "burst": int64(20)},
	}
	for _, value := range valid {
		if err := schema.VisitJSON(value); err != nil {
			t.Errorf("schema rejected valid request %#v: %v", value, err)
		}
	}

	invalid := []map[string]interface{}{
		{"kind": "capacity"},
		{"kind": "capacity", "limit": int64(10), "tokens": int64(1)},
		{"kind": "capacity", "limit": int64(10), "unknown": true},
		{"kind": "concurrency", "limit": int64(9_007_199_254_740_992)},
		{"kind": "unknown", "limit": int64(10)},
		{"kind": "rate", "tokens": int64(10), "interval_ms": int64(1000)},
		{"kind": "rate", "limit": int64(10), "tokens": int64(10), "interval_ms": int64(1000), "burst": int64(20)},
		{"kind": "rate", "tokens": int64(10), "interval_ms": int64(0), "burst": int64(20)},
		{"kind": "rate", "tokens": int64(10), "interval_ms": int64(3_600_001), "burst": int64(20)},
	}
	for _, value := range invalid {
		if err := schema.VisitJSON(value); err == nil {
			t.Errorf("schema accepted invalid request %#v", value)
		}
	}
}

func TestTeamQuotaPolicyWriteRequestDocumentsRuntimeCrossFieldConstraint(t *testing.T) {
	loader := openapi3.NewLoader()
	document, err := loader.LoadFromFile("openapi.yaml")
	if err != nil {
		t.Fatalf("load OpenAPI document: %v", err)
	}
	schema := document.Components.Schemas["TeamQuotaPolicyWriteRequest"].Value
	constraints := fmt.Sprint(schema.Extensions["x-sandbox0-runtime-constraints"])
	if !strings.Contains(constraints, "burst must be greater than or equal to tokens") {
		t.Fatalf("runtime constraints = %q, want explicit burst >= tokens constraint", constraints)
	}
	if !strings.Contains(constraints, "kind must match the canonical kind of the path key") {
		t.Fatalf("runtime constraints = %q, want explicit key/kind constraint", constraints)
	}

	// OpenAPI 3.0 cannot compare two sibling numeric fields. Keep that
	// limitation explicit instead of representing it as an executable schema
	// rule that validators cannot enforce.
	value := map[string]interface{}{
		"kind":        "rate",
		"tokens":      int64(20),
		"interval_ms": int64(1000),
		"burst":       int64(10),
	}
	if err := schema.VisitJSON(value); err != nil {
		t.Fatalf("schema unexpectedly encoded the runtime-only cross-field constraint: %v", err)
	}
}

func TestAuthenticatedOperationsDocumentAdmissionResponses(t *testing.T) {
	loader := openapi3.NewLoader()
	document, err := loader.LoadFromFile("openapi.yaml")
	if err != nil {
		t.Fatalf("load OpenAPI document: %v", err)
	}
	if err := document.Validate(context.Background()); err != nil {
		t.Fatalf("validate OpenAPI document: %v", err)
	}

	for path, item := range document.Paths.Map() {
		for method, operation := range pathOperations(item) {
			if !usesSecurityScheme(operation, "bearerAuth") {
				continue
			}
			if operation.Responses == nil ||
				operation.Responses.Status(http.StatusTooManyRequests) == nil {
				t.Errorf("%s %s does not document admission response 429", method, path)
			}
			if operation.Responses == nil ||
				operation.Responses.Status(http.StatusServiceUnavailable) == nil {
				t.Errorf("%s %s does not document admission response 503", method, path)
			}
		}
	}
}

func pathOperations(item *openapi3.PathItem) map[string]*openapi3.Operation {
	if item == nil {
		return nil
	}
	return map[string]*openapi3.Operation{
		http.MethodDelete: item.Delete,
		http.MethodGet:    item.Get,
		http.MethodHead:   item.Head,
		http.MethodPatch:  item.Patch,
		http.MethodPost:   item.Post,
		http.MethodPut:    item.Put,
	}
}

func usesSecurityScheme(operation *openapi3.Operation, scheme string) bool {
	if operation == nil || operation.Security == nil {
		return false
	}
	for _, requirement := range *operation.Security {
		if _, ok := requirement[scheme]; ok {
			return true
		}
	}
	return false
}
