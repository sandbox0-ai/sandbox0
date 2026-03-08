package utils

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/getkin/kin-openapi/openapi3"
)

const defaultContentType = "application/json"

var (
	openAPISpecOnce sync.Once
	openAPISpec     *openapi3.T
	openAPISpecErr  error
)

// ContractT captures the testing methods needed by the contract helpers.
type ContractT interface {
	Helper()
	Fatalf(format string, args ...any)
}

// ValidateRequestExample validates a request payload against openapi.yaml.
func ValidateRequestExample(t ContractT, method, path, contentType string, payload any) {
	t.Helper()
	spec := loadOpenAPISpec(t)

	operation, err := findOperation(spec, method, path)
	if err != nil {
		t.Fatalf("resolve operation: %v", err)
	}
	if operation.RequestBody == nil || operation.RequestBody.Value == nil {
		if isEmptyPayload(payload) {
			return
		}
		t.Fatalf("request body not defined for %s %s", strings.ToUpper(method), path)
	}

	body := operation.RequestBody.Value
	if body.Required && isEmptyPayload(payload) {
		t.Fatalf("request body is required for %s %s", strings.ToUpper(method), path)
	}
	if !body.Required && isEmptyPayload(payload) {
		return
	}

	ct := normalizeContentType(contentType)
	media := body.Content.Get(ct)
	if media == nil || media.Schema == nil || media.Schema.Value == nil {
		t.Fatalf("request schema missing for %s %s (%s)", strings.ToUpper(method), path, ct)
	}

	value, err := normalizePayload(contentType, payload)
	if err != nil {
		t.Fatalf("normalize request payload: %v", err)
	}
	if err := media.Schema.Value.VisitJSON(value); err != nil {
		t.Fatalf("request payload does not match schema for %s %s (%s): %v", strings.ToUpper(method), path, ct, err)
	}
}

// ValidateResponseExample validates a response payload against openapi.yaml.
func ValidateResponseExample(t ContractT, method, path string, status int, contentType string, payload any) {
	t.Helper()
	spec := loadOpenAPISpec(t)

	operation, err := findOperation(spec, method, path)
	if err != nil {
		t.Fatalf("resolve operation: %v", err)
	}
	if operation.Responses == nil {
		t.Fatalf("responses not defined for %s %s", strings.ToUpper(method), path)
	}

	responseRef := operation.Responses.Status(status)
	if responseRef == nil {
		responseRef = operation.Responses.Default()
	}
	if responseRef == nil || responseRef.Value == nil {
		t.Fatalf("response %d not defined for %s %s", status, strings.ToUpper(method), path)
	}

	ct := normalizeContentType(contentType)
	if len(responseRef.Value.Content) == 0 {
		if isEmptyPayload(payload) {
			return
		}
		t.Fatalf("response schema missing for %s %s (%s, %d)", strings.ToUpper(method), path, ct, status)
	}

	media := responseRef.Value.Content.Get(ct)
	if media == nil || media.Schema == nil || media.Schema.Value == nil {
		t.Fatalf("response schema missing for %s %s (%s, %d)", strings.ToUpper(method), path, ct, status)
	}

	value, err := normalizePayload(contentType, payload)
	if err != nil {
		t.Fatalf("normalize response payload: %v", err)
	}
	if err := media.Schema.Value.VisitJSON(value); err != nil {
		t.Fatalf("response payload does not match schema for %s %s (%s, %d): %v", strings.ToUpper(method), path, ct, status, err)
	}
}

func loadOpenAPISpec(t ContractT) *openapi3.T {
	t.Helper()
	openAPISpecOnce.Do(func() {
		openAPISpec, openAPISpecErr = readOpenAPISpec()
	})
	if openAPISpecErr != nil {
		t.Fatalf("load openapi spec: %v", openAPISpecErr)
	}
	return openAPISpec
}

func readOpenAPISpec() (*openapi3.T, error) {
	specPath, err := resolveSpecPath()
	if err != nil {
		return nil, err
	}
	loader := openapi3.NewLoader()
	loader.IsExternalRefsAllowed = true

	doc, err := loader.LoadFromFile(specPath)
	if err != nil {
		return nil, err
	}
	if err := doc.Validate(loader.Context); err != nil {
		return nil, err
	}
	return doc, nil
}

func resolveSpecPath() (string, error) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("resolve caller path")
	}
	root := filepath.Dir(filename)
	for i := 0; i < 3; i++ {
		root = filepath.Dir(root)
	}
	if root == "" || root == string(filepath.Separator) {
		return "", fmt.Errorf("resolve infra root")
	}
	return filepath.Join(root, "pkg", "apispec", "openapi.yaml"), nil
}

func findOperation(spec *openapi3.T, method, path string) (*openapi3.Operation, error) {
	if spec == nil || spec.Paths == nil {
		return nil, fmt.Errorf("openapi spec not loaded")
	}
	if strings.TrimSpace(method) == "" || strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("method and path are required")
	}
	item := spec.Paths.Find(path)
	if item == nil {
		return nil, fmt.Errorf("path not found: %s", path)
	}
	operation := item.GetOperation(strings.ToUpper(method))
	if operation == nil {
		return nil, fmt.Errorf("operation not found: %s %s", strings.ToUpper(method), path)
	}
	return operation, nil
}

func normalizePayload(contentType string, payload any) (any, error) {
	if payload == nil {
		return nil, nil
	}
	if !isJSONContentType(contentType) {
		switch value := payload.(type) {
		case []byte:
			if len(value) == 0 {
				return nil, nil
			}
			return string(value), nil
		case string:
			if strings.TrimSpace(value) == "" {
				return nil, nil
			}
			return value, nil
		default:
			return payload, nil
		}
	}
	switch value := payload.(type) {
	case []byte:
		if len(value) == 0 {
			return nil, nil
		}
		var out any
		if err := json.Unmarshal(value, &out); err != nil {
			return nil, err
		}
		return out, nil
	case string:
		if strings.TrimSpace(value) == "" {
			return nil, nil
		}
		var out any
		if err := json.Unmarshal([]byte(value), &out); err != nil {
			return nil, err
		}
		return out, nil
	default:
		raw, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		var out any
		if err := json.Unmarshal(raw, &out); err != nil {
			return nil, err
		}
		return out, nil
	}
}

func isEmptyPayload(payload any) bool {
	if payload == nil {
		return true
	}
	switch value := payload.(type) {
	case []byte:
		return len(value) == 0
	case string:
		return strings.TrimSpace(value) == ""
	default:
		return false
	}
}

func isJSONContentType(contentType string) bool {
	contentType = strings.TrimSpace(strings.ToLower(contentType))
	return contentType == "" || strings.Contains(contentType, "json")
}

func normalizeContentType(contentType string) string {
	contentType = strings.TrimSpace(contentType)
	if contentType == "" {
		return defaultContentType
	}
	return contentType
}
