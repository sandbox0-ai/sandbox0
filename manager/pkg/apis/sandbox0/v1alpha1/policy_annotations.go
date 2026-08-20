package v1alpha1

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// ParseNetworkPolicyFromAnnotation parses network policy spec from annotation JSON.
// Returns nil if the annotation is empty.
func ParseNetworkPolicyFromAnnotation(annotationValue string) (*NetworkPolicySpec, error) {
	if annotationValue == "" {
		return nil, nil
	}

	var spec NetworkPolicySpec
	if err := json.Unmarshal([]byte(annotationValue), &spec); err != nil {
		return nil, err
	}
	return &spec, nil
}

// ParseNetworkPolicyFromAnnotationStrict rejects unknown fields and trailing
// JSON so an authenticated policy digest has one understood schema.
func ParseNetworkPolicyFromAnnotationStrict(annotationValue string) (*NetworkPolicySpec, error) {
	if annotationValue == "" {
		return nil, nil
	}
	var spec NetworkPolicySpec
	decoder := json.NewDecoder(bytes.NewBufferString(annotationValue))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&spec); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("network policy contains trailing data")
	}
	return &spec, nil
}

// NetworkPolicyToAnnotation serializes network policy spec to annotation JSON.
func NetworkPolicyToAnnotation(spec *NetworkPolicySpec) (string, error) {
	if spec == nil {
		return "", nil
	}
	data, err := json.Marshal(spec)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
