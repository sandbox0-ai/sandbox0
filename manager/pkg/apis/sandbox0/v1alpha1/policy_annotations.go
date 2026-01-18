package v1alpha1

import (
	"encoding/json"
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

// ParseBandwidthPolicyFromAnnotation parses bandwidth policy spec from annotation JSON.
// Returns nil if the annotation is empty.
func ParseBandwidthPolicyFromAnnotation(annotationValue string) (*BandwidthPolicySpec, error) {
	if annotationValue == "" {
		return nil, nil
	}

	var spec BandwidthPolicySpec
	if err := json.Unmarshal([]byte(annotationValue), &spec); err != nil {
		return nil, err
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

// BandwidthPolicyToAnnotation serializes bandwidth policy spec to annotation JSON.
func BandwidthPolicyToAnnotation(spec *BandwidthPolicySpec) (string, error) {
	if spec == nil {
		return "", nil
	}
	data, err := json.Marshal(spec)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
