package licensing

import (
	"fmt"

	"github.com/sandbox0-ai/infra/pkg/license"
)

type Feature string

const (
	FeatureMultiCluster Feature = "multi_cluster"
	FeatureSSO          Feature = "sso"
)

// Entitlements exposes licensed feature checks without leaking license internals.
type Entitlements interface {
	Enabled(feature Feature) bool
	Require(feature Feature) error
}

type FeatureNotLicensedError struct {
	Feature Feature
	Cause   error
}

func (e *FeatureNotLicensedError) Error() string {
	if e.Cause == nil {
		return fmt.Sprintf("feature %q is not licensed", e.Feature)
	}
	return fmt.Sprintf("feature %q is not licensed: %v", e.Feature, e.Cause)
}

func (e *FeatureNotLicensedError) Unwrap() error {
	return e.Cause
}

type fileEntitlements struct {
	checker *license.Checker
	loadErr error
}

// LoadFileEntitlements loads a signed enterprise license once and serves feature checks.
func LoadFileEntitlements(path string) Entitlements {
	checker, err := license.LoadFromFile(path)
	return &fileEntitlements{
		checker: checker,
		loadErr: err,
	}
}

func (e *fileEntitlements) Enabled(feature Feature) bool {
	if e == nil || e.loadErr != nil || e.checker == nil {
		return false
	}
	return e.checker.HasFeature(string(feature))
}

func (e *fileEntitlements) Require(feature Feature) error {
	if e.Enabled(feature) {
		return nil
	}
	return &FeatureNotLicensedError{
		Feature: feature,
		Cause:   e.loadErr,
	}
}

type staticEntitlements struct {
	features map[Feature]struct{}
}

func NewStaticEntitlements(features ...Feature) Entitlements {
	set := make(map[Feature]struct{}, len(features))
	for _, feature := range features {
		set[feature] = struct{}{}
	}
	return &staticEntitlements{features: set}
}

func (e *staticEntitlements) Enabled(feature Feature) bool {
	if e == nil {
		return false
	}
	_, ok := e.features[feature]
	return ok
}

func (e *staticEntitlements) Require(feature Feature) error {
	if e.Enabled(feature) {
		return nil
	}
	return &FeatureNotLicensedError{Feature: feature}
}
