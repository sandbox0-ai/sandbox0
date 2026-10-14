// Package providerinstance defines lightweight scoped provider receipts.
package providerinstance

import (
	"context"
	"errors"
	"regexp"
)

// Scope pins the provider namespace independently of Nomad and ESS membership.
type Scope struct {
	Provider   string `json:"provider"`
	AccountID  string `json:"account_id"`
	RegionID   string `json:"region_id"`
	InstanceID string `json:"instance_id"`
}

// Observation carries only the exact requested identity and provider receipts.
// False Present means an authenticated, explicit instance-not-found response;
// API failure, stopped instances, and missing ESS membership do not qualify.
type Observation struct {
	Scope             Scope  `json:"scope"`
	Present           bool   `json:"present"`
	RequestID         string `json:"request_id"`
	IdentityRequestID string `json:"identity_request_id"`
}

// Observer is a read-only provider boundary. Regional callers must separately
// fence the exact node incarnation, retain observations, and settle its writer.
type Observer interface {
	Observe(context.Context, Scope) (Observation, error)
}

var (
	canonicalRegion  = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)+$`)
	canonicalReceipt = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,256}$`)
)

func (o Observation) ValidateFor(scope Scope) error {
	if !canonicalReceipt.MatchString(scope.Provider) || !canonicalReceipt.MatchString(scope.AccountID) ||
		!canonicalRegion.MatchString(scope.RegionID) || len(scope.RegionID) > 64 ||
		!canonicalReceipt.MatchString(scope.InstanceID) || o.Scope != scope || !canonicalReceipt.MatchString(o.RequestID) ||
		!canonicalReceipt.MatchString(o.IdentityRequestID) {
		return errors.New("provider instance observation does not match its scope or receipts")
	}
	return nil
}
