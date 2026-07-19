// Package tokenbucket provides token-bucket admission primitives.
//
// The package does not define quota ownership or failure policy. Callers that
// implement distributed team quotas should use RedisBucket and handle backend
// failures as quota-enforcement failures rather than silently failing open.
package tokenbucket
