package egressauthstore

import (
	"context"
	"errors"
)

var ErrCredentialSourceInUse = errors.New("credential source is in use")
var ErrCredentialSourceResolverKindImmutable = errors.New("credential source resolver_kind is immutable")

// BindingStore is the shared manager/broker contract for effective sandbox bindings
// and credential source metadata.
type BindingStore interface {
	GetBindings(ctx context.Context, teamID, sandboxID string) (*BindingRecord, error)
	UpsertBindings(ctx context.Context, record *BindingRecord) error
	DeleteBindings(ctx context.Context, teamID, sandboxID string) error
	GetSourceByRef(ctx context.Context, teamID, ref string) (*CredentialSource, error)
	GetSourceVersion(ctx context.Context, sourceID, version int64) (*CredentialSourceVersion, error)
}

// SourceStore owns control-plane CRUD for credential sources.
type SourceStore interface {
	ListSourceMetadata(ctx context.Context, teamID string) ([]CredentialSourceMetadata, error)
	GetSourceMetadata(ctx context.Context, teamID, name string) (*CredentialSourceMetadata, error)
	PutSource(ctx context.Context, teamID string, record *CredentialSourceWriteRequest) (*CredentialSourceMetadata, error)
	DeleteSource(ctx context.Context, teamID, name string) error
}
