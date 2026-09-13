package objectstore

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// PrepareCredentials resolves a provider's initial identity without accessing
// buckets or objects. S3-compatible stores reuse the SDK's existing credential
// cache; this creates no second cache or background refresher. Providers without
// this optional capability keep their existing initialization behavior.
// Success is not proof of object permissions, connectivity, or future validity.
func PrepareCredentials(ctx context.Context, store Store) error {
	if err := credentialPreparationContext(ctx); err != nil {
		return err
	}
	if store == nil {
		return fmt.Errorf("credential preparation requires an object store")
	}
	if preparer, ok := store.(interface{ PrepareCredentials(context.Context) error }); ok {
		return preparer.PrepareCredentials(ctx)
	}
	return nil
}

func credentialPreparationContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("credential preparation context is required")
	}
	return ctx.Err()
}

func (s *s3Store) PrepareCredentials(ctx context.Context) error {
	if err := credentialPreparationContext(ctx); err != nil {
		return err
	}
	if s == nil || s.client == nil {
		return fmt.Errorf("credential preparation requires an S3 client")
	}
	provider := s.client.Options().Credentials
	// The S3 client normalizes explicitly anonymous credentials to nil.
	// Preserve that existing unsigned-client contract without inventing identity.
	if provider == nil || aws.IsCredentialsProvider(provider, aws.AnonymousCredentials{}) {
		return nil
	}
	_, err := provider.Retrieve(ctx)
	return err
}

func (s *encryptedStore) PrepareCredentials(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("credential preparation requires an encrypted store")
	}
	return PrepareCredentials(ctx, s.store)
}
