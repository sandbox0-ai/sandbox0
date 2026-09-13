package objectstore

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

func TestCredentialPreparationReusesSigningCacheWithoutObjectRequests(t *testing.T) {
	var retrieves, requests atomic.Int32
	provider := aws.NewCredentialsCache(aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		retrieves.Add(1)
		return aws.Credentials{AccessKeyID: "test-key", SecretAccessKey: "test-secret", CanExpire: true, Expires: time.Now().Add(time.Hour)}, nil
	}))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requests.Add(1)
		if req.Method != http.MethodGet || req.Header.Get("Authorization") == "" {
			t.Error("expected a signed demand GET, not an initialization request")
		}
		w.Header().Set("Content-Length", "4")
		w.Header().Set("Content-Range", "bytes 0-3/4")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("data"))
	}))
	defer server.Close()
	base := &s3Store{provider: TypeOSS, bucket: "test-bucket", client: s3.New(s3.Options{
		Region: "test-region", Credentials: provider, BaseEndpoint: aws.String(server.URL), UsePathStyle: true,
	})}
	wrapped := &encryptedStore{store: &encryptedStore{store: base}}
	var workers sync.WaitGroup
	errorsOut := make(chan error, 18)
	for range 18 {
		workers.Add(1)
		go func() { defer workers.Done(); errorsOut <- PrepareCredentials(t.Context(), wrapped) }()
	}
	workers.Wait()
	close(errorsOut)
	for err := range errorsOut {
		require.NoError(t, err)
	}
	require.EqualValues(t, 1, retrieves.Load())
	require.Zero(t, requests.Load(), "preparation must not touch any bucket or object")
	require.NoError(t, base.Create(), "OSS bucket lifecycle remains infrastructure-owned")
	require.Zero(t, requests.Load())
	reader, err := base.GetContext(t.Context(), "demanded-object", 0, 4)
	require.NoError(t, err)
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, "data", string(got))
	require.EqualValues(t, 1, retrieves.Load(), "the actual signer must use the prepared cache")
	require.EqualValues(t, 1, requests.Load())
	provider.Invalidate()
	require.NoError(t, PrepareCredentials(t.Context(), wrapped))
	require.EqualValues(t, 2, retrieves.Load(), "SDK invalidation remains authoritative")
	require.EqualValues(t, 1, requests.Load())
}

func TestCredentialPreparationPreservesProviderFailureAndCancellation(t *testing.T) {
	failure := errors.New("credential endpoint unavailable")
	var calls atomic.Int32
	base := &s3Store{client: s3.New(s3.Options{Region: "test-region", Credentials: aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
		calls.Add(1)
		return aws.Credentials{}, failure
	})})}
	require.ErrorIs(t, PrepareCredentials(t.Context(), &encryptedStore{store: base}), failure)
	require.EqualValues(t, 1, calls.Load())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, PrepareCredentials(ctx, base), context.Canceled)
	require.ErrorIs(t, base.PrepareCredentials(ctx), context.Canceled)
	require.EqualValues(t, 1, calls.Load(), "canceled preparation must not retrieve credentials")
	require.Error(t, PrepareCredentials(nil, base))
	require.Error(t, base.PrepareCredentials(nil))
}

type preparationContextKey struct{}

func TestCredentialPreparationPropagatesContextAndHasNoPrivateCache(t *testing.T) {
	ctx := context.WithValue(t.Context(), preparationContextKey{}, "startup")
	var calls atomic.Int32
	base := &s3Store{client: s3.New(s3.Options{Region: "test-region", Credentials: aws.CredentialsProviderFunc(func(got context.Context) (aws.Credentials, error) {
		require.Equal(t, "startup", got.Value(preparationContextKey{}))
		calls.Add(1)
		return aws.Credentials{AccessKeyID: "test-key", SecretAccessKey: "test-secret"}, nil
	})})}
	for range 2 {
		require.NoError(t, PrepareCredentials(ctx, base))
	}
	require.EqualValues(t, 2, calls.Load(), "an uncached provider must not gain a second cache")
}

func TestCredentialPreparationSupportsAnonymousAndUnpreparedStores(t *testing.T) {
	base := &s3Store{client: s3.New(s3.Options{Region: "test-region", Credentials: aws.AnonymousCredentials{}})}
	require.NoError(t, PrepareCredentials(t.Context(), base))
	require.NoError(t, PrepareCredentials(t.Context(), NewMemoryStore(t.Name())))
	require.NoError(t, PrepareCredentials(t.Context(), &encryptedStore{store: NewMemoryStore(t.Name())}))
	require.Error(t, PrepareCredentials(t.Context(), nil))
	require.Error(t, PrepareCredentials(t.Context(), (*s3Store)(nil)))
	require.Error(t, PrepareCredentials(t.Context(), (*encryptedStore)(nil)))
	require.Error(t, PrepareCredentials(t.Context(), &s3Store{}))
	require.Error(t, PrepareCredentials(t.Context(), &encryptedStore{}))
}
