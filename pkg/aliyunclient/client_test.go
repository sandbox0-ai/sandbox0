package aliyunclient

import (
	"fmt"
	"sync"
	"testing"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/auth/credentials"
	"github.com/stretchr/testify/require"
)

type rotatingProvider struct {
	calls int
	value credentials.Credentials
}

func (p *rotatingProvider) GetCredentials() (*credentials.Credentials, error) {
	p.calls++
	p.value.SecurityToken = fmt.Sprintf("test-token-%d", p.calls)
	return &p.value, nil
}

func (*rotatingProvider) GetProviderName() string { return "test" }

func TestCredentialsRefreshRemainsSerializedAndPreviouslyIssuedValuesAreStable(t *testing.T) {
	source := &rotatingProvider{}
	provider := &synchronizedProvider{source: source}
	first, err := provider.GetCredentials()
	require.NoError(t, err)
	var wg sync.WaitGroup
	errors := make(chan error, 32)
	for range 32 {
		wg.Go(func() {
			_, err := provider.GetCredentials()
			errors <- err
		})
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	require.Equal(t, 33, source.calls)
	require.Equal(t, "test-token-1", first.SecurityToken)
	require.Equal(t, "test-token-33", source.value.SecurityToken)
}
