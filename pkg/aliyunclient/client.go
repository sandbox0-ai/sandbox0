// Package aliyunclient configures authenticated Alibaba Cloud control-plane clients.
package aliyunclient

import (
	"sync"
	"time"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/auth/credentials"
)

// Config bounds API calls and keeps signed requests on HTTPS. Mutations are not retried.
func Config() *sdk.Config {
	return sdk.NewConfig().WithScheme("HTTPS").WithTimeout(15 * time.Second)
}

// Credentials uses the SDK's refreshable chain, including automatic IMDSv2 role
// discovery. The legacy NewClientWithProvider entry point snapshots credentials
// and requires an explicit role environment variable even on an ECS instance.
func Credentials() credentials.CredentialsProvider {
	return &synchronizedProvider{source: credentials.NewDefaultCredentialsProvider()}
}

type synchronizedProvider struct {
	mu     sync.Mutex
	source credentials.CredentialsProvider
}

func (p *synchronizedProvider) GetCredentials() (*credentials.Credentials, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	value, err := p.source.GetCredentials()
	if value == nil {
		return nil, err
	}
	// Callers sign outside the lock; retain an immutable view across a refresh.
	copy := *value
	return &copy, err
}

func (p *synchronizedProvider) GetProviderName() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.source.GetProviderName()
}
