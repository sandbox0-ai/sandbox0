package registry

import (
	"context"
	"fmt"
	"strings"
	"time"

	cr "github.com/aliyun/alibaba-cloud-sdk-go/services/cr_ee"
	"github.com/sandbox0-ai/infra/infra-operator/api/config"
)

type aliyunProvider struct {
	cfg     config.RegistryAliyunConfig
	secrets secretReader
}

func (p *aliyunProvider) GetPushCredentials(ctx context.Context, teamID string) (*Credential, error) {
	registry := normalizeRegistryHost(p.cfg.Registry)
	if registry == "" {
		return nil, fmt.Errorf("aliyun registry is required")
	}
	if p.cfg.Region == "" {
		return nil, fmt.Errorf("aliyun region is required")
	}
	if p.cfg.InstanceID == "" {
		return nil, fmt.Errorf("aliyun instanceId is required")
	}
	if strings.TrimSpace(p.cfg.CredentialsSecret) == "" {
		return nil, fmt.Errorf("aliyun credentials secret is required")
	}
	accessKeyKey := strings.TrimSpace(p.cfg.AccessKeyKey)
	if accessKeyKey == "" {
		accessKeyKey = "accessKeyId"
	}
	secretKeyKey := strings.TrimSpace(p.cfg.SecretKeyKey)
	if secretKeyKey == "" {
		secretKeyKey = "accessKeySecret"
	}
	accessKey, err := p.secrets.read(ctx, p.cfg.CredentialsSecret, accessKeyKey)
	if err != nil {
		return nil, fmt.Errorf("read aliyun access key: %w", err)
	}
	secretKey, err := p.secrets.read(ctx, p.cfg.CredentialsSecret, secretKeyKey)
	if err != nil {
		return nil, fmt.Errorf("read aliyun secret key: %w", err)
	}

	client, err := cr.NewClientWithAccessKey(p.cfg.Region, accessKey, secretKey)
	if err != nil {
		return nil, fmt.Errorf("create aliyun cr client: %w", err)
	}
	request := cr.CreateGetAuthorizationTokenRequest()
	request.Scheme = "https"
	request.InstanceId = p.cfg.InstanceID
	response, err := client.GetAuthorizationToken(request)
	if err != nil {
		return nil, fmt.Errorf("get aliyun authorization token: %w", err)
	}
	if response == nil {
		return nil, fmt.Errorf("aliyun authorization response is empty")
	}
	if !response.GetAuthorizationTokenIsSuccess {
		code := strings.TrimSpace(response.Code)
		if code == "" {
			code = "unknown_error"
		}
		return nil, fmt.Errorf("aliyun authorization request failed: %s", code)
	}
	expiresAt := time.Time{}
	if response.ExpireTime > 0 {
		expiresAt = time.UnixMilli(int64(response.ExpireTime))
	}

	return &Credential{
		Provider:  "aliyun",
		Registry:  registry,
		Username:  response.TempUsername,
		Password:  response.AuthorizationToken,
		ExpiresAt: expiresAt,
	}, nil
}
