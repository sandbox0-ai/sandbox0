package registry

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
)

type awsProvider struct {
	cfg     config.RegistryAWSConfig
	secrets secretReader
}

func (p *awsProvider) GetPushCredentials(ctx context.Context, teamID string) (*Credential, error) {
	if p.cfg.Region == "" {
		return nil, fmt.Errorf("aws region is required")
	}
	if strings.TrimSpace(p.cfg.AccessKeySecret) == "" {
		return nil, fmt.Errorf("aws access key secret is required")
	}
	accessKeyKey := p.cfg.AccessKeyKey
	if accessKeyKey == "" {
		accessKeyKey = "accessKeyId"
	}
	secretKeyKey := p.cfg.SecretKeyKey
	if secretKeyKey == "" {
		secretKeyKey = "secretAccessKey"
	}

	accessKey, err := p.secrets.read(ctx, p.cfg.AccessKeySecret, accessKeyKey)
	if err != nil {
		return nil, fmt.Errorf("read aws access key: %w", err)
	}
	secretKey, err := p.secrets.read(ctx, p.cfg.AccessKeySecret, secretKeyKey)
	if err != nil {
		return nil, fmt.Errorf("read aws secret key: %w", err)
	}
	sessionToken := ""
	if strings.TrimSpace(p.cfg.SessionTokenKey) != "" {
		sessionToken, err = p.secrets.read(ctx, p.cfg.AccessKeySecret, p.cfg.SessionTokenKey)
		if err != nil {
			return nil, fmt.Errorf("read aws session token: %w", err)
		}
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(
		ctx,
		awsconfig.WithRegion(p.cfg.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, sessionToken)),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	client := ecr.NewFromConfig(awsCfg)
	input := &ecr.GetAuthorizationTokenInput{}
	if strings.TrimSpace(p.cfg.RegistryID) != "" {
		input.RegistryIds = []string{p.cfg.RegistryID}
	}
	resp, err := client.GetAuthorizationToken(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("get ecr auth token: %w", err)
	}
	if len(resp.AuthorizationData) == 0 || resp.AuthorizationData[0].AuthorizationToken == nil {
		return nil, fmt.Errorf("ecr auth token response is empty")
	}

	token := aws.ToString(resp.AuthorizationData[0].AuthorizationToken)
	decoded, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return nil, fmt.Errorf("decode ecr auth token: %w", err)
	}
	parts := strings.SplitN(string(decoded), ":", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid ecr auth token format")
	}
	registry := p.cfg.RegistryOverride
	if registry == "" && resp.AuthorizationData[0].ProxyEndpoint != nil {
		registry = aws.ToString(resp.AuthorizationData[0].ProxyEndpoint)
	}
	registry = normalizeRegistryHost(registry)
	if registry == "" {
		return nil, fmt.Errorf("registry host is required")
	}

	expiresAt := time.Time{}
	if resp.AuthorizationData[0].ExpiresAt != nil {
		expiresAt = aws.ToTime(resp.AuthorizationData[0].ExpiresAt)
	}

	return &Credential{
		Provider:     "aws",
		PushRegistry: registry,
		Username:     parts[0],
		Password:     parts[1],
		ExpiresAt:    timePtr(expiresAt),
	}, nil
}
