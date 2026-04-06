package registry

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
	ecrtypes "github.com/aws/aws-sdk-go-v2/service/ecr/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
)

type awsECRAPI interface {
	DescribeRepositories(context.Context, *ecr.DescribeRepositoriesInput, ...func(*ecr.Options)) (*ecr.DescribeRepositoriesOutput, error)
	CreateRepository(context.Context, *ecr.CreateRepositoryInput, ...func(*ecr.Options)) (*ecr.CreateRepositoryOutput, error)
	GetAuthorizationToken(context.Context, *ecr.GetAuthorizationTokenInput, ...func(*ecr.Options)) (*ecr.GetAuthorizationTokenOutput, error)
}

type awsProvider struct {
	cfg     config.RegistryAWSConfig
	secrets secretReader
}

func (p *awsProvider) GetPushCredentials(ctx context.Context, req PushCredentialsRequest) (*Credential, error) {
	if p.cfg.Region == "" {
		return nil, fmt.Errorf("aws region is required")
	}
	accessKey := strings.TrimSpace(p.cfg.AccessKeyID)
	secretKey := strings.TrimSpace(p.cfg.SecretAccessKey)
	sessionToken := strings.TrimSpace(p.cfg.SessionToken)
	if accessKey == "" || secretKey == "" {
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

		var err error
		accessKey, err = p.secrets.read(ctx, p.cfg.AccessKeySecret, accessKeyKey)
		if err != nil {
			return nil, fmt.Errorf("read aws access key: %w", err)
		}
		secretKey, err = p.secrets.read(ctx, p.cfg.AccessKeySecret, secretKeyKey)
		if err != nil {
			return nil, fmt.Errorf("read aws secret key: %w", err)
		}
		if strings.TrimSpace(p.cfg.SessionTokenKey) != "" {
			sessionToken, err = p.secrets.read(ctx, p.cfg.AccessKeySecret, p.cfg.SessionTokenKey)
			if err != nil {
				return nil, fmt.Errorf("read aws session token: %w", err)
			}
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

	registryID := strings.TrimSpace(p.cfg.RegistryID)
	registry := normalizeRegistryHost(p.cfg.RegistryOverride)
	targetRepository, err := resolveAWSTargetRepository(req.TeamID, req.TargetImage)
	if err != nil {
		return nil, err
	}

	resolvedRegistryID := registryID
	if strings.TrimSpace(p.cfg.AssumeRoleARN) != "" {
		awsCfg, resolvedRegistryID, err = p.assumeRoleForPush(ctx, awsCfg, req, registry, registryID, targetRepository)
		if err != nil {
			return nil, err
		}
	} else if targetRepository != "" {
		resolvedRegistryID, err = p.resolveRegistryID(ctx, awsCfg, registry, registryID)
		if err != nil {
			return nil, err
		}
	}

	client := p.newECRClient(awsCfg)
	if targetRepository != "" {
		if err := ensureAWSECRRepository(ctx, client, resolvedRegistryID, targetRepository, req.TeamID); err != nil {
			return nil, err
		}
	}

	input := &ecr.GetAuthorizationTokenInput{}
	if resolvedRegistryID != "" {
		input.RegistryIds = []string{resolvedRegistryID}
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

func (p *awsProvider) newECRClient(cfg aws.Config) awsECRAPI {
	return ecr.NewFromConfig(cfg)
}

func (p *awsProvider) assumeRoleForPush(ctx context.Context, baseCfg aws.Config, req PushCredentialsRequest, registryHost, registryID, targetRepository string) (aws.Config, string, error) {
	if strings.TrimSpace(req.TeamID) == "" {
		return aws.Config{}, "", fmt.Errorf("aws assume role requires team id")
	}

	resolvedRegistryID, err := p.resolveRegistryID(ctx, baseCfg, registryHost, registryID)
	if err != nil {
		return aws.Config{}, "", err
	}
	policy, err := buildAWSECRSessionPolicy(p.cfg.Region, resolvedRegistryID, req.TeamID, targetRepository)
	if err != nil {
		return aws.Config{}, "", err
	}

	stsClient := sts.NewFromConfig(baseCfg)
	input := &sts.AssumeRoleInput{
		RoleArn:         aws.String(strings.TrimSpace(p.cfg.AssumeRoleARN)),
		RoleSessionName: aws.String(buildAWSRegistrySessionName(req.TeamID)),
		Policy:          aws.String(policy),
	}
	if strings.TrimSpace(p.cfg.ExternalID) != "" {
		input.ExternalId = aws.String(strings.TrimSpace(p.cfg.ExternalID))
	}
	resp, err := stsClient.AssumeRole(ctx, input)
	if err != nil {
		return aws.Config{}, "", fmt.Errorf("assume aws registry role: %w", err)
	}
	if resp.Credentials == nil {
		return aws.Config{}, "", fmt.Errorf("assume aws registry role returned no credentials")
	}

	assumed := baseCfg
	assumed.Region = p.cfg.Region
	assumed.Credentials = credentials.NewStaticCredentialsProvider(
		aws.ToString(resp.Credentials.AccessKeyId),
		aws.ToString(resp.Credentials.SecretAccessKey),
		aws.ToString(resp.Credentials.SessionToken),
	)
	return assumed, resolvedRegistryID, nil
}

func (p *awsProvider) resolveRegistryID(ctx context.Context, awsCfg aws.Config, registryHost, registryID string) (string, error) {
	if value := strings.TrimSpace(registryID); value != "" {
		return value, nil
	}
	if value := parseAWSRegistryIDFromHost(registryHost); value != "" {
		return value, nil
	}
	if value := parseAWSAccountIDFromARN(p.cfg.AssumeRoleARN); value != "" {
		return value, nil
	}

	resp, err := sts.NewFromConfig(awsCfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return "", fmt.Errorf("resolve aws registry account id: %w", err)
	}
	if accountID := strings.TrimSpace(aws.ToString(resp.Account)); accountID != "" {
		return accountID, nil
	}
	return "", fmt.Errorf("resolve aws registry account id: empty account id")
}

func buildAWSECRSessionPolicy(region, registryID, teamID, repository string) (string, error) {
	if strings.TrimSpace(region) == "" {
		return "", fmt.Errorf("aws region is required for registry session policy")
	}
	if strings.TrimSpace(registryID) == "" {
		return "", fmt.Errorf("aws registry id is required for registry session policy")
	}
	if strings.TrimSpace(repository) != "" {
		return buildAWSECRRepositorySessionPolicy(region, registryID, repository)
	}

	prefix := naming.TeamImageRepositoryPrefix(teamID)
	if prefix == "" {
		return "", fmt.Errorf("team id is required for registry session policy")
	}

	partition := awsPartitionForRegion(region)
	repoBaseARN := fmt.Sprintf("arn:%s:ecr:%s:%s:repository/%s", partition, region, registryID, prefix)
	policy := map[string]any{
		"Version": "2012-10-17",
		"Statement": []map[string]any{
			{
				"Effect":   "Allow",
				"Action":   []string{"ecr:GetAuthorizationToken"},
				"Resource": "*",
			},
			{
				"Effect": "Allow",
				"Action": []string{
					"ecr:BatchCheckLayerAvailability",
					"ecr:CompleteLayerUpload",
					"ecr:InitiateLayerUpload",
					"ecr:PutImage",
					"ecr:UploadLayerPart",
				},
				"Resource": []string{repoBaseARN, repoBaseARN + "/*"},
			},
		},
	}
	data, err := json.Marshal(policy)
	if err != nil {
		return "", fmt.Errorf("marshal aws registry session policy: %w", err)
	}
	return string(data), nil
}

func buildAWSECRRepositorySessionPolicy(region, registryID, repository string) (string, error) {
	trimmedRepository := strings.Trim(strings.TrimSpace(repository), "/")
	if trimmedRepository == "" {
		return "", fmt.Errorf("aws repository is required for registry session policy")
	}
	partition := awsPartitionForRegion(region)
	repoARN := fmt.Sprintf("arn:%s:ecr:%s:%s:repository/%s", partition, region, registryID, trimmedRepository)
	policy := map[string]any{
		"Version": "2012-10-17",
		"Statement": []map[string]any{
			{
				"Effect":   "Allow",
				"Action":   []string{"ecr:GetAuthorizationToken"},
				"Resource": "*",
			},
			{
				"Effect": "Allow",
				"Action": []string{
					"ecr:CreateRepository",
					"ecr:DescribeRepositories",
					"ecr:TagResource",
				},
				"Resource": repoARN,
			},
			{
				"Effect": "Allow",
				"Action": []string{
					"ecr:BatchCheckLayerAvailability",
					"ecr:CompleteLayerUpload",
					"ecr:InitiateLayerUpload",
					"ecr:PutImage",
					"ecr:UploadLayerPart",
				},
				"Resource": repoARN,
			},
		},
	}
	data, err := json.Marshal(policy)
	if err != nil {
		return "", fmt.Errorf("marshal aws registry session policy: %w", err)
	}
	return string(data), nil
}

func ensureAWSECRRepository(ctx context.Context, client awsECRAPI, registryID, repositoryName, teamID string) error {
	trimmedRepository := strings.Trim(strings.TrimSpace(repositoryName), "/")
	if trimmedRepository == "" {
		return nil
	}

	describeInput := &ecr.DescribeRepositoriesInput{
		RepositoryNames: []string{trimmedRepository},
	}
	if strings.TrimSpace(registryID) != "" {
		describeInput.RegistryId = aws.String(strings.TrimSpace(registryID))
	}
	_, err := client.DescribeRepositories(ctx, describeInput)
	if err == nil {
		return nil
	}

	var notFound *ecrtypes.RepositoryNotFoundException
	if !errors.As(err, &notFound) {
		return fmt.Errorf("describe aws ecr repository %q: %w", trimmedRepository, err)
	}

	createInput := &ecr.CreateRepositoryInput{
		RepositoryName:     aws.String(trimmedRepository),
		ImageTagMutability: ecrtypes.ImageTagMutabilityMutable,
		ImageScanningConfiguration: &ecrtypes.ImageScanningConfiguration{
			ScanOnPush: true,
		},
		Tags: buildAWSECRRepositoryTags(teamID),
	}
	if strings.TrimSpace(registryID) != "" {
		createInput.RegistryId = aws.String(strings.TrimSpace(registryID))
	}
	_, err = client.CreateRepository(ctx, createInput)
	if err == nil {
		return nil
	}

	var alreadyExists *ecrtypes.RepositoryAlreadyExistsException
	if errors.As(err, &alreadyExists) {
		return nil
	}
	return fmt.Errorf("create aws ecr repository %q: %w", trimmedRepository, err)
}

func buildAWSECRRepositoryTags(teamID string) []ecrtypes.Tag {
	if strings.TrimSpace(teamID) == "" {
		return nil
	}
	return []ecrtypes.Tag{
		{Key: aws.String("sandbox0.ai/registry-scope"), Value: aws.String("team")},
		{Key: aws.String("sandbox0.ai/team-id"), Value: aws.String(strings.TrimSpace(teamID))},
		{Key: aws.String("sandbox0.ai/team-key"), Value: aws.String(naming.TeamKey(teamID))},
	}
}

func resolveAWSTargetRepository(teamID, targetImage string) (string, error) {
	trimmedTarget := strings.TrimSpace(targetImage)
	if trimmedTarget == "" {
		return "", nil
	}

	_, repository, err := naming.SplitImageReference(trimmedTarget)
	if err != nil {
		return "", fmt.Errorf("invalid aws target image: %w", err)
	}
	prefix := naming.TeamImageRepositoryPrefix(teamID)
	if prefix == "" {
		return "", fmt.Errorf("aws target image requires team id")
	}
	if repository == prefix || strings.HasPrefix(repository, prefix+"/") {
		return repository, nil
	}
	firstSegment, _, _ := strings.Cut(repository, "/")
	if strings.HasPrefix(firstSegment, "t-") {
		return "", fmt.Errorf("target image %q is outside team registry prefix %q", targetImage, prefix)
	}
	return prefix + "/" + repository, nil
}

func buildAWSRegistrySessionName(teamID string) string {
	return "sandbox0-ecr-" + naming.TeamKey(teamID)
}

func parseAWSRegistryIDFromHost(registryHost string) string {
	normalized := normalizeRegistryHost(registryHost)
	if normalized == "" {
		return ""
	}
	prefix, _, ok := strings.Cut(normalized, ".dkr.ecr.")
	if !ok || len(prefix) != 12 {
		return ""
	}
	for _, ch := range prefix {
		if ch < '0' || ch > '9' {
			return ""
		}
	}
	return prefix
}

func parseAWSAccountIDFromARN(arn string) string {
	parts := strings.Split(strings.TrimSpace(arn), ":")
	if len(parts) < 5 {
		return ""
	}
	accountID := strings.TrimSpace(parts[4])
	if len(accountID) != 12 {
		return ""
	}
	for _, ch := range accountID {
		if ch < '0' || ch > '9' {
			return ""
		}
	}
	return accountID
}

func awsPartitionForRegion(region string) string {
	trimmed := strings.TrimSpace(region)
	switch {
	case strings.HasPrefix(trimmed, "cn-"):
		return "aws-cn"
	case strings.HasPrefix(trimmed, "us-gov-"):
		return "aws-us-gov"
	default:
		return "aws"
	}
}
