package registry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"
	"unicode"

	aliyunerrors "github.com/aliyun/alibaba-cloud-sdk-go/sdk/errors"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	cr "github.com/aliyun/alibaba-cloud-sdk-go/services/cr_ee"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/sts"
	distref "github.com/distribution/reference"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
)

const defaultAliyunRegistrySessionDuration = 6 * time.Hour

type aliyunCRAPI interface {
	GetRepository(*cr.GetRepositoryRequest) (*cr.GetRepositoryResponse, error)
	CreateRepository(*cr.CreateRepositoryRequest) (*cr.CreateRepositoryResponse, error)
	GetAuthorizationToken(*cr.GetAuthorizationTokenRequest) (*cr.GetAuthorizationTokenResponse, error)
}

type aliyunSTSAPI interface {
	AssumeRole(*sts.AssumeRoleRequest) (*sts.AssumeRoleResponse, error)
}

type aliyunProvider struct {
	cfg config.RegistryAliyunConfig

	newAccessKeyCRClient func(region, accessKey, secretKey string) (aliyunCRAPI, error)
	newSTSClient         func(region, accessKey, secretKey string) (aliyunSTSAPI, error)
	newSTSCRClient       func(region, accessKey, secretKey, securityToken string) (aliyunCRAPI, error)
}

func (p *aliyunProvider) GetPushCredentials(ctx context.Context, req PushCredentialsRequest) (*Credential, error) {
	registryHost := normalizeRegistryHost(p.cfg.Registry)
	if registryHost == "" {
		return nil, fmt.Errorf("aliyun registry is required")
	}
	region := strings.TrimSpace(p.cfg.Region)
	if region == "" {
		return nil, fmt.Errorf("aliyun region is required")
	}
	instanceID := strings.TrimSpace(p.cfg.InstanceID)
	if instanceID == "" {
		return nil, fmt.Errorf("aliyun instanceId is required")
	}
	namespace := strings.Trim(strings.TrimSpace(p.cfg.Namespace), "/")
	if namespace == "" {
		return nil, fmt.Errorf("aliyun registry namespace is required")
	}
	roleARN := strings.TrimSpace(p.cfg.AssumeRoleARN)
	if roleARN == "" {
		return nil, fmt.Errorf("aliyun registry assumeRoleArn is required")
	}

	repository, targetTag, err := resolveAliyunTarget(req.TeamID, req.TargetImage)
	if err != nil {
		return nil, err
	}
	accountID, err := parseAliyunAccountIDFromRoleARN(roleARN)
	if err != nil {
		return nil, err
	}
	accessKey, secretKey, err := p.baseCredentials(ctx)
	if err != nil {
		return nil, err
	}

	baseClient, err := p.accessKeyCRClient(region, accessKey, secretKey)
	if err != nil {
		return nil, fmt.Errorf("create aliyun cr management client: %w", err)
	}
	if err := ensureAliyunRepository(baseClient, instanceID, namespace, repository); err != nil {
		return nil, err
	}

	policy, err := buildAliyunRegistrySessionPolicy(region, accountID, instanceID, namespace, repository)
	if err != nil {
		return nil, err
	}
	stsClient, err := p.stsClient(region, accessKey, secretKey)
	if err != nil {
		return nil, fmt.Errorf("create aliyun sts client: %w", err)
	}
	assumeRequest := sts.CreateAssumeRoleRequest()
	assumeRequest.Scheme = "https"
	assumeRequest.RoleArn = roleARN
	assumeRequest.RoleSessionName = "sandbox0-acr-" + naming.TeamKey(req.TeamID)
	assumeRequest.Policy = policy
	assumeRequest.DurationSeconds = requests.NewInteger64(int64(p.sessionDuration().Seconds()))
	if externalID := strings.TrimSpace(p.cfg.ExternalID); externalID != "" {
		assumeRequest.ExternalId = externalID
	}
	assumed, err := stsClient.AssumeRole(assumeRequest)
	if err != nil {
		return nil, fmt.Errorf("assume aliyun registry role: %w", err)
	}
	if assumed == nil || strings.TrimSpace(assumed.Credentials.AccessKeyId) == "" ||
		strings.TrimSpace(assumed.Credentials.AccessKeySecret) == "" ||
		strings.TrimSpace(assumed.Credentials.SecurityToken) == "" {
		return nil, fmt.Errorf("assume aliyun registry role returned no credentials")
	}

	client, err := p.stsCRClient(
		region,
		assumed.Credentials.AccessKeyId,
		assumed.Credentials.AccessKeySecret,
		assumed.Credentials.SecurityToken,
	)
	if err != nil {
		return nil, fmt.Errorf("create aliyun cr session client: %w", err)
	}
	tokenRequest := cr.CreateGetAuthorizationTokenRequest()
	tokenRequest.Scheme = "https"
	tokenRequest.InstanceId = instanceID
	response, err := client.GetAuthorizationToken(tokenRequest)
	if err != nil {
		return nil, fmt.Errorf("get aliyun authorization token: %w", err)
	}
	if response == nil {
		return nil, fmt.Errorf("aliyun authorization response is empty")
	}
	if !response.GetAuthorizationTokenIsSuccess {
		return nil, fmt.Errorf("aliyun authorization request failed: %s", aliyunResponseCode(response.Code))
	}

	expiresAt := time.Time{}
	if response.ExpireTime > 0 {
		expiresAt = time.UnixMilli(int64(response.ExpireTime)).UTC()
	}
	if stsExpiresAt, parseErr := time.Parse(time.RFC3339, strings.TrimSpace(assumed.Credentials.Expiration)); parseErr == nil &&
		(expiresAt.IsZero() || stsExpiresAt.Before(expiresAt)) {
		expiresAt = stsExpiresAt
	}

	return &Credential{
		Provider:     "aliyun",
		PushRegistry: registryHost + "/" + namespace,
		Username:     response.TempUsername,
		Password:     response.AuthorizationToken,
		ExpiresAt:    timePtr(expiresAt),
		TargetTag:    targetTag,
	}, nil
}

func (p *aliyunProvider) baseCredentials(ctx context.Context) (string, string, error) {
	accessKey, err := credentialValue(p.cfg.AccessKeyID, p.cfg.AccessKeyIDFile, "aliyun access key id")
	if err != nil {
		return "", "", err
	}
	secretKey, err := credentialValue(p.cfg.AccessKeySecret, p.cfg.AccessKeySecretFile, "aliyun access key secret")
	if err != nil {
		return "", "", err
	}
	if accessKey == "" || secretKey == "" {
		return "", "", fmt.Errorf("aliyun access key id and access key secret are required")
	}
	return accessKey, secretKey, nil
}

func (p *aliyunProvider) accessKeyCRClient(region, accessKey, secretKey string) (aliyunCRAPI, error) {
	if p.newAccessKeyCRClient != nil {
		return p.newAccessKeyCRClient(region, accessKey, secretKey)
	}
	return cr.NewClientWithAccessKey(region, accessKey, secretKey)
}

func (p *aliyunProvider) stsClient(region, accessKey, secretKey string) (aliyunSTSAPI, error) {
	if p.newSTSClient != nil {
		return p.newSTSClient(region, accessKey, secretKey)
	}
	return sts.NewClientWithAccessKey(region, accessKey, secretKey)
}

func (p *aliyunProvider) stsCRClient(region, accessKey, secretKey, securityToken string) (aliyunCRAPI, error) {
	if p.newSTSCRClient != nil {
		return p.newSTSCRClient(region, accessKey, secretKey, securityToken)
	}
	return cr.NewClientWithStsToken(region, accessKey, secretKey, securityToken)
}

func (p *aliyunProvider) sessionDuration() time.Duration {
	seconds := p.cfg.SessionDuration
	if seconds == 0 {
		return defaultAliyunRegistrySessionDuration
	}
	return time.Duration(seconds) * time.Second
}

func resolveAliyunTarget(teamID, targetImage string) (repository, tag string, err error) {
	repository = naming.TeamImageRepositoryPrefix(teamID)
	if repository == "" {
		return "", "", fmt.Errorf("%w: aliyun target image requires team id", ErrInvalidTargetImage)
	}
	trimmedTarget := strings.TrimSpace(targetImage)
	if trimmedTarget == "" {
		return repository, "", nil
	}
	named, err := distref.ParseNormalizedNamed(trimmedTarget)
	if err != nil {
		return "", "", fmt.Errorf("%w: invalid aliyun target image: %v", ErrInvalidTargetImage, err)
	}
	if _, ok := named.(distref.Digested); ok {
		return "", "", fmt.Errorf("%w: aliyun target image must use a tag, not a digest", ErrInvalidTargetImage)
	}
	named = distref.TagNameOnly(named)
	canonical := named.String()
	logicalRepository := distref.Path(named)
	firstSegment, _, _ := strings.Cut(logicalRepository, "/")
	if strings.HasPrefix(firstSegment, "t-") && firstSegment != repository {
		return "", "", fmt.Errorf("%w: target image %q is outside team repository %q", ErrInvalidTargetImage, targetImage, repository)
	}
	logicalTag := "latest"
	if tagged, ok := named.(distref.Tagged); ok {
		logicalTag = tagged.Tag()
	}
	prefix := sanitizeAliyunTagPart(path.Base(logicalRepository) + "-" + logicalTag)
	if len(prefix) > 96 {
		prefix = prefix[:96]
	}
	digest := sha256.Sum256([]byte(canonical))
	return repository, prefix + "-" + hex.EncodeToString(digest[:6]), nil
}

func sanitizeAliyunTagPart(value string) string {
	var builder strings.Builder
	lastSeparator := false
	for _, char := range strings.ToLower(strings.TrimSpace(value)) {
		if unicode.IsLetter(char) || unicode.IsDigit(char) || char == '.' || char == '_' {
			builder.WriteRune(char)
			lastSeparator = false
			continue
		}
		if !lastSeparator && builder.Len() > 0 {
			builder.WriteByte('-')
			lastSeparator = true
		}
	}
	result := strings.Trim(builder.String(), "-._")
	if result == "" {
		return "image"
	}
	return result
}

func buildAliyunRegistrySessionPolicy(region, accountID, instanceID, namespace, repository string) (string, error) {
	for description, value := range map[string]string{
		"aliyun region":      region,
		"aliyun account id":  accountID,
		"aliyun instance id": instanceID,
		"aliyun namespace":   namespace,
		"aliyun repository":  repository,
	} {
		if strings.TrimSpace(value) == "" {
			return "", fmt.Errorf("%s is required for registry session policy", description)
		}
	}
	repositoryARN := fmt.Sprintf(
		"acs:cr:%s:%s:repository/%s/%s/%s",
		strings.TrimSpace(region),
		strings.TrimSpace(accountID),
		strings.TrimSpace(instanceID),
		strings.Trim(strings.TrimSpace(namespace), "/"),
		strings.Trim(strings.TrimSpace(repository), "/"),
	)
	policy := map[string]any{
		"Version": "1",
		"Statement": []map[string]any{
			{
				"Effect":   "Allow",
				"Action":   []string{"cr:GetAuthorizationToken"},
				"Resource": "*",
			},
			{
				"Effect":   "Allow",
				"Action":   []string{"cr:PushRepository", "cr:PullRepository"},
				"Resource": repositoryARN,
			},
		},
	}
	data, err := json.Marshal(policy)
	if err != nil {
		return "", fmt.Errorf("marshal aliyun registry session policy: %w", err)
	}
	return string(data), nil
}

func parseAliyunAccountIDFromRoleARN(roleARN string) (string, error) {
	parts := strings.Split(strings.TrimSpace(roleARN), ":")
	if len(parts) != 5 || parts[0] != "acs" || parts[1] != "ram" || parts[2] != "" ||
		strings.TrimSpace(parts[3]) == "" || !strings.HasPrefix(parts[4], "role/") {
		return "", fmt.Errorf("aliyun registry assumeRoleArn is invalid")
	}
	return strings.TrimSpace(parts[3]), nil
}

func ensureAliyunRepository(client aliyunCRAPI, instanceID, namespace, repository string) error {
	request := cr.CreateGetRepositoryRequest()
	request.Scheme = "https"
	request.InstanceId = instanceID
	request.RepoNamespaceName = namespace
	request.RepoName = repository
	response, err := client.GetRepository(request)
	if err == nil && response != nil && response.GetRepositoryIsSuccess {
		return nil
	}
	if err == nil && response != nil && !aliyunCodeMeansNotFound(response.Code) {
		return fmt.Errorf("get aliyun repository %q failed: %s", repository, aliyunResponseCode(response.Code))
	}
	if err != nil && !aliyunErrorMeansNotFound(err) {
		return fmt.Errorf("get aliyun repository %q: %w", repository, err)
	}

	createRequest := cr.CreateCreateRepositoryRequest()
	createRequest.Scheme = "https"
	createRequest.InstanceId = instanceID
	createRequest.RepoNamespaceName = namespace
	createRequest.RepoName = repository
	createRequest.RepoType = "PRIVATE"
	createRequest.Summary = "Sandbox0 team template images"
	createRequest.Detail = "Managed by Sandbox0."
	created, createErr := client.CreateRepository(createRequest)
	if createErr != nil {
		if aliyunErrorMeansAlreadyExists(createErr) {
			return nil
		}
		return fmt.Errorf("create aliyun repository %q: %w", repository, createErr)
	}
	if created == nil {
		return fmt.Errorf("create aliyun repository %q returned no response", repository)
	}
	if !created.CreateRepositoryIsSuccess && !aliyunCodeMeansAlreadyExists(created.Code) {
		return fmt.Errorf("create aliyun repository %q failed: %s", repository, aliyunResponseCode(created.Code))
	}
	return nil
}

func aliyunErrorMeansNotFound(err error) bool {
	var serverErr *aliyunerrors.ServerError
	return errors.As(err, &serverErr) && (serverErr.HttpStatus() == 404 || aliyunCodeMeansNotFound(serverErr.ErrorCode()))
}

func aliyunErrorMeansAlreadyExists(err error) bool {
	var serverErr *aliyunerrors.ServerError
	return errors.As(err, &serverErr) && aliyunCodeMeansAlreadyExists(serverErr.ErrorCode())
}

func aliyunCodeMeansNotFound(code string) bool {
	normalized := strings.ToUpper(strings.TrimSpace(code))
	return strings.Contains(normalized, "NOT_EXIST") || strings.Contains(normalized, "NOT_FOUND") || normalized == "404"
}

func aliyunCodeMeansAlreadyExists(code string) bool {
	normalized := strings.ToUpper(strings.TrimSpace(code))
	return strings.Contains(normalized, "ALREADY_EXIST") || strings.Contains(normalized, "ALREADYEXIST") || strings.Contains(normalized, "EXISTS")
}

func aliyunResponseCode(code string) string {
	if value := strings.TrimSpace(code); value != "" {
		return value
	}
	return "unknown_error"
}
