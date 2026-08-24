package registry

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	cr "github.com/aliyun/alibaba-cloud-sdk-go/services/cr_ee"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/sts"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
)

func TestAliyunProviderReturnsRepositoryScopedReferences(t *testing.T) {
	t.Parallel()

	management := &fakeAliyunCRClient{
		getRepositoryResponse:    &cr.GetRepositoryResponse{GetRepositoryIsSuccess: false, Code: "REPO_NOT_EXIST"},
		createRepositoryResponse: &cr.CreateRepositoryResponse{CreateRepositoryIsSuccess: true},
	}
	session := &fakeAliyunCRClient{
		tokenResponse: &cr.GetAuthorizationTokenResponse{
			GetAuthorizationTokenIsSuccess: true,
			TempUsername:                   "cr_temp_user",
			AuthorizationToken:             "secret-token",
			ExpireTime:                     time.Now().Add(12 * time.Hour).UnixMilli(),
		},
	}
	stsClient := &fakeAliyunSTSClient{
		response: &sts.AssumeRoleResponse{Credentials: sts.Credentials{
			AccessKeyId:     "sts-ak",
			AccessKeySecret: "sts-sk",
			SecurityToken:   "sts-token",
			Expiration:      time.Now().Add(6 * time.Hour).UTC().Format(time.RFC3339),
		}},
	}
	base := &aliyunProvider{
		cfg: config.RegistryAliyunConfig{
			Registry:        "registry.example.com",
			Namespace:       "sandbox0",
			Region:          "us-east-1",
			InstanceID:      "cri-instance",
			AssumeRoleARN:   "acs:ram::123456789012:role/sandbox0-acr-upload",
			AccessKeyID:     "base-ak",
			AccessKeySecret: "base-sk",
		},
		newAccessKeyCRClient: func(string, string, string) (aliyunCRAPI, error) { return management, nil },
		newSTSClient:         func(string, string, string) (aliyunSTSAPI, error) { return stsClient, nil },
		newSTSCRClient:       func(string, string, string, string) (aliyunCRAPI, error) { return session, nil },
	}
	provider := &providerWithPullRegistry{base: base, pullRegistry: "registry-vpc.example.com/sandbox0"}

	credential, err := provider.GetPushCredentials(t.Context(), PushCredentialsRequest{
		TeamID:      "team-1",
		TargetImage: "probe:test",
	})
	if err != nil {
		t.Fatalf("GetPushCredentials() error = %v", err)
	}
	repository := naming.TeamImageRepositoryPrefix("team-1")
	if got, want := credential.PushRegistry, "registry.example.com/sandbox0/"+repository; got != want {
		t.Fatalf("PushRegistry = %q, want %q", got, want)
	}
	if got, want := credential.PullRegistry, "registry-vpc.example.com/sandbox0/"+repository; got != want {
		t.Fatalf("PullRegistry = %q, want %q", got, want)
	}
	if !strings.HasPrefix(credential.PushImage, credential.PushRegistry+":probe-test-") {
		t.Fatalf("PushImage = %q", credential.PushImage)
	}
	if got, want := credential.PullImage, credential.PullRegistry+":"+credential.TargetTag; got != want {
		t.Fatalf("PullImage = %q, want %q", got, want)
	}
	if management.created == nil || management.created.RepoName != repository || management.created.RepoNamespaceName != "sandbox0" {
		t.Fatalf("created repository = %#v", management.created)
	}
	if stsClient.request == nil {
		t.Fatal("AssumeRole request was not recorded")
	}
	if !strings.Contains(stsClient.request.Policy, "repository/cri-instance/sandbox0/"+repository) {
		t.Fatalf("session policy does not scope repository: %s", stsClient.request.Policy)
	}
}

func TestBuildAliyunRegistrySessionPolicy(t *testing.T) {
	t.Parallel()

	policy, err := buildAliyunRegistrySessionPolicy("us-east-1", "123456789012", "cri-1", "sandbox0", "t-team")
	if err != nil {
		t.Fatalf("buildAliyunRegistrySessionPolicy() error = %v", err)
	}
	var decoded struct {
		Version   string `json:"Version"`
		Statement []struct {
			Action   []string `json:"Action"`
			Resource string   `json:"Resource"`
		} `json:"Statement"`
	}
	if err := json.Unmarshal([]byte(policy), &decoded); err != nil {
		t.Fatalf("unmarshal policy: %v", err)
	}
	if decoded.Version != "1" || len(decoded.Statement) != 2 {
		t.Fatalf("unexpected policy %#v", decoded)
	}
	if got, want := decoded.Statement[1].Resource, "acs:cr:us-east-1:123456789012:repository/cri-1/sandbox0/t-team"; got != want {
		t.Fatalf("repository resource = %q, want %q", got, want)
	}
}

func TestResolveAliyunTarget(t *testing.T) {
	t.Parallel()

	repository, tag, err := resolveAliyunTarget("team-1", "probe:test")
	if err != nil {
		t.Fatalf("resolveAliyunTarget() error = %v", err)
	}
	if repository != naming.TeamImageRepositoryPrefix("team-1") {
		t.Fatalf("repository = %q", repository)
	}
	if !strings.HasPrefix(tag, "probe-test-") || len(tag) > 128 {
		t.Fatalf("tag = %q", tag)
	}
	_, sameTag, err := resolveAliyunTarget("team-1", "probe:test")
	if err != nil || sameTag != tag {
		t.Fatalf("target mapping is not deterministic: %q, %v", sameTag, err)
	}
	if _, _, err := resolveAliyunTarget("team-1", "t-other/probe:test"); err == nil {
		t.Fatal("cross-team target did not fail")
	}
	repositoryWithoutTarget, tagWithoutTarget, err := resolveAliyunTarget("team-1", "")
	if err != nil {
		t.Fatalf("resolveAliyunTarget() without target error = %v", err)
	}
	if repositoryWithoutTarget != repository || tagWithoutTarget != "" {
		t.Fatalf("targetless mapping = %q, %q", repositoryWithoutTarget, tagWithoutTarget)
	}
}

type fakeAliyunCRClient struct {
	getRepositoryResponse    *cr.GetRepositoryResponse
	getRepositoryError       error
	createRepositoryResponse *cr.CreateRepositoryResponse
	createRepositoryError    error
	tokenResponse            *cr.GetAuthorizationTokenResponse
	tokenError               error
	created                  *cr.CreateRepositoryRequest
}

func (f *fakeAliyunCRClient) GetRepository(*cr.GetRepositoryRequest) (*cr.GetRepositoryResponse, error) {
	return f.getRepositoryResponse, f.getRepositoryError
}

func (f *fakeAliyunCRClient) CreateRepository(request *cr.CreateRepositoryRequest) (*cr.CreateRepositoryResponse, error) {
	f.created = request
	return f.createRepositoryResponse, f.createRepositoryError
}

func (f *fakeAliyunCRClient) GetAuthorizationToken(*cr.GetAuthorizationTokenRequest) (*cr.GetAuthorizationTokenResponse, error) {
	return f.tokenResponse, f.tokenError
}

type fakeAliyunSTSClient struct {
	response *sts.AssumeRoleResponse
	err      error
	request  *sts.AssumeRoleRequest
}

func (f *fakeAliyunSTSClient) AssumeRole(request *sts.AssumeRoleRequest) (*sts.AssumeRoleResponse, error) {
	f.request = request
	return f.response, f.err
}
