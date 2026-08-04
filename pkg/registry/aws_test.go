package registry

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
	ecrtypes "github.com/aws/aws-sdk-go-v2/service/ecr/types"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
)

func TestBuildAWSECRSessionPolicyForRepository(t *testing.T) {
	t.Parallel()

	repository := naming.TeamImageRepositoryPrefix("team-1") + "/my-app"
	policy, err := buildAWSECRSessionPolicy("us-east-1", "123456789012", "team-1", repository)
	if err != nil {
		t.Fatalf("buildAWSECRSessionPolicy returned error: %v", err)
	}

	var decoded struct {
		Version   string `json:"Version"`
		Statement []struct {
			Action   []string `json:"Action"`
			Resource any      `json:"Resource"`
		} `json:"Statement"`
	}
	if err := json.Unmarshal([]byte(policy), &decoded); err != nil {
		t.Fatalf("unmarshal policy: %v", err)
	}
	if decoded.Version != "2012-10-17" {
		t.Fatalf("unexpected version %q", decoded.Version)
	}
	if len(decoded.Statement) != 3 {
		t.Fatalf("unexpected statement count %d", len(decoded.Statement))
	}
	want := "arn:aws:ecr:us-east-1:123456789012:repository/" + repository
	if decoded.Statement[1].Resource != want {
		t.Fatalf("unexpected repository resource %v", decoded.Statement[1].Resource)
	}
}

func TestBuildAWSECRSessionPolicyForTeamPrefix(t *testing.T) {
	t.Parallel()

	policy, err := buildAWSECRSessionPolicy("us-east-1", "123456789012", "team-1", "")
	if err != nil {
		t.Fatalf("buildAWSECRSessionPolicy returned error: %v", err)
	}

	var decoded struct {
		Version   string `json:"Version"`
		Statement []struct {
			Resource any `json:"Resource"`
		} `json:"Statement"`
	}
	if err := json.Unmarshal([]byte(policy), &decoded); err != nil {
		t.Fatalf("unmarshal policy: %v", err)
	}
	if len(decoded.Statement) != 2 {
		t.Fatalf("unexpected statement count %d", len(decoded.Statement))
	}
	prefix := naming.TeamImageRepositoryPrefix("team-1")
	resources, ok := decoded.Statement[1].Resource.([]any)
	if !ok || len(resources) != 2 {
		t.Fatalf("unexpected resource payload %#v", decoded.Statement[1].Resource)
	}
	want := "arn:aws:ecr:us-east-1:123456789012:repository/" + prefix
	if resources[0] != want {
		t.Fatalf("unexpected first resource %v", resources[0])
	}
	if resources[1] != want+"/*" {
		t.Fatalf("unexpected second resource %v", resources[1])
	}
}

func TestResolveAWSTargetRepository(t *testing.T) {
	t.Parallel()
	prefix := naming.TeamImageRepositoryPrefix("team-1")

	tests := []struct {
		name      string
		target    string
		want      string
		wantError bool
	}{
		{name: "simple", target: "my-app:v1", want: prefix + "/my-app"},
		{name: "nested", target: "apps/my-app:v1", want: prefix + "/apps/my-app"},
		{name: "already scoped", target: prefix + "/my-app:v1", want: prefix + "/my-app"},
		{name: "other team", target: "t-other/my-app:v1", wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveAWSTargetRepository("team-1", tt.target)
			if tt.wantError {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveAWSTargetRepository returned error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("repository = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestEnsureAWSECRRepository(t *testing.T) {
	t.Parallel()

	t.Run("creates when missing", func(t *testing.T) {
		repository := naming.TeamImageRepositoryPrefix("team-1") + "/my-app"
		client := &fakeECRClient{describeErr: &ecrtypes.RepositoryNotFoundException{}}
		if err := ensureAWSECRRepository(context.Background(), client, "123456789012", repository, "team-1"); err != nil {
			t.Fatalf("ensureAWSECRRepository returned error: %v", err)
		}
		if client.created == nil {
			t.Fatal("expected repository to be created")
		}
		if got := aws.ToString(client.created.RepositoryName); got != repository {
			t.Fatalf("created repository = %q, want %q", got, repository)
		}
	})

	t.Run("ignores existing", func(t *testing.T) {
		repository := naming.TeamImageRepositoryPrefix("team-1") + "/my-app"
		client := &fakeECRClient{}
		if err := ensureAWSECRRepository(context.Background(), client, "123456789012", repository, "team-1"); err != nil {
			t.Fatalf("ensureAWSECRRepository returned error: %v", err)
		}
		if client.created != nil {
			t.Fatal("did not expect repository creation")
		}
	})

	t.Run("returns non-not-found error", func(t *testing.T) {
		repository := naming.TeamImageRepositoryPrefix("team-1") + "/my-app"
		client := &fakeECRClient{describeErr: errors.New("boom")}
		if err := ensureAWSECRRepository(context.Background(), client, "123456789012", repository, "team-1"); err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestParseAWSIdentifiers(t *testing.T) {
	t.Parallel()

	if got := parseAWSRegistryIDFromHost("123456789012.dkr.ecr.us-east-1.amazonaws.com"); got != "123456789012" {
		t.Fatalf("unexpected registry id %q", got)
	}
	if got := parseAWSAccountIDFromARN("arn:aws:iam::123456789012:role/sandbox0-ecr-broker"); got != "123456789012" {
		t.Fatalf("unexpected account id %q", got)
	}
	if got := awsPartitionForRegion("cn-north-1"); got != "aws-cn" {
		t.Fatalf("unexpected cn partition %q", got)
	}
}

type fakeECRClient struct {
	describeErr error
	createErr   error
	created     *ecr.CreateRepositoryInput
}

func (f *fakeECRClient) DescribeRepositories(context.Context, *ecr.DescribeRepositoriesInput, ...func(*ecr.Options)) (*ecr.DescribeRepositoriesOutput, error) {
	if f.describeErr != nil {
		return nil, f.describeErr
	}
	return &ecr.DescribeRepositoriesOutput{}, nil
}

func (f *fakeECRClient) CreateRepository(_ context.Context, input *ecr.CreateRepositoryInput, _ ...func(*ecr.Options)) (*ecr.CreateRepositoryOutput, error) {
	f.created = input
	if f.createErr != nil {
		return nil, f.createErr
	}
	return &ecr.CreateRepositoryOutput{}, nil
}

func (f *fakeECRClient) GetAuthorizationToken(context.Context, *ecr.GetAuthorizationTokenInput, ...func(*ecr.Options)) (*ecr.GetAuthorizationTokenOutput, error) {
	return nil, errors.New("unexpected GetAuthorizationToken call")
}
