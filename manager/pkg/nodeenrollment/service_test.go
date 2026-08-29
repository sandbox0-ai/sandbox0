package nodeenrollment

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/stretchr/testify/require"
)

type enrollmentStoreStub struct {
	challenge string
	status    sandboxstore.RuntimeNodePoolNodeUsage
}

func TestLoadRuntimeArtifactRequiresTrustedOwnerAndClosedManifest(t *testing.T) {
	file := filepath.Join(t.TempDir(), "runtime-artifact.json")
	payload := `{"source_commit":"1111111111111111111111111111111111111111","object_key":"sandbox0-nomad-runtime/release.tar.gz","sha256":"2222222222222222222222222222222222222222222222222222222222222222","oss_endpoint":"https://oss.internal","oss_bucket":"runtime"}`
	require.NoError(t, os.WriteFile(file, []byte(payload), 0o600))
	artifact, err := loadRuntimeArtifact(file, uint32(os.Geteuid()))
	require.NoError(t, err)
	require.Equal(t, "sandbox0-nomad-runtime/release.tar.gz", artifact.ObjectKey)
	if os.Geteuid() != 0 {
		_, err = LoadRuntimeArtifact(file)
		require.ErrorContains(t, err, "unsafe")
	}
	require.NoError(t, os.Chmod(file, 0o666))
	_, err = loadRuntimeArtifact(file, uint32(os.Geteuid()))
	require.ErrorContains(t, err, "unsafe")
}

func (s *enrollmentStoreStub) EnsureRuntimeNodePoolState(context.Context, string, string) (*sandboxstore.RuntimeNodePoolState, error) {
	return &sandboxstore.RuntimeNodePoolState{PoolID: "elastic", ClusterID: "nomad"}, nil
}
func (s *enrollmentStoreStub) PutRuntimeNodeEnrollmentChallenge(_ context.Context, _, _, _, challenge string, _ time.Duration) error {
	s.challenge = challenge
	return nil
}
func (s *enrollmentStoreStub) ConsumeRuntimeNodeEnrollmentChallenge(_ context.Context, _, _, _, challenge string) error {
	if challenge != s.challenge || challenge == "" {
		return fmt.Errorf("challenge mismatch")
	}
	s.challenge = ""
	return nil
}
func (s *enrollmentStoreStub) ReserveRuntimeNode(context.Context, *sandboxstore.ReserveRuntimeNodeRequest) (*sandboxstore.RuntimeNodePoolNodeUsage, error) {
	return &s.status, nil
}
func (s *enrollmentStoreStub) ActivateRuntimeNode(context.Context, *sandboxstore.ActivateRuntimeNodeRequest) error {
	s.status.State = sandboxstore.RuntimeNodeInstanceActive
	return nil
}
func (s *enrollmentStoreStub) RuntimeNodeAdmissionReady(context.Context, string, string, string) (bool, error) {
	return true, nil
}
func (s *enrollmentStoreStub) GetRuntimeNodeDrainStatus(context.Context, string, string) (*sandboxstore.RuntimeNodeDrainStatus, error) {
	return &sandboxstore.RuntimeNodeDrainStatus{Instance: s.status}, nil
}

type enrollmentMembershipStub struct{}

func (enrollmentMembershipStub) ValidateElasticInstance(context.Context, AliyunInstanceIdentity) error {
	return nil
}

type enrollmentCloudStub struct{}

func (enrollmentCloudStub) PrepareElasticInstance(context.Context, string, string) error { return nil }

type enrollmentNomadStub struct {
	alreadyAdmitted bool
}

func (n *enrollmentNomadStub) IssueClientIntroductionToken(context.Context, string) (string, error) {
	return "a.b.c", nil
}
func (n *enrollmentNomadStub) ValidateRegisteredNode(_ context.Context, _, _, _ string, admitted bool) error {
	n.alreadyAdmitted = admitted
	return nil
}
func (n *enrollmentNomadStub) FenceRegisteredNode(context.Context, string) error { return nil }
func (n *enrollmentNomadStub) AdmitRegisteredNode(context.Context, string, string, string) error {
	return nil
}

type enrollmentIssuerStub struct{}

func (enrollmentIssuerStub) IssueNomadBootstrap(context.Context, string, string, []byte) ([]byte, []byte, error) {
	return []byte("bootstrap"), []byte("ca"), nil
}
func (enrollmentIssuerStub) IssueNomadExact(context.Context, string, string, string, []byte) ([]byte, error) {
	return []byte("exact"), nil
}
func (enrollmentIssuerStub) IssueNodeAuthority(context.Context, string, string, []byte) ([]byte, error) {
	return []byte("authority"), nil
}

type enrollmentRendererStub struct{}

func (enrollmentRendererStub) Render(RuntimeConfigIdentity) ([]byte, error) {
	return []byte("runtime"), nil
}

func TestFinalizeDistinguishesInitialEnrollmentFromActiveIdentityRenewal(t *testing.T) {
	for _, test := range []struct {
		name            string
		state           string
		nodeID          string
		alreadyAdmitted bool
	}{
		{name: "initial", state: sandboxstore.RuntimeNodeInstanceEnrolling},
		{name: "renewal", state: sandboxstore.RuntimeNodeInstanceActive,
			nodeID: "11111111-1111-1111-1111-111111111111", alreadyAdmitted: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			signerPEM, signerCertificate, signerKey := testAliyunIdentitySigner(t)
			verifier, err := NewAliyunIdentityVerifier(AliyunIdentityPolicy{
				RegionID: "us-east-1", OwnerAccountID: "1234", ImageID: "image-1",
				InstanceTypes: []string{"ecs.test"}, SignerCertPEM: signerPEM,
			})
			require.NoError(t, err)
			store := &enrollmentStoreStub{status: sandboxstore.RuntimeNodePoolNodeUsage{
				PoolID: "elastic", ProviderInstanceID: "i-test", State: test.state,
				NodeID: test.nodeID, AllocationCIDR: "172.27.0.0/26",
			}}
			nomad := &enrollmentNomadStub{}
			service, err := NewService(store, verifier, enrollmentMembershipStub{}, enrollmentCloudStub{},
				nomad, enrollmentIssuerStub{}, enrollmentRendererStub{}, Config{
					PoolID: "elastic", ClusterID: "nomad", RegionID: "ali-ue1",
					CloudRegion: "us-east-1", AllocationSupernet: "172.27.0.0/17", AllocationPrefix: 26,
					RuntimeArtifact: RuntimeArtifact{
						SourceCommit: "1111111111111111111111111111111111111111",
						ObjectKey:    "sandbox0-nomad-runtime/release.tar.gz",
						SHA256:       "2222222222222222222222222222222222222222222222222222222222222222",
						OSSEndpoint:  "https://oss.internal", OSSBucket: "runtime",
					},
				})
			require.NoError(t, err)
			challenge, err := service.Challenge(t.Context(), "i-test", "172.16.1.4")
			require.NoError(t, err)
			document := []byte(`{"zone-id":"us-east-1a","serial-number":"serial","instance-id":"i-test","region-id":"us-east-1","private-ipv4":"172.16.1.4","owner-account-id":"1234","mac":"00:11:22:33:44:55","image-id":"image-1","instance-type":"ecs.test"}`)
			signed := append(append([]byte(nil), document[:len(document)-1]...),
				[]byte(`,"audience":"`+challenge.Audience+`"}`)...)
			nodeID := "11111111-1111-1111-1111-111111111111"
			response, err := service.Finalize(t.Context(), "i-test", "172.16.1.4", FinalizeRequest{
				Challenge: challenge.Audience, Document: document,
				SignatureBase64: signAliyunIdentity(t, signed, signerCertificate, signerKey),
				NomadNodeID:     nodeID, NomadCSRPEM: []byte("nomad-csr"),
				AuthorityCSRPEM: []byte("authority-csr"),
			})
			require.NoError(t, err)
			require.Equal(t, test.alreadyAdmitted, nomad.alreadyAdmitted)
			require.Equal(t, []byte("runtime"), response.NodeRuntimeArchive)
		})
	}
}
