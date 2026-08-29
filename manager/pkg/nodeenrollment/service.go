package nodeenrollment

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
)

type Store interface {
	EnsureRuntimeNodePoolState(context.Context, string, string) (*sandboxstore.RuntimeNodePoolState, error)
	PutRuntimeNodeEnrollmentChallenge(context.Context, string, string, string, string, time.Duration) error
	ConsumeRuntimeNodeEnrollmentChallenge(context.Context, string, string, string, string) error
	ReserveRuntimeNode(context.Context, *sandboxstore.ReserveRuntimeNodeRequest) (*sandboxstore.RuntimeNodePoolNodeUsage, error)
	ActivateRuntimeNode(context.Context, *sandboxstore.ActivateRuntimeNodeRequest) error
	RuntimeNodeAdmissionReady(context.Context, string, string, string) (bool, error)
	GetRuntimeNodeDrainStatus(context.Context, string, string) (*sandboxstore.RuntimeNodeDrainStatus, error)
}

type Membership interface {
	ValidateElasticInstance(context.Context, AliyunInstanceIdentity) error
}

type CloudPreparer interface {
	PrepareElasticInstance(context.Context, string, string) error
}

type NomadEnrollment interface {
	IssueClientIntroductionToken(context.Context, string) (string, error)
	ValidateRegisteredNode(context.Context, string, string, string, bool) error
	FenceRegisteredNode(context.Context, string) error
	AdmitRegisteredNode(context.Context, string, string, string) error
}

type CertificateIssuer interface {
	IssueNomadBootstrap(context.Context, string, string, []byte) ([]byte, []byte, error)
	IssueNomadExact(context.Context, string, string, string, []byte) ([]byte, error)
	IssueNodeAuthority(context.Context, string, string, []byte) ([]byte, error)
}

type RuntimeConfigRenderer interface {
	Render(RuntimeConfigIdentity) ([]byte, error)
}

type Config struct {
	PoolID             string
	ClusterID          string
	RegionID           string
	CloudRegion        string
	AllocationSupernet string
	AllocationPrefix   int
	ChallengeTTL       time.Duration
	RuntimeArtifact    RuntimeArtifact
}

type RuntimeArtifact struct {
	SourceCommit string `json:"source_commit"`
	ObjectKey    string `json:"object_key"`
	SHA256       string `json:"sha256"`
	OSSEndpoint  string `json:"oss_endpoint"`
	OSSBucket    string `json:"oss_bucket"`
}

// LoadRuntimeArtifact reads the atomically updated immutable-release pointer
// used by manager. The referenced object remains content-addressed; only this
// small control-plane manifest changes during a rollout.
func LoadRuntimeArtifact(file string) (RuntimeArtifact, error) {
	return loadRuntimeArtifact(file, 0)
}

func loadRuntimeArtifact(file string, trustedUID uint32) (RuntimeArtifact, error) {
	if !filepath.IsAbs(file) || filepath.Clean(file) != file {
		return RuntimeArtifact{}, errors.New("node enrollment runtime manifest path must be canonical and absolute")
	}
	info, err := os.Lstat(file)
	if err != nil {
		return RuntimeArtifact{}, err
	}
	stat, ownerOK := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || info.Mode()&0o022 != 0 || !ownerOK || stat.Uid != trustedUID ||
		info.Size() <= 0 || info.Size() > 64<<10 {
		return RuntimeArtifact{}, errors.New("node enrollment runtime manifest file is unsafe")
	}
	payload, err := os.ReadFile(file)
	if err != nil {
		return RuntimeArtifact{}, err
	}
	var artifact RuntimeArtifact
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&artifact); err != nil {
		return RuntimeArtifact{}, fmt.Errorf("decode node enrollment runtime manifest: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return RuntimeArtifact{}, errors.New("node enrollment runtime manifest contains trailing data")
	}
	return artifact, nil
}

type Service struct {
	store         Store
	identity      *AliyunIdentityVerifier
	membership    Membership
	cloud         CloudPreparer
	nomad         NomadEnrollment
	issuer        CertificateIssuer
	runtimeConfig RuntimeConfigRenderer
	config        Config
}

type Challenge struct {
	Audience string `json:"audience"`
	TTLMS    int64  `json:"ttl_ms"`
}

type BootstrapRequest struct {
	Challenge       string `json:"challenge"`
	Document        []byte `json:"document"`
	SignatureBase64 string `json:"signature_base64"`
	NomadCSRPEM     []byte `json:"nomad_csr_pem"`
}

type BootstrapResponse struct {
	ProviderInstanceID    string          `json:"provider_instance_id"`
	NodeName              string          `json:"node_name"`
	NodeUID               string          `json:"node_uid"`
	AllocationCIDR        string          `json:"allocation_cidr"`
	NomadCertificatePEM   []byte          `json:"nomad_certificate_pem"`
	NomadCACertificatePEM []byte          `json:"nomad_ca_certificate_pem"`
	NomadIntroductionJWT  string          `json:"nomad_introduction_jwt"`
	RuntimeArtifact       RuntimeArtifact `json:"runtime_artifact"`
}

type FinalizeRequest struct {
	Challenge       string `json:"challenge"`
	Document        []byte `json:"document"`
	SignatureBase64 string `json:"signature_base64"`
	NomadNodeID     string `json:"nomad_node_id"`
	NomadCSRPEM     []byte `json:"nomad_csr_pem"`
	AuthorityCSRPEM []byte `json:"authority_csr_pem"`
}

type FinalizeResponse struct {
	NodeUID                     string `json:"node_uid"`
	AgentUID                    string `json:"agent_uid"`
	AuthorityCommonName         string `json:"authority_common_name"`
	NomadCertificatePEM         []byte `json:"nomad_certificate_pem"`
	NodeAuthorityCertificatePEM []byte `json:"node_authority_certificate_pem"`
	NodeRuntimeArchive          []byte `json:"node_runtime_archive"`
}

type AdmitRequest struct {
	Challenge       string `json:"challenge"`
	Document        []byte `json:"document"`
	SignatureBase64 string `json:"signature_base64"`
	NomadNodeID     string `json:"nomad_node_id"`
}

type AdmitResponse struct {
	NodeUID  string `json:"node_uid"`
	Admitted bool   `json:"admitted"`
}

func NewService(
	store Store,
	identity *AliyunIdentityVerifier,
	membership Membership,
	cloud CloudPreparer,
	nomad NomadEnrollment,
	issuer CertificateIssuer,
	runtimeConfig RuntimeConfigRenderer,
	config Config,
) (*Service, error) {
	if store == nil || identity == nil || membership == nil || cloud == nil || nomad == nil ||
		issuer == nil || runtimeConfig == nil {
		return nil, errors.New("node enrollment dependencies are required")
	}
	config.PoolID = strings.TrimSpace(config.PoolID)
	config.ClusterID = strings.TrimSpace(config.ClusterID)
	config.RegionID = strings.TrimSpace(config.RegionID)
	config.CloudRegion = strings.TrimSpace(config.CloudRegion)
	if config.ChallengeTTL == 0 {
		config.ChallengeTTL = 2 * time.Minute
	}
	prefix, err := netip.ParsePrefix(config.AllocationSupernet)
	if config.PoolID == "" || config.ClusterID == "" || config.RegionID == "" ||
		config.CloudRegion == "" || err != nil || !prefix.Addr().IsPrivate() ||
		config.AllocationPrefix != 26 || config.ChallengeTTL < time.Second || config.ChallengeTTL > 5*time.Minute {
		return nil, errors.New("node enrollment policy is invalid")
	}
	if !regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(config.RuntimeArtifact.SourceCommit) ||
		!regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(config.RuntimeArtifact.SHA256) ||
		!strings.HasPrefix(config.RuntimeArtifact.ObjectKey, "sandbox0-nomad-runtime/") ||
		strings.TrimSpace(config.RuntimeArtifact.OSSEndpoint) == "" ||
		strings.TrimSpace(config.RuntimeArtifact.OSSBucket) == "" {
		return nil, errors.New("node enrollment runtime artifact is invalid")
	}
	return &Service{store: store, identity: identity, membership: membership, cloud: cloud,
		nomad: nomad, issuer: issuer, runtimeConfig: runtimeConfig, config: config}, nil
}

func (s *Service) Challenge(
	ctx context.Context,
	providerInstanceID, remoteIP string,
) (Challenge, error) {
	providerInstanceID = strings.TrimSpace(providerInstanceID)
	if providerInstanceID == "" || len(providerInstanceID) > 256 {
		return Challenge{}, errors.New("provider instance ID is invalid")
	}
	if _, err := s.store.EnsureRuntimeNodePoolState(ctx, s.config.PoolID, s.config.ClusterID); err != nil {
		return Challenge{}, err
	}
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		return Challenge{}, err
	}
	audience := base64.RawURLEncoding.EncodeToString(random)
	if err := s.store.PutRuntimeNodeEnrollmentChallenge(ctx, s.config.PoolID,
		providerInstanceID, remoteIP, audience, s.config.ChallengeTTL); err != nil {
		return Challenge{}, err
	}
	return Challenge{Audience: audience, TTLMS: s.config.ChallengeTTL.Milliseconds()}, nil
}

func (s *Service) Bootstrap(
	ctx context.Context,
	providerInstanceID, remoteIP string,
	request BootstrapRequest,
) (*BootstrapResponse, error) {
	identity, err := s.verifyRequest(ctx, providerInstanceID, remoteIP,
		request.Challenge, request.Document, request.SignatureBase64)
	if err != nil {
		return nil, err
	}
	nodeName := elasticNodeName(identity.InstanceID)
	nodeUID := fmt.Sprintf("ecs/%s/%s", s.config.CloudRegion, identity.InstanceID)
	reservation, err := s.store.ReserveRuntimeNode(ctx, &sandboxstore.ReserveRuntimeNodeRequest{
		PoolID: s.config.PoolID, ProviderInstanceID: identity.InstanceID,
		PoolKind: sandboxstore.RuntimeNodePoolKindElastic, ClusterID: s.config.ClusterID,
		NodeName: nodeName, NodeUID: nodeUID, PrivateIP: identity.PrivateIPv4,
		AllocationSupernet: s.config.AllocationSupernet, AllocationPrefix: s.config.AllocationPrefix,
	})
	if err != nil {
		return nil, err
	}
	if err := s.cloud.PrepareElasticInstance(ctx, identity.InstanceID, reservation.AllocationCIDR); err != nil {
		return nil, fmt.Errorf("prepare elastic instance network: %w", err)
	}
	certificate, ca, err := s.issuer.IssueNomadBootstrap(ctx, nodeName, identity.PrivateIPv4, request.NomadCSRPEM)
	if err != nil {
		return nil, err
	}
	introduction, err := s.nomad.IssueClientIntroductionToken(ctx, nodeName)
	if err != nil {
		return nil, fmt.Errorf("issue Nomad client introduction token: %w", err)
	}
	return &BootstrapResponse{
		ProviderInstanceID: identity.InstanceID, NodeName: nodeName, NodeUID: nodeUID,
		AllocationCIDR: reservation.AllocationCIDR, NomadCertificatePEM: certificate,
		NomadCACertificatePEM: ca, NomadIntroductionJWT: introduction,
		RuntimeArtifact: s.config.RuntimeArtifact,
	}, nil
}

func (s *Service) Finalize(
	ctx context.Context,
	providerInstanceID, remoteIP string,
	request FinalizeRequest,
) (*FinalizeResponse, error) {
	identity, err := s.verifyRequest(ctx, providerInstanceID, remoteIP,
		request.Challenge, request.Document, request.SignatureBase64)
	if err != nil {
		return nil, err
	}
	nodeName := elasticNodeName(identity.InstanceID)
	status, err := s.store.GetRuntimeNodeDrainStatus(ctx, s.config.PoolID, identity.InstanceID)
	if err != nil {
		return nil, err
	}
	alreadyAdmitted := status.Instance.State == sandboxstore.RuntimeNodeInstanceActive &&
		status.Instance.NodeID == request.NomadNodeID
	if status.Instance.State != sandboxstore.RuntimeNodeInstanceEnrolling && !alreadyAdmitted {
		return nil, errors.New("runtime node is not eligible for identity finalization")
	}
	if err := s.nomad.ValidateRegisteredNode(ctx, request.NomadNodeID, nodeName,
		identity.PrivateIPv4, alreadyAdmitted); err != nil {
		return nil, fmt.Errorf("validate registered Nomad node: %w", err)
	}
	if err := s.nomad.FenceRegisteredNode(ctx, request.NomadNodeID); err != nil {
		return nil, fmt.Errorf("fence registered Nomad node: %w", err)
	}
	nodeUID := fmt.Sprintf("ecs/%s/%s", s.config.CloudRegion, identity.InstanceID)
	agentUID := fmt.Sprintf("ctld/%s/%s", s.config.RegionID, identity.InstanceID)
	commonName := "ctld-" + strings.TrimPrefix(nodeName, "s0-")
	nomadCert, err := s.issuer.IssueNomadExact(ctx, request.NomadNodeID, identity.PrivateIPv4, nodeName, request.NomadCSRPEM)
	if err != nil {
		return nil, err
	}
	authorityCert, err := s.issuer.IssueNodeAuthority(ctx, commonName, agentUID, request.AuthorityCSRPEM)
	if err != nil {
		return nil, err
	}
	if err := s.store.ActivateRuntimeNode(ctx, &sandboxstore.ActivateRuntimeNodeRequest{
		PoolID: s.config.PoolID, ProviderInstanceID: identity.InstanceID,
		NomadNodeID: request.NomadNodeID, AuthorityCommonName: commonName, AgentUID: agentUID,
	}); err != nil {
		return nil, err
	}
	status, err = s.store.GetRuntimeNodeDrainStatus(ctx, s.config.PoolID, identity.InstanceID)
	if err != nil {
		return nil, err
	}
	runtimeArchive, err := s.runtimeConfig.Render(RuntimeConfigIdentity{
		NodeName: nodeName, NodeID: request.NomadNodeID, NodeUID: nodeUID,
		AgentUID: agentUID, PrivateIP: identity.PrivateIPv4,
		AllocationCIDR: status.Instance.AllocationCIDR,
		RegionID:       s.config.RegionID, ClusterID: s.config.ClusterID,
	})
	if err != nil {
		return nil, err
	}
	return &FinalizeResponse{NodeUID: nodeUID, AgentUID: agentUID,
		AuthorityCommonName: commonName, NomadCertificatePEM: nomadCert,
		NodeAuthorityCertificatePEM: authorityCert, NodeRuntimeArchive: runtimeArchive}, nil
}

// Admit is deliberately separate from certificate finalization. The worker
// first installs its exact identities and starts ctld; only a live capacity
// heartbeat plus node metadata rendered as admitted can reopen Nomad
// scheduling. This prevents warm carriers from racing node initialization.
func (s *Service) Admit(
	ctx context.Context,
	providerInstanceID, remoteIP string,
	request AdmitRequest,
) (*AdmitResponse, error) {
	identity, err := s.verifyRequest(ctx, providerInstanceID, remoteIP,
		request.Challenge, request.Document, request.SignatureBase64)
	if err != nil {
		return nil, err
	}
	ready, err := s.store.RuntimeNodeAdmissionReady(ctx, s.config.PoolID,
		identity.InstanceID, request.NomadNodeID)
	if err != nil {
		return nil, err
	}
	if !ready {
		return nil, errors.New("runtime node has not established a live capacity heartbeat")
	}
	if err := s.nomad.AdmitRegisteredNode(ctx, request.NomadNodeID,
		elasticNodeName(identity.InstanceID), identity.PrivateIPv4); err != nil {
		return nil, fmt.Errorf("admit registered Nomad node: %w", err)
	}
	return &AdmitResponse{
		NodeUID:  fmt.Sprintf("ecs/%s/%s", s.config.CloudRegion, identity.InstanceID),
		Admitted: true,
	}, nil
}

func (s *Service) verifyRequest(
	ctx context.Context,
	providerInstanceID, remoteIP, challenge string,
	document []byte,
	signature string,
) (AliyunInstanceIdentity, error) {
	identity, err := s.identity.Verify(document, signature, challenge, remoteIP)
	if err != nil {
		return AliyunInstanceIdentity{}, err
	}
	if identity.InstanceID != strings.TrimSpace(providerInstanceID) {
		return AliyunInstanceIdentity{}, errors.New("provider instance ID does not match signed identity")
	}
	if err := s.membership.ValidateElasticInstance(ctx, identity); err != nil {
		return AliyunInstanceIdentity{}, fmt.Errorf("validate ESS membership: %w", err)
	}
	if err := s.store.ConsumeRuntimeNodeEnrollmentChallenge(ctx, s.config.PoolID,
		identity.InstanceID, remoteIP, challenge); err != nil {
		return AliyunInstanceIdentity{}, err
	}
	return identity, nil
}

var nonNodeName = regexp.MustCompile(`[^a-z0-9-]+`)

func elasticNodeName(instanceID string) string {
	name := "s0-" + strings.Trim(nonNodeName.ReplaceAllString(strings.ToLower(instanceID), "-"), "-")
	if len(name) > 63 {
		name = name[:63]
	}
	return name
}
