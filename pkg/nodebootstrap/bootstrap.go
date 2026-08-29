package nodebootstrap

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeenrollment"
)

const (
	exactNomadCertificateFile    = "/etc/nomad.d/nomad.pem"
	exactNomadCAFile             = "/etc/nomad.d/nomad-ca.pem"
	exactNomadKeyFile            = "/etc/nomad.d/nomad-key.pem"
	nomadConfigFile              = "/etc/nomad.d/nomad.hcl"
	nomadIntroductionTokenFile   = "/etc/nomad.d/client-intro.token"
	nomadClientIDFile            = "/opt/nomad/data/client/client-id"
	managerCAFile                = "/etc/sandbox0/pki/manager-ca.pem"
	ctldCertificateFile          = "/etc/sandbox0/pki/ctld.pem"
	ctldKeyFile                  = "/etc/sandbox0/pki/ctld-key.pem"
	managerTokenFile             = "/etc/sandbox0/tokens/manager.token"
	nomadRuntimeCAFile           = "/etc/sandbox0/pki/nomad-ca.pem"
	nomadRuntimeCertificateFile  = "/etc/sandbox0/pki/nomad.pem"
	nomadRuntimeKeyFile          = "/etc/sandbox0/pki/nomad-key.pem"
	nodeBootstrapCompleteFile    = "/etc/sandbox0/node-bootstrap-complete"
	initialRegistrationTimeout   = 5 * time.Minute
	enrollmentConvergenceTimeout = 5 * time.Minute
)

var nomadNodeIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

type exactIdentity struct {
	SchemaVersion         int    `json:"schema_version"`
	ProviderInstanceID    string `json:"provider_instance_id"`
	PrivateIP             string `json:"private_ip"`
	NodeName              string `json:"node_name"`
	NodeID                string `json:"node_id"`
	NodeUID               string `json:"node_uid"`
	AgentUID              string `json:"agent_uid"`
	AuthorityCommonName   string `json:"authority_common_name"`
	AllocationCIDR        string `json:"allocation_cidr"`
	RuntimeSourceCommit   string `json:"runtime_source_commit"`
	RuntimeBundleSHA256   string `json:"runtime_bundle_sha256"`
	EnrollmentCompletedAt string `json:"enrollment_completed_at"`
}

type commandRunner interface {
	Run(context.Context, string, ...string) error
}

type execRunner struct{}

func (execRunner) Run(ctx context.Context, name string, args ...string) error {
	command := exec.CommandContext(ctx, name, args...)
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	if err := command.Run(); err != nil {
		message := strings.TrimSpace(output.String())
		if len(message) > 4096 {
			message = message[len(message)-4096:]
		}
		return fmt.Errorf("run %s: %w: %s", name, err, message)
	}
	return nil
}

// Bootstrapper owns the enrollment and host installation transaction for one
// disposable worker. Provider lifecycle hooks remain fail-closed until this
// transaction has produced capacity heartbeats and manager admission.
type Bootstrapper struct {
	config     Config
	metadata   *MetadataClient
	enrollment *EnrollmentClient
	runner     commandRunner
	now        func() time.Time
}

func New(config Config) (*Bootstrapper, error) {
	if err := config.normalize(); err != nil {
		return nil, err
	}
	enrollment, err := NewEnrollmentClient(config.EnrollmentURL, config.EnrollmentCAFile)
	if err != nil {
		return nil, err
	}
	return &Bootstrapper{config: config, metadata: NewMetadataClient(),
		enrollment: enrollment, runner: execRunner{}, now: time.Now}, nil
}

// Initial installs the immutable runtime already downloaded by cloud-init,
// rotates the bootstrap Nomad identity to the exact node ID, starts ctld, and
// requests scheduling admission only after the node advertises ready metadata.
func (b *Bootstrapper) Initial(ctx context.Context, responseFile string) error {
	if os.Geteuid() != 0 {
		return errors.New("node bootstrap must run as root")
	}
	response, err := loadBootstrapResponse(responseFile)
	if err != nil {
		return err
	}
	instanceID, err := b.metadata.InstanceID(ctx)
	if err != nil {
		return err
	}
	if instanceID != response.ProviderInstanceID {
		return errors.New("bootstrap response belongs to another ECS instance")
	}
	identity, err := b.currentMetadataIdentity(ctx, instanceID)
	if err != nil {
		return err
	}
	release, err := b.validateRuntimeRelease(response.RuntimeArtifact)
	if err != nil {
		return err
	}
	if err := b.prepareNomadHost(ctx, release); err != nil {
		return err
	}
	nomadSigner, err := loadSigner(b.config.NomadKeyFile)
	if err != nil {
		return err
	}
	datacenter := strings.ReplaceAll(b.config.RegionID, "-", "_")
	if err := verifyIssuedCertificate(response.NomadCertificatePEM, response.NomadCACertificatePEM,
		nomadSigner, "client."+datacenter+".nomad", identity.privateIP,
		"spiffe://sandbox0.internal/"+b.config.RegionID+"/nomad/client/bootstrap/"+response.NodeName,
		x509.ExtKeyUsageClientAuth); err != nil {
		return fmt.Errorf("verify Nomad bootstrap identity: %w", err)
	}
	configPayload, err := renderNomadClientConfig(b.config, response.NodeName, identity.privateIP, false)
	if err != nil {
		return err
	}
	if err := atomicWriteFile(nomadConfigFile, configPayload, 0o600); err != nil {
		return err
	}
	for _, file := range []struct {
		name    string
		payload []byte
		mode    fs.FileMode
	}{
		{exactNomadCAFile, response.NomadCACertificatePEM, 0o644},
		{exactNomadCertificateFile, response.NomadCertificatePEM, 0o644},
	} {
		if err := atomicWriteFile(file.name, file.payload, file.mode); err != nil {
			return err
		}
	}
	keyPayload, err := os.ReadFile(b.config.NomadKeyFile)
	if err != nil {
		return err
	}
	if err := atomicWriteFile(exactNomadKeyFile, keyPayload, 0o600); err != nil {
		return err
	}
	if strings.Count(response.NomadIntroductionJWT, ".") != 2 {
		return errors.New("bootstrap response has an invalid Nomad introduction token")
	}
	if err := atomicWriteFile(nomadIntroductionTokenFile, []byte(response.NomadIntroductionJWT), 0o600); err != nil {
		return err
	}
	if err := b.runner.Run(ctx, "systemctl", "daemon-reload"); err != nil {
		return err
	}
	if err := b.runner.Run(ctx, "systemctl", "enable", "--now", "nomad.service"); err != nil {
		return err
	}
	nodeID, err := waitForNomadNodeID(ctx, initialRegistrationTimeout)
	if err != nil {
		return err
	}
	authoritySigner, err := ensureAuthoritySigner(b.config.AuthorityKeyFile)
	if err != nil {
		return err
	}
	nomadCSR, err := makeCSR(nomadSigner)
	if err != nil {
		return err
	}
	authorityCSR, err := makeCSR(authoritySigner)
	if err != nil {
		return err
	}
	finalized, err := b.finalizeUntilReady(ctx, instanceID, nodeID, nomadCSR, authorityCSR)
	if err != nil {
		return err
	}
	if finalized.NodeUID != response.NodeUID {
		return errors.New("finalized node UID differs from its bootstrap reservation")
	}
	staged, err := stageRuntimeConfig(finalized.NodeRuntimeArchive)
	if err != nil {
		return err
	}
	defer staged.close()
	if err := validateRuntimeConfigIdentity(staged, response.NodeName, nodeID, finalized.NodeUID,
		b.config.RegionID, b.config.ClusterID, response.AllocationCIDR); err != nil {
		return err
	}
	managerCA, err := os.ReadFile(staged.path("etc/sandbox0/pki/manager-ca.pem"))
	if err != nil {
		return err
	}
	if err := b.verifyExactCertificates(finalized, nomadSigner, authoritySigner,
		response.NodeName, nodeID, identity.privateIP, managerCA); err != nil {
		return err
	}
	if err := b.runner.Run(ctx, "systemctl", "stop", "nomad.service"); err != nil {
		return err
	}
	if err := staged.install(); err != nil {
		return err
	}
	if err := b.installExactIdentity(response, finalized, nodeID, identity.privateIP,
		keyPayload, authoritySigner); err != nil {
		return err
	}
	if err := b.installCTLD(ctx, release, staged); err != nil {
		return err
	}
	if err := b.runner.Run(ctx, "systemctl", "enable", "--now", "nomad.service"); err != nil {
		return err
	}
	admittedConfig, err := renderNomadClientConfig(b.config, response.NodeName, identity.privateIP, true)
	if err != nil {
		return err
	}
	if err := atomicWriteFile(nomadConfigFile, admittedConfig, 0o600); err != nil {
		return err
	}
	if err := b.runner.Run(ctx, "systemctl", "restart", "nomad.service"); err != nil {
		return err
	}
	if err := b.admitUntilReady(ctx, instanceID, nodeID, finalized.NodeUID); err != nil {
		return err
	}
	if err := b.installRenewalTimer(ctx, release); err != nil {
		return err
	}
	for _, file := range []string{responseFile, b.config.NomadKeyFile, b.config.AuthorityKeyFile} {
		if err := os.Remove(file); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove bootstrap-only identity material: %w", err)
		}
	}
	return atomicWriteFile(nodeBootstrapCompleteFile,
		[]byte(b.now().UTC().Format(time.RFC3339)+"\n"), 0o644)
}

// Renew rotates short-lived exact certificates without changing any sandbox
// state. Manager temporarily fences new placements; Nomad reload and the A/B
// ctld rollout preserve existing allocations and reopen scheduling afterward.
func (b *Bootstrapper) Renew(ctx context.Context) error {
	if os.Geteuid() != 0 {
		return errors.New("node bootstrap renewal must run as root")
	}
	needsRenewal, err := certificateNeedsRenewal(exactNomadCertificateFile,
		time.Duration(b.config.RenewBeforeSeconds)*time.Second)
	if err != nil || !needsRenewal {
		return err
	}
	identity, err := loadExactIdentity(b.config.ExactIdentityFile)
	if err != nil {
		return err
	}
	providerID, err := b.metadata.InstanceID(ctx)
	if err != nil {
		return err
	}
	if providerID != identity.ProviderInstanceID {
		return errors.New("exact node identity belongs to another ECS instance")
	}
	metadata, err := b.currentMetadataIdentity(ctx, providerID)
	if err != nil {
		return err
	}
	if metadata.privateIP != identity.PrivateIP {
		return errors.New("exact node identity private address changed")
	}
	nomadSigner, err := loadSigner(exactNomadKeyFile)
	if err != nil {
		return err
	}
	authoritySigner, err := loadSigner(ctldKeyFile)
	if err != nil {
		return err
	}
	nomadCSR, err := makeCSR(nomadSigner)
	if err != nil {
		return err
	}
	authorityCSR, err := makeCSR(authoritySigner)
	if err != nil {
		return err
	}
	finalized, err := b.finalizeUntilReady(ctx, providerID, identity.NodeID, nomadCSR, authorityCSR)
	if err != nil {
		return err
	}
	if finalized.NodeUID != identity.NodeUID || finalized.AgentUID != identity.AgentUID ||
		finalized.AuthorityCommonName != identity.AuthorityCommonName {
		return errors.New("renewed node identity differs from the durable exact identity")
	}
	managerCA, err := os.ReadFile(managerCAFile)
	if err != nil {
		return err
	}
	if err := b.verifyExactCertificates(finalized, nomadSigner, authoritySigner,
		identity.NodeName, identity.NodeID, identity.PrivateIP, managerCA); err != nil {
		return err
	}
	if err := atomicWriteFile(exactNomadCertificateFile, finalized.NomadCertificatePEM, 0o644); err != nil {
		return err
	}
	if err := atomicWriteFile(nomadRuntimeCertificateFile, finalized.NomadCertificatePEM, 0o644); err != nil {
		return err
	}
	if err := atomicWriteFile(ctldCertificateFile, finalized.NodeAuthorityCertificatePEM, 0o644); err != nil {
		return err
	}
	if err := b.runner.Run(ctx, "systemctl", "reload", "nomad.service"); err != nil {
		return err
	}
	if err := b.runner.Run(ctx, "/usr/local/libexec/sandbox0/ctld-rollout-node"); err != nil {
		return err
	}
	return b.admitUntilReady(ctx, providerID, identity.NodeID, identity.NodeUID)
}

func (b *Bootstrapper) currentMetadataIdentity(ctx context.Context, instanceID string) (signedIdentity, error) {
	// This audience is used only to obtain and locally parse a fresh document;
	// manager receives separately challenged signed documents for every mutation.
	identity, err := b.metadata.SignedIdentity(ctx, "sandbox0-local-identity-check")
	if err != nil {
		return signedIdentity{}, err
	}
	if identity.instanceID != instanceID || net.ParseIP(identity.privateIP) == nil {
		return signedIdentity{}, errors.New("instance metadata identity changed during bootstrap")
	}
	return identity, nil
}

func (b *Bootstrapper) finalizeUntilReady(
	ctx context.Context,
	instanceID, nodeID string,
	nomadCSR, authorityCSR []byte,
) (*nodeenrollment.FinalizeResponse, error) {
	deadline := b.now().Add(enrollmentConvergenceTimeout)
	var lastErr error
	for b.now().Before(deadline) {
		challenge, err := b.enrollment.Challenge(ctx, instanceID)
		if err == nil {
			var identity signedIdentity
			identity, err = b.metadata.SignedIdentity(ctx, challenge.Audience)
			if err == nil && identity.instanceID == instanceID {
				var response *nodeenrollment.FinalizeResponse
				response, err = b.enrollment.Finalize(ctx, identity, challenge, nodeID, nomadCSR, authorityCSR)
				if err == nil {
					return response, nil
				}
			}
		}
		lastErr = err
		if err := sleepContext(ctx, 5*time.Second); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("exact node enrollment did not converge: %w", lastErr)
}

func (b *Bootstrapper) admitUntilReady(ctx context.Context, instanceID, nodeID, nodeUID string) error {
	deadline := b.now().Add(enrollmentConvergenceTimeout)
	var lastErr error
	for b.now().Before(deadline) {
		challenge, err := b.enrollment.Challenge(ctx, instanceID)
		if err == nil {
			var identity signedIdentity
			identity, err = b.metadata.SignedIdentity(ctx, challenge.Audience)
			if err == nil && identity.instanceID == instanceID {
				var response *nodeenrollment.AdmitResponse
				response, err = b.enrollment.Admit(ctx, identity, challenge, nodeID)
				if err == nil && response.Admitted && response.NodeUID == nodeUID {
					return nil
				}
				if err == nil {
					err = errors.New("manager returned a non-exact node admission")
				}
			}
		}
		lastErr = err
		if err := sleepContext(ctx, 5*time.Second); err != nil {
			return err
		}
	}
	return fmt.Errorf("node scheduling admission did not converge: %w", lastErr)
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func loadBootstrapResponse(file string) (*nodeenrollment.BootstrapResponse, error) {
	payload, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	if len(payload) == 0 || len(payload) > 1<<20 {
		return nil, errors.New("node bootstrap response size is invalid")
	}
	var response nodeenrollment.BootstrapResponse
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&response); err != nil {
		return nil, err
	}
	if response.ProviderInstanceID == "" || response.NodeName == "" || response.NodeUID == "" ||
		response.AllocationCIDR == "" || len(response.NomadCertificatePEM) == 0 ||
		len(response.NomadCACertificatePEM) == 0 || response.NomadIntroductionJWT == "" {
		return nil, errors.New("node bootstrap response is incomplete")
	}
	return &response, nil
}

func loadExactIdentity(file string) (*exactIdentity, error) {
	payload, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	var identity exactIdentity
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&identity); err != nil {
		return nil, err
	}
	if identity.SchemaVersion != 1 || identity.ProviderInstanceID == "" ||
		!nomadNodeIDPattern.MatchString(identity.NodeID) || identity.NodeUID == "" ||
		identity.AgentUID == "" || identity.AuthorityCommonName == "" {
		return nil, errors.New("exact node identity file is invalid")
	}
	return &identity, nil
}

func waitForNomadNodeID(ctx context.Context, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		payload, err := os.ReadFile(nomadClientIDFile)
		if err == nil {
			nodeID := strings.TrimSpace(string(payload))
			if nomadNodeIDPattern.MatchString(nodeID) {
				return nodeID, nil
			}
		}
		if err := sleepContext(ctx, time.Second); err != nil {
			return "", err
		}
	}
	return "", errors.New("nomad client did not persist its exact node ID")
}
