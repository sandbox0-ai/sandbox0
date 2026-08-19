package rootfswriterauthority

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
)

const maxResponseBytes = 64 << 10

// PublishGenerationRequest submits a node's local detach proof and sealed
// block generation to the regional writer authority.
type PublishGenerationRequest struct {
	WriterEpoch             int64                         `json:"writer_epoch"`
	BindingVersion          int                           `json:"binding_version"`
	BindingDigest           string                        `json:"binding_digest"`
	OperationID             string                        `json:"operation_id"`
	ProofDigest             string                        `json:"proof_digest"`
	ExpectedOldGenerationID string                        `json:"expected_old_generation_id"`
	Generation              sandboxstore.RootFSGeneration `json:"generation"`
}

// CrashAbandonBeginRequest asks the regional authority to fence one expired
// writer before the node discards its unsealed physical branch.
type CrashAbandonBeginRequest struct {
	WriterEpoch             int64  `json:"writer_epoch"`
	BindingVersion          int    `json:"binding_version"`
	BindingDigest           string `json:"binding_digest"`
	OperationID             string `json:"operation_id"`
	ExpectedOldGenerationID string `json:"expected_old_generation_id"`
}

// CrashAbandonCompleteRequest submits the node's terminal absence proof after
// the regional fence has become authoritative.
type CrashAbandonCompleteRequest struct {
	CrashAbandonBeginRequest
	Proof rootfshandoff.CrashFenceProof `json:"proof"`
}

type ManagerClientConfig struct {
	BaseURL        string
	CAFile         string
	ClientCertFile string
	ClientKeyFile  string
	TokenFile      string
	Timeout        time.Duration
}

type ManagerClient struct {
	baseURL   *url.URL
	tokenFile string
	http      *http.Client
}

func NewManagerClient(config ManagerClientConfig) (*ManagerClient, error) {
	baseURL, err := url.Parse(strings.TrimSpace(config.BaseURL))
	if err != nil || baseURL.Scheme != "https" || baseURL.Host == "" || baseURL.User != nil || baseURL.RawQuery != "" || baseURL.Fragment != "" {
		return nil, fmt.Errorf("writer authority URL must be an HTTPS origin: %w", errdefs.ErrInvalidArgument)
	}
	if strings.TrimSpace(config.CAFile) == "" || strings.TrimSpace(config.TokenFile) == "" {
		return nil, fmt.Errorf("writer authority CA and projected token files are required: %w", errdefs.ErrInvalidArgument)
	}
	var clientCertificates []tls.Certificate
	if strings.TrimSpace(config.ClientCertFile) != "" || strings.TrimSpace(config.ClientKeyFile) != "" {
		if strings.TrimSpace(config.ClientCertFile) == "" || strings.TrimSpace(config.ClientKeyFile) == "" {
			return nil, fmt.Errorf("writer authority client certificate and key must be configured together: %w", errdefs.ErrInvalidArgument)
		}
		clientCertificate, err := tls.LoadX509KeyPair(config.ClientCertFile, config.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load writer authority client identity: %w", err)
		}
		clientCertificates = append(clientCertificates, clientCertificate)
	}
	caPEM, err := os.ReadFile(config.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read writer authority CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("writer authority CA contains no certificates: %w", errdefs.ErrInvalidArgument)
	}
	if config.Timeout <= 0 {
		config.Timeout = 2 * time.Second
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{
		MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: clientCertificates,
	}
	return &ManagerClient{
		baseURL: baseURL, tokenFile: strings.TrimSpace(config.TokenFile),
		http: &http.Client{Transport: transport, Timeout: config.Timeout},
	}, nil
}

func (c *ManagerClient) ConsumeWriterGrant(ctx context.Context, stage rootfshandoff.StageRequest) (protocol.LeaseObservation, error) {
	binding, err := writerGrantBinding(stage)
	if err != nil {
		return protocol.LeaseObservation{}, err
	}
	var observation protocol.LeaseObservation
	if err := c.putWriterGrant(ctx, "consume", protocol.ConsumePath(stage.Identity.WriterGrantID), protocol.ConsumeRequest(binding), &observation); err != nil {
		return protocol.LeaseObservation{}, err
	}
	return observation, nil
}

// PublishWriterGrant terminally publishes one locally sealed generation.
func (c *ManagerClient) PublishWriterGrant(ctx context.Context, stage rootfshandoff.StageRequest, request PublishGenerationRequest) error {
	binding, err := durableWriterGrantBinding(stage)
	if err != nil {
		return err
	}
	request.WriterEpoch = binding.WriterEpoch
	request.BindingVersion = binding.BindingVersion
	request.BindingDigest = binding.BindingDigest
	if strings.TrimSpace(request.OperationID) == "" || strings.TrimSpace(request.ProofDigest) == "" ||
		strings.TrimSpace(request.ExpectedOldGenerationID) == "" || request.Generation.Descriptor == nil {
		return fmt.Errorf("invalid terminal writer publication: %w", errdefs.ErrInvalidArgument)
	}
	return c.putWriterGrant(ctx, "publish", protocol.TerminalPath(stage.Identity.WriterGrantID)+"/publish", request, nil)
}

// BeginCrashAbandonWriterGrant establishes the regional lease fence before
// local unplanned cleanup. Callers may retry the exact operation while the
// lease remains within the server's renewal grace period.
func (c *ManagerClient) BeginCrashAbandonWriterGrant(
	ctx context.Context,
	stage rootfshandoff.StageRequest,
	operationID string,
) error {
	request, err := crashAbandonBeginRequest(stage, operationID)
	if err != nil {
		return err
	}
	path := protocol.TerminalPath(stage.Identity.WriterGrantID) + "/crash-abandon/begin"
	return c.putWriterGrant(ctx, "begin crash abandon", path, request, nil)
}

// CompleteCrashAbandonWriterGrant retires the fenced grant without advancing
// its durable generation.
func (c *ManagerClient) CompleteCrashAbandonWriterGrant(
	ctx context.Context,
	stage rootfshandoff.StageRequest,
	operationID string,
	proof rootfshandoff.CrashFenceProof,
) error {
	request, err := crashAbandonBeginRequest(stage, operationID)
	if err != nil {
		return err
	}
	if err := proof.Validate(); err != nil {
		return fmt.Errorf("validate crash fence proof: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	if proof.OperationID != request.OperationID || proof.WriterGrantID != stage.Identity.WriterGrantID ||
		proof.WriterEpoch != request.WriterEpoch || proof.BindingVersion != request.BindingVersion ||
		proof.BindingDigest != request.BindingDigest || proof.InitialGeneration != request.ExpectedOldGenerationID {
		return fmt.Errorf("crash fence proof does not match the writer binding: %w", errdefs.ErrInvalidArgument)
	}
	path := protocol.TerminalPath(stage.Identity.WriterGrantID) + "/crash-abandon/complete"
	return c.putWriterGrant(ctx, "complete crash abandon", path, CrashAbandonCompleteRequest{
		CrashAbandonBeginRequest: request,
		Proof:                    proof,
	}, nil)
}

func crashAbandonBeginRequest(
	stage rootfshandoff.StageRequest,
	operationID string,
) (CrashAbandonBeginRequest, error) {
	binding, err := durableWriterGrantBinding(stage)
	if err != nil {
		return CrashAbandonBeginRequest{}, err
	}
	request := CrashAbandonBeginRequest{
		WriterEpoch: binding.WriterEpoch, BindingVersion: binding.BindingVersion,
		BindingDigest: binding.BindingDigest, OperationID: strings.TrimSpace(operationID),
		ExpectedOldGenerationID: strings.TrimSpace(stage.InitialGeneration),
	}
	if request.OperationID == "" || request.ExpectedOldGenerationID == "" {
		return CrashAbandonBeginRequest{}, fmt.Errorf("invalid crash abandon request: %w", errdefs.ErrInvalidArgument)
	}
	return request, nil
}

// RenewWriterGrant extends the lease for the exact consumed Stage binding.
// The authenticated node is the durable owner, so renewal does not retain or
// transmit the one-time raw grant token used by ConsumeWriterGrant.
func (c *ManagerClient) RenewWriterGrant(ctx context.Context, stage rootfshandoff.StageRequest) (protocol.LeaseObservation, error) {
	binding, err := durableWriterGrantBinding(stage)
	if err != nil {
		return protocol.LeaseObservation{}, err
	}
	request := protocol.RenewRequest(binding)
	if err := request.Validate(); err != nil {
		return protocol.LeaseObservation{}, fmt.Errorf("validate writer renewal: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	var observation protocol.LeaseObservation
	if err := c.putWriterGrant(ctx, "renew", protocol.RenewPath(stage.Identity.WriterGrantID), request, &observation); err != nil {
		return protocol.LeaseObservation{}, err
	}
	return observation, nil
}

// RenewWriterGrants authenticates one node request and renews multiple exact
// durable bindings. Per-grant authorization results remain independent.
func (c *ManagerClient) RenewWriterGrants(ctx context.Context, stages []rootfshandoff.StageRequest) (protocol.BatchRenewResponse, error) {
	request := protocol.BatchRenewRequest{Items: make([]protocol.BatchRenewItem, 0, len(stages))}
	for _, stage := range stages {
		binding, err := durableWriterGrantBinding(stage)
		if err != nil {
			return protocol.BatchRenewResponse{}, err
		}
		request.Items = append(request.Items, protocol.BatchRenewItem{
			GrantID: stage.Identity.WriterGrantID, RenewRequest: protocol.RenewRequest(binding),
		})
	}
	if err := request.Validate(); err != nil {
		return protocol.BatchRenewResponse{}, fmt.Errorf("validate writer renewal batch: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	var response protocol.BatchRenewResponse
	if err := c.putWriterGrant(ctx, "batch renew", protocol.BatchRenewPath, request, &response); err != nil {
		return protocol.BatchRenewResponse{}, err
	}
	if err := response.Validate(len(request.Items)); err != nil {
		return protocol.BatchRenewResponse{}, fmt.Errorf("validate writer renewal batch response: %w: %w", err, errdefs.ErrUnavailable)
	}
	return response, nil
}

func durableWriterGrantBinding(stage rootfshandoff.StageRequest) (protocol.TerminalRequest, error) {
	if err := stage.ValidateDurableBinding(); err != nil {
		return protocol.TerminalRequest{}, fmt.Errorf("validate durable writer binding: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	digest, err := stage.BindingDigest()
	if err != nil {
		return protocol.TerminalRequest{}, fmt.Errorf("digest durable writer binding: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	return protocol.TerminalRequest{
		WriterEpoch: stage.Identity.WriterEpoch, BindingVersion: stage.BindingVersion,
		BindingDigest: hex.EncodeToString(digest[:]),
	}, nil
}

// VerifyTerminalWriterGrant proves that the regional writer authority has
// irreversibly retired or canceled the exact durable Stage binding.
func (c *ManagerClient) VerifyTerminalWriterGrant(ctx context.Context, stage rootfshandoff.StageRequest) error {
	binding, err := durableWriterGrantBinding(stage)
	if err != nil {
		return err
	}
	request := protocol.TerminalRequest(binding)
	if err := request.Validate(); err != nil {
		return fmt.Errorf("validate terminal writer proof: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	return c.putWriterGrant(ctx, "verify terminal", protocol.TerminalPath(stage.Identity.WriterGrantID), request, nil)
}

func writerGrantBinding(stage rootfshandoff.StageRequest) (protocol.ConsumeRequest, error) {
	if err := stage.Validate(); err != nil {
		return protocol.ConsumeRequest{}, fmt.Errorf("validate writer binding: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	digest, err := stage.BindingDigest()
	if err != nil {
		return protocol.ConsumeRequest{}, fmt.Errorf("digest writer binding: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	return protocol.ConsumeRequest{
		WriterEpoch: stage.Identity.WriterEpoch, BindingVersion: stage.BindingVersion,
		BindingDigest: hex.EncodeToString(digest[:]), WriterToken: stage.Identity.WriterGrantToken,
	}, nil
}

func (c *ManagerClient) putWriterGrant(ctx context.Context, operation, path string, body, result any) error {
	token, err := os.ReadFile(c.tokenFile)
	if err != nil {
		return fmt.Errorf("read projected service account token: %w: %w", err, errdefs.ErrUnavailable)
	}
	bearer := strings.TrimSpace(string(token))
	if bearer == "" {
		return fmt.Errorf("projected service account token is empty: %w", errdefs.ErrUnavailable)
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	target := *c.baseURL
	target.Path = strings.TrimRight(target.Path, "/") + path
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, target.String(), bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+bearer)
	request.Header.Set("Content-Type", "application/json")
	response, err := c.http.Do(request)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return fmt.Errorf("%s writer grant: %w: %w", operation, err, errdefs.ErrUnavailable)
	}
	defer response.Body.Close()
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		if result != nil {
			if err := json.NewDecoder(io.LimitReader(response.Body, maxResponseBytes)).Decode(result); err != nil {
				return fmt.Errorf("decode %s writer grant response: %w: %w", operation, err, errdefs.ErrUnavailable)
			}
			if observation, ok := result.(*protocol.LeaseObservation); ok {
				if err := observation.Validate(); err != nil {
					return fmt.Errorf("validate %s writer grant response: %w: %w", operation, err, errdefs.ErrUnavailable)
				}
			}
		}
		return nil
	}
	responsePayload, _ := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes))
	message := fmt.Sprintf("manager writer authority returned %s: %s", response.Status, strings.TrimSpace(string(responsePayload)))
	switch response.StatusCode {
	case http.StatusBadRequest:
		return fmt.Errorf("%s: %w", message, errdefs.ErrInvalidArgument)
	case http.StatusUnauthorized, http.StatusForbidden:
		return fmt.Errorf("%s: %w", message, errdefs.ErrPermissionDenied)
	case http.StatusNotFound:
		return fmt.Errorf("%s: %w", message, errdefs.ErrNotFound)
	case http.StatusPreconditionFailed:
		return fmt.Errorf("%s: %w", message, errdefs.ErrFailedPrecondition)
	case http.StatusGatewayTimeout:
		return fmt.Errorf("%s: %w", message, context.DeadlineExceeded)
	default:
		if response.StatusCode >= 400 && response.StatusCode < 500 {
			return fmt.Errorf("%s: %w", message, errdefs.ErrFailedPrecondition)
		}
		return fmt.Errorf("%s: %w", message, errdefs.ErrUnavailable)
	}
}
