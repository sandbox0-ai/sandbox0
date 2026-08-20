package runtimeslot

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/containerd/errdefs"
	"github.com/gorilla/websocket"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
)

const (
	defaultNodeChannelOperationTimeout = 30 * time.Second
	defaultNodeChannelReconnectMin     = 100 * time.Millisecond
	defaultNodeChannelReconnectMax     = 10 * time.Second
	defaultNodeChannelHandshakeTimeout = 5 * time.Second
	defaultNodeChannelConnectionMaxAge = 5 * time.Minute
	defaultNodeChannelMaxConcurrent    = 64
	maxNodeChannelCredentialBytes      = 64 << 10
)

// NodeChannelExecutor owns node-local privilege. Claim and CommandReady must
// call the exact root-owned driver socket from Target; Cleanup must call the
// plugin-independent root-owned cleanup runtime.
type NodeChannelExecutor interface {
	Claim(context.Context, NodeChannelTarget, NodeClaimControlRequest) (NodeControlResponse, error)
	CommandReady(context.Context, NodeChannelTarget, CommandReadyControlRequest) (NodeControlResponse, error)
	Cleanup(context.Context, NodeChannelTarget, NodeCleanupControlRequest) (NodeCleanupControlProof, error)
}

// NodeChannelNetworkExecutor owns ctld-backed policy application. It is
// separate so a node agent cannot advertise network preparation until the
// authoritative local runtime is configured.
type NodeChannelNetworkExecutor interface {
	PrepareNetwork(context.Context, NodeChannelTarget, NodeNetworkPrepareControlRequest) (rootfshandoff.NetworkPolicyToken, error)
}

// NodeChannelAgentConfig configures one node-initiated mTLS command stream.
// Certificates, CA, boot ID, and bearer token are reloaded on reconnect.
type NodeChannelAgentConfig struct {
	BaseURL         string
	CAFile          string
	ClientCertFile  string
	ClientKeyFile   string
	TokenFile       string
	PeerURISAN      string
	ClusterID       string
	NodeID          string
	NodeUID         string
	NodeBootIDFile  string
	Executor        NodeChannelExecutor
	NetworkExecutor NodeChannelNetworkExecutor

	OperationTimeout time.Duration
	ReconnectMin     time.Duration
	ReconnectMax     time.Duration
	ConnectionMaxAge time.Duration
	MaxConcurrent    int
	AgentInstanceID  string
}

// NodeChannelAgent maintains an outbound stream and executes a bounded number
// of independent commands concurrently. A response loss is safe because
// node-local operations are exact-retry protocols and cleanup proof is durable
// before it is returned.
type NodeChannelAgent struct {
	config          NodeChannelAgentConfig
	baseURL         *url.URL
	agentInstanceID string
}

// NewNodeChannelAgent validates immutable channel policy.
func NewNodeChannelAgent(config NodeChannelAgentConfig) (*NodeChannelAgent, error) {
	baseURL, err := url.Parse(strings.TrimSpace(config.BaseURL))
	if err != nil || baseURL.Scheme != "https" || baseURL.Host == "" || baseURL.User != nil ||
		baseURL.Path != "" || baseURL.RawPath != "" || baseURL.RawQuery != "" || baseURL.Fragment != "" ||
		config.BaseURL != strings.TrimSpace(config.BaseURL) {
		return nil, fmt.Errorf("node channel URL must be a canonical HTTPS origin: %w", errdefs.ErrInvalidArgument)
	}
	identityFields := []struct{ name, value string }{
		{name: "cluster_id", value: config.ClusterID},
		{name: "node_id", value: config.NodeID},
		{name: "node_uid", value: config.NodeUID},
	}
	for _, field := range identityFields {
		if err := validateRequiredID(field.name, field.value); err != nil {
			return nil, fmt.Errorf("node channel: %v: %w", err, errdefs.ErrInvalidArgument)
		}
	}
	files := []struct{ name, path string }{
		{name: "CA", path: config.CAFile},
		{name: "client certificate", path: config.ClientCertFile},
		{name: "client key", path: config.ClientKeyFile},
		{name: "bearer token", path: config.TokenFile},
		{name: "node boot ID", path: config.NodeBootIDFile},
	}
	for _, file := range files {
		path := strings.TrimSpace(file.path)
		if path != file.path || !filepath.IsAbs(path) || path == string(filepath.Separator) || filepath.Clean(path) != path {
			return nil, fmt.Errorf("node channel %s file must be a canonical non-root absolute path: %w", file.name, errdefs.ErrInvalidArgument)
		}
	}
	peerURI, err := url.Parse(config.PeerURISAN)
	if err != nil || peerURI.Scheme != "spiffe" || peerURI.Host == "" || peerURI.User != nil ||
		peerURI.RawQuery != "" || peerURI.Fragment != "" || peerURI.String() != config.PeerURISAN ||
		len(config.PeerURISAN) > 2048 {
		return nil, fmt.Errorf("node channel peer URI SAN must be canonical SPIFFE: %w", errdefs.ErrInvalidArgument)
	}
	if config.Executor == nil {
		return nil, errors.New("node channel executor is required")
	}
	if config.OperationTimeout == 0 {
		config.OperationTimeout = defaultNodeChannelOperationTimeout
	}
	if config.OperationTimeout < time.Second || config.OperationTimeout > 5*time.Minute {
		return nil, fmt.Errorf("node channel operation timeout must be between one second and five minutes: %w", errdefs.ErrInvalidArgument)
	}
	if config.ReconnectMin == 0 {
		config.ReconnectMin = defaultNodeChannelReconnectMin
	}
	if config.ReconnectMax == 0 {
		config.ReconnectMax = defaultNodeChannelReconnectMax
	}
	if config.ReconnectMin <= 0 || config.ReconnectMax < config.ReconnectMin || config.ReconnectMax > time.Minute {
		return nil, fmt.Errorf("node channel reconnect bounds are invalid: %w", errdefs.ErrInvalidArgument)
	}
	if config.ConnectionMaxAge == 0 {
		config.ConnectionMaxAge = defaultNodeChannelConnectionMaxAge
	}
	if config.ConnectionMaxAge < time.Minute || config.ConnectionMaxAge > time.Hour {
		return nil, fmt.Errorf("node channel connection max age must be between one minute and one hour: %w", errdefs.ErrInvalidArgument)
	}
	if config.MaxConcurrent == 0 {
		config.MaxConcurrent = defaultNodeChannelMaxConcurrent
	}
	if config.MaxConcurrent < 1 || config.MaxConcurrent > 1024 {
		return nil, fmt.Errorf("node channel concurrency must be between one and 1024: %w", errdefs.ErrInvalidArgument)
	}
	agentInstanceID := strings.TrimSpace(config.AgentInstanceID)
	if agentInstanceID == "" {
		identity := make([]byte, 16)
		if _, err := rand.Read(identity); err != nil {
			return nil, fmt.Errorf("generate node channel agent identity: %w", err)
		}
		agentInstanceID = "agent-" + hex.EncodeToString(identity)
	}
	if agentInstanceID != config.AgentInstanceID && config.AgentInstanceID != "" {
		return nil, fmt.Errorf("node channel agent instance ID must be canonical: %w", errdefs.ErrInvalidArgument)
	}
	if err := validateRequiredID("agent_instance_id", agentInstanceID); err != nil {
		return nil, fmt.Errorf("node channel: %v: %w", err, errdefs.ErrInvalidArgument)
	}
	return &NodeChannelAgent{config: config, baseURL: baseURL, agentInstanceID: agentInstanceID}, nil
}

// Run reconnects transient failures with bounded jitter. Authentication and
// local configuration failures terminate instead of creating a retry storm.
func (a *NodeChannelAgent) Run(ctx context.Context) error {
	if a == nil || a.baseURL == nil || a.config.Executor == nil {
		return fmt.Errorf("node channel agent is not initialized: %w", errdefs.ErrUnavailable)
	}
	backoff := a.config.ReconnectMin
	for {
		connectedAt, err := a.runConnection(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errdefs.IsPermissionDenied(err) || errdefs.IsInvalidArgument(err) {
			return err
		}
		if !connectedAt.IsZero() && time.Since(connectedAt) >= 30*time.Second {
			backoff = a.config.ReconnectMin
		}
		delay := jitterNodeChannelBackoff(backoff)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		backoff = min(backoff*2, a.config.ReconnectMax)
	}
}

func (a *NodeChannelAgent) runConnection(ctx context.Context) (time.Time, error) {
	bootID, err := readNodeChannelCredential(a.config.NodeBootIDFile, "node boot ID")
	if err != nil {
		return time.Time{}, err
	}
	if err := validateRequiredID("node_boot_id", bootID); err != nil {
		return time.Time{}, fmt.Errorf("node channel boot identity: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	connection, err := a.dial(ctx)
	if err != nil {
		return time.Time{}, err
	}
	connectedAt := time.Now()
	defer connection.Close()
	connection.SetReadLimit(NodeChannelMaxBytes)
	hello := NodeChannelHello{
		Version: NodeChannelVersion, AgentInstanceID: a.agentInstanceID,
		ClusterID: a.config.ClusterID, NodeID: a.config.NodeID,
		NodeUID: a.config.NodeUID, NodeBootID: bootID,
		Capabilities: []NodeChannelCommandKind{
			NodeChannelCommandClaim, NodeChannelCommandCommandReady, NodeChannelCommandCleanup,
		},
	}
	if a.config.NetworkExecutor != nil {
		hello.Capabilities = append([]NodeChannelCommandKind{NodeChannelCommandNetworkPrepare}, hello.Capabilities...)
	}
	if err := hello.Validate(); err != nil {
		return connectedAt, fmt.Errorf("validate node channel hello: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	if err := writeNodeChannelMessage(connection, hello, time.Now().Add(defaultNodeChannelHandshakeTimeout)); err != nil {
		return connectedAt, fmt.Errorf("write node channel hello: %w: %w", err, errdefs.ErrUnavailable)
	}
	_ = connection.SetReadDeadline(connectedAt.Add(a.config.ConnectionMaxAge))
	connectionCtx, cancelConnection := context.WithCancel(ctx)
	stop := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = connection.Close()
		case <-stop:
		}
	}()
	var operations sync.WaitGroup
	var writeMu sync.Mutex
	semaphore := make(chan struct{}, a.config.MaxConcurrent)
	defer func() {
		cancelConnection()
		_ = connection.Close()
		operations.Wait()
		close(stop)
	}()
	for {
		messageType, payload, err := connection.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return connectedAt, ctx.Err()
			}
			return connectedAt, fmt.Errorf("read node channel command: %w: %w", err, errdefs.ErrUnavailable)
		}
		if messageType != websocket.TextMessage || len(payload) > NodeChannelMaxBytes {
			return connectedAt, fmt.Errorf("node channel command must be bounded text JSON: %w", errdefs.ErrUnavailable)
		}
		var command NodeChannelCommand
		if err := decodeNodeChannelMessage(payload, &command); err != nil {
			return connectedAt, fmt.Errorf("decode node channel command: %w: %w", err, errdefs.ErrUnavailable)
		}
		if err := command.Validate(); err != nil || !hello.Supports(command.Kind) ||
			command.Target.ClusterID != hello.ClusterID || command.Target.NodeID != hello.NodeID ||
			command.Target.NodeUID != hello.NodeUID || command.Target.NodeBootID != hello.NodeBootID {
			return connectedAt, fmt.Errorf("node channel command target is invalid: %v: %w", err, errdefs.ErrPermissionDenied)
		}
		select {
		case semaphore <- struct{}{}:
		case <-connectionCtx.Done():
			return connectedAt, connectionCtx.Err()
		}
		operations.Add(1)
		go func(command NodeChannelCommand) {
			defer operations.Done()
			defer func() { <-semaphore }()
			result := a.execute(connectionCtx, command)
			if err := result.ValidateFor(command); err != nil {
				_ = connection.Close()
				return
			}
			writeMu.Lock()
			err := writeNodeChannelMessage(connection, result, time.Now().Add(defaultNodeChannelHandshakeTimeout))
			writeMu.Unlock()
			if err != nil {
				_ = connection.Close()
			}
		}(command)
	}
}

func (a *NodeChannelAgent) execute(ctx context.Context, command NodeChannelCommand) NodeChannelResult {
	result := NodeChannelResult{Version: NodeChannelVersion, RequestID: command.RequestID, Kind: command.Kind}
	operationCtx, cancel := context.WithTimeout(ctx, a.config.OperationTimeout)
	defer cancel()
	var err error
	switch command.Kind {
	case NodeChannelCommandNetworkPrepare:
		if a.config.NetworkExecutor == nil {
			err = fmt.Errorf("node network policy executor is unavailable: %w", errdefs.ErrFailedPrecondition)
			break
		}
		var token rootfshandoff.NetworkPolicyToken
		token, err = a.config.NetworkExecutor.PrepareNetwork(operationCtx, command.Target, *command.NetworkPrepare)
		if err == nil {
			result.NetworkPolicyToken = &token
		}
	case NodeChannelCommandClaim:
		var response NodeControlResponse
		response, err = a.config.Executor.Claim(operationCtx, command.Target, *command.Claim)
		if err == nil {
			result.ControlResponse = &response
		}
	case NodeChannelCommandCommandReady:
		var response NodeControlResponse
		response, err = a.config.Executor.CommandReady(operationCtx, command.Target, *command.CommandReady)
		if err == nil {
			result.ControlResponse = &response
		}
	case NodeChannelCommandCleanup:
		var proof NodeCleanupControlProof
		proof, err = a.config.Executor.Cleanup(operationCtx, command.Target, *command.Cleanup)
		if err == nil {
			result.CleanupProof = &proof
		}
	default:
		err = fmt.Errorf("unsupported node channel command %q: %w", command.Kind, errdefs.ErrInvalidArgument)
	}
	if err == nil {
		if validationErr := result.ValidateFor(command); validationErr == nil {
			return result
		} else {
			err = fmt.Errorf("node executor returned an invalid result: %w: %w", validationErr, errdefs.ErrUnavailable)
			result.NetworkPolicyToken = nil
			result.ControlResponse = nil
			result.CleanupProof = nil
		}
	}
	result.Error = boundedNodeChannelError(err)
	result.ErrorClass = classifyNodeChannelError(err)
	return result
}

func (a *NodeChannelAgent) dial(ctx context.Context) (*websocket.Conn, error) {
	caPEM, err := os.ReadFile(a.config.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read node channel CA: %w: %w", err, errdefs.ErrUnavailable)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("node channel CA contains no certificates: %w", errdefs.ErrInvalidArgument)
	}
	certificate, err := tls.LoadX509KeyPair(a.config.ClientCertFile, a.config.ClientKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load node channel client identity: %w: %w", err, errdefs.ErrUnavailable)
	}
	token, err := readNodeChannelCredential(a.config.TokenFile, "bearer token")
	if err != nil {
		return nil, err
	}
	target := *a.baseURL
	target.Scheme = "wss"
	target.Path = NodeChannelPath
	target.RawPath = NodeChannelPath
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12, RootCAs: roots,
		Certificates: []tls.Certificate{certificate}, ServerName: a.baseURL.Hostname(),
		VerifyConnection: func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 {
				return errors.New("node channel endpoint presented no peer certificate")
			}
			for _, identity := range state.PeerCertificates[0].URIs {
				if identity.String() == a.config.PeerURISAN {
					return nil
				}
			}
			return fmt.Errorf("node channel endpoint certificate lacks URI SAN %q", a.config.PeerURISAN)
		},
	}
	dialer := websocket.Dialer{
		HandshakeTimeout: defaultNodeChannelHandshakeTimeout,
		Proxy:            nil,
		TLSClientConfig:  tlsConfig,
		Subprotocols:     []string{NodeChannelSubprotocol},
	}
	header := http.Header{"Authorization": []string{"Bearer " + token}}
	connection, response, err := dialer.DialContext(ctx, target.String(), header)
	if response != nil {
		defer response.Body.Close()
	}
	if err == nil {
		if connection.Subprotocol() != NodeChannelSubprotocol {
			_ = connection.Close()
			return nil, fmt.Errorf("node channel server omitted the required subprotocol: %w", errdefs.ErrUnavailable)
		}
		return connection, nil
	}
	if response == nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, fmt.Errorf("dial node channel: %w: %w", err, errdefs.ErrUnavailable)
	}
	payload, _ := io.ReadAll(io.LimitReader(response.Body, NodeChannelMaxError+1))
	message := strings.TrimSpace(string(payload))
	if message == "" {
		message = http.StatusText(response.StatusCode)
	}
	var base error = errdefs.ErrUnavailable
	switch response.StatusCode {
	case http.StatusBadRequest, http.StatusMethodNotAllowed:
		base = errdefs.ErrInvalidArgument
	case http.StatusUnauthorized, http.StatusForbidden:
		base = errdefs.ErrPermissionDenied
	}
	return nil, fmt.Errorf("dial node channel returned HTTP %d: %s: %w", response.StatusCode, message, base)
}

func writeNodeChannelMessage(connection *websocket.Conn, value any, deadline time.Time) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(payload) > NodeChannelMaxBytes {
		return fmt.Errorf("node channel message exceeds %d bytes", NodeChannelMaxBytes)
	}
	_ = connection.SetWriteDeadline(deadline)
	err = connection.WriteMessage(websocket.TextMessage, payload)
	_ = connection.SetWriteDeadline(time.Time{})
	return err
}

func decodeNodeChannelMessage(payload []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("node channel message must contain exactly one JSON value")
	}
	return nil
}

func readNodeChannelCredential(path, name string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open node channel %s: %w: %w", name, err, errdefs.ErrUnavailable)
	}
	defer file.Close()
	payload, err := io.ReadAll(io.LimitReader(file, maxNodeChannelCredentialBytes+1))
	if err != nil {
		return "", fmt.Errorf("read node channel %s: %w: %w", name, err, errdefs.ErrUnavailable)
	}
	if len(payload) > maxNodeChannelCredentialBytes {
		return "", fmt.Errorf("node channel %s exceeds %d bytes: %w", name, maxNodeChannelCredentialBytes, errdefs.ErrInvalidArgument)
	}
	value := strings.TrimSpace(string(payload))
	if value == "" || len(strings.Fields(value)) != 1 || strings.ContainsAny(value, "\r\n") {
		return "", fmt.Errorf("node channel %s is empty or non-canonical: %w", name, errdefs.ErrInvalidArgument)
	}
	return value, nil
}

func classifyNodeChannelError(err error) NodeChannelErrorClass {
	switch {
	case errdefs.IsInvalidArgument(err):
		return NodeChannelErrorInvalidArgument
	case errdefs.IsNotFound(err):
		return NodeChannelErrorNotFound
	case errdefs.IsAlreadyExists(err):
		return NodeChannelErrorAlreadyExists
	case errdefs.IsFailedPrecondition(err):
		return NodeChannelErrorFailedPrecondition
	case errdefs.IsPermissionDenied(err):
		return NodeChannelErrorPermissionDenied
	case errdefs.IsResourceExhausted(err):
		return NodeChannelErrorResourceExhausted
	case errdefs.IsUnavailable(err), errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return NodeChannelErrorUnavailable
	default:
		return NodeChannelErrorInternal
	}
}

func boundedNodeChannelError(err error) string {
	message := "node operation failed"
	if err != nil {
		message = strings.TrimSpace(strings.ToValidUTF8(err.Error(), "�"))
	}
	if message == "" {
		message = "node operation failed"
	}
	if len(message) <= NodeChannelMaxError {
		return message
	}
	payload := []byte(message[:NodeChannelMaxError])
	for !utf8.Valid(payload) && len(payload) > 0 {
		payload = payload[:len(payload)-1]
	}
	return strings.TrimSpace(string(payload))
}

func jitterNodeChannelBackoff(base time.Duration) time.Duration {
	// Uniformly choose [80%, 120%]. Failure to obtain entropy falls back to the
	// bounded base and does not weaken channel authentication.
	width := max(base*2/5, time.Nanosecond)
	value, err := rand.Int(rand.Reader, big.NewInt(int64(width)))
	if err != nil {
		return base
	}
	return base*4/5 + time.Duration(value.Int64())
}
