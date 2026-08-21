// Copyright 2026 Sandbox0 Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nomadruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

const (
	nodeRuntimeRPCMaxBytes         = 2 << 20
	runtimeSlotJournalRegisterPath = "/v1/runtime-slots/register"
)

type nodeRuntimeRPCRequest struct {
	Stage        rootfshandoff.StageRequest                 `json:"stage"`
	Consumer     ConsumerRequest                            `json:"consumer,omitempty"`
	Lease        ConsumerLease                              `json:"lease,omitempty"`
	Fork         rootfshandoff.RunningForkCheckpointRequest `json:"fork,omitempty"`
	OperationID  string                                     `json:"operation_id,omitempty"`
	Observation  CrashTaskObservation                       `json:"observation,omitempty"`
	SlotRegister RuntimeSlotRegistration                    `json:"slot_register,omitempty"`
	SlotCleanup  protocol.NodeCleanupControlRequest         `json:"slot_cleanup,omitempty"`
}

type nodeRuntimeRPCResponse struct {
	Info        *RuntimeInfo                              `json:"info,omitempty"`
	Mount       rootfssession.Mount                       `json:"mount,omitempty"`
	Lease       ConsumerLease                             `json:"lease,omitempty"`
	Retire      rootfssession.RetireResult                `json:"retire,omitempty"`
	Crash       rootfshandoff.CrashFenceProof             `json:"crash,omitempty"`
	Checkpoint  rootfshandoff.RunningForkCheckpointResult `json:"checkpoint,omitempty"`
	SlotCleanup protocol.NodeCleanupControlProof          `json:"slot_cleanup,omitempty"`
	Error       string                                    `json:"error,omitempty"`
	ErrorClass  string                                    `json:"error_class,omitempty"`
}

type Client struct {
	http *http.Client
}

type runtimeSlotCleaner interface {
	RegisterRuntimeSlot(context.Context, RuntimeSlotRegistration) error
	CleanupRuntimeSlot(context.Context, protocol.NodeCleanupControlRequest) (protocol.NodeCleanupControlProof, error)
}

type runtimeInfoProvider interface {
	RuntimeInfo() (RuntimeInfo, error)
}

func (c *Client) Ping(ctx context.Context) error {
	var response nodeRuntimeRPCResponse
	return c.call(ctx, "/v1/health", nodeRuntimeRPCRequest{}, &response)
}

// RuntimeInfo returns validated root-owned ctld metadata from the same health
// transaction used to prove the privileged node runtime is available.
func (c *Client) RuntimeInfo(ctx context.Context) (RuntimeInfo, error) {
	var response nodeRuntimeRPCResponse
	if err := c.call(ctx, "/v1/health", nodeRuntimeRPCRequest{}, &response); err != nil {
		return RuntimeInfo{}, err
	}
	if response.Info == nil {
		return RuntimeInfo{}, fmt.Errorf("ctld Nomad runtime health response lacks runtime info: %w", errdefs.ErrUnavailable)
	}
	if err := response.Info.Validate(); err != nil {
		return RuntimeInfo{}, fmt.Errorf("validate ctld Nomad runtime health response: %w", err)
	}
	return *response.Info, nil
}

func NewClient(socketPath string) (*Client, error) {
	socketPath = filepath.Clean(strings.TrimSpace(socketPath))
	if !filepath.IsAbs(socketPath) || socketPath == string(filepath.Separator) {
		return nil, fmt.Errorf("ctld Nomad runtime socket must be a non-root absolute path")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
	}
	return &Client{http: &http.Client{Transport: transport}}, nil
}

// RequestRunningRootFSFork asks the root-owned ctld node runtime to capture and
// regionally publish one exact live checkpoint. It is the narrow control
// entrypoint used by node administration tooling; the socket remains the
// authorization boundary.
func RequestRunningRootFSFork(
	ctx context.Context,
	socketPath string,
	stage rootfshandoff.StageRequest,
	fork rootfshandoff.RunningForkCheckpointRequest,
) (rootfshandoff.RunningForkCheckpointResult, error) {
	client, err := NewClient(socketPath)
	if err != nil {
		return rootfshandoff.RunningForkCheckpointResult{}, err
	}
	result, err := client.CaptureRunningFork(ctx, stage.WithoutWriterGrantToken(), fork)
	if err != nil {
		return rootfshandoff.RunningForkCheckpointResult{}, err
	}
	if err := result.Validate(); err != nil {
		return rootfshandoff.RunningForkCheckpointResult{}, fmt.Errorf("validate ctld Nomad runtime running fork result: %w: %w", err, errdefs.ErrUnavailable)
	}
	return result, nil
}

func (c *Client) Ensure(
	ctx context.Context,
	stage rootfshandoff.StageRequest,
	_ func(error),
) (rootfssession.Mount, error) {
	var response nodeRuntimeRPCResponse
	err := c.call(ctx, "/v1/sessions/ensure", nodeRuntimeRPCRequest{Stage: stage}, &response)
	return response.Mount, err
}

func (c *Client) RegisterConsumer(
	ctx context.Context,
	stage rootfshandoff.StageRequest,
	consumer ConsumerRequest,
) (ConsumerLease, error) {
	var response nodeRuntimeRPCResponse
	err := c.call(ctx, "/v1/sessions/consumer/register", nodeRuntimeRPCRequest{
		Stage: stage.WithoutWriterGrantToken(), Consumer: consumer,
	}, &response)
	return response.Lease, err
}

func (c *Client) RenewConsumer(
	ctx context.Context,
	stage rootfshandoff.StageRequest,
	lease ConsumerLease,
) (ConsumerLease, error) {
	var response nodeRuntimeRPCResponse
	err := c.call(ctx, "/v1/sessions/consumer/renew", nodeRuntimeRPCRequest{
		Stage: stage.WithoutWriterGrantToken(), Lease: lease,
	}, &response)
	return response.Lease, err
}

func (c *Client) Retire(
	ctx context.Context,
	stage rootfshandoff.StageRequest,
	operationID string,
) (rootfssession.RetireResult, error) {
	var response nodeRuntimeRPCResponse
	err := c.call(ctx, "/v1/sessions/retire", nodeRuntimeRPCRequest{
		Stage: stage.WithoutWriterGrantToken(), OperationID: operationID,
	}, &response)
	return response.Retire, err
}

func (c *Client) CaptureRunningFork(
	ctx context.Context,
	stage rootfshandoff.StageRequest,
	fork rootfshandoff.RunningForkCheckpointRequest,
) (rootfshandoff.RunningForkCheckpointResult, error) {
	var response nodeRuntimeRPCResponse
	err := c.call(ctx, "/v1/sessions/fork-running", nodeRuntimeRPCRequest{
		Stage: stage.WithoutWriterGrantToken(), Fork: fork,
	}, &response)
	return response.Checkpoint, err
}

func (c *Client) CrashFence(
	ctx context.Context,
	stage rootfshandoff.StageRequest,
	operationID string,
	observation CrashTaskObservation,
) (rootfshandoff.CrashFenceProof, error) {
	var response nodeRuntimeRPCResponse
	err := c.call(ctx, "/v1/sessions/crash-fence", nodeRuntimeRPCRequest{
		Stage: stage.WithoutWriterGrantToken(), OperationID: operationID, Observation: observation,
	}, &response)
	return response.Crash, err
}

func (c *Client) CleanupRuntimeSlot(
	ctx context.Context,
	request protocol.NodeCleanupControlRequest,
) (protocol.NodeCleanupControlProof, error) {
	var response nodeRuntimeRPCResponse
	err := c.call(ctx, protocol.NodeCleanupControlPath, nodeRuntimeRPCRequest{SlotCleanup: request}, &response)
	if err != nil {
		return protocol.NodeCleanupControlProof{}, err
	}
	if err := response.SlotCleanup.Validate(); err != nil || response.SlotCleanup.Request() != request {
		return protocol.NodeCleanupControlProof{}, fmt.Errorf(
			"ctld Nomad runtime returned another runtime slot cleanup proof: %v: %w", err, errdefs.ErrUnavailable,
		)
	}
	return response.SlotCleanup, nil
}

func (c *Client) RegisterRuntimeSlot(
	ctx context.Context,
	registration RuntimeSlotRegistration,
) error {
	var response nodeRuntimeRPCResponse
	return c.call(ctx, runtimeSlotJournalRegisterPath, nodeRuntimeRPCRequest{SlotRegister: registration}, &response)
}

func (c *Client) call(
	ctx context.Context,
	path string,
	request nodeRuntimeRPCRequest,
	response *nodeRuntimeRPCResponse,
) error {
	payload, err := json.Marshal(request)
	if err != nil {
		return err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPut, "http://ctld-nomad-runtime"+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpResponse, err := c.http.Do(httpRequest)
	if err != nil {
		return fmt.Errorf("call ctld Nomad runtime: %w", err)
	}
	defer httpResponse.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(httpResponse.Body, nodeRuntimeRPCMaxBytes))
	if err := decoder.Decode(response); err != nil {
		return fmt.Errorf("decode ctld Nomad runtime response: %w", err)
	}
	if httpResponse.StatusCode/100 != 2 {
		return remoteRootFSError(response.Error, response.ErrorClass)
	}
	return nil
}

func serveNodeRuntime(
	ctx context.Context,
	socketPath string,
	runtime Runtime,
	onWriterLeaseLost func(rootfshandoff.StageRequest, error),
	health func(context.Context) error,
	cleaner runtimeSlotCleaner,
) error {
	if runtime == nil {
		return fmt.Errorf("ctld Nomad runtime backend is required")
	}
	socketPath = filepath.Clean(strings.TrimSpace(socketPath))
	if !filepath.IsAbs(socketPath) || socketPath == string(filepath.Separator) {
		return fmt.Errorf("ctld Nomad runtime socket must be a non-root absolute path")
	}
	if err := os.MkdirAll(filepath.Dir(socketPath), 0o750); err != nil {
		return fmt.Errorf("create ctld Nomad runtime socket directory: %w", err)
	}
	if info, err := os.Lstat(socketPath); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return fmt.Errorf("refuse to replace non-socket ctld Nomad runtime path %s", socketPath)
		}
		if err := os.Remove(socketPath); err != nil {
			return fmt.Errorf("remove stale ctld Nomad runtime socket: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect ctld Nomad runtime socket: %w", err)
	}
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("listen on ctld Nomad runtime socket: %w", err)
	}
	defer listener.Close()
	defer os.Remove(socketPath)
	if err := os.Chmod(socketPath, 0o600); err != nil {
		return fmt.Errorf("secure ctld Nomad runtime socket: %w", err)
	}
	server := &http.Server{
		Handler:           nodeRuntimeRPCHandler(runtime, onWriterLeaseLost, health, cleaner),
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
		}
	}()
	err = server.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func nodeRuntimeRPCHandler(
	runtime Runtime,
	onWriterLeaseLost func(rootfshandoff.StageRequest, error),
	health func(context.Context) error,
	cleaner runtimeSlotCleaner,
) http.Handler {
	mux := http.NewServeMux()
	handle := func(path string, operation func(context.Context, nodeRuntimeRPCRequest) (nodeRuntimeRPCResponse, error)) {
		mux.HandleFunc(path, func(writer http.ResponseWriter, request *http.Request) {
			if request.Method != http.MethodPut {
				writer.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			defer request.Body.Close()
			request.Body = http.MaxBytesReader(writer, request.Body, nodeRuntimeRPCMaxBytes)
			var body nodeRuntimeRPCRequest
			decoder := json.NewDecoder(request.Body)
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&body); err != nil {
				writeNodeRuntimeRPCResponse(writer, nodeRuntimeRPCResponse{}, fmt.Errorf("decode request: %w: %w", err, errdefs.ErrInvalidArgument))
				return
			}
			if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
				writeNodeRuntimeRPCResponse(writer, nodeRuntimeRPCResponse{}, fmt.Errorf("request must contain exactly one JSON value: %w", errdefs.ErrInvalidArgument))
				return
			}
			response, err := operation(request.Context(), body)
			writeNodeRuntimeRPCResponse(writer, response, err)
		})
	}
	handle("/v1/sessions/ensure", func(ctx context.Context, request nodeRuntimeRPCRequest) (nodeRuntimeRPCResponse, error) {
		mount, err := runtime.Ensure(ctx, request.Stage, func(err error) {
			if onWriterLeaseLost != nil {
				onWriterLeaseLost(request.Stage.WithoutWriterGrantToken(), err)
			}
		})
		return nodeRuntimeRPCResponse{Mount: mount}, err
	})
	handle("/v1/health", func(ctx context.Context, _ nodeRuntimeRPCRequest) (nodeRuntimeRPCResponse, error) {
		if health != nil {
			if err := health(ctx); err != nil {
				return nodeRuntimeRPCResponse{}, err
			}
		}
		provider, ok := runtime.(runtimeInfoProvider)
		if !ok {
			return nodeRuntimeRPCResponse{}, nil
		}
		info, err := provider.RuntimeInfo()
		if err != nil {
			return nodeRuntimeRPCResponse{}, err
		}
		return nodeRuntimeRPCResponse{Info: &info}, nil
	})
	handle("/v1/sessions/consumer/register", func(ctx context.Context, request nodeRuntimeRPCRequest) (nodeRuntimeRPCResponse, error) {
		lease, err := runtime.RegisterConsumer(ctx, request.Stage, request.Consumer)
		return nodeRuntimeRPCResponse{Lease: lease}, err
	})
	handle("/v1/sessions/consumer/renew", func(ctx context.Context, request nodeRuntimeRPCRequest) (nodeRuntimeRPCResponse, error) {
		lease, err := runtime.RenewConsumer(ctx, request.Stage, request.Lease)
		return nodeRuntimeRPCResponse{Lease: lease}, err
	})
	handle("/v1/sessions/fork-running", func(ctx context.Context, request nodeRuntimeRPCRequest) (nodeRuntimeRPCResponse, error) {
		checkpoint, err := runtime.CaptureRunningFork(ctx, request.Stage, request.Fork)
		return nodeRuntimeRPCResponse{Checkpoint: checkpoint}, err
	})
	handle("/v1/sessions/retire", func(ctx context.Context, request nodeRuntimeRPCRequest) (nodeRuntimeRPCResponse, error) {
		result, err := runtime.Retire(ctx, request.Stage, request.OperationID)
		return nodeRuntimeRPCResponse{Retire: result}, err
	})
	handle("/v1/sessions/crash-fence", func(ctx context.Context, request nodeRuntimeRPCRequest) (nodeRuntimeRPCResponse, error) {
		proof, err := runtime.CrashFence(ctx, request.Stage, request.OperationID, request.Observation)
		return nodeRuntimeRPCResponse{Crash: proof}, err
	})
	handle(protocol.NodeCleanupControlPath, func(ctx context.Context, request nodeRuntimeRPCRequest) (nodeRuntimeRPCResponse, error) {
		if cleaner == nil {
			return nodeRuntimeRPCResponse{}, fmt.Errorf("runtime slot cleaner is unavailable: %w", errdefs.ErrUnavailable)
		}
		proof, err := cleaner.CleanupRuntimeSlot(ctx, request.SlotCleanup)
		return nodeRuntimeRPCResponse{SlotCleanup: proof}, err
	})
	handle(runtimeSlotJournalRegisterPath, func(ctx context.Context, request nodeRuntimeRPCRequest) (nodeRuntimeRPCResponse, error) {
		if cleaner == nil {
			return nodeRuntimeRPCResponse{}, fmt.Errorf("runtime slot journal is unavailable: %w", errdefs.ErrUnavailable)
		}
		return nodeRuntimeRPCResponse{}, cleaner.RegisterRuntimeSlot(ctx, request.SlotRegister)
	})
	return mux
}

func writeNodeRuntimeRPCResponse(writer http.ResponseWriter, response nodeRuntimeRPCResponse, err error) {
	writer.Header().Set("Content-Type", "application/json")
	if err != nil {
		response.Error = err.Error()
		response.ErrorClass = rootFSErrorClass(err)
		writer.WriteHeader(rootFSErrorStatus(err))
	}
	_ = json.NewEncoder(writer).Encode(response)
}

func rootFSErrorClass(err error) string {
	var consumedAttach *ConsumedAttachError
	if errors.As(err, &consumedAttach) {
		return "consumed_attach"
	}
	for _, candidate := range []struct {
		name string
		err  error
	}{
		{"invalid_argument", errdefs.ErrInvalidArgument}, {"not_found", errdefs.ErrNotFound},
		{"already_exists", errdefs.ErrAlreadyExists}, {"failed_precondition", errdefs.ErrFailedPrecondition},
		{"permission_denied", errdefs.ErrPermissionDenied}, {"unavailable", errdefs.ErrUnavailable},
	} {
		if errors.Is(err, candidate.err) {
			return candidate.name
		}
	}
	return "internal"
}

func rootFSErrorStatus(err error) int {
	switch rootFSErrorClass(err) {
	case "consumed_attach":
		return http.StatusServiceUnavailable
	case "invalid_argument":
		return http.StatusBadRequest
	case "not_found":
		return http.StatusNotFound
	case "already_exists":
		return http.StatusConflict
	case "failed_precondition":
		return http.StatusPreconditionFailed
	case "permission_denied":
		return http.StatusForbidden
	case "unavailable":
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

func remoteRootFSError(message, class string) error {
	if class == "consumed_attach" {
		return &ConsumedAttachError{Err: fmt.Errorf("%s: %w", strings.TrimSpace(message), errdefs.ErrUnavailable)}
	}
	base := map[string]error{
		"invalid_argument": errdefs.ErrInvalidArgument, "not_found": errdefs.ErrNotFound,
		"already_exists": errdefs.ErrAlreadyExists, "failed_precondition": errdefs.ErrFailedPrecondition,
		"permission_denied": errdefs.ErrPermissionDenied, "unavailable": errdefs.ErrUnavailable,
	}[class]
	if base == nil {
		base = errors.New("ctld Nomad runtime internal error")
	}
	return fmt.Errorf("%s: %w", strings.TrimSpace(message), base)
}
