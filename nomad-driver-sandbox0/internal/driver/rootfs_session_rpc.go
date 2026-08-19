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

package driver

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
)

const rootFSSessionRPCMaxBytes = 2 << 20

type rootFSSessionRPCRequest struct {
	Stage       rootfshandoff.StageRequest                 `json:"stage"`
	Consumer    RootFSConsumerRequest                      `json:"consumer,omitempty"`
	Lease       RootFSConsumerLease                        `json:"lease,omitempty"`
	Fork        rootfshandoff.RunningForkCheckpointRequest `json:"fork,omitempty"`
	OperationID string                                     `json:"operation_id,omitempty"`
	Observation crashTaskObservation                       `json:"observation,omitempty"`
}

type rootFSSessionRPCResponse struct {
	Mount      rootfssession.Mount                       `json:"mount,omitempty"`
	Lease      RootFSConsumerLease                       `json:"lease,omitempty"`
	Retire     rootfssession.RetireResult                `json:"retire,omitempty"`
	Crash      rootfshandoff.CrashFenceProof             `json:"crash,omitempty"`
	Checkpoint rootfshandoff.RunningForkCheckpointResult `json:"checkpoint,omitempty"`
	Error      string                                    `json:"error,omitempty"`
	ErrorClass string                                    `json:"error_class,omitempty"`
}

type rootFSSessionClient struct {
	http *http.Client
}

func (c *rootFSSessionClient) Ping(ctx context.Context) error {
	var response rootFSSessionRPCResponse
	return c.call(ctx, "/v1/health", rootFSSessionRPCRequest{}, &response)
}

func newRootFSSessionClient(socketPath string) (*rootFSSessionClient, error) {
	socketPath = filepath.Clean(strings.TrimSpace(socketPath))
	if !filepath.IsAbs(socketPath) || socketPath == string(filepath.Separator) {
		return nil, fmt.Errorf("RootFS session daemon socket must be a non-root absolute path")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
	}
	return &rootFSSessionClient{http: &http.Client{Transport: transport}}, nil
}

// RequestRunningRootFSFork asks the root-owned node daemon to capture and
// regionally publish one exact live checkpoint. It is the narrow control
// entrypoint used by node administration tooling; the socket remains the
// authorization boundary.
func RequestRunningRootFSFork(
	ctx context.Context,
	socketPath string,
	stage rootfshandoff.StageRequest,
	fork rootfshandoff.RunningForkCheckpointRequest,
) (rootfshandoff.RunningForkCheckpointResult, error) {
	client, err := newRootFSSessionClient(socketPath)
	if err != nil {
		return rootfshandoff.RunningForkCheckpointResult{}, err
	}
	result, err := client.CaptureRunningFork(ctx, stage.WithoutWriterGrantToken(), fork)
	if err != nil {
		return rootfshandoff.RunningForkCheckpointResult{}, err
	}
	if err := result.Validate(); err != nil {
		return rootfshandoff.RunningForkCheckpointResult{}, fmt.Errorf("validate session daemon running fork result: %w: %w", err, errdefs.ErrUnavailable)
	}
	return result, nil
}

func (c *rootFSSessionClient) Ensure(
	ctx context.Context,
	stage rootfshandoff.StageRequest,
	_ func(error),
) (rootfssession.Mount, error) {
	var response rootFSSessionRPCResponse
	err := c.call(ctx, "/v1/sessions/ensure", rootFSSessionRPCRequest{Stage: stage}, &response)
	return response.Mount, err
}

func (c *rootFSSessionClient) RegisterConsumer(
	ctx context.Context,
	stage rootfshandoff.StageRequest,
	consumer RootFSConsumerRequest,
) (RootFSConsumerLease, error) {
	var response rootFSSessionRPCResponse
	err := c.call(ctx, "/v1/sessions/consumer/register", rootFSSessionRPCRequest{
		Stage: stage.WithoutWriterGrantToken(), Consumer: consumer,
	}, &response)
	return response.Lease, err
}

func (c *rootFSSessionClient) RenewConsumer(
	ctx context.Context,
	stage rootfshandoff.StageRequest,
	lease RootFSConsumerLease,
) (RootFSConsumerLease, error) {
	var response rootFSSessionRPCResponse
	err := c.call(ctx, "/v1/sessions/consumer/renew", rootFSSessionRPCRequest{
		Stage: stage.WithoutWriterGrantToken(), Lease: lease,
	}, &response)
	return response.Lease, err
}

func (c *rootFSSessionClient) Retire(
	ctx context.Context,
	stage rootfshandoff.StageRequest,
	operationID string,
) (rootfssession.RetireResult, error) {
	var response rootFSSessionRPCResponse
	err := c.call(ctx, "/v1/sessions/retire", rootFSSessionRPCRequest{
		Stage: stage.WithoutWriterGrantToken(), OperationID: operationID,
	}, &response)
	return response.Retire, err
}

func (c *rootFSSessionClient) CaptureRunningFork(
	ctx context.Context,
	stage rootfshandoff.StageRequest,
	fork rootfshandoff.RunningForkCheckpointRequest,
) (rootfshandoff.RunningForkCheckpointResult, error) {
	var response rootFSSessionRPCResponse
	err := c.call(ctx, "/v1/sessions/fork-running", rootFSSessionRPCRequest{
		Stage: stage.WithoutWriterGrantToken(), Fork: fork,
	}, &response)
	return response.Checkpoint, err
}

func (c *rootFSSessionClient) CrashFence(
	ctx context.Context,
	stage rootfshandoff.StageRequest,
	operationID string,
	observation crashTaskObservation,
) (rootfshandoff.CrashFenceProof, error) {
	var response rootFSSessionRPCResponse
	err := c.call(ctx, "/v1/sessions/crash-fence", rootFSSessionRPCRequest{
		Stage: stage.WithoutWriterGrantToken(), OperationID: operationID, Observation: observation,
	}, &response)
	return response.Crash, err
}

func (c *rootFSSessionClient) call(
	ctx context.Context,
	path string,
	request rootFSSessionRPCRequest,
	response *rootFSSessionRPCResponse,
) error {
	payload, err := json.Marshal(request)
	if err != nil {
		return err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPut, "http://rootfs-sessiond"+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpResponse, err := c.http.Do(httpRequest)
	if err != nil {
		return fmt.Errorf("call RootFS session daemon: %w", err)
	}
	defer httpResponse.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(httpResponse.Body, rootFSSessionRPCMaxBytes))
	if err := decoder.Decode(response); err != nil {
		return fmt.Errorf("decode RootFS session daemon response: %w", err)
	}
	if httpResponse.StatusCode/100 != 2 {
		return remoteRootFSError(response.Error, response.ErrorClass)
	}
	return nil
}

func serveRootFSSessionRuntime(
	ctx context.Context,
	socketPath string,
	runtime RootFSRuntime,
	onWriterLeaseLost func(rootfshandoff.StageRequest, error),
	health func(context.Context) error,
) error {
	if runtime == nil {
		return fmt.Errorf("RootFS session runtime is required")
	}
	socketPath = filepath.Clean(strings.TrimSpace(socketPath))
	if !filepath.IsAbs(socketPath) || socketPath == string(filepath.Separator) {
		return fmt.Errorf("RootFS session daemon socket must be a non-root absolute path")
	}
	if err := os.MkdirAll(filepath.Dir(socketPath), 0o750); err != nil {
		return fmt.Errorf("create RootFS session socket directory: %w", err)
	}
	if info, err := os.Lstat(socketPath); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return fmt.Errorf("refuse to replace non-socket RootFS session path %s", socketPath)
		}
		if err := os.Remove(socketPath); err != nil {
			return fmt.Errorf("remove stale RootFS session socket: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect RootFS session socket: %w", err)
	}
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("listen on RootFS session socket: %w", err)
	}
	defer listener.Close()
	defer os.Remove(socketPath)
	if err := os.Chmod(socketPath, 0o600); err != nil {
		return fmt.Errorf("secure RootFS session socket: %w", err)
	}
	server := &http.Server{
		Handler:           rootFSSessionRPCHandler(runtime, onWriterLeaseLost, health),
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

func rootFSSessionRPCHandler(
	runtime RootFSRuntime,
	onWriterLeaseLost func(rootfshandoff.StageRequest, error),
	health func(context.Context) error,
) http.Handler {
	mux := http.NewServeMux()
	handle := func(path string, operation func(context.Context, rootFSSessionRPCRequest) (rootFSSessionRPCResponse, error)) {
		mux.HandleFunc(path, func(writer http.ResponseWriter, request *http.Request) {
			if request.Method != http.MethodPut {
				writer.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			defer request.Body.Close()
			request.Body = http.MaxBytesReader(writer, request.Body, rootFSSessionRPCMaxBytes)
			var body rootFSSessionRPCRequest
			decoder := json.NewDecoder(request.Body)
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&body); err != nil {
				writeRootFSSessionRPCResponse(writer, rootFSSessionRPCResponse{}, fmt.Errorf("decode request: %w: %w", err, errdefs.ErrInvalidArgument))
				return
			}
			if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
				writeRootFSSessionRPCResponse(writer, rootFSSessionRPCResponse{}, fmt.Errorf("request must contain exactly one JSON value: %w", errdefs.ErrInvalidArgument))
				return
			}
			response, err := operation(request.Context(), body)
			writeRootFSSessionRPCResponse(writer, response, err)
		})
	}
	handle("/v1/sessions/ensure", func(ctx context.Context, request rootFSSessionRPCRequest) (rootFSSessionRPCResponse, error) {
		mount, err := runtime.Ensure(ctx, request.Stage, func(err error) {
			if onWriterLeaseLost != nil {
				onWriterLeaseLost(request.Stage.WithoutWriterGrantToken(), err)
			}
		})
		return rootFSSessionRPCResponse{Mount: mount}, err
	})
	handle("/v1/health", func(ctx context.Context, _ rootFSSessionRPCRequest) (rootFSSessionRPCResponse, error) {
		if health != nil {
			return rootFSSessionRPCResponse{}, health(ctx)
		}
		return rootFSSessionRPCResponse{}, nil
	})
	handle("/v1/sessions/consumer/register", func(ctx context.Context, request rootFSSessionRPCRequest) (rootFSSessionRPCResponse, error) {
		lease, err := runtime.RegisterConsumer(ctx, request.Stage, request.Consumer)
		return rootFSSessionRPCResponse{Lease: lease}, err
	})
	handle("/v1/sessions/consumer/renew", func(ctx context.Context, request rootFSSessionRPCRequest) (rootFSSessionRPCResponse, error) {
		lease, err := runtime.RenewConsumer(ctx, request.Stage, request.Lease)
		return rootFSSessionRPCResponse{Lease: lease}, err
	})
	handle("/v1/sessions/fork-running", func(ctx context.Context, request rootFSSessionRPCRequest) (rootFSSessionRPCResponse, error) {
		checkpoint, err := runtime.CaptureRunningFork(ctx, request.Stage, request.Fork)
		return rootFSSessionRPCResponse{Checkpoint: checkpoint}, err
	})
	handle("/v1/sessions/retire", func(ctx context.Context, request rootFSSessionRPCRequest) (rootFSSessionRPCResponse, error) {
		result, err := runtime.Retire(ctx, request.Stage, request.OperationID)
		return rootFSSessionRPCResponse{Retire: result}, err
	})
	handle("/v1/sessions/crash-fence", func(ctx context.Context, request rootFSSessionRPCRequest) (rootFSSessionRPCResponse, error) {
		proof, err := runtime.CrashFence(ctx, request.Stage, request.OperationID, request.Observation)
		return rootFSSessionRPCResponse{Crash: proof}, err
	})
	return mux
}

func writeRootFSSessionRPCResponse(writer http.ResponseWriter, response rootFSSessionRPCResponse, err error) {
	writer.Header().Set("Content-Type", "application/json")
	if err != nil {
		response.Error = err.Error()
		response.ErrorClass = rootFSErrorClass(err)
		writer.WriteHeader(rootFSErrorStatus(err))
	}
	_ = json.NewEncoder(writer).Encode(response)
}

func rootFSErrorClass(err error) string {
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
	base := map[string]error{
		"invalid_argument": errdefs.ErrInvalidArgument, "not_found": errdefs.ErrNotFound,
		"already_exists": errdefs.ErrAlreadyExists, "failed_precondition": errdefs.ErrFailedPrecondition,
		"permission_denied": errdefs.ErrPermissionDenied, "unavailable": errdefs.ErrUnavailable,
	}[class]
	if base == nil {
		base = errors.New("RootFS session daemon internal error")
	}
	return fmt.Errorf("%s: %w", strings.TrimSpace(message), base)
}
