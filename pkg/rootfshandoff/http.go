package rootfshandoff

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
)

const maxRequestBytes = 1 << 20

type Controller interface {
	Stage(ctx context.Context, request StageRequest) error
	MarkReady(ctx context.Context, request ReadyRequest) error
	Status(ctx context.Context, parent string) (ParentStatus, error)
	Remove(ctx context.Context, key string) error
}

type GateController interface {
	CreateGate(ctx context.Context, request GateRequest) (GateImage, error)
	DeleteGate(ctx context.Context, slotNonce string) error
}

type IncarnationController interface {
	Incarnation(context.Context) (RuntimeIncarnation, error)
}

type ConsumerController interface {
	VerifyConsumer(context.Context, ConsumerRequest) error
}

type RetireController interface {
	BeginRetire(ctx context.Context, request RetireRequest) error
	RetireResult(ctx context.Context, request RetireRequest) (RetireResult, error)
}

type CrashFenceController interface {
	CrashFence(ctx context.Context, request CrashFenceRequest) (CrashFenceResult, error)
}

// WriterGrantController consumes one regional writer grant for the exact
// immutable Stage binding. Implementations must make exact retries idempotent.
type WriterGrantController interface {
	ConsumeWriterGrant(ctx context.Context, request StageRequest) (protocol.LeaseObservation, error)
	RenewWriterGrant(ctx context.Context, request StageRequest) (protocol.LeaseObservation, error)
	VerifyTerminalWriterGrant(ctx context.Context, request StageRequest) error
}

type WriterGrantBatchController interface {
	RenewWriterGrants(ctx context.Context, requests []StageRequest) (protocol.BatchRenewResponse, error)
}

// NewWriterGrantHandler exposes the node-local ctld authority proxy. The
// listener must additionally enforce Unix peer credentials; HTTP paths are not
// an authentication boundary.
func NewWriterGrantHandler(controller WriterGrantController) http.Handler {
	mux := http.NewServeMux()
	if batch, ok := controller.(WriterGrantBatchController); ok {
		mux.HandleFunc("/v1/writer-grants:renew", func(writer http.ResponseWriter, request *http.Request) {
			if request.Method != http.MethodPut {
				writer.Header().Set("Allow", http.MethodPut)
				writeErrorStatus(writer, http.StatusMethodNotAllowed, fmt.Errorf("method is not supported"))
				return
			}
			var body WriterGrantBatchRenewRequest
			if err := decodeRequest(writer, request, &body); err != nil {
				writeError(writer, err)
				return
			}
			if len(body.Items) == 0 || len(body.Items) > protocol.MaxBatchRenewItems {
				writeError(writer, fmt.Errorf("invalid writer renewal batch size: %w", errdefs.ErrInvalidArgument))
				return
			}
			for _, item := range body.Items {
				if err := item.ValidateDurableBinding(); err != nil {
					writeError(writer, fmt.Errorf("invalid durable writer binding: %w: %w", err, errdefs.ErrInvalidArgument))
					return
				}
			}
			response, err := batch.RenewWriterGrants(request.Context(), body.Items)
			if err != nil {
				writeError(writer, err)
				return
			}
			writeJSON(writer, http.StatusOK, response)
		})
	}
	mux.HandleFunc("/v1/writer-grants/", func(writer http.ResponseWriter, request *http.Request) {
		grantID, action, err := parseWriterGrantPath(request.URL.EscapedPath())
		if err != nil {
			writeError(writer, fmt.Errorf("invalid writer grant ID: %w", errdefs.ErrInvalidArgument))
			return
		}
		if request.Method != http.MethodPut {
			writer.Header().Set("Allow", "PUT")
			writeErrorStatus(writer, http.StatusMethodNotAllowed, fmt.Errorf("method is not supported"))
			return
		}
		var body StageRequest
		if err := decodeRequest(writer, request, &body); err != nil {
			writeError(writer, err)
			return
		}
		if body.Identity.WriterGrantID != grantID {
			writeError(writer, fmt.Errorf("path and body writer grant ID differ: %w", errdefs.ErrInvalidArgument))
			return
		}
		switch action {
		case "renew", "terminal":
			if err := body.ValidateDurableBinding(); err != nil {
				writeError(writer, fmt.Errorf("invalid durable writer binding: %w: %w", err, errdefs.ErrInvalidArgument))
				return
			}
			var err error
			var observation protocol.LeaseObservation
			if action == "renew" {
				observation, err = controller.RenewWriterGrant(request.Context(), body)
			} else {
				err = controller.VerifyTerminalWriterGrant(request.Context(), body)
			}
			if err != nil {
				writeError(writer, err)
				return
			}
			if action == "renew" {
				writeJSON(writer, http.StatusOK, observation)
				return
			}
		default:
			observation, err := controller.ConsumeWriterGrant(request.Context(), body)
			if err != nil {
				writeError(writer, err)
				return
			}
			writeJSON(writer, http.StatusOK, observation)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	})
	return mux
}

func parseWriterGrantPath(path string) (string, string, error) {
	const prefix = "/v1/writer-grants/"
	suffix := strings.TrimPrefix(path, prefix)
	if suffix == path || suffix == "" {
		return "", "", fmt.Errorf("writer grant ID is required")
	}
	parts := strings.Split(suffix, "/")
	if len(parts) > 2 || len(parts) == 2 && parts[1] != "renew" && parts[1] != "terminal" {
		return "", "", fmt.Errorf("unsupported writer grant path")
	}
	grantID, err := url.PathUnescape(parts[0])
	if err != nil || grantID == "" || strings.Contains(grantID, "/") {
		return "", "", fmt.Errorf("invalid writer grant ID")
	}
	if len(parts) == 2 {
		return grantID, parts[1], nil
	}
	return grantID, "consume", nil
}

func NewHandler(controller Controller) http.Handler {
	mux := http.NewServeMux()
	if incarnation, ok := controller.(IncarnationController); ok {
		mux.HandleFunc("/v1/incarnation", func(writer http.ResponseWriter, request *http.Request) {
			if request.Method != http.MethodGet {
				writer.Header().Set("Allow", http.MethodGet)
				writeErrorStatus(writer, http.StatusMethodNotAllowed, fmt.Errorf("method is not supported"))
				return
			}
			result, err := incarnation.Incarnation(request.Context())
			if err != nil {
				writeError(writer, err)
				return
			}
			writeJSON(writer, http.StatusOK, result)
		})
	}
	mux.HandleFunc("/v1/parents/", func(writer http.ResponseWriter, request *http.Request) {
		serveParent(controller, writer, request)
	})
	if gates, ok := controller.(GateController); ok {
		mux.HandleFunc("/v1/gates/", func(writer http.ResponseWriter, request *http.Request) {
			serveGate(gates, writer, request)
		})
	}
	return mux
}

func serveGate(controller GateController, writer http.ResponseWriter, request *http.Request) {
	slotNonce, err := url.PathUnescape(strings.TrimPrefix(request.URL.Path, "/v1/gates/"))
	if err != nil || slotNonce == "" || strings.Contains(slotNonce, "/") {
		writeError(writer, fmt.Errorf("invalid slot nonce: %w", errdefs.ErrInvalidArgument))
		return
	}
	switch request.Method {
	case http.MethodPut:
		var body GateRequest
		if err := decodeRequest(writer, request, &body); err != nil {
			writeError(writer, err)
			return
		}
		if body.SlotNonce != slotNonce {
			writeError(writer, fmt.Errorf("path and body slot nonce differ: %w", errdefs.ErrInvalidArgument))
			return
		}
		result, err := controller.CreateGate(request.Context(), body)
		if err != nil {
			writeError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, result)
	case http.MethodDelete:
		if err := controller.DeleteGate(request.Context(), slotNonce); err != nil {
			writeError(writer, err)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	default:
		writer.Header().Set("Allow", "PUT, DELETE")
		writeErrorStatus(writer, http.StatusMethodNotAllowed, fmt.Errorf("method is not supported"))
	}
}

func serveParent(controller Controller, writer http.ResponseWriter, request *http.Request) {
	suffix := strings.TrimPrefix(request.URL.Path, "/v1/parents/")
	parts := strings.Split(suffix, "/")
	if len(parts) == 0 || parts[0] == "" {
		writeError(writer, fmt.Errorf("parent is required: %w", errdefs.ErrInvalidArgument))
		return
	}
	parent, err := url.PathUnescape(parts[0])
	if err != nil {
		writeError(writer, fmt.Errorf("decode parent: %w: %w", err, errdefs.ErrInvalidArgument))
		return
	}
	switch {
	case len(parts) == 1 && request.Method == http.MethodPut:
		var body StageRequest
		if err := decodeRequest(writer, request, &body); err != nil {
			writeError(writer, err)
			return
		}
		if body.Parent != parent {
			writeError(writer, fmt.Errorf("path and body parent differ: %w", errdefs.ErrInvalidArgument))
			return
		}
		if err := controller.Stage(request.Context(), body); err != nil {
			writeError(writer, err)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	case len(parts) == 2 && parts[1] == "ready" && request.Method == http.MethodPut:
		var body ReadyRequest
		if err := decodeRequest(writer, request, &body); err != nil {
			writeError(writer, err)
			return
		}
		if body.Parent != parent {
			writeError(writer, fmt.Errorf("path and body parent differ: %w", errdefs.ErrInvalidArgument))
			return
		}
		if err := controller.MarkReady(request.Context(), body); err != nil {
			writeError(writer, err)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	case len(parts) == 2 && parts[1] == "consumer" && request.Method == http.MethodPut:
		consumer, ok := controller.(ConsumerController)
		if !ok {
			writeError(writer, fmt.Errorf("RootFS consumer verification is unavailable: %w", errdefs.ErrUnavailable))
			return
		}
		var body ConsumerRequest
		if err := decodeRequest(writer, request, &body); err != nil {
			writeError(writer, err)
			return
		}
		if body.Parent != parent {
			writeError(writer, fmt.Errorf("path and body parent differ: %w", errdefs.ErrInvalidArgument))
			return
		}
		if err := body.Validate(); err != nil {
			writeError(writer, fmt.Errorf("invalid consumer request: %w: %w", err, errdefs.ErrInvalidArgument))
			return
		}
		if err := consumer.VerifyConsumer(request.Context(), body); err != nil {
			writeError(writer, err)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	case len(parts) == 2 && parts[1] == "retire" && (request.Method == http.MethodPut || request.Method == http.MethodGet):
		retire, ok := controller.(RetireController)
		if !ok {
			writeError(writer, fmt.Errorf("planned RootFS retire is unavailable: %w", errdefs.ErrUnavailable))
			return
		}
		var body RetireRequest
		if request.Method == http.MethodPut {
			if err := decodeRequest(writer, request, &body); err != nil {
				writeError(writer, err)
				return
			}
		} else {
			body = RetireRequest{Parent: parent, OperationID: request.URL.Query().Get("operation_id")}
		}
		if body.Parent != parent {
			writeError(writer, fmt.Errorf("path and body parent differ: %w", errdefs.ErrInvalidArgument))
			return
		}
		if err := body.Validate(); err != nil {
			writeError(writer, fmt.Errorf("invalid retire request: %w: %w", err, errdefs.ErrInvalidArgument))
			return
		}
		if request.Method == http.MethodPut {
			if err := retire.BeginRetire(request.Context(), body); err != nil {
				writeError(writer, err)
				return
			}
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		result, err := retire.RetireResult(request.Context(), body)
		if err != nil {
			writeError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, result)
	case len(parts) == 2 && parts[1] == "crash-fence" && request.Method == http.MethodPut:
		fencer, ok := controller.(CrashFenceController)
		if !ok {
			writeError(writer, fmt.Errorf("RootFS crash fencing is unavailable: %w", errdefs.ErrUnavailable))
			return
		}
		var body CrashFenceRequest
		if err := decodeRequest(writer, request, &body); err != nil {
			writeError(writer, err)
			return
		}
		if body.Parent != parent {
			writeError(writer, fmt.Errorf("path and body parent differ: %w", errdefs.ErrInvalidArgument))
			return
		}
		if err := body.Validate(); err != nil {
			writeError(writer, fmt.Errorf("invalid crash fence request: %w: %w", err, errdefs.ErrInvalidArgument))
			return
		}
		result, err := fencer.CrashFence(request.Context(), body)
		if err != nil {
			writeError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, result)
	case len(parts) == 1 && request.Method == http.MethodGet:
		status, err := controller.Status(request.Context(), parent)
		if err != nil {
			writeError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, status)
	case len(parts) == 1 && request.Method == http.MethodDelete:
		if err := controller.Remove(request.Context(), parent); err != nil {
			writeError(writer, err)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	default:
		writer.Header().Set("Allow", "GET, PUT, DELETE")
		writeErrorStatus(writer, http.StatusMethodNotAllowed, fmt.Errorf("method or path is not supported"))
	}
}

func decodeRequest(writer http.ResponseWriter, request *http.Request, target any) error {
	request.Body = http.MaxBytesReader(writer, request.Body, maxRequestBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode request: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("request must contain one JSON value: %w", errdefs.ErrInvalidArgument)
	}
	return nil
}

func writeError(writer http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errdefs.IsInvalidArgument(err):
		status = http.StatusBadRequest
	case errdefs.IsPermissionDenied(err):
		status = http.StatusForbidden
	case errdefs.IsNotFound(err):
		status = http.StatusNotFound
	case errdefs.IsAlreadyExists(err):
		status = http.StatusConflict
	case errdefs.IsFailedPrecondition(err):
		status = http.StatusPreconditionFailed
	case errors.Is(err, context.Canceled):
		status = http.StatusRequestTimeout
	case errors.Is(err, context.DeadlineExceeded):
		status = http.StatusGatewayTimeout
	}
	writeErrorStatus(writer, status, err)
}

func writeErrorStatus(writer http.ResponseWriter, status int, err error) {
	writeJSON(writer, status, map[string]string{"error": err.Error()})
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

type Client struct {
	http *http.Client
}

func NewUnixClient(socketPath string) *Client {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, "unix", socketPath)
		},
	}
	return &Client{http: &http.Client{Transport: transport}}
}

func (c *Client) Stage(ctx context.Context, request StageRequest) error {
	return c.put(ctx, parentURL(request.Parent), request)
}

func (c *Client) MarkReady(ctx context.Context, request ReadyRequest) error {
	return c.put(ctx, parentURL(request.Parent)+"/ready", request)
}

func (c *Client) VerifyConsumer(ctx context.Context, request ConsumerRequest) error {
	if err := request.Validate(); err != nil {
		return fmt.Errorf("invalid consumer request: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	return c.put(ctx, parentURL(request.Parent)+"/consumer", request)
}

func (c *Client) Status(ctx context.Context, parent string) (ParentStatus, error) {
	var result ParentStatus
	if err := c.do(ctx, http.MethodGet, parentURL(parent), nil, &result); err != nil {
		return ParentStatus{}, err
	}
	return result, nil
}

func (c *Client) Remove(ctx context.Context, parent string) error {
	return c.do(ctx, http.MethodDelete, parentURL(parent), nil, nil)
}

func (c *Client) BeginRetire(ctx context.Context, request RetireRequest) error {
	if err := request.Validate(); err != nil {
		return fmt.Errorf("invalid retire request: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	return c.put(ctx, parentURL(request.Parent)+"/retire", request)
}

func (c *Client) RetireResult(ctx context.Context, request RetireRequest) (RetireResult, error) {
	if err := request.Validate(); err != nil {
		return RetireResult{}, fmt.Errorf("invalid retire request: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	var result RetireResult
	path := parentURL(request.Parent) + "/retire?operation_id=" + url.QueryEscape(request.OperationID)
	if err := c.do(ctx, http.MethodGet, path, nil, &result); err != nil {
		return RetireResult{}, err
	}
	if err := result.Validate(); err != nil {
		return RetireResult{}, fmt.Errorf("invalid retire result: %w: %w", err, errdefs.ErrUnavailable)
	}
	return result, nil
}

func (c *Client) CrashFence(ctx context.Context, request CrashFenceRequest) (CrashFenceResult, error) {
	if err := request.Validate(); err != nil {
		return CrashFenceResult{}, fmt.Errorf("invalid crash fence request: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	var result CrashFenceResult
	if err := c.do(ctx, http.MethodPut, parentURL(request.Parent)+"/crash-fence", request, &result); err != nil {
		return CrashFenceResult{}, err
	}
	if err := result.Validate(); err != nil {
		return CrashFenceResult{}, fmt.Errorf("invalid crash fence result: %w: %w", err, errdefs.ErrUnavailable)
	}
	return result, nil
}

func (c *Client) CreateGate(ctx context.Context, request GateRequest) (GateImage, error) {
	var result GateImage
	if err := c.do(ctx, http.MethodPut, gateURL(request.SlotNonce), request, &result); err != nil {
		return GateImage{}, err
	}
	return result, nil
}

func (c *Client) DeleteGate(ctx context.Context, slotNonce string) error {
	return c.do(ctx, http.MethodDelete, gateURL(slotNonce), nil, nil)
}

func (c *Client) Incarnation(ctx context.Context) (RuntimeIncarnation, error) {
	var result RuntimeIncarnation
	if err := c.do(ctx, http.MethodGet, "/v1/incarnation", nil, &result); err != nil {
		return RuntimeIncarnation{}, err
	}
	if err := result.Validate(); err != nil {
		return RuntimeIncarnation{}, fmt.Errorf("invalid rootfs runtime incarnation: %w: %w", err, errdefs.ErrUnavailable)
	}
	return result, nil
}

func (c *Client) ConsumeWriterGrant(ctx context.Context, request StageRequest) (protocol.LeaseObservation, error) {
	var observation protocol.LeaseObservation
	if err := c.do(ctx, http.MethodPut, writerGrantURL(request.Identity.WriterGrantID), request, &observation); err != nil {
		return protocol.LeaseObservation{}, err
	}
	if err := observation.Validate(); err != nil {
		return protocol.LeaseObservation{}, fmt.Errorf("invalid writer lease observation: %w: %w", err, errdefs.ErrUnavailable)
	}
	return observation, nil
}

func (c *Client) RenewWriterGrant(ctx context.Context, request StageRequest) (protocol.LeaseObservation, error) {
	var observation protocol.LeaseObservation
	if err := c.do(ctx, http.MethodPut, writerGrantURL(request.Identity.WriterGrantID)+"/renew", request.WithoutWriterGrantToken(), &observation); err != nil {
		return protocol.LeaseObservation{}, err
	}
	if err := observation.Validate(); err != nil {
		return protocol.LeaseObservation{}, fmt.Errorf("invalid writer lease observation: %w: %w", err, errdefs.ErrUnavailable)
	}
	return observation, nil
}

func (c *Client) RenewWriterGrants(ctx context.Context, requests []StageRequest) (protocol.BatchRenewResponse, error) {
	var response protocol.BatchRenewResponse
	body := WriterGrantBatchRenewRequest{Items: make([]StageRequest, 0, len(requests))}
	for _, request := range requests {
		body.Items = append(body.Items, request.WithoutWriterGrantToken())
	}
	if err := c.do(ctx, http.MethodPut, "/v1/writer-grants:renew", body, &response); err != nil {
		return protocol.BatchRenewResponse{}, err
	}
	if err := response.Validate(len(requests)); err != nil {
		return protocol.BatchRenewResponse{}, fmt.Errorf("invalid writer renewal batch response: %w: %w", err, errdefs.ErrUnavailable)
	}
	return response, nil
}

func (c *Client) VerifyTerminalWriterGrant(ctx context.Context, request StageRequest) error {
	return c.put(ctx, writerGrantURL(request.Identity.WriterGrantID)+"/terminal", request.WithoutWriterGrantToken())
}

func (c *Client) put(ctx context.Context, path string, body any) error {
	return c.do(ctx, http.MethodPut, path, body, nil)
}

func (c *Client) do(ctx context.Context, method, path string, body, result any) error {
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(payload)
	}
	request, err := http.NewRequestWithContext(ctx, method, "http://unix"+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		payload, _ := io.ReadAll(io.LimitReader(response.Body, maxRequestBytes))
		message := fmt.Sprintf("rootfs handoff API returned %s: %s", response.Status, strings.TrimSpace(string(payload)))
		switch response.StatusCode {
		case http.StatusBadRequest:
			return fmt.Errorf("%s: %w", message, errdefs.ErrInvalidArgument)
		case http.StatusUnauthorized, http.StatusForbidden:
			return fmt.Errorf("%s: %w", message, errdefs.ErrPermissionDenied)
		case http.StatusNotFound:
			return fmt.Errorf("%s: %w", message, errdefs.ErrNotFound)
		case http.StatusConflict:
			return fmt.Errorf("%s: %w", message, errdefs.ErrAlreadyExists)
		case http.StatusPreconditionFailed:
			return fmt.Errorf("%s: %w", message, errdefs.ErrFailedPrecondition)
		case http.StatusGatewayTimeout:
			return fmt.Errorf("%s: %w", message, context.DeadlineExceeded)
		default:
			return fmt.Errorf("%s: %w", message, errdefs.ErrUnavailable)
		}
	}
	if result == nil || response.StatusCode == http.StatusNoContent {
		return nil
	}
	return json.NewDecoder(response.Body).Decode(result)
}

func parentURL(parent string) string {
	return "/v1/parents/" + url.PathEscape(parent)
}

func gateURL(slotNonce string) string {
	return "/v1/gates/" + url.PathEscape(slotNonce)
}

func writerGrantURL(grantID string) string {
	return "/v1/writer-grants/" + url.PathEscape(grantID)
}
