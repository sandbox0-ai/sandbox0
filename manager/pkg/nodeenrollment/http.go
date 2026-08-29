package nodeenrollment

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
)

const maxEnrollmentRequestBytes = 256 << 10

type HTTPHandler struct {
	service *Service
}

func NewHTTPHandler(service *Service) (*HTTPHandler, error) {
	if service == nil {
		return nil, errors.New("node enrollment service is required")
	}
	return &HTTPHandler{service: service}, nil
}

func (h *HTTPHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if request.Header.Get("X-Forwarded-For") != "" || request.Header.Get("Forwarded") != "" {
		http.Error(writer, "forwarded enrollment identity is forbidden", http.StatusBadRequest)
		return
	}
	remoteIP, _, err := net.SplitHostPort(request.RemoteAddr)
	if err != nil || net.ParseIP(remoteIP) == nil {
		http.Error(writer, "caller address is invalid", http.StatusBadRequest)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	switch request.URL.Path {
	case "/internal/v1/node-enrollment/challenge":
		var body struct {
			ProviderInstanceID string `json:"provider_instance_id"`
		}
		if !decodeEnrollmentJSON(writer, request, &body) {
			return
		}
		result, err := h.service.Challenge(request.Context(), body.ProviderInstanceID, remoteIP)
		writeEnrollmentJSON(writer, result, err)
	case "/internal/v1/node-enrollment/bootstrap":
		var body struct {
			ProviderInstanceID string `json:"provider_instance_id"`
			BootstrapRequest
		}
		if !decodeEnrollmentJSON(writer, request, &body) {
			return
		}
		result, err := h.service.Bootstrap(request.Context(), body.ProviderInstanceID, remoteIP, body.BootstrapRequest)
		writeEnrollmentJSON(writer, result, err)
	case "/internal/v1/node-enrollment/finalize":
		var body struct {
			ProviderInstanceID string `json:"provider_instance_id"`
			FinalizeRequest
		}
		if !decodeEnrollmentJSON(writer, request, &body) {
			return
		}
		result, err := h.service.Finalize(request.Context(), body.ProviderInstanceID, remoteIP, body.FinalizeRequest)
		writeEnrollmentJSON(writer, result, err)
	case "/internal/v1/node-enrollment/admit":
		var body struct {
			ProviderInstanceID string `json:"provider_instance_id"`
			AdmitRequest
		}
		if !decodeEnrollmentJSON(writer, request, &body) {
			return
		}
		result, err := h.service.Admit(request.Context(), body.ProviderInstanceID, remoteIP, body.AdmitRequest)
		writeEnrollmentJSON(writer, result, err)
	default:
		http.NotFound(writer, request)
	}
}

func decodeEnrollmentJSON(writer http.ResponseWriter, request *http.Request, target any) bool {
	if !strings.HasPrefix(request.Header.Get("Content-Type"), "application/json") {
		http.Error(writer, "content type must be application/json", http.StatusUnsupportedMediaType)
		return false
	}
	reader := http.MaxBytesReader(writer, request.Body, maxEnrollmentRequestBytes)
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		http.Error(writer, "invalid enrollment request", http.StatusBadRequest)
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		http.Error(writer, "enrollment request contains trailing data", http.StatusBadRequest)
		return false
	}
	return true
}

func writeEnrollmentJSON(writer http.ResponseWriter, value any, err error) {
	if err != nil {
		// Enrollment errors intentionally do not disclose which cloud, challenge,
		// certificate, or membership check rejected the caller.
		http.Error(writer, "node enrollment rejected", http.StatusForbidden)
		return
	}
	// All current response values are closed structs containing scalar and byte
	// slice fields, so JSON marshaling before writing headers is deterministic
	// and prevents a partial success response on encoding failure.
	payload, encodeErr := json.Marshal(value)
	if encodeErr != nil {
		http.Error(writer, "encode enrollment response", http.StatusInternalServerError)
		return
	}
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(append(payload, '\n'))
}
