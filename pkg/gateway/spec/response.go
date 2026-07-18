package spec

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
)

// Error represents a standardized error payload.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
}

// Response represents a standardized response envelope.
type Response struct {
	Success bool   `json:"success"`
	Data    any    `json:"data,omitempty"`
	Error   *Error `json:"error,omitempty"`
}

// rawResponse is used when decoding typed response payloads.
type rawResponse struct {
	Success bool            `json:"success"`
	Data    json.RawMessage `json:"data,omitempty"`
	Error   *Error          `json:"error,omitempty"`
}

const (
	CodeBadRequest          = "bad_request"
	CodeUnauthorized        = "unauthorized"
	CodeForbidden           = "forbidden"
	CodeNotFound            = "not_found"
	CodeConflict            = "conflict"
	CodeTemplateNotReady    = "template_not_ready"
	CodeClaimStartThrottled = "claim_start_throttled"
	CodeUnavailable         = "unavailable"
	CodeInternal            = "internal_error"
	CodeNotLicensed         = "feature_not_licensed"
)

// successresp builds a success envelope.
func successresp(data any) Response {
	return Response{
		Success: true,
		Data:    data,
	}
}

// errorresp builds an error envelope.
func errorresp(code, message string, details ...any) Response {
	resp := Response{
		Success: false,
		Error: &Error{
			Code:    code,
			Message: message,
		},
	}
	if len(details) > 0 {
		resp.Error.Details = details[0]
	}
	return resp
}

// write writes a response envelope using net/http.
func write(w http.ResponseWriter, status int, resp Response) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(resp)
}

// WriteSuccess writes a success envelope using net/http.
func WriteSuccess(w http.ResponseWriter, status int, data any) error {
	return write(w, status, successresp(data))
}

// WriteError writes an error envelope using net/http.
func WriteError(w http.ResponseWriter, status int, code, message string, details ...any) error {
	return write(w, status, errorresp(code, message, details...))
}

// jsonfunc writes a response envelope using gin.
func jsonfunc(c *gin.Context, status int, resp Response) {
	c.JSON(status, resp)
}

// JSONSuccess writes a success envelope using gin.
func JSONSuccess(c *gin.Context, status int, data any) {
	jsonfunc(c, status, successresp(data))
}

// JSONError writes an error envelope using gin.
func JSONError(c *gin.Context, status int, code, message string, details ...any) {
	jsonfunc(c, status, errorresp(code, message, details...))
}

// DecodeResponse decodes a standardized response and unmarshals the data payload.
func DecodeResponse[T any](r io.Reader) (*T, *Error, error) {
	var raw rawResponse
	if err := json.NewDecoder(r).Decode(&raw); err != nil {
		return nil, nil, err
	}
	if raw.Error != nil || !raw.Success {
		return nil, raw.Error, nil
	}
	var out T
	if len(raw.Data) == 0 {
		return &out, nil, nil
	}
	if err := json.Unmarshal(raw.Data, &out); err != nil {
		return nil, nil, err
	}
	return &out, nil, nil
}

// DecodeErrorMessage decodes a standardized error envelope and returns its
// message when present.
func DecodeErrorMessage(body []byte) (string, bool) {
	_, apiErr, err := DecodeResponse[json.RawMessage](bytes.NewReader(body))
	if err != nil || apiErr == nil || apiErr.Message == "" {
		return "", false
	}
	return apiErr.Message, true
}

// DecodeResponseOrRaw decodes either a standardized response envelope or a raw
// JSON payload. This is for compatibility paths where an upstream service may
// still return a bare response body.
func DecodeResponseOrRaw[T any](r io.Reader) (*T, error) {
	body, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	var raw rawResponse
	if err := json.Unmarshal(body, &raw); err == nil && looksLikeEnvelope(raw) {
		if raw.Error != nil {
			return nil, errors.New(raw.Error.Message)
		}
		if !raw.Success {
			return nil, errors.New("response was not successful")
		}
		if rawDataIsEmpty(raw.Data) {
			return nil, errors.New("response missing data")
		}
		var out T
		if err := json.Unmarshal(raw.Data, &out); err != nil {
			return nil, err
		}
		return &out, nil
	}

	var out T
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func looksLikeEnvelope(raw rawResponse) bool {
	return raw.Success || len(raw.Data) > 0 || raw.Error != nil
}

func rawDataIsEmpty(data json.RawMessage) bool {
	trimmed := bytes.TrimSpace(data)
	return len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null"))
}
