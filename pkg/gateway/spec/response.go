package spec

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
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
	CodeGone                = "gone"
	CodeConflict            = "conflict"
	CodeTemplateNotReady    = "template_not_ready"
	CodeSandboxResumeFailed = "sandbox_resume_failed"
	CodeAdmissionRestricted = "admission_restricted"
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

// JSONWriter is implemented by HTTP frameworks that can serialize a response.
type JSONWriter interface {
	JSON(status int, value any)
}

// jsonfunc writes a response envelope through a framework JSON response.
func jsonfunc(c JSONWriter, status int, resp Response) {
	c.JSON(status, resp)
}

// JSONSuccess writes a success envelope through a framework JSON response.
func JSONSuccess(c JSONWriter, status int, data any) {
	jsonfunc(c, status, successresp(data))
}

// JSONError writes an error envelope through a framework JSON response.
func JSONError(c JSONWriter, status int, code, message string, details ...any) {
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
