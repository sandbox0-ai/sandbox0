package handlers

import (
	"net/http"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
)

// writeJSON writes a JSON response.
func writeJSON(w http.ResponseWriter, status int, data any) {
	_ = spec.WriteSuccess(w, status, data)
}

// writeError writes an error response.
func writeError(w http.ResponseWriter, status int, errCode string, message string) {
	_ = spec.WriteError(w, status, errCode, message)
}
