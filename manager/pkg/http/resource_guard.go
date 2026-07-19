package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/resourceguard"
)

func limitJSONRequestBody(c *gin.Context, resource string, maxBytes int64) bool {
	if err := resourceguard.LimitJSONBody(c.Request, resource, maxBytes); err != nil {
		if resourceguard.IsTooLarge(err) {
			spec.JSONError(
				c,
				http.StatusRequestEntityTooLarge,
				"request_too_large",
				resource+" is too large",
			)
		} else {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		}
		return false
	}
	return true
}

func writeResourceTooLarge(c *gin.Context, err error, resource string) bool {
	if !resourceguard.IsTooLarge(err) {
		return false
	}
	spec.JSONError(
		c,
		http.StatusRequestEntityTooLarge,
		"request_too_large",
		resource+" is too large",
	)
	return true
}
