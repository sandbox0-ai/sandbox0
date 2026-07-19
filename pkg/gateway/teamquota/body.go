package teamquota

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
)

// RateLimitBodyBytes buffers one structurally bounded request body and admits
// its exact byte size before downstream code can persist it.
func (c *Controller) RateLimitBodyBytes(
	key coreteamquota.Key,
	maxBytes int64,
) gin.HandlerFunc {
	return func(ginCtx *gin.Context) {
		if maxBytes <= 0 {
			c.abortUnavailable(ginCtx, "team quota request-body limit is invalid", nil)
			return
		}
		authCtx := middleware.GetAuthContext(ginCtx)
		if authCtx == nil || authCtx.TeamID == "" {
			c.abortUnavailable(ginCtx, "team quota requires an authenticated team", nil)
			return
		}
		if ginCtx.Request == nil || ginCtx.Request.Body == nil {
			spec.JSONError(ginCtx, http.StatusBadRequest, spec.CodeBadRequest, "request body is required")
			ginCtx.Abort()
			return
		}
		if ginCtx.Request.ContentLength > maxBytes {
			spec.JSONError(
				ginCtx,
				http.StatusRequestEntityTooLarge,
				spec.CodeBadRequest,
				fmt.Sprintf("request body exceeds %d bytes", maxBytes),
			)
			ginCtx.Abort()
			return
		}

		originalBody := ginCtx.Request.Body
		body, err := io.ReadAll(io.LimitReader(originalBody, maxBytes+1))
		_ = originalBody.Close()
		if err != nil {
			spec.JSONError(ginCtx, http.StatusBadRequest, spec.CodeBadRequest, "failed to read request body")
			ginCtx.Abort()
			return
		}
		if int64(len(body)) > maxBytes {
			spec.JSONError(
				ginCtx,
				http.StatusRequestEntityTooLarge,
				spec.CodeBadRequest,
				fmt.Sprintf("request body exceeds %d bytes", maxBytes),
			)
			ginCtx.Abort()
			return
		}
		ginCtx.Request.Body = io.NopCloser(bytes.NewReader(body))
		ginCtx.Request.ContentLength = int64(len(body))

		if len(body) > 0 {
			err = c.TakeRate(
				ginCtx.Request.Context(),
				authCtx.TeamID,
				key,
				int64(len(body)),
			)
			if errors.Is(err, tokenbucket.ErrCostExceedsBurst) {
				err = &coreteamquota.RateExceededError{
					TeamID:     authCtx.TeamID,
					Key:        key,
					Remaining:  0,
					RetryAfter: time.Second,
				}
			}
			switch {
			case err == nil:
			case coreteamquota.IsRateExceeded(err):
				c.abortRateExceeded(ginCtx, err, fmt.Sprintf("team %s quota exceeded", key))
				return
			default:
				c.abortUnavailable(ginCtx, fmt.Sprintf("team %s quota unavailable", key), err)
				return
			}
		}
		ginCtx.Next()
	}
}
