package admission

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"go.uber.org/zap"
)

type Handler struct {
	store  Store
	logger *zap.Logger
}

func NewHandler(store Store, logger *zap.Logger) *Handler {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Handler{store: store, logger: logger}
}

func (h *Handler) Put(c *gin.Context) {
	teamID := strings.TrimSpace(c.Param("team_id"))
	if _, err := uuid.Parse(teamID); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "team_id must be a UUID")
		return
	}

	var update Update
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&update); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid admission update")
		return
	}
	if err := ensureJSONEOF(decoder); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid admission update")
		return
	}

	normalized, err := update.Validate()
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	if h == nil || h.store == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "admission state store is unavailable")
		return
	}

	result, err := h.store.Put(c.Request.Context(), teamID, normalized)
	if errors.Is(err, ErrVersionConflict) {
		spec.JSONError(c, http.StatusConflict, spec.CodeConflict, err.Error())
		return
	}
	if err != nil {
		h.logger.Error("Failed to update team admission state",
			zap.String("team_id", teamID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to update team admission state")
		return
	}
	spec.JSONSuccess(c, http.StatusOK, result)
}

func NewUsageMiddleware(reader Reader, logger *zap.Logger, requireState bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !StartsUsage(c.Request.Method, c.FullPath()) &&
			!StartsUsage(c.Request.Method, c.Request.URL.Path) {
			c.Next()
			return
		}
		if reader == nil && !requireState {
			// Self-hosted gateways have no external billing projection. Their
			// explicit default is to keep admission disabled.
			c.Next()
			return
		}

		authCtx := middleware.GetAuthContext(c)
		teamID := ""
		if authCtx != nil {
			teamID = authCtx.TeamID
		}
		if !EnforceUsageStart(c, reader, logger, requireState, teamID) {
			return
		}
		c.Next()
	}
}

// EnforceUsageStart applies one team's admission state at the point where an
// operation can start billable work. It is shared by route middleware and
// runtime auto-resume paths. This is deliberately soft prepaid: it rejects new
// spend and resume paths but never terminates an already-running sandbox.
func EnforceUsageStart(
	c *gin.Context,
	reader Reader,
	logger *zap.Logger,
	requireState bool,
	teamID string,
) bool {
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "team context is required")
		c.Abort()
		return false
	}
	if reader == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "admission state is unavailable")
		c.Abort()
		return false
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	record, found, err := reader.Get(c.Request.Context(), teamID)
	if err != nil {
		logger.Error("Failed to read team admission state",
			zap.String("team_id", teamID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "admission state is unavailable")
		c.Abort()
		return false
	}
	if !found && requireState {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "admission state is not initialized")
		c.Abort()
		return false
	}
	if found && record.State == StateRestricted {
		spec.JSONError(c, http.StatusForbidden, spec.CodeAdmissionRestricted, "team is not allowed to start additional usage", gin.H{
			"reason":  record.Reason,
			"version": record.Version,
		})
		c.Abort()
		return false
	}
	return true
}

func StartsUsage(method, routePattern string) bool {
	routePattern = strings.TrimSuffix(routePattern, "/")
	switch method {
	case http.MethodPut:
		return hasSinglePathSegment(routePattern, "/api/v1/sandboxes/") ||
			hasSinglePathSegment(routePattern, "/api/v1/templates/")
	case http.MethodPost:
		switch routePattern {
		case "/api/v1/sandboxes",
			"/api/v1/templates",
			"/api/v1/templates/from-sandbox":
			return true
		}
		if hasResourceAction(routePattern, "/api/v1/sandboxes/", "resume", "fork", "snapshots") {
			return true
		}
		return false
	}
	return false
}

func hasSinglePathSegment(value, prefix string) bool {
	remainder, found := strings.CutPrefix(value, prefix)
	return found && remainder != "" && !strings.Contains(remainder, "/")
}

func hasResourceAction(value, prefix string, actions ...string) bool {
	remainder, found := strings.CutPrefix(value, prefix)
	if !found {
		return false
	}
	parts := strings.Split(remainder, "/")
	if len(parts) != 2 || parts[0] == "" {
		return false
	}
	for _, action := range actions {
		if parts[1] == action {
			return true
		}
	}
	return false
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("multiple JSON values")
	}
	return err
}
