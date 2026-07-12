package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	gatewayauthn "github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

const maxAuditBufferedResponseBytes = 8 << 20

var sandboxAuditActions = map[string]string{
	"GET /api/v1/sandboxes/:id/audit/events":                       "audit.read",
	"GET /api/v1/sandboxes/:id/observability/events":               "audit.read",
	"GET /api/v1/sandboxes/:id/audit/events/:event_id/verify":      "audit.verify",
	"GET /api/v1/sandboxes/:id/observability/logs":                 "logs.read",
	"GET /api/v1/sandboxes/:id/metrics":                            "metrics.read",
	"GET /api/v1/sandboxes/:id/metrics/catalog":                    "metrics.catalog.read",
	"POST /api/v1/sandboxes":                                       "sandbox.create",
	"GET /api/v1/sandboxes/:id":                                    "sandbox.read",
	"GET /api/v1/sandboxes/:id/status":                             "sandbox.status.read",
	"PUT /api/v1/sandboxes/:id":                                    "sandbox.update",
	"DELETE /api/v1/sandboxes/:id":                                 "sandbox.delete",
	"POST /api/v1/sandboxes/:id/pause":                             "sandbox.pause",
	"POST /api/v1/sandboxes/:id/resume":                            "sandbox.resume",
	"POST /api/v1/sandboxes/:id/refresh":                           "sandbox.refresh",
	"POST /api/v1/sandboxes/:id/fork":                              "sandbox.fork",
	"POST /api/v1/sandboxes/:id/snapshots":                         "sandbox.rootfs_snapshot.create",
	"GET /api/v1/sandboxes/:id/snapshots":                          "sandbox.rootfs_snapshot.list",
	"POST /api/v1/sandboxes/:id/rootfs/restore":                    "sandbox.rootfs.restore",
	"GET /api/v1/sandboxes/:id/network":                            "sandbox.network_policy.read",
	"PUT /api/v1/sandboxes/:id/network":                            "sandbox.network_policy.update",
	"GET /api/v1/sandboxes/:id/services":                           "sandbox.services.read",
	"PUT /api/v1/sandboxes/:id/services":                           "sandbox.services.update",
	"GET /api/v1/sandboxes/:id/sessions":                           "session.list",
	"POST /api/v1/sandboxes/:id/sessions":                          "session.create",
	"GET /api/v1/sandboxes/:id/sessions/:session_id":               "session.read",
	"PUT /api/v1/sandboxes/:id/sessions/:session_id":               "session.update",
	"DELETE /api/v1/sandboxes/:id/sessions/:session_id":            "session.delete",
	"PUT /api/v1/sandboxes/:id/sessions/:session_id/desired-state": "session.desired_state.update",
	"POST /api/v1/sandboxes/:id/sessions/:session_id/attempts":     "session.attempt.create",
	"POST /api/v1/sandboxes/:id/sessions/:session_id/inputs":       "session.input",
	"POST /api/v1/sandboxes/:id/sessions/:session_id/signals":      "session.signal",
	"PUT /api/v1/sandboxes/:id/sessions/:session_id/terminal":      "session.terminal.update",
	"GET /api/v1/sandboxes/:id/sessions/:session_id/events":        "session.events.read",
	"GET /api/v1/sandboxes/:id/sessions/:session_id/events/stream": "session.events.stream",
	"GET /api/v1/sandboxes/:id/sessions/:session_id/ws":            "session.connect",
	"GET /api/v1/sandboxes/:id/contexts":                           "process.list",
	"POST /api/v1/sandboxes/:id/contexts":                          "process.create",
	"GET /api/v1/sandboxes/:id/contexts/:ctx_id":                   "process.read",
	"DELETE /api/v1/sandboxes/:id/contexts/:ctx_id":                "process.delete",
	"POST /api/v1/sandboxes/:id/contexts/:ctx_id/restart":          "process.restart",
	"POST /api/v1/sandboxes/:id/contexts/:ctx_id/input":            "process.input",
	"POST /api/v1/sandboxes/:id/contexts/:ctx_id/exec":             "process.exec",
	"POST /api/v1/sandboxes/:id/contexts/:ctx_id/resize":           "process.terminal.resize",
	"POST /api/v1/sandboxes/:id/contexts/:ctx_id/signal":           "process.signal",
	"GET /api/v1/sandboxes/:id/contexts/:ctx_id/ws":                "process.connect",
	"GET /api/v1/sandboxes/:id/files":                              "file.read",
	"POST /api/v1/sandboxes/:id/files":                             "file.write",
	"DELETE /api/v1/sandboxes/:id/files":                           "file.delete",
	"GET /api/v1/sandboxes/:id/files/watch":                        "file.watch",
	"POST /api/v1/sandboxes/:id/files/move":                        "file.move",
	"GET /api/v1/sandboxes/:id/files/stat":                         "file.stat",
	"GET /api/v1/sandboxes/:id/files/list":                         "directory.list",
}

func (s *Server) auditSandboxRequests() gin.HandlerFunc {
	return func(c *gin.Context) {
		if s == nil || s.cfg == nil || !s.cfg.SandboxObservability.AuditEnabled {
			c.Next()
			return
		}
		action := sandboxAuditActions[c.Request.Method+" "+c.FullPath()]
		if action == "" {
			c.Next()
			return
		}
		if s.auditWriter == nil {
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox audit backend is unavailable")
			c.Abort()
			return
		}
		authCtx := gatewayauthn.FromContext(c.Request.Context())
		if authCtx == nil || strings.TrimSpace(authCtx.TeamID) == "" {
			spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "authenticated team is required")
			c.Abort()
			return
		}
		operationID := authCtx.OperationID
		if operationID == "" {
			operationID = uuid.NewString()
			authCtx.OperationID = operationID
		}
		attempt, err := s.newSandboxAPIEvent(c, authCtx, action, operationID, "", sandboxobservability.EventPhaseAttempt, sandboxobservability.OutcomeAccepted)
		if err == nil {
			err = s.auditWriter.InsertEvents(c.Request.Context(), []sandboxobservability.Event{attempt})
		}
		if err != nil {
			s.logger.Error("Failed to persist sandbox audit attempt", zap.String("action", action), zap.Error(err))
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox audit is unavailable; operation was not executed")
			c.Abort()
			return
		}

		var buffered *auditBufferedResponseWriter
		if auditShouldBufferResponse(c) {
			buffered = newAuditBufferedResponseWriter(c.Writer, maxAuditBufferedResponseBytes)
			c.Writer = buffered
		}
		defer func() {
			recovered := recover()
			if buffered != nil && action == "sandbox.create" {
				if sandboxID := sandboxIDFromCreateResponse(buffered.body.Bytes()); sandboxID != "" {
					c.Set("audit_result_sandbox_id", sandboxID)
				}
			}
			outcome := auditOutcomeForStatus(c.Writer.Status())
			if recovered != nil || (buffered != nil && buffered.err != nil) {
				outcome = sandboxobservability.OutcomeUnknown
			}
			result, resultErr := s.newSandboxAPIEvent(c, authCtx, action, operationID, attempt.EventID, sandboxobservability.EventPhaseResult, outcome)
			if resultErr == nil {
				resultCtx, cancel := context.WithTimeout(context.WithoutCancel(c.Request.Context()), 5*time.Second)
				resultErr = s.persistAuditResult(resultCtx, result)
				cancel()
			}
			if resultErr != nil {
				s.logger.Error("Failed to durably persist sandbox audit result",
					zap.String("action", action),
					zap.String("operation_id", operationID),
					zap.Error(resultErr))
			}
			if buffered != nil {
				c.Writer = buffered.ResponseWriter
				switch {
				case recovered != nil:
					// Gin recovery owns the final 500 response. The successful
					// handler response has never reached the client.
				case resultErr != nil || buffered.err != nil:
					buffered.discardHeaders()
					spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "operation may have completed; canonical audit result is pending", gin.H{
						"operation_id": operationID,
						"outcome":      "unknown",
					})
				default:
					if commitErr := buffered.commit(); commitErr != nil {
						s.logger.Error("Failed to commit buffered sandbox response", zap.Error(commitErr))
					}
				}
			}
			if recovered != nil {
				panic(recovered)
			}
		}()
		c.Next()
	}
}

func auditShouldBufferResponse(c *gin.Context) bool {
	if c == nil || c.Request == nil {
		return false
	}
	switch c.Request.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return false
	default:
		return true
	}
}

func auditOutcomeForStatus(status int) sandboxobservability.Outcome {
	switch {
	case status == http.StatusAccepted:
		return sandboxobservability.OutcomeAccepted
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return sandboxobservability.OutcomeDenied
	case status >= http.StatusInternalServerError:
		return sandboxobservability.OutcomeUnknown
	case status >= http.StatusBadRequest:
		return sandboxobservability.OutcomeFailed
	default:
		return sandboxobservability.OutcomeSucceeded
	}
}

func (s *Server) persistAuditResult(ctx context.Context, event sandboxobservability.Event) error {
	if s.auditResults != nil {
		return s.auditResults.Persist(ctx, event)
	}
	if s.auditWriter == nil {
		return fmt.Errorf("sandbox audit writer is unavailable")
	}
	return s.auditWriter.InsertEvents(ctx, []sandboxobservability.Event{event})
}

func (s *Server) newSandboxAPIEvent(c *gin.Context, authCtx *gatewayauthn.AuthContext, action, operationID, parentEventID string, phase sandboxobservability.EventPhase, outcome sandboxobservability.Outcome) (sandboxobservability.Event, error) {
	now := time.Now().UTC()
	sandboxID := strings.TrimSpace(c.Param("id"))
	if phase == sandboxobservability.EventPhaseResult {
		if resultID, ok := c.Get("audit_result_sandbox_id"); ok {
			if value, _ := resultID.(string); strings.TrimSpace(value) != "" {
				sandboxID = strings.TrimSpace(value)
			}
		}
	}
	if sandboxID == "" {
		sandboxID = "collection"
	}
	principal := authCtx.Principal()
	requestID := authCtx.RequestID
	if requestID == "" {
		requestID = truncateAuditString(strings.TrimSpace(c.GetHeader("X-Request-ID")), 128)
	}
	traceID := ""
	spanContext := trace.SpanFromContext(c.Request.Context()).SpanContext()
	if spanContext.IsValid() {
		traceID = spanContext.TraceID().String()
	}
	statusCode := 0
	if phase == sandboxobservability.EventPhaseResult {
		statusCode = c.Writer.Status()
	}
	event := sandboxobservability.Event{
		EventID:       uuid.NewString(),
		SchemaVersion: sandboxobservability.CurrentEventSchemaVersion,
		TeamID:        authCtx.TeamID,
		SandboxID:     sandboxID,
		RegionID:      s.cfg.RegionID,
		ClusterID:     s.cfg.ClusterID,
		OccurredAt:    now,
		IngestedAt:    now,
		Source:        sandboxobservability.SourceClusterGateway,
		EventType:     sandboxobservability.EventTypeAPIAccess,
		Phase:         phase,
		Outcome:       outcome,
		Actor: sandboxobservability.AuditActor{
			Kind:       sandboxobservability.ActorKind(principal.Kind),
			ID:         principal.ID,
			UserID:     principal.UserID,
			APIKeyID:   principal.APIKeyID,
			AuthMethod: string(principal.AuthMethod),
		},
		Action:        action,
		Resource:      auditResourceForAction(c, action, sandboxID),
		OperationID:   operationID,
		ParentEventID: parentEventID,
		Producer:      sandboxobservability.AuditProducer{Service: "cluster-gateway"},
		Request: sandboxobservability.AuditRequest{
			RequestID:  requestID,
			TraceID:    traceID,
			SourceIP:   auditSocketPeerIP(c.Request.RemoteAddr),
			UserAgent:  truncateAuditString(c.Request.UserAgent(), 512),
			HTTPMethod: c.Request.Method,
			Route:      c.FullPath(),
			StatusCode: statusCode,
		},
	}
	event.Cursor = event.EventID
	event.Watermark = event.EventID
	if err := sandboxobservability.SignEvent(&event, s.auditSigningKey); err != nil {
		return sandboxobservability.Event{}, fmt.Errorf("sign audit event: %w", err)
	}
	return event, nil
}

func auditResourceForAction(c *gin.Context, action, sandboxID string) sandboxobservability.AuditResource {
	resource := sandboxobservability.AuditResource{Type: "sandbox", ID: sandboxID}
	switch {
	case strings.HasPrefix(action, "process."):
		resource.Type = "sandbox_process"
		if id := strings.TrimSpace(c.Param("ctx_id")); id != "" {
			resource.ID = id
		}
		resource.Subresource = sandboxID
	case strings.HasPrefix(action, "session."):
		resource.Type = "sandbox_session"
		if id := strings.TrimSpace(c.Param("session_id")); id != "" {
			resource.ID = id
		}
		resource.Subresource = sandboxID
	case strings.HasPrefix(action, "file.") || strings.HasPrefix(action, "directory."):
		resource.Type = "sandbox_file"
		resource.Subresource = action
	case strings.HasPrefix(action, "sandbox.network_policy"):
		resource.Type = "sandbox_network_policy"
	case strings.HasPrefix(action, "audit."):
		resource.Type = "sandbox_audit"
	case strings.HasPrefix(action, "logs."):
		resource.Type = "sandbox_logs"
	case strings.HasPrefix(action, "metrics."):
		resource.Type = "sandbox_metrics"
	default:
		resource.Subresource = strings.TrimPrefix(c.FullPath(), "/api/v1/sandboxes/:id/")
	}
	return resource
}

type auditBufferedResponseWriter struct {
	gin.ResponseWriter
	body   bytes.Buffer
	limit  int
	status int
	err    error
}

func newAuditBufferedResponseWriter(writer gin.ResponseWriter, limit int) *auditBufferedResponseWriter {
	return &auditBufferedResponseWriter{ResponseWriter: writer, limit: limit, status: -1}
}

func (w *auditBufferedResponseWriter) WriteHeader(statusCode int) {
	if w.status < 0 {
		w.status = statusCode
	}
}

func (w *auditBufferedResponseWriter) WriteHeaderNow() {
	if w.status < 0 {
		w.status = http.StatusOK
	}
}

func (w *auditBufferedResponseWriter) Write(data []byte) (int, error) {
	w.WriteHeaderNow()
	if w.err != nil {
		return 0, w.err
	}
	if len(data) > w.limit-w.body.Len() {
		w.err = fmt.Errorf("audit response exceeds %d buffered bytes", w.limit)
		return 0, w.err
	}
	return w.body.Write(data)
}

func (w *auditBufferedResponseWriter) WriteString(value string) (int, error) {
	return w.Write([]byte(value))
}

func (w *auditBufferedResponseWriter) Flush() {
	w.WriteHeaderNow()
}

func (w *auditBufferedResponseWriter) Status() int {
	if w.status < 0 {
		return http.StatusOK
	}
	return w.status
}

func (w *auditBufferedResponseWriter) Size() int { return w.body.Len() }

func (w *auditBufferedResponseWriter) Written() bool { return w.status >= 0 }

func (w *auditBufferedResponseWriter) commit() error {
	if w.status >= 0 {
		w.ResponseWriter.WriteHeader(w.status)
	}
	if w.body.Len() == 0 {
		return nil
	}
	_, err := w.ResponseWriter.Write(w.body.Bytes())
	return err
}

func (w *auditBufferedResponseWriter) discardHeaders() {
	header := w.Header()
	for _, name := range []string{"Content-Length", "Content-Encoding", "ETag", "Last-Modified"} {
		header.Del(name)
	}
}

func sandboxIDFromCreateResponse(payload []byte) string {
	var response struct {
		Data struct {
			ID        string `json:"id"`
			SandboxID string `json:"sandbox_id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(payload, &response); err != nil {
		return ""
	}
	if strings.TrimSpace(response.Data.ID) != "" {
		return strings.TrimSpace(response.Data.ID)
	}
	return strings.TrimSpace(response.Data.SandboxID)
}

func truncateAuditString(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}

func (s *Server) beginExposureAudit(c *gin.Context, sandbox *mgr.Sandbox, service *mgr.SandboxAppService, route *mgr.SandboxAppServiceRoute) (func(), bool) {
	noop := func() {}
	if s == nil || s.cfg == nil || !s.cfg.SandboxObservability.AuditEnabled {
		return noop, true
	}
	if s.auditWriter == nil || sandbox == nil || service == nil || route == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox audit backend is unavailable")
		return noop, false
	}
	operationID := uuid.NewString()
	action := "sandbox.service.invoke"
	if c.Request.Method == http.MethodOptions {
		action = "sandbox.service.preflight"
	}
	actorForPhase := func(phase sandboxobservability.EventPhase) sandboxobservability.AuditActor {
		actor := sandboxobservability.AuditActor{Kind: sandboxobservability.ActorKindAnonymous, AuthMethod: "none"}
		if phase != sandboxobservability.EventPhaseResult || route.Auth == nil || route.Auth.Mode == "" || route.Auth.Mode == mgr.SandboxAppServiceRouteAuthModeNone {
			return actor
		}
		if authenticated, _ := c.Get(exposureRouteAuthenticatedKey); authenticated == true {
			return sandboxobservability.AuditActor{
				Kind:       sandboxobservability.ActorKindExposureCredential,
				ID:         route.ID,
				AuthMethod: route.Auth.Mode,
			}
		}
		return actor
	}
	newEvent := func(phase sandboxobservability.EventPhase, outcome sandboxobservability.Outcome, parentID string) (sandboxobservability.Event, error) {
		now := time.Now().UTC()
		event := sandboxobservability.Event{
			EventID:       uuid.NewString(),
			SchemaVersion: sandboxobservability.CurrentEventSchemaVersion,
			TeamID:        sandbox.TeamID,
			SandboxID:     sandbox.ID,
			RegionID:      s.cfg.RegionID,
			ClusterID:     s.cfg.ClusterID,
			OccurredAt:    now,
			IngestedAt:    now,
			Source:        sandboxobservability.SourceClusterGateway,
			EventType:     sandboxobservability.EventTypeAPIAccess,
			Phase:         phase,
			Outcome:       outcome,
			Actor:         actorForPhase(phase),
			Action:        action,
			Resource: sandboxobservability.AuditResource{
				Type: "sandbox_service", ID: service.ID, Subresource: sandbox.ID + "/" + route.ID,
			},
			OperationID:   operationID,
			ParentEventID: parentID,
			Producer:      sandboxobservability.AuditProducer{Service: "cluster-gateway"},
			Request: sandboxobservability.AuditRequest{
				RequestID: truncateAuditString(strings.TrimSpace(c.GetHeader("X-Request-ID")), 128),
				SourceIP:  auditSocketPeerIP(c.Request.RemoteAddr), UserAgent: truncateAuditString(c.Request.UserAgent(), 512),
				HTTPMethod: c.Request.Method, Route: route.PathPrefix,
			},
		}
		if phase == sandboxobservability.EventPhaseResult {
			event.Request.StatusCode = c.Writer.Status()
		}
		event.Cursor = event.EventID
		event.Watermark = event.EventID
		if err := sandboxobservability.SignEvent(&event, s.auditSigningKey); err != nil {
			return sandboxobservability.Event{}, err
		}
		return event, nil
	}
	attempt, err := newEvent(sandboxobservability.EventPhaseAttempt, sandboxobservability.OutcomeAccepted, "")
	if err == nil {
		err = s.auditWriter.InsertEvents(c.Request.Context(), []sandboxobservability.Event{attempt})
	}
	if err != nil {
		s.logger.Error("Failed to persist public exposure audit attempt", zap.Error(err))
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox audit is unavailable; invocation was not executed")
		return noop, false
	}
	return func() {
		outcome := auditOutcomeForStatus(c.Writer.Status())
		if panicked, _ := c.Get(exposureAuditPanickedKey); panicked == true {
			outcome = sandboxobservability.OutcomeUnknown
		}
		result, resultErr := newEvent(sandboxobservability.EventPhaseResult, outcome, attempt.EventID)
		if resultErr == nil {
			resultCtx, cancel := context.WithTimeout(context.WithoutCancel(c.Request.Context()), 5*time.Second)
			resultErr = s.persistAuditResult(resultCtx, result)
			cancel()
		}
		if resultErr != nil {
			s.logger.Error("Failed to persist public exposure audit result", zap.String("operation_id", operationID), zap.Error(resultErr))
		}
	}, true
}

func auditSocketPeerIP(remoteAddr string) string {
	host, _, err := net.SplitHostPort(strings.TrimSpace(remoteAddr))
	if err == nil {
		return truncateAuditString(host, 128)
	}
	return truncateAuditString(strings.TrimSpace(remoteAddr), 128)
}
