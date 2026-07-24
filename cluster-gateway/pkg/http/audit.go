package http

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

const (
	maxAuditBufferedResponseBytes = 8 << 20
	auditCanonicalDeliveryTimeout = 5 * time.Second
)

type sandboxAuditRoutePolicy struct {
	Action         string
	BufferResponse bool
}

func auditReadRoute(action string) sandboxAuditRoutePolicy {
	return sandboxAuditRoutePolicy{Action: action}
}

func auditMutationRoute(action string) sandboxAuditRoutePolicy {
	return sandboxAuditRoutePolicy{Action: action, BufferResponse: true}
}

var sandboxAuditRoutePolicies = map[string]sandboxAuditRoutePolicy{
	"GET /api/v1/sandboxes/:id/observability/events":               auditReadRoute("audit.read"),
	"GET /api/v1/sandboxes/:id/observability/logs":                 auditReadRoute("logs.read"),
	"GET /api/v1/sandboxes/:id/metrics":                            auditReadRoute("metrics.read"),
	"GET /api/v1/sandboxes/:id/metrics/catalog":                    auditReadRoute("metrics.catalog.read"),
	"GET /api/v1/sandboxes":                                        auditReadRoute("sandbox.list"),
	"POST /api/v1/sandboxes":                                       auditMutationRoute("sandbox.create"),
	"GET /api/v1/sandboxes/:id":                                    auditReadRoute("sandbox.read"),
	"GET /api/v1/sandboxes/:id/status":                             auditReadRoute("sandbox.status.read"),
	"PUT /api/v1/sandboxes/:id":                                    auditMutationRoute("sandbox.update"),
	"DELETE /api/v1/sandboxes/:id":                                 auditMutationRoute("sandbox.delete"),
	"POST /api/v1/sandboxes/:id/pause":                             auditMutationRoute("sandbox.pause"),
	"POST /api/v1/sandboxes/:id/resume":                            auditMutationRoute("sandbox.resume"),
	"POST /api/v1/sandboxes/:id/refresh":                           auditMutationRoute("sandbox.refresh"),
	"POST /api/v1/sandboxes/:id/fork":                              auditMutationRoute("sandbox.fork"),
	"POST /api/v1/sandboxes/:id/snapshots":                         auditMutationRoute("sandbox.rootfs_snapshot.create"),
	"GET /api/v1/sandboxes/:id/snapshots":                          auditReadRoute("sandbox.rootfs_snapshot.list"),
	"POST /api/v1/sandboxes/:id/rootfs/restore":                    auditMutationRoute("sandbox.rootfs.restore"),
	"GET /api/v1/sandboxes/:id/network":                            auditReadRoute("sandbox.network_policy.read"),
	"PUT /api/v1/sandboxes/:id/network":                            auditMutationRoute("sandbox.network_policy.update"),
	"GET /api/v1/sandboxes/:id/services":                           auditReadRoute("sandbox.services.read"),
	"PUT /api/v1/sandboxes/:id/services":                           auditMutationRoute("sandbox.services.update"),
	"GET /api/v1/sandboxes/:id/sessions":                           auditReadRoute("session.list"),
	"POST /api/v1/sandboxes/:id/sessions":                          auditMutationRoute("session.create"),
	"GET /api/v1/sandboxes/:id/sessions/:session_id":               auditReadRoute("session.read"),
	"PUT /api/v1/sandboxes/:id/sessions/:session_id":               auditMutationRoute("session.update"),
	"DELETE /api/v1/sandboxes/:id/sessions/:session_id":            auditMutationRoute("session.delete"),
	"PUT /api/v1/sandboxes/:id/sessions/:session_id/desired-state": auditMutationRoute("session.desired_state.update"),
	"POST /api/v1/sandboxes/:id/sessions/:session_id/attempts":     auditMutationRoute("session.attempt.create"),
	"POST /api/v1/sandboxes/:id/sessions/:session_id/inputs":       auditMutationRoute("session.input"),
	"POST /api/v1/sandboxes/:id/sessions/:session_id/signals":      auditMutationRoute("session.signal"),
	"PUT /api/v1/sandboxes/:id/sessions/:session_id/terminal":      auditMutationRoute("session.terminal.update"),
	"GET /api/v1/sandboxes/:id/sessions/:session_id/events":        auditReadRoute("session.events.read"),
	"GET /api/v1/sandboxes/:id/sessions/:session_id/events/stream": auditReadRoute("session.events.stream"),
	"GET /api/v1/sandboxes/:id/sessions/:session_id/ws":            auditReadRoute("session.connect"),
	"GET /api/v1/sandboxes/:id/contexts":                           auditReadRoute("process.list"),
	"POST /api/v1/sandboxes/:id/contexts":                          auditMutationRoute("process.create"),
	"GET /api/v1/sandboxes/:id/contexts/:ctx_id":                   auditReadRoute("process.read"),
	"DELETE /api/v1/sandboxes/:id/contexts/:ctx_id":                auditMutationRoute("process.delete"),
	"POST /api/v1/sandboxes/:id/contexts/:ctx_id/restart":          auditMutationRoute("process.restart"),
	"POST /api/v1/sandboxes/:id/contexts/:ctx_id/input":            auditMutationRoute("process.input"),
	"POST /api/v1/sandboxes/:id/contexts/:ctx_id/exec":             auditMutationRoute("process.exec"),
	"POST /api/v1/sandboxes/:id/contexts/:ctx_id/resize":           auditMutationRoute("process.terminal.resize"),
	"POST /api/v1/sandboxes/:id/contexts/:ctx_id/signal":           auditMutationRoute("process.signal"),
	"GET /api/v1/sandboxes/:id/contexts/:ctx_id/ws":                auditReadRoute("process.connect"),
	"GET /api/v1/sandboxes/:id/files":                              auditReadRoute("file.read"),
	"POST /api/v1/sandboxes/:id/files":                             auditMutationRoute("file.write"),
	"DELETE /api/v1/sandboxes/:id/files":                           auditMutationRoute("file.delete"),
	"GET /api/v1/sandboxes/:id/files/watch":                        auditReadRoute("file.watch"),
	"POST /api/v1/sandboxes/:id/files/move":                        auditMutationRoute("file.move"),
	"GET /api/v1/sandboxes/:id/files/stat":                         auditReadRoute("file.stat"),
	"GET /api/v1/sandboxes/:id/files/list":                         auditReadRoute("directory.list"),
}

func (s *Server) auditSandboxRequests() gin.HandlerFunc {
	return func(c *gin.Context) {
		if s == nil || s.cfg == nil || !s.cfg.SandboxObservability.AuditEnabled {
			c.Next()
			return
		}
		policy, ok := sandboxAuditRoutePolicies[c.Request.Method+" "+c.FullPath()]
		if !ok || policy.Action == "" {
			c.Next()
			return
		}
		action := policy.Action
		if s.auditDelivery == nil {
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
		attemptDeliveryMode := s.auditAttemptDeliveryModeForRoute(policy)
		attempt, err := s.newSandboxAPIEvent(c, authCtx, action, operationID, "", sandboxobservability.EventPhaseAttempt, sandboxobservability.OutcomeAccepted, 0)
		if err == nil {
			// Canonical admission is bounded. Its local batch record is fsynced
			// only if ClickHouse does not acknowledge the pre-execution attempt.
			attemptCtx, cancel := context.WithTimeout(c.Request.Context(), auditCanonicalDeliveryTimeout)
			err = s.deliverAuditEvent(attemptCtx, attempt, attemptDeliveryMode)
			cancel()
		}
		if err != nil {
			c.Abort()
			auditAttemptState := "unrecorded"
			auditResultState := "unrecorded"
			if errors.Is(err, errAuditDeliveryPending) {
				auditAttemptState = "pending"
				if resultErr := s.persistFailedAuditAdmission(c, attempt); resultErr != nil {
					s.logger.Error("Failed to persist rejected sandbox audit result",
						zap.String("action", action),
						zap.String("operation_id", operationID),
						zap.String("attempt_event_id", attempt.EventID),
						zap.Error(resultErr),
					)
				} else {
					auditResultState = "captured"
				}
			}
			s.logger.Error("Failed to persist sandbox audit attempt", zap.String("action", action), zap.Error(err))
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox audit is unavailable; operation was not executed", failedAuditAdmissionDetails(operationID, auditAttemptState, auditResultState))
			return
		}

		var buffered *auditBufferedResponseWriter
		if policy.BufferResponse {
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
			resultStatusCode := c.Writer.Status()
			if recovered != nil || (buffered != nil && buffered.err != nil) {
				outcome = sandboxobservability.OutcomeUnknown
			}
			if recovered != nil {
				resultStatusCode = http.StatusInternalServerError
			} else if buffered != nil && buffered.err != nil {
				resultStatusCode = http.StatusServiceUnavailable
			}
			result, resultErr := s.newSandboxAPIEvent(c, authCtx, action, operationID, attempt.EventID, sandboxobservability.EventPhaseResult, outcome, resultStatusCode)
			if resultErr == nil {
				resultCtx, cancel := context.WithTimeout(context.WithoutCancel(c.Request.Context()), auditCanonicalDeliveryTimeout)
				// Mutation responses remain buffered until their result has
				// durable custody. Canonical admission still happens before the
				// handler, while the configured delivery mode controls whether
				// the result also waits for ClickHouse.
				resultErr = s.deliverAuditEvent(resultCtx, result, s.configuredAuditDeliveryMode())
				cancel()
			}
			if resultErr != nil {
				fields := []zap.Field{
					zap.String("action", action),
					zap.String("operation_id", operationID),
					zap.Error(resultErr),
				}
				if errors.Is(resultErr, errAuditDeliveryPending) {
					s.logger.Warn("Canonical sandbox audit result is pending durable replay", fields...)
				} else {
					s.logger.Error("Sandbox audit result is unrecorded", fields...)
				}
			}
			if buffered != nil {
				c.Writer = buffered.ResponseWriter
				switch {
				case recovered != nil:
					// Gin recovery owns the final 500 response. The successful
					// handler response has never reached the client.
				case errors.Is(resultErr, errAuditDeliveryPending):
					spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "operation may have completed; canonical audit result is pending", gin.H{
						"operation_id": operationID,
						"outcome":      "unknown",
						"audit_result": "pending",
					})
				case resultErr != nil:
					spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "operation may have completed; audit result could not be durably recorded", gin.H{
						"operation_id": operationID,
						"outcome":      "unknown",
						"audit_result": "unrecorded",
					})
				case buffered.err != nil:
					s.logger.Error("Sandbox response could not be safely delivered after its audit result was recorded",
						zap.String("action", action),
						zap.String("operation_id", operationID),
						zap.Error(buffered.err),
					)
					spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "operation may have completed; response could not be safely delivered", gin.H{
						"operation_id": operationID,
						"outcome":      "unknown",
						"audit_result": "recorded",
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

func (s *Server) configuredAuditDeliveryMode() sandboxobservability.AuditDeliveryMode {
	if s == nil || s.cfg == nil {
		return sandboxobservability.AuditDeliveryModeDurableAsync
	}
	return sandboxobservability.NormalizeAuditDeliveryMode(s.cfg.SandboxObservability.AuditDeliveryMode)
}

func (s *Server) auditAttemptDeliveryModeForRoute(policy sandboxAuditRoutePolicy) sandboxobservability.AuditDeliveryMode {
	if policy.BufferResponse {
		return sandboxobservability.AuditDeliveryModeCanonicalSync
	}
	return s.configuredAuditDeliveryMode()
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

func (s *Server) deliverAuditEvent(ctx context.Context, event sandboxobservability.Event, mode sandboxobservability.AuditDeliveryMode) error {
	if s == nil || s.auditDelivery == nil {
		return fmt.Errorf("%w: sandbox audit delivery is unavailable", errAuditUnrecorded)
	}
	if sandboxobservability.NormalizeAuditDeliveryMode(mode) == sandboxobservability.AuditDeliveryModeCanonicalSync {
		if event.Phase == sandboxobservability.EventPhaseAttempt {
			return s.auditDelivery.PersistCanonicalAdmission(ctx, event)
		}
		return s.auditDelivery.PersistCanonical(ctx, event)
	}
	return s.auditDelivery.EnqueueDurable(ctx, event)
}

func (s *Server) persistFailedAuditAdmission(c *gin.Context, attempt sandboxobservability.Event) error {
	now := time.Now().UTC()
	result := attempt
	result.EventID = uuid.NewString()
	result.OccurredAt = now
	result.IngestedAt = now
	result.Phase = sandboxobservability.EventPhaseResult
	result.Outcome = sandboxobservability.OutcomeFailed
	result.ParentEventID = attempt.EventID
	result.Request.StatusCode = http.StatusServiceUnavailable
	result.Attributes = failedAuditAdmissionAttributes()
	if err := sandboxobservability.SignEvent(&result, s.auditSigningKey); err != nil {
		return fmt.Errorf("sign failed audit admission result: %w", err)
	}
	resultCtx, cancel := context.WithTimeout(context.WithoutCancel(c.Request.Context()), auditCanonicalDeliveryTimeout)
	defer cancel()
	return s.auditDelivery.EnqueueDurable(resultCtx, result)
}

func failedAuditAdmissionAttributes() map[string]any {
	return map[string]any{
		"execution_started":  false,
		"failure_code":       "canonical_ack_unavailable",
		"failure_stage":      "canonical_audit_admission",
		"operation_executed": false,
	}
}

func failedAuditAdmissionDetails(operationID, auditAttemptState, auditResultState string) gin.H {
	return gin.H{
		"audit_attempt":      auditAttemptState,
		"audit_result":       auditResultState,
		"execution_started":  false,
		"failure_code":       "canonical_ack_unavailable",
		"failure_stage":      "canonical_audit_admission",
		"operation_executed": false,
		"operation_id":       operationID,
		"outcome":            sandboxobservability.OutcomeFailed,
	}
}

func (s *Server) newSandboxAPIEvent(c *gin.Context, authCtx *gatewayauthn.AuthContext, action, operationID, parentEventID string, phase sandboxobservability.EventPhase, outcome sandboxobservability.Outcome, resultStatusCode int) (sandboxobservability.Event, error) {
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
		statusCode = resultStatusCode
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
	header http.Header
	body   bytes.Buffer
	limit  int
	status int
	err    error
}

func newAuditBufferedResponseWriter(writer gin.ResponseWriter, limit int) *auditBufferedResponseWriter {
	return &auditBufferedResponseWriter{
		ResponseWriter: writer,
		header:         writer.Header().Clone(),
		limit:          limit,
		status:         -1,
	}
}

func (w *auditBufferedResponseWriter) Header() http.Header { return w.header }

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

// Streaming and connection takeover would expose a response before its audit
// result reaches canonical storage. Record those attempts as delivery errors
// instead of forwarding them to the underlying writer.
func (w *auditBufferedResponseWriter) Flush() {
	w.WriteHeaderNow()
	w.setUnsupportedError("streaming flush")
}

func (w *auditBufferedResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	err := w.setUnsupportedError("connection hijacking")
	return nil, nil, err
}

func (w *auditBufferedResponseWriter) CloseNotify() <-chan bool {
	return w.ResponseWriter.CloseNotify()
}

func (w *auditBufferedResponseWriter) Pusher() http.Pusher { return nil }

func (w *auditBufferedResponseWriter) Status() int {
	if w.status < 0 {
		return http.StatusOK
	}
	return w.status
}

func (w *auditBufferedResponseWriter) Size() int { return w.body.Len() }

func (w *auditBufferedResponseWriter) Written() bool { return w.status >= 0 }

func (w *auditBufferedResponseWriter) commit() error {
	replaceHTTPHeaders(w.ResponseWriter.Header(), w.header)
	if w.status >= 0 {
		w.ResponseWriter.WriteHeader(w.status)
	}
	if w.body.Len() == 0 {
		return nil
	}
	_, err := w.ResponseWriter.Write(w.body.Bytes())
	return err
}

func (w *auditBufferedResponseWriter) setUnsupportedError(operation string) error {
	if w.err == nil {
		w.err = fmt.Errorf("audit buffered response does not support %s", operation)
	}
	return w.err
}

func replaceHTTPHeaders(destination, source http.Header) {
	for name := range destination {
		delete(destination, name)
	}
	for name, values := range source {
		destination[name] = append([]string(nil), values...)
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
	if s.auditDelivery == nil || sandbox == nil || service == nil || route == nil {
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
		if err := sandboxobservability.SignEvent(&event, s.auditSigningKey); err != nil {
			return sandboxobservability.Event{}, err
		}
		return event, nil
	}
	attempt, err := newEvent(sandboxobservability.EventPhaseAttempt, sandboxobservability.OutcomeAccepted, "")
	if err == nil {
		// Canonical admission is bounded. Its local batch record is fsynced
		// only if ClickHouse does not acknowledge the pre-execution attempt.
		attemptCtx, cancel := context.WithTimeout(c.Request.Context(), auditCanonicalDeliveryTimeout)
		err = s.deliverAuditEvent(attemptCtx, attempt, s.configuredAuditDeliveryMode())
		cancel()
	}
	if err != nil {
		c.Abort()
		auditAttemptState := "unrecorded"
		auditResultState := "unrecorded"
		if errors.Is(err, errAuditDeliveryPending) {
			auditAttemptState = "pending"
			if resultErr := s.persistFailedAuditAdmission(c, attempt); resultErr != nil {
				s.logger.Error("Failed to persist rejected public exposure audit result",
					zap.String("operation_id", operationID),
					zap.String("attempt_event_id", attempt.EventID),
					zap.Error(resultErr),
				)
			} else {
				auditResultState = "captured"
			}
		}
		s.logger.Error("Failed to persist public exposure audit attempt", zap.Error(err))
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox audit is unavailable; invocation was not executed", failedAuditAdmissionDetails(operationID, auditAttemptState, auditResultState))
		return noop, false
	}
	return func() {
		outcome := auditOutcomeForStatus(c.Writer.Status())
		if panicked, _ := c.Get(exposureAuditPanickedKey); panicked == true {
			outcome = sandboxobservability.OutcomeUnknown
		}
		result, resultErr := newEvent(sandboxobservability.EventPhaseResult, outcome, attempt.EventID)
		if resultErr == nil {
			resultCtx, cancel := context.WithTimeout(context.WithoutCancel(c.Request.Context()), auditCanonicalDeliveryTimeout)
			resultErr = s.deliverAuditEvent(resultCtx, result, s.configuredAuditDeliveryMode())
			cancel()
		}
		if resultErr != nil {
			fields := []zap.Field{zap.String("operation_id", operationID), zap.Error(resultErr)}
			if errors.Is(resultErr, errAuditDeliveryPending) {
				s.logger.Warn("Canonical public exposure audit result is pending durable replay", fields...)
			} else {
				s.logger.Error("Public exposure audit result is unrecorded", fields...)
			}
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
