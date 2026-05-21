package http

import (
	"context"
	"errors"
	"net"
	nethttp "net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/functions"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

func (s *Server) startFunctionSpan(ctx context.Context, name string, fn *functions.Function, rev *functions.Revision, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	tracer := otel.Tracer("function-gateway")
	if s != nil && s.obsProvider != nil {
		tracer = s.obsProvider.Tracer()
	}
	base := functionTraceAttributes(fn, rev, "", nil)
	base = append(base, attrs...)
	return tracer.Start(ctx, name, trace.WithAttributes(base...))
}

func functionTraceAttributes(fn *functions.Function, rev *functions.Revision, routeID string, inst *functions.RuntimeInstance) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 8)
	if fn != nil {
		attrs = append(attrs,
			attribute.String("sandbox0.team_id", fn.TeamID),
			attribute.String("sandbox0.function_id", fn.ID),
		)
	}
	if rev != nil {
		attrs = append(attrs,
			attribute.String("sandbox0.function_revision_id", rev.ID),
			attribute.Int("sandbox0.function_revision_number", rev.RevisionNumber),
		)
	}
	if routeID != "" {
		attrs = append(attrs, attribute.String("sandbox0.function_route_id", routeID))
	}
	if inst != nil {
		attrs = append(attrs,
			attribute.String("sandbox0.function_runtime_instance_id", inst.ID),
			attribute.String("sandbox0.runtime_sandbox_id", inst.SandboxID),
		)
		if inst.ContextID != nil {
			attrs = append(attrs, attribute.String("sandbox0.runtime_context_id", *inst.ContextID))
		}
	}
	return attrs
}

func setFunctionSpanAttributes(ctx context.Context, fn *functions.Function, rev *functions.Revision, routeID string, inst *functions.RuntimeInstance) {
	span := trace.SpanFromContext(ctx)
	if !span.SpanContext().IsValid() {
		return
	}
	span.SetAttributes(functionTraceAttributes(fn, rev, routeID, inst)...)
}

func (s *Server) observeFunctionRequest(c *gin.Context, fn *functions.Function, rev *functions.Revision, routeID string, inst *functions.RuntimeInstance, started time.Time) {
	if c == nil || fn == nil || rev == nil || started.IsZero() {
		return
	}
	status := c.Writer.Status()
	if status == 0 {
		status = nethttp.StatusOK
	}
	duration := time.Since(started)
	responseSize := c.Writer.Size()
	if s != nil && s.functionMetrics != nil {
		s.functionMetrics.ObserveFunctionRequest(fn.TeamID, fn.ID, rev.ID, routeID, c.Request.Method, status, duration, responseSize)
	}
	if s != nil && s.logger != nil {
		fields := []zap.Field{
			zap.String("team_id", fn.TeamID),
			zap.String("function_id", fn.ID),
			zap.String("revision_id", rev.ID),
			zap.Int("revision_number", rev.RevisionNumber),
			zap.String("route_id", routeID),
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
			zap.Int("status", status),
			zap.Duration("latency", duration),
			zap.Int("response_size", responseSize),
		}
		if inst != nil {
			fields = append(fields,
				zap.String("runtime_instance_id", inst.ID),
				zap.String("runtime_sandbox_id", inst.SandboxID),
			)
			if inst.ContextID != nil {
				fields = append(fields, zap.String("runtime_context_id", *inst.ContextID))
			}
		}
		spanCtx := trace.SpanFromContext(c.Request.Context()).SpanContext()
		if spanCtx.IsValid() {
			fields = append(fields,
				zap.String("trace_id", spanCtx.TraceID().String()),
				zap.String("span_id", spanCtx.SpanID().String()),
			)
		}
		switch {
		case status >= 500:
			s.logger.Error("Function request", fields...)
		case status >= 400:
			s.logger.Warn("Function request", fields...)
		default:
			s.logger.Info("Function request", fields...)
		}
	}
}

func (s *Server) observeFunctionIngressFailure(fn *functions.Function, rev *functions.Revision, routeID, reason string, status int) {
	if s == nil || s.functionMetrics == nil || fn == nil || rev == nil {
		return
	}
	s.functionMetrics.ObserveFunctionIngressFailure(fn.TeamID, fn.ID, rev.ID, routeID, reason, status)
}

func (s *Server) observeRuntimeAcquire(fn *functions.Function, rev *functions.Revision, path, result string, err error) {
	if s == nil || s.functionMetrics == nil || fn == nil || rev == nil {
		return
	}
	s.functionMetrics.ObserveRuntimeAcquire(fn.TeamID, fn.ID, rev.ID, path, result, functionRuntimeErrorReason(err))
}

func (s *Server) observeRuntimeStartup(fn *functions.Function, rev *functions.Revision, result string, readiness functions.RuntimeReadinessState, duration time.Duration, err error) {
	if s == nil || s.functionMetrics == nil || fn == nil || rev == nil {
		return
	}
	s.functionMetrics.ObserveRuntimeStartup(fn.TeamID, fn.ID, rev.ID, result, string(readiness), duration)
	if err != nil {
		s.functionMetrics.ObserveRuntimeStartupFailure(fn.TeamID, fn.ID, rev.ID, functionRuntimeErrorReason(err))
	}
}

func (s *Server) observeRuntimeScaleDown(inst *functions.RuntimeInstance, result string, err error, duration time.Duration) {
	if s == nil || s.functionMetrics == nil || inst == nil {
		return
	}
	s.functionMetrics.ObserveRuntimeScaleDown(inst.TeamID, inst.FunctionID, inst.RevisionID, result, functionRuntimeErrorReason(err), duration)
}

func (s *Server) recordRuntimeLifecycleEvent(ctx context.Context, fn *functions.Function, rev *functions.Revision, inst *functions.RuntimeInstance, phase functions.RuntimePhase, readiness functions.RuntimeReadinessState, reason string, err error, duration time.Duration) {
	if s == nil || fn == nil || rev == nil || phase == "" {
		return
	}
	message := ""
	if err != nil {
		message = err.Error()
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = functionRuntimeErrorReason(err)
	}
	durationMS := durationMSPtr(duration)
	if s.functionMetrics != nil {
		s.functionMetrics.ObserveRuntimeLifecycleEvent(fn.TeamID, fn.ID, rev.ID, string(phase), reason, duration)
	}

	var runtimeInstanceID *string
	var runtimeSandboxID *string
	var runtimeContextID *string
	if inst != nil {
		if strings.TrimSpace(inst.ID) != "" {
			runtimeInstanceID = &inst.ID
		}
		if strings.TrimSpace(inst.SandboxID) != "" {
			runtimeSandboxID = &inst.SandboxID
		}
		runtimeContextID = inst.ContextID
	}
	if s.functionRepo != nil {
		_, appendErr := s.functionRepo.AppendRuntimeEvent(ctx, &functions.RuntimeEvent{
			TeamID:            fn.TeamID,
			FunctionID:        fn.ID,
			RevisionID:        rev.ID,
			RuntimeInstanceID: runtimeInstanceID,
			RuntimeSandboxID:  runtimeSandboxID,
			RuntimeContextID:  runtimeContextID,
			Phase:             phase,
			ReadinessState:    readiness,
			Reason:            reason,
			Message:           message,
			StartupDurationMS: durationMS,
		})
		if appendErr != nil && s.logger != nil {
			s.logger.Warn("Failed to append function runtime lifecycle event",
				zap.String("function_id", fn.ID),
				zap.String("revision_id", rev.ID),
				zap.String("phase", string(phase)),
				zap.Error(appendErr),
			)
		}
	}
	if s.logger != nil {
		fields := []zap.Field{
			zap.String("team_id", fn.TeamID),
			zap.String("function_id", fn.ID),
			zap.String("revision_id", rev.ID),
			zap.String("phase", string(phase)),
			zap.String("readiness_state", string(readiness)),
			zap.String("reason", reason),
		}
		if duration > 0 {
			fields = append(fields, zap.Duration("duration", duration))
		}
		if inst != nil {
			fields = append(fields,
				zap.String("runtime_instance_id", inst.ID),
				zap.String("runtime_sandbox_id", inst.SandboxID),
			)
			if inst.ContextID != nil {
				fields = append(fields, zap.String("runtime_context_id", *inst.ContextID))
			}
		}
		spanCtx := trace.SpanFromContext(ctx).SpanContext()
		if spanCtx.IsValid() {
			fields = append(fields,
				zap.String("trace_id", spanCtx.TraceID().String()),
				zap.String("span_id", spanCtx.SpanID().String()),
			)
		}
		if err != nil {
			fields = append(fields, zap.Error(err))
			s.logger.Warn("Function runtime lifecycle event", fields...)
		} else {
			s.logger.Info("Function runtime lifecycle event", fields...)
		}
	}
}

func functionRuntimeErrorReason(err error) string {
	if err == nil {
		return "none"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	var runtimeErr functionRuntimeHTTPError
	if errors.As(err, &runtimeErr) {
		switch {
		case runtimeErr.status == nethttp.StatusTooManyRequests:
			return "upstream_rate_limited"
		case runtimeErr.status >= 500:
			return "upstream_5xx"
		case runtimeErr.status >= 400:
			return "upstream_4xx"
		default:
			return "upstream_status"
		}
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "health check"):
		return "health_check_failed"
	case strings.Contains(msg, "did not start listening"):
		return "tcp_readiness_failed"
	case strings.Contains(msg, "capacity"):
		return "capacity_unavailable"
	case strings.Contains(msg, "not found"):
		return "not_found"
	default:
		return "error"
	}
}

func durationMSPtr(duration time.Duration) *int {
	if duration <= 0 {
		return nil
	}
	ms := int(duration.Round(time.Millisecond) / time.Millisecond)
	if ms < 0 {
		return nil
	}
	return &ms
}

func finishSpan(span trace.Span, err error) {
	if span == nil {
		return
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}
