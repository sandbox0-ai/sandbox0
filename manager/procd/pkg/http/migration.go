package http

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
)

// MigrationController retains the restored process's in-memory activation and
// session state. It must never instantiate a replacement procd or supervisor.
type MigrationController interface {
	PrepareMigration(runtimecontrol.MigrationAssignment) error
	RestoreMigration(runtimecontrol.MigrationAssignment) error
	CancelPreparedMigration(runtimecontrol.MigrationAssignment) error
}

type ServerOption func(*Server)

func WithMigrationController(controller MigrationController) ServerOption {
	return func(s *Server) { s.migrationController = controller }
}

func (s *Server) runtimeMigrationHandler(w http.ResponseWriter, r *http.Request) {
	var request procdapi.RuntimeMigrationRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "invalid migration request")
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "migration request has trailing content")
		return
	}
	digest, err := request.Digest()
	if err != nil {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	permission, _ := request.Permission()
	claims := internalauth.ClaimsFromContext(r.Context())
	if claims == nil || claims.Caller != internalauth.ServiceManager || claims.Target != internalauth.ServiceProcd ||
		claims.IsSystem || claims.TeamID != request.Assignment.Target.TeamID ||
		claims.SandboxID != request.Assignment.Target.SandboxID || !slices.Contains(claims.Permissions, permission) {
		_ = spec.WriteError(w, http.StatusForbidden, spec.CodeForbidden, "exact manager migration authorization is required")
		return
	}
	if request.InstanceID != s.instanceID {
		_ = spec.WriteError(w, http.StatusConflict, spec.CodeConflict, "migration procd instance changed")
		return
	}
	if s.migrationController == nil || s.barrier == nil {
		_ = spec.WriteError(w, http.StatusServiceUnavailable, spec.CodeUnavailable, "runtime migration is not configured")
		return
	}
	s.migrationMu.Lock()
	defer s.migrationMu.Unlock()
	assignmentDigest, _ := request.Assignment.Digest()
	if request.LifecycleEpoch < s.migrationEpoch ||
		(request.LifecycleEpoch == s.migrationEpoch && s.migrationAssignmentDigest != "" && s.migrationAssignmentDigest != assignmentDigest) {
		_ = spec.WriteError(w, http.StatusConflict, spec.CodeConflict, "migration lifecycle authority is stale")
		return
	}
	if s.migrationPrepared && (s.migrationEpoch != request.LifecycleEpoch || s.migrationAssignmentDigest != assignmentDigest) {
		_ = spec.WriteError(w, http.StatusConflict, spec.CodeConflict, "another runtime migration owns the source")
		return
	}
	response := procdapi.RuntimeMigrationResponse{InstanceID: s.instanceID, RequestDigest: digest,
		RuntimeGeneration: request.Assignment.SourceGeneration, State: "ready"}
	switch request.Action {
	case procdapi.MigrationPrepare:
		if s.migrationCanceled && request.LifecycleEpoch == s.migrationEpoch {
			err = errors.New("migration preparation was canceled")
			break
		}
		if err = s.migrationController.PrepareMigration(request.Assignment); err != nil {
			break
		}
		s.migrationPrepared = true
		s.migrationCanceled = false
		s.migrationDrained = false
		s.migrationEpoch = request.LifecycleEpoch
		s.migrationAssignmentDigest = assignmentDigest
		// Close admission first, then drain API mutations that entered before
		// the gate. Timeout keeps controller readiness closed; only the same
		// operation may retry or cancel. This is not a guest execution freeze.
		_, err = s.barrier.setActive(r, lifecycleBarrierRequest{Active: true,
			Epoch: request.LifecycleEpoch, RuntimeGeneration: request.Assignment.SourceGeneration})
		s.migrationDrained = err == nil
		response.State = "prepared"
	case procdapi.MigrationRestore, procdapi.MigrationCancel:
		if request.Action == procdapi.MigrationRestore && (s.migrationEpoch != request.LifecycleEpoch || s.migrationAssignmentDigest != assignmentDigest) {
			err = errors.New("migration does not match the prepared process")
			break
		}
		if request.Action == procdapi.MigrationRestore {
			if !s.migrationDrained {
				err = errors.New("migration source API drain did not complete")
				break
			}
			err = s.migrationController.RestoreMigration(request.Assignment)
			response.RuntimeGeneration = request.Assignment.Target.RuntimeGeneration
		} else {
			err = s.migrationController.CancelPreparedMigration(request.Assignment)
			if err == nil {
				s.migrationDrained = false
				s.migrationCanceled = true
				s.migrationEpoch = request.LifecycleEpoch
				s.migrationAssignmentDigest = assignmentDigest
			}
		}
		if err == nil && s.migrationPrepared {
			// A cancellation that precedes preparation, or a replay after
			// handover, owns no API barrier. It must not clear a later pause.
			_, err = s.barrier.setActive(r, lifecycleBarrierRequest{Active: false})
			if err == nil {
				s.migrationPrepared = false
			}
		}
	}
	if err != nil {
		_ = spec.WriteError(w, http.StatusConflict, spec.CodeConflict, err.Error())
		return
	}
	_ = spec.WriteSuccess(w, http.StatusOK, response)
}

// Legacy pause/resume/barrier controls must not mutate the captured workload or
// reopen API admission while an exact migration owns the process. Serialize
// them with prepare so an already-authenticated request cannot race the gate.
func (s *Server) migrationLifecycleMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !procdLifecycleControlRequest(r) {
			next.ServeHTTP(w, r)
			return
		}
		s.migrationMu.Lock()
		defer s.migrationMu.Unlock()
		if s.migrationPrepared {
			_ = spec.WriteError(w, http.StatusConflict, spec.CodeConflict, "runtime migration owns lifecycle control")
			return
		}
		next.ServeHTTP(w, r)
	})
}
