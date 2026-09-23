package http

import (
	"context"
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

type CheckpointController interface {
	PrepareCheckpoint(runtimecontrol.CheckpointCaptureAssignment) error
	RestoreCheckpoint(runtimecontrol.CheckpointRestoreAssignment) error
	CancelPreparedCheckpoint(runtimecontrol.CheckpointCaptureAssignment) error
}

func WithCheckpointController(controller CheckpointController) ServerOption {
	return func(s *Server) { s.checkpointController = controller }
}

// runtimeCheckpointHandler shares lifecycle exclusion with migration. Its
// target-independent prepare state is part of the execution image; restore
// needs a separately signed, exact target command before reopening admission.
func (s *Server) runtimeCheckpointHandler(w http.ResponseWriter, r *http.Request) {
	var request procdapi.RuntimeCheckpointRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "invalid checkpoint request")
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "checkpoint request has trailing content")
		return
	}
	digest, err := request.Digest()
	if err != nil {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	team, sandbox := request.ActingSandbox()
	permission, _ := request.Permission()
	claims := internalauth.ClaimsFromContext(r.Context())
	if claims == nil || claims.Caller != internalauth.ServiceManager || claims.Target != internalauth.ServiceProcd ||
		claims.IsSystem || claims.TeamID != team || claims.SandboxID != sandbox ||
		!slices.Contains(claims.Permissions, permission) {
		_ = spec.WriteError(w, http.StatusForbidden, spec.CodeForbidden, "exact manager checkpoint authorization is required")
		return
	}
	if request.InstanceID != s.instanceID {
		_ = spec.WriteError(w, http.StatusConflict, spec.CodeConflict, "checkpoint procd instance changed")
		return
	}
	if s.checkpointController == nil || s.barrier == nil {
		_ = spec.WriteError(w, http.StatusServiceUnavailable, spec.CodeUnavailable, "runtime checkpoint is not configured")
		return
	}
	s.migrationMu.Lock()
	defer s.migrationMu.Unlock()
	if s.migrationPrepared {
		_ = spec.WriteError(w, http.StatusConflict, spec.CodeConflict, "migration owns the source process")
		return
	}
	captureDigest, _ := request.Capture.Digest()
	response := procdapi.RuntimeCheckpointResponse{InstanceID: s.instanceID, RequestDigest: digest,
		RuntimeGeneration: request.Capture.RuntimeGeneration, State: "ready"}
	if request.Action == procdapi.MigrationRestore {
		// A child's lifecycle counter is independent of its parent's. Compare
		// the saved capture epoch here, not the child's epoch with source state.
		if s.checkpointEpoch != request.CaptureEpoch || s.checkpointCaptureDigest != captureDigest ||
			s.checkpointCanceled || !s.checkpointDrained ||
			(s.checkpointRestoreDigest != "" && s.checkpointRestoreDigest != digest) {
			_ = spec.WriteError(w, http.StatusConflict, spec.CodeConflict, "restore does not match the prepared process")
			return
		}
		s.checkpointRestoreDigest = digest
		err = s.checkpointController.RestoreCheckpoint(*request.Restore)
		if err == nil {
			if s.checkpointPrepared {
				_, err = s.barrier.setActive(r, lifecycleBarrierRequest{Active: false})
			}
			if err == nil {
				s.checkpointPrepared = false
				s.migrationEpoch = request.LifecycleEpoch
				s.migrationAssignmentDigest = "checkpoint-restored:" + digest
				response.RuntimeGeneration = request.Restore.Target.RuntimeGeneration
			}
		}
	} else {
		controlDigest := "checkpoint:" + captureDigest
		if request.LifecycleEpoch < s.migrationEpoch ||
			(request.LifecycleEpoch == s.migrationEpoch && s.migrationAssignmentDigest != "" &&
				s.migrationAssignmentDigest != controlDigest) ||
			(s.checkpointPrepared && (s.checkpointEpoch != request.CaptureEpoch || s.checkpointCaptureDigest != captureDigest)) {
			_ = spec.WriteError(w, http.StatusConflict, spec.CodeConflict, "checkpoint lifecycle authority is stale")
			return
		}
		switch request.Action {
		case procdapi.MigrationPrepare:
			if s.checkpointEpoch == request.CaptureEpoch && s.checkpointCaptureDigest == captureDigest && s.checkpointCanceled {
				err = errors.New("checkpoint preparation was canceled")
				break
			}
			if controller, ok := s.checkpointController.(interface {
				PrepareCheckpointContext(context.Context, runtimecontrol.CheckpointCaptureAssignment) error
			}); ok {
				err = controller.PrepareCheckpointContext(r.Context(), request.Capture)
			} else {
				err = s.checkpointController.PrepareCheckpoint(request.Capture)
			}
			if err != nil {
				break
			}
			s.checkpointPrepared, s.checkpointCanceled, s.checkpointDrained = true, false, false
			s.checkpointEpoch, s.checkpointCaptureDigest = request.CaptureEpoch, captureDigest
			s.checkpointRestoreDigest = ""
			s.migrationEpoch, s.migrationAssignmentDigest = request.LifecycleEpoch, controlDigest
			_, err = s.barrier.setActive(r, lifecycleBarrierRequest{Active: true,
				Epoch: request.CaptureEpoch, RuntimeGeneration: request.Capture.RuntimeGeneration})
			s.checkpointDrained = err == nil
			response.State = "prepared"
		case procdapi.MigrationCancel:
			if err = s.checkpointController.CancelPreparedCheckpoint(request.Capture); err != nil {
				break
			}
			if s.checkpointPrepared {
				_, err = s.barrier.setActive(r, lifecycleBarrierRequest{Active: false})
			}
			if err == nil {
				s.checkpointPrepared, s.checkpointCanceled, s.checkpointDrained = false, true, false
				s.checkpointEpoch, s.checkpointCaptureDigest = request.CaptureEpoch, captureDigest
				s.migrationEpoch, s.migrationAssignmentDigest = request.LifecycleEpoch, controlDigest
			}
		}
	}
	if err != nil {
		_ = spec.WriteError(w, http.StatusConflict, spec.CodeConflict, err.Error())
		return
	}
	_ = spec.WriteSuccess(w, http.StatusOK, response)
}
