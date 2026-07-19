package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/storagequota"
	volumepkg "github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
)

const storageOperationUnavailableRetryAfterSeconds = 1

func (s *Server) admitStorageOperation(w http.ResponseWriter, r *http.Request, teamID string) bool {
	if err := s.admitStorageOperationForTeam(r.Context(), teamID); err != nil {
		s.writeStorageOperationQuotaError(w, err)
		return false
	}
	return true
}

func (s *Server) admitStorageOperationForTeam(ctx context.Context, teamID string) error {
	if s == nil || s.storageOperations == nil {
		return &teamquota.UnavailableError{
			Operation: "admit storage operation",
			Err:       errors.New("storage operation quota is not configured"),
		}
	}
	return s.storageOperations.Admit(ctx, teamID)
}

func (s *Server) writeStorageOperationQuotaError(w http.ResponseWriter, err error) {
	var exceeded *teamquota.RateExceededError
	if errors.As(err, &exceeded) {
		retryAfter := exceeded.RetryAfter
		if retryAfter <= 0 {
			retryAfter = time.Second
		}
		seconds := int64((retryAfter + time.Second - 1) / time.Second)
		w.Header().Set("Retry-After", strconv.FormatInt(seconds, 10))
		_ = spec.WriteError(w, http.StatusTooManyRequests, "quota_exceeded", err.Error())
		return
	}
	w.Header().Set("Retry-After", strconv.Itoa(storageOperationUnavailableRetryAfterSeconds))
	_ = spec.WriteError(w, http.StatusServiceUnavailable, spec.CodeUnavailable, err.Error())
}

func (s *Server) writeStorageQuotaMutationError(w http.ResponseWriter, err error) bool {
	if teamquota.IsRateExceeded(err) {
		s.writeStorageOperationQuotaError(w, err)
		return true
	}
	if teamquota.IsExceeded(err) {
		_ = spec.WriteError(w, http.StatusTooManyRequests, "quota_exceeded", err.Error())
		return true
	}
	if teamquota.IsUnavailable(err) {
		w.Header().Set("Retry-After", strconv.Itoa(storageOperationUnavailableRetryAfterSeconds))
		_ = spec.WriteError(w, http.StatusServiceUnavailable, spec.CodeUnavailable, err.Error())
		return true
	}
	return false
}

func (s *Server) createVolumeWithQuota(ctx context.Context, volume *db.SandboxVolume, create func() error) error {
	if volume == nil {
		return &teamquota.UnavailableError{
			Operation: "create volume quota allocation",
			Err:       fmt.Errorf("volume is required"),
		}
	}
	if s == nil || s.storageQuota == nil {
		return &teamquota.UnavailableError{
			Operation: "create volume quota allocation",
			Err:       fmt.Errorf("storage quota service is not configured"),
		}
	}
	objectCount := storagequota.CatalogObjectCount
	if volumepkg.NormalizeBackend(volume.Backend) == volumepkg.BackendS0FS {
		objectCount += s0fs.InitialStateObjectCount
	}
	target := storagequota.VolumeTarget(0, objectCount)
	zero := storagequota.VolumeTarget(0, 0)
	created := false
	return s.storageQuota.Mutate(
		ctx,
		s.storageQuota.VolumeOwner(volume.TeamID, volume.ID),
		"volume_create",
		func() (teamquota.Values, error) {
			return zero.Clone(), nil
		},
		func(teamquota.Values) (teamquota.Values, error) {
			return target.Clone(), nil
		},
		func() error {
			if err := create(); err != nil {
				return err
			}
			created = true
			return nil
		},
		func() (teamquota.Values, error) {
			if !created {
				return zero.Clone(), nil
			}
			return target.Clone(), nil
		},
	)
}
