package http

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/infra/storage-proxy/pkg/db"
)

type createSandboxVolumeRequest struct {
	TeamID     string `json:"team_id"`
	UserID     string `json:"user_id"`
	CacheSize  string `json:"cache_size"`
	Prefetch   int    `json:"prefetch"`
	BufferSize string `json:"buffer_size"`
	Writeback  bool   `json:"writeback"`
	ReadOnly   bool   `json:"read_only"`
}

func (s *Server) createSandboxVolume(w http.ResponseWriter, r *http.Request) {
	var req createSandboxVolumeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.TeamID == "" || req.UserID == "" {
		http.Error(w, "team_id and user_id are required", http.StatusBadRequest)
		return
	}

	// Set defaults if not provided
	if req.CacheSize == "" {
		req.CacheSize = "1G"
	}
	if req.BufferSize == "" {
		req.BufferSize = "32M"
	}

	vol := &db.SandboxVolume{
		ID:         uuid.New().String(),
		TeamID:     req.TeamID,
		UserID:     req.UserID,
		CacheSize:  req.CacheSize,
		Prefetch:   req.Prefetch,
		BufferSize: req.BufferSize,
		Writeback:  req.Writeback,
		ReadOnly:   req.ReadOnly,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	if err := s.repo.CreateSandboxVolume(r.Context(), vol); err != nil {
		s.logger.WithError(err).Error("Failed to create sandbox volume")
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(vol)
}

func (s *Server) listSandboxVolumes(w http.ResponseWriter, r *http.Request) {
	teamID := r.URL.Query().Get("team_id")
	if teamID == "" {
		http.Error(w, "team_id is required", http.StatusBadRequest)
		return
	}

	volumes, err := s.repo.ListSandboxVolumesByTeam(r.Context(), teamID)
	if err != nil {
		s.logger.WithError(err).Error("Failed to list sandbox volumes")
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(volumes)
}

func (s *Server) getSandboxVolume(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}

	vol, err := s.repo.GetSandboxVolume(r.Context(), id)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		s.logger.WithError(err).Error("Failed to get sandbox volume")
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(vol)
}
