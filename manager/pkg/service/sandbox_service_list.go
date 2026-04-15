package service

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/labels"
)

// ListSandboxesRequest represents a request to list sandboxes
type ListSandboxesRequest struct {
	TeamID     string
	Status     string
	TemplateID string
	Paused     *bool
	Limit      int
	Offset     int
}

// ListSandboxesResponse represents the response from listing sandboxes
type ListSandboxesResponse struct {
	Sandboxes []*SandboxSummary `json:"sandboxes"`
	Count     int               `json:"count"`
	HasMore   bool              `json:"has_more"`
}

// SandboxSummary represents a summary of a sandbox for listing
type SandboxSummary struct {
	ID            string            `json:"id"`
	TemplateID    string            `json:"template_id"`
	Status        string            `json:"status"`
	Paused        bool              `json:"paused"`
	PowerState    SandboxPowerState `json:"power_state"`
	CreatedAt     time.Time         `json:"created_at"`
	ExpiresAt     time.Time         `json:"expires_at"`
	HardExpiresAt time.Time         `json:"hard_expires_at"`
}

// ListSandboxes lists all sandboxes for a team with optional filters
func (s *SandboxService) ListSandboxes(ctx context.Context, req *ListSandboxesRequest) (*ListSandboxesResponse, error) {
	s.logger.Info("Listing sandboxes",
		zap.String("teamID", req.TeamID),
		zap.String("status", req.Status),
		zap.String("templateID", req.TemplateID),
	)

	// Set defaults
	if req.Limit <= 0 {
		req.Limit = 50
	}
	if req.Limit > 200 {
		req.Limit = 200
	}

	// List all active pods (exclude idle pool)
	pods, err := s.podLister.List(labels.SelectorFromSet(map[string]string{
		controller.LabelPoolType: controller.PoolTypeActive,
	}))
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}

	// Filter and convert pods to sandbox summaries
	var summaries []*SandboxSummary
	for _, pod := range pods {
		// Filter by team_id from annotations
		teamID := pod.Annotations[controller.AnnotationTeamID]
		if teamID != req.TeamID {
			continue
		}

		// Get status from pod phase
		status := s.podPhaseToSandboxStatus(pod.Status.Phase)

		// Filter by status if specified
		if req.Status != "" && status != req.Status {
			continue
		}

		// Filter by template_id if specified
		templateID := pod.Labels[controller.LabelTemplateID]
		if req.TemplateID != "" && templateID != req.TemplateID {
			continue
		}

		// Filter by paused state if specified
		powerState := sandboxPowerStateFromAnnotations(pod.Annotations)
		paused := powerState.Observed == SandboxPowerStatePaused
		if req.Paused != nil && paused != *req.Paused {
			continue
		}

		// Parse timestamps (both can be zero when disabled or not set).
		expiresAt := parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationExpiresAt)
		hardExpiresAt := parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationHardExpiresAt)

		summaries = append(summaries, &SandboxSummary{
			ID:            pod.Name,
			TemplateID:    templateID,
			Status:        status,
			Paused:        paused,
			PowerState:    powerState,
			CreatedAt:     pod.CreationTimestamp.Time,
			ExpiresAt:     expiresAt,
			HardExpiresAt: hardExpiresAt,
		})
	}

	// Sort by creation timestamp (descending - newest first)
	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].CreatedAt.After(summaries[j].CreatedAt)
	})

	// Get total count before pagination
	totalCount := len(summaries)

	// Apply pagination
	hasMore := false
	if req.Offset >= totalCount {
		summaries = []*SandboxSummary{}
	} else {
		end := req.Offset + req.Limit
		if end >= totalCount {
			end = totalCount
		} else {
			hasMore = true
		}
		summaries = summaries[req.Offset:end]
	}

	s.logger.Info("Listed sandboxes",
		zap.String("teamID", req.TeamID),
		zap.Int("count", totalCount),
		zap.Int("returned", len(summaries)),
		zap.Bool("hasMore", hasMore),
	)

	return &ListSandboxesResponse{
		Sandboxes: summaries,
		Count:     totalCount,
		HasMore:   hasMore,
	}, nil
}
