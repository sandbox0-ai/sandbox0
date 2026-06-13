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
	ID                string    `json:"id"`
	TemplateID        string    `json:"template_id"`
	Status            string    `json:"status"`
	Paused            bool      `json:"paused"`
	RuntimeGeneration int64     `json:"runtime_generation"`
	CreatedAt         time.Time `json:"created_at"`
	ExpiresAt         time.Time `json:"expires_at"`
	HardExpiresAt     time.Time `json:"hard_expires_at"`
	UpdatedAt         time.Time `json:"updated_at"`
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

	if s.sandboxStore != nil {
		return s.listSandboxesFromStore(ctx, req)
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

		status := s.podToSandboxStatus(pod)

		// Filter by status if specified
		if req.Status != "" && status != req.Status {
			continue
		}

		// Filter by template_id if specified
		templateID := sandboxTemplateIDFromLabels(pod.Labels)
		if req.TemplateID != "" && templateID != req.TemplateID {
			continue
		}

		// Filter by paused state if specified.
		paused := status == SandboxStatusPaused
		if req.Paused != nil && paused != *req.Paused {
			continue
		}

		// Parse timestamps (both can be zero when disabled or not set).
		expiresAt := parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationExpiresAt)
		hardExpiresAt := parseRFC3339AnnotationTime(pod.Annotations, controller.AnnotationHardExpiresAt)

		summaries = append(summaries, &SandboxSummary{
			ID:                sandboxIDFromPod(pod),
			TemplateID:        templateID,
			Status:            status,
			Paused:            paused,
			RuntimeGeneration: runtimeGenerationFromPod(pod),
			CreatedAt:         pod.CreationTimestamp.Time,
			ExpiresAt:         expiresAt,
			HardExpiresAt:     hardExpiresAt,
			UpdatedAt:         pod.CreationTimestamp.Time,
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

func (s *SandboxService) listSandboxesFromStore(ctx context.Context, req *ListSandboxesRequest) (*ListSandboxesResponse, error) {
	records, err := s.sandboxStore.ListSandboxes(ctx, req)
	if err != nil {
		return nil, err
	}
	summaries := make([]*SandboxSummary, 0, len(records))
	for _, record := range records {
		sandbox := s.recordToSandbox(record)
		if record.CurrentPodName != "" && !recordLifecycleStatusOverridesPod(record.Status) {
			if pod, err := s.getSandboxPod(ctx, record.ID); err == nil {
				sandbox = s.podToSandbox(ctx, pod, record.ID)
			}
		}
		if req.Paused != nil && sandbox.Paused != *req.Paused {
			continue
		}
		summaries = append(summaries, &SandboxSummary{
			ID:                sandbox.ID,
			TemplateID:        sandbox.TemplateID,
			Status:            sandbox.Status,
			Paused:            sandbox.Paused,
			RuntimeGeneration: sandbox.RuntimeGeneration,
			CreatedAt:         sandbox.CreatedAt,
			ExpiresAt:         sandbox.ExpiresAt,
			HardExpiresAt:     sandbox.HardExpiresAt,
			UpdatedAt:         sandbox.UpdatedAt,
		})
	}

	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].CreatedAt.After(summaries[j].CreatedAt)
	})
	totalCount := len(summaries)
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
	return &ListSandboxesResponse{Sandboxes: summaries, Count: totalCount, HasMore: hasMore}, nil
}
