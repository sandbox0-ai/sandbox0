package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	corev1 "k8s.io/api/core/v1"
)

const (
	// DefaultSandboxLogContainer is the sandbox main container created by the manager.
	DefaultSandboxLogContainer        = "procd"
	DefaultSandboxLogTailLines  int64 = 200
	MaxSandboxLogTailLines      int64 = 5000
	DefaultSandboxLogLimitBytes int64 = 1 << 20
	MaxSandboxLogLimitBytes     int64 = 8 << 20
)

var (
	ErrSandboxTeamMismatch         = errors.New("sandbox belongs to a different team")
	ErrSandboxLogContainerNotFound = errors.New("sandbox log container not found")
)

// SandboxLogsOptions controls a bounded snapshot read from Kubernetes pod logs.
type SandboxLogsOptions struct {
	Container    string `json:"container"`
	TailLines    int64  `json:"tail_lines"`
	LimitBytes   int64  `json:"limit_bytes"`
	Previous     bool   `json:"previous"`
	Timestamps   bool   `json:"timestamps"`
	SinceSeconds *int64 `json:"since_seconds,omitempty"`
}

// SandboxLogsResponse carries a bounded log snapshot plus Kubernetes metadata.
type SandboxLogsResponse struct {
	SandboxID string `json:"sandbox_id"`
	PodName   string `json:"pod_name"`
	Container string `json:"container"`
	Previous  bool   `json:"previous"`
	Logs      string `json:"logs"`
}

// SandboxLogsStream is an open Kubernetes pod log stream. Callers must close Body.
type SandboxLogsStream struct {
	SandboxID string
	PodName   string
	Container string
	Previous  bool
	Body      io.ReadCloser
}

// GetSandboxLogs returns a bounded snapshot of logs from one container in the sandbox pod.
func (s *SandboxService) GetSandboxLogs(ctx context.Context, sandboxID, teamID string, opts *SandboxLogsOptions) (*SandboxLogsResponse, error) {
	stream, err := s.openSandboxLogs(ctx, sandboxID, teamID, opts, false, true)
	if err != nil {
		return nil, err
	}
	data, readErr := io.ReadAll(stream.Body)
	closeErr := stream.Body.Close()
	if readErr != nil {
		return nil, fmt.Errorf("read sandbox pod logs: %w", readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close sandbox pod logs: %w", closeErr)
	}

	return &SandboxLogsResponse{
		SandboxID: stream.SandboxID,
		PodName:   stream.PodName,
		Container: stream.Container,
		Previous:  stream.Previous,
		Logs:      string(data),
	}, nil
}

// StreamSandboxLogs returns a followable stream of logs from one container in the sandbox pod.
func (s *SandboxService) StreamSandboxLogs(ctx context.Context, sandboxID, teamID string, opts *SandboxLogsOptions) (*SandboxLogsStream, error) {
	return s.openSandboxLogs(ctx, sandboxID, teamID, opts, true, false)
}

func (s *SandboxService) openSandboxLogs(ctx context.Context, sandboxID, teamID string, opts *SandboxLogsOptions, follow bool, defaultLimit bool) (*SandboxLogsStream, error) {
	if s.k8sClient == nil {
		return nil, errors.New("kubernetes client is not configured")
	}

	pod, err := s.getSandboxPod(ctx, sandboxID)
	if err != nil {
		return nil, err
	}
	if teamID != "" && pod.Annotations[controller.AnnotationTeamID] != teamID {
		return nil, ErrSandboxTeamMismatch
	}

	options := normalizeSandboxLogsOptions(opts, defaultLimit)
	if !podHasContainer(pod, options.Container) {
		return nil, fmt.Errorf("%w: %s", ErrSandboxLogContainerNotFound, options.Container)
	}

	logOptions := &corev1.PodLogOptions{
		Container:  options.Container,
		Follow:     follow,
		Previous:   options.Previous,
		Timestamps: options.Timestamps,
		TailLines:  &options.TailLines,
	}
	if options.LimitBytes > 0 {
		logOptions.LimitBytes = &options.LimitBytes
	}
	if options.SinceSeconds != nil {
		logOptions.SinceSeconds = options.SinceSeconds
	}

	stream, err := s.k8sClient.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, logOptions).Stream(ctx)
	if err != nil {
		return nil, fmt.Errorf("stream sandbox pod logs: %w", err)
	}

	return &SandboxLogsStream{
		SandboxID: sandboxID,
		PodName:   pod.Name,
		Container: options.Container,
		Previous:  options.Previous,
		Body:      stream,
	}, nil
}

func normalizeSandboxLogsOptions(opts *SandboxLogsOptions, defaultLimit bool) SandboxLogsOptions {
	options := SandboxLogsOptions{}
	if opts != nil {
		options = *opts
	}
	options.Container = strings.TrimSpace(options.Container)
	if options.Container == "" {
		options.Container = DefaultSandboxLogContainer
	}
	if options.TailLines == 0 {
		options.TailLines = DefaultSandboxLogTailLines
	} else if options.TailLines > MaxSandboxLogTailLines {
		options.TailLines = MaxSandboxLogTailLines
	} else if options.TailLines < 0 {
		options.TailLines = DefaultSandboxLogTailLines
	}
	if options.LimitBytes == 0 && defaultLimit {
		options.LimitBytes = DefaultSandboxLogLimitBytes
	} else if options.LimitBytes > MaxSandboxLogLimitBytes {
		options.LimitBytes = MaxSandboxLogLimitBytes
	} else if options.LimitBytes < 0 {
		options.LimitBytes = 0
	}
	if options.SinceSeconds != nil && *options.SinceSeconds < 1 {
		options.SinceSeconds = nil
	}
	return options
}

func podHasContainer(pod *corev1.Pod, name string) bool {
	for _, container := range pod.Spec.Containers {
		if container.Name == name {
			return true
		}
	}
	for _, container := range pod.Spec.InitContainers {
		if container.Name == name {
			return true
		}
	}
	for _, container := range pod.Spec.EphemeralContainers {
		if container.Name == name {
			return true
		}
	}
	return false
}
