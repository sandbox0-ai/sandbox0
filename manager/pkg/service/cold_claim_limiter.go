package service

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
)

type coldClaimLimiter struct {
	mu                 sync.Mutex
	maxPerTemplate     int
	acquireTimeout     time.Duration
	slotsByTemplateKey map[string]chan struct{}
}

func newColdClaimLimiter(maxPerTemplate int, acquireTimeout time.Duration) *coldClaimLimiter {
	if maxPerTemplate <= 0 {
		return nil
	}
	return &coldClaimLimiter{
		maxPerTemplate:     maxPerTemplate,
		acquireTimeout:     acquireTimeout,
		slotsByTemplateKey: make(map[string]chan struct{}),
	}
}

func (l *coldClaimLimiter) acquire(ctx context.Context, templateKey string) (func(), error) {
	if l == nil {
		return func() {}, nil
	}
	templateKey = strings.TrimSpace(templateKey)
	if templateKey == "" {
		templateKey = "unknown"
	}
	slots := l.slotsForTemplate(templateKey)
	if l.acquireTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, l.acquireTimeout)
		defer cancel()
	}

	select {
	case slots <- struct{}{}:
		var once sync.Once
		return func() {
			once.Do(func() {
				<-slots
			})
		}, nil
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, ErrColdClaimCapacityUnavailable
		}
		return nil, ctx.Err()
	}
}

func (s *SandboxService) acquireColdClaimSlot(ctx context.Context, template *v1alpha1.SandboxTemplate) (func(), error) {
	if s == nil || s.coldClaimLimiter == nil {
		return func() {}, nil
	}
	templateKey := "unknown"
	if template != nil {
		templateKey = template.Namespace + "/" + template.Name
	}
	return s.coldClaimLimiter.acquire(ctx, templateKey)
}

func (l *coldClaimLimiter) slotsForTemplate(templateKey string) chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	slots := l.slotsByTemplateKey[templateKey]
	if slots == nil {
		slots = make(chan struct{}, l.maxPerTemplate)
		l.slotsByTemplateKey[templateKey] = slots
	}
	return slots
}
