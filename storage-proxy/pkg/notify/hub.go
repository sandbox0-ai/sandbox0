package notify

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"github.com/sirupsen/logrus"
)

const (
	defaultQueueSize = 256
)

// Broadcaster publishes events to subscribers.
type Broadcaster interface {
	Publish(ctx context.Context, event *pb.WatchEvent)
}

// LocalBroadcaster publishes only to the local hub.
type LocalBroadcaster struct {
	hub *Hub
}

func NewLocalBroadcaster(hub *Hub) *LocalBroadcaster {
	return &LocalBroadcaster{hub: hub}
}

func (b *LocalBroadcaster) Publish(_ context.Context, event *pb.WatchEvent) {
	if b == nil || b.hub == nil || event == nil {
		return
	}
	b.hub.Publish(event)
}

// Hub manages subscriptions for watch events.
type Hub struct {
	mu        sync.RWMutex
	subs      map[string]*subscription
	byVolume  map[string]map[string]*subscription
	queueSize int
	logger    *logrus.Logger
}

type subscription struct {
	id  string
	req *pb.WatchRequest
	ch  chan *pb.WatchEvent
}

// NewHub creates a new event hub.
func NewHub(logger *logrus.Logger, queueSize int) *Hub {
	if queueSize <= 0 {
		queueSize = defaultQueueSize
	}
	return &Hub{
		subs:      make(map[string]*subscription),
		byVolume:  make(map[string]map[string]*subscription),
		queueSize: queueSize,
		logger:    logger,
	}
}

// Subscribe registers a watch request and returns a channel plus a cancel function.
func (h *Hub) Subscribe(req *pb.WatchRequest) (string, <-chan *pb.WatchEvent, func()) {
	if req == nil {
		ch := make(chan *pb.WatchEvent)
		close(ch)
		return "", ch, func() {}
	}

	cleaned := &pb.WatchRequest{
		VolumeId:    req.VolumeId,
		PathPrefix:  strings.TrimRight(req.PathPrefix, "/"),
		Recursive:   req.Recursive,
		IncludeSelf: req.IncludeSelf,
		SandboxId:   req.SandboxId,
	}

	sub := &subscription{
		id:  uuid.New().String(),
		req: cleaned,
		ch:  make(chan *pb.WatchEvent, h.queueSize),
	}

	h.mu.Lock()
	h.subs[sub.id] = sub
	if sub.req.VolumeId != "" {
		if h.byVolume[sub.req.VolumeId] == nil {
			h.byVolume[sub.req.VolumeId] = make(map[string]*subscription)
		}
		h.byVolume[sub.req.VolumeId][sub.id] = sub
	}
	h.mu.Unlock()

	cancel := func() {
		h.Unsubscribe(sub.id)
	}

	return sub.id, sub.ch, cancel
}

// Unsubscribe removes a subscription.
func (h *Hub) Unsubscribe(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	sub, ok := h.subs[id]
	if !ok {
		return
	}
	delete(h.subs, id)
	if sub.req != nil && sub.req.VolumeId != "" {
		if subs := h.byVolume[sub.req.VolumeId]; subs != nil {
			delete(subs, id)
			if len(subs) == 0 {
				delete(h.byVolume, sub.req.VolumeId)
			}
		}
	}
	close(sub.ch)
}

// Publish broadcasts an event to matching subscribers.
func (h *Hub) Publish(event *pb.WatchEvent) {
	if event == nil {
		return
	}
	if event.TimestampUnix == 0 {
		event.TimestampUnix = time.Now().Unix()
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	var targets map[string]*subscription
	if event.VolumeId != "" {
		targets = h.byVolume[event.VolumeId]
	} else {
		targets = h.subs
	}

	for _, sub := range targets {
		if !matches(sub.req, event) {
			continue
		}
		select {
		case sub.ch <- event:
		default:
			if h.logger != nil {
				h.logger.WithFields(logrus.Fields{
					"volume_id": event.VolumeId,
					"watch_id":  sub.id,
				}).Debug("Dropping watch event due to full queue")
			}
		}
	}
}

func matches(req *pb.WatchRequest, event *pb.WatchEvent) bool {
	if req == nil || event == nil {
		return false
	}
	if req.VolumeId != "" && req.VolumeId != event.VolumeId {
		return false
	}
	if !req.IncludeSelf && req.SandboxId != "" && req.SandboxId == event.OriginSandboxId {
		return false
	}
	if req.PathPrefix == "" {
		return true
	}
	if event.Path == "" && event.OldPath == "" {
		return true
	}
	if pathMatches(req, event.Path) {
		return true
	}
	if event.OldPath != "" && pathMatches(req, event.OldPath) {
		return true
	}
	return false
}

func pathMatches(req *pb.WatchRequest, path string) bool {
	if path == "" {
		return false
	}
	if path == req.PathPrefix {
		return true
	}
	if req.Recursive && strings.HasPrefix(path, req.PathPrefix+"/") {
		return true
	}
	return false
}
