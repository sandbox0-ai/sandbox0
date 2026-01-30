package notify

import (
	"testing"
	"time"

	pb "github.com/sandbox0-ai/infra/storage-proxy/proto/fs"
	"github.com/sirupsen/logrus"
)

func TestHubPublishFilters(t *testing.T) {
	hub := NewHub(logrus.New(), 4)

	req := &pb.WatchRequest{
		VolumeId:    "vol-1",
		PathPrefix:  "/dir",
		Recursive:   true,
		IncludeSelf: false,
		SandboxId:   "sandbox-1",
	}

	_, ch, cancel := hub.Subscribe(req)
	defer cancel()

	hub.Publish(&pb.WatchEvent{
		VolumeId:        "vol-1",
		EventType:       pb.WatchEventType_WATCH_EVENT_TYPE_CREATE,
		Path:            "/dir/file.txt",
		OriginSandboxId: "sandbox-2",
		TimestampUnix:   time.Now().Unix(),
		OriginInstance:  "instance-a",
	})

	select {
	case <-ch:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected event to be delivered")
	}

	hub.Publish(&pb.WatchEvent{
		VolumeId:        "vol-1",
		EventType:       pb.WatchEventType_WATCH_EVENT_TYPE_WRITE,
		Path:            "/dir/skip.txt",
		OriginSandboxId: "sandbox-1",
	})

	select {
	case <-ch:
		t.Fatal("expected self event to be filtered")
	case <-time.After(200 * time.Millisecond):
	}

	hub.Publish(&pb.WatchEvent{
		VolumeId:  "vol-2",
		EventType: pb.WatchEventType_WATCH_EVENT_TYPE_WRITE,
		Path:      "/dir/other.txt",
	})

	select {
	case <-ch:
		t.Fatal("expected other volume event to be filtered")
	case <-time.After(200 * time.Millisecond):
	}

	hub.Publish(&pb.WatchEvent{
		VolumeId:  "vol-1",
		EventType: pb.WatchEventType_WATCH_EVENT_TYPE_RENAME,
		Path:      "/other/new.txt",
		OldPath:   "/dir/old.txt",
	})

	select {
	case <-ch:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected rename old_path event to be delivered")
	}
}
