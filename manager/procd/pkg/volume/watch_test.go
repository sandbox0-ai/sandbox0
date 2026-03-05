package volume

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/file"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
)

func TestMapWatchEventType(t *testing.T) {
	cases := []struct {
		eventType pb.WatchEventType
		expected  file.EventType
	}{
		{pb.WatchEventType_WATCH_EVENT_TYPE_CREATE, file.EventCreate},
		{pb.WatchEventType_WATCH_EVENT_TYPE_WRITE, file.EventWrite},
		{pb.WatchEventType_WATCH_EVENT_TYPE_REMOVE, file.EventRemove},
		{pb.WatchEventType_WATCH_EVENT_TYPE_RENAME, file.EventRename},
		{pb.WatchEventType_WATCH_EVENT_TYPE_CHMOD, file.EventChmod},
		{pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE, file.EventInvalidate},
		{pb.WatchEventType_WATCH_EVENT_TYPE_UNSPECIFIED, file.EventInvalidate},
	}

	for _, tc := range cases {
		if got := mapWatchEventType(tc.eventType); got != tc.expected {
			t.Fatalf("expected %s, got %s", tc.expected, got)
		}
	}
}

func TestFileWatchEvent(t *testing.T) {
	event := fileWatchEvent(file.EventWrite, "/mnt/a.txt", "/mnt/old.txt")
	if event.Type != file.EventWrite {
		t.Fatalf("unexpected event type")
	}
	if event.Path != "/mnt/a.txt" {
		t.Fatalf("unexpected event path")
	}
	if event.OldPath != "/mnt/old.txt" {
		t.Fatalf("unexpected event old path")
	}
}
