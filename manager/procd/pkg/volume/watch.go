package volume

import (
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/file"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
)

func mapWatchEventType(eventType pb.WatchEventType) file.EventType {
	switch eventType {
	case pb.WatchEventType_WATCH_EVENT_TYPE_CREATE:
		return file.EventCreate
	case pb.WatchEventType_WATCH_EVENT_TYPE_WRITE:
		return file.EventWrite
	case pb.WatchEventType_WATCH_EVENT_TYPE_REMOVE:
		return file.EventRemove
	case pb.WatchEventType_WATCH_EVENT_TYPE_RENAME:
		return file.EventRename
	case pb.WatchEventType_WATCH_EVENT_TYPE_CHMOD:
		return file.EventChmod
	case pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE:
		return file.EventInvalidate
	default:
		return file.EventInvalidate
	}
}

func fileWatchEvent(eventType file.EventType, path, oldPath string) file.WatchEvent {
	return file.WatchEvent{
		Type:    eventType,
		Path:    path,
		OldPath: oldPath,
	}
}
