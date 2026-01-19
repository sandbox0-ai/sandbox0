package file

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/google/uuid"
)

// EventType represents the type of file system event.
type EventType string

const (
	EventCreate EventType = "create"
	EventWrite  EventType = "write"
	EventRemove EventType = "remove"
	EventRename EventType = "rename"
	EventChmod  EventType = "chmod"
)

// WatchEvent represents a file system event.
type WatchEvent struct {
	WatchID string    `json:"watch_id"`
	Type    EventType `json:"type"`
	Path    string    `json:"path"`
	OldPath string    `json:"old_path,omitempty"`
}

// Watcher represents a directory watcher.
type Watcher struct {
	ID        string
	Path      string
	Recursive bool
	EventChan chan WatchEvent
	cancel    context.CancelFunc
}

// WatcherManager manages multiple watchers.
type WatcherManager struct {
	mu       sync.RWMutex
	watchers map[string]*Watcher
	fsNotify *fsnotify.Watcher
	closed   bool
}

// NewWatcherManager creates a new watcher manager.
func NewWatcherManager() (*WatcherManager, error) {
	fw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	wm := &WatcherManager{
		watchers: make(map[string]*Watcher),
		fsNotify: fw,
	}

	go wm.eventLoop()

	return wm, nil
}

func (wm *WatcherManager) eventLoop() {
	for {
		select {
		case event, ok := <-wm.fsNotify.Events:
			if !ok {
				return
			}
			wm.handleFsEvent(event)

		case err, ok := <-wm.fsNotify.Errors:
			if !ok {
				return
			}
			// Log error but continue
			_ = err
		}
	}
}

func (wm *WatcherManager) handleFsEvent(event fsnotify.Event) {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	// Convert event type
	var eventType EventType
	switch {
	case event.Op&fsnotify.Create == fsnotify.Create:
		eventType = EventCreate
	case event.Op&fsnotify.Write == fsnotify.Write:
		eventType = EventWrite
	case event.Op&fsnotify.Remove == fsnotify.Remove:
		eventType = EventRemove
	case event.Op&fsnotify.Rename == fsnotify.Rename:
		eventType = EventRename
	case event.Op&fsnotify.Chmod == fsnotify.Chmod:
		eventType = EventChmod
	default:
		return
	}

	// Broadcast to matching watchers
	for _, watcher := range wm.watchers {
		if wm.matchWatcher(watcher, event.Name) {
			watchEvent := WatchEvent{
				WatchID: watcher.ID,
				Type:    eventType,
				Path:    event.Name,
			}

			select {
			case watcher.EventChan <- watchEvent:
			default:
				// Channel full, drop event
			}
		}
	}
}

func (wm *WatcherManager) matchWatcher(watcher *Watcher, eventPath string) bool {
	if eventPath == watcher.Path {
		return true
	}
	if watcher.Recursive && strings.HasPrefix(eventPath, watcher.Path+string(filepath.Separator)) {
		return true
	}
	return false
}

// WatchDir creates a watcher for a directory.
func (wm *WatcherManager) WatchDir(path string, recursive bool) (*Watcher, error) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if wm.closed {
		return nil, ErrWatcherClosed
	}

	eventChan := make(chan WatchEvent, 100)
	ctx, cancel := context.WithCancel(context.Background())

	watcher := &Watcher{
		ID:        "watch-" + uuid.New().String()[:8],
		Path:      path,
		Recursive: recursive,
		EventChan: eventChan,
		cancel:    cancel,
	}

	// Add to fsnotify
	if err := wm.fsNotify.Add(path); err != nil {
		cancel()
		return nil, err
	}

	// If recursive, add all subdirectories
	if recursive {
		filepath.Walk(path, func(walkPath string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			if info.IsDir() && walkPath != path {
				wm.fsNotify.Add(walkPath)
			}
			return nil
		})
	}

	wm.watchers[watcher.ID] = watcher

	// Start cleanup goroutine
	go func() {
		<-ctx.Done()
		wm.UnwatchDir(watcher.ID)
	}()

	return watcher, nil
}

// UnwatchDir removes a watcher.
func (wm *WatcherManager) UnwatchDir(watchID string) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	watcher, ok := wm.watchers[watchID]
	if !ok {
		return ErrWatcherNotFound
	}

	// Remove from fsnotify
	wm.fsNotify.Remove(watcher.Path)

	// Close event channel
	close(watcher.EventChan)

	delete(wm.watchers, watchID)

	return nil
}

// Close closes the watcher manager.
func (wm *WatcherManager) Close() error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if wm.closed {
		return nil
	}

	wm.closed = true

	// Close all watchers
	for id, watcher := range wm.watchers {
		watcher.cancel()
		close(watcher.EventChan)
		delete(wm.watchers, id)
	}

	return wm.fsNotify.Close()
}
