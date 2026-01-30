package file

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWatcherManagerEmit(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "procd-watch-*")
	if err != nil {
		t.Fatalf("mkdir temp: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	wm, err := NewWatcherManager()
	if err != nil {
		t.Fatalf("new watcher manager: %v", err)
	}
	defer wm.Close()

	watcher, err := wm.WatchDir(tmpDir, true)
	if err != nil {
		t.Fatalf("watch dir: %v", err)
	}
	defer wm.UnwatchDir(watcher.ID)

	targetPath := filepath.Join(tmpDir, "file.txt")
	wm.Emit(WatchEvent{
		Type: EventWrite,
		Path: targetPath,
	})

	select {
	case evt := <-watcher.EventChan:
		if evt.WatchID != watcher.ID {
			t.Fatalf("expected watch id %s, got %s", watcher.ID, evt.WatchID)
		}
		if evt.Path != targetPath {
			t.Fatalf("expected path %s, got %s", targetPath, evt.Path)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected event to be delivered")
	}
}
