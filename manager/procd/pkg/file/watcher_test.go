package file

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWatcherManagerRecursiveAddsNewDir(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "procd-watch-recursive-*")
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

	subDir := filepath.Join(tmpDir, "nested")
	if err := os.Mkdir(subDir, 0o755); err != nil {
		t.Fatalf("mkdir nested: %v", err)
	}

	filePath := filepath.Join(subDir, "file.txt")
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case evt := <-watcher.EventChan:
			if evt.Path == filePath {
				return
			}
		case <-ticker.C:
			if err := os.WriteFile(filePath, []byte(time.Now().String()), 0o644); err != nil {
				t.Fatalf("write file: %v", err)
			}
		case <-deadline:
			t.Fatalf("expected event for file in new dir")
		}
	}
}
