// Package file provides file system operations for Procd.
package file

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestSanitizePath tests path resolution.
func TestSanitizePath(t *testing.T) {
	tests := []struct {
		name     string
		rootPath string
		input    string
		wantAbs  bool // whether result should be absolute
	}{
		{
			name:     "relative simple file",
			rootPath: "/tmp/test",
			input:    "file.txt",
			wantAbs:  true,
		},
		{
			name:     "relative nested path",
			rootPath: "/tmp/test",
			input:    "dir1/dir2/file.txt",
			wantAbs:  true,
		},
		{
			name:     "relative path with dot",
			rootPath: "/tmp/test",
			input:    "./file.txt",
			wantAbs:  true,
		},
		{
			name:     "relative path with double dot",
			rootPath: "/tmp/test",
			input:    "../etc/passwd",
			wantAbs:  true,
		},
		{
			name:     "relative path with multiple double dots",
			rootPath: "/tmp/test",
			input:    "../../../../../etc/passwd",
			wantAbs:  true,
		},
		{
			name:     "relative path that resolves to root",
			rootPath: "/tmp/test",
			input:    "foo/..",
			wantAbs:  true,
		},
		{
			name:     "absolute path",
			rootPath: "/tmp/test",
			input:    "/etc/passwd",
			wantAbs:  true,
		},
		{
			name:     "absolute path within root",
			rootPath: "/tmp/test",
			input:    "/tmp/test/dir/file.txt",
			wantAbs:  true,
		},
		{
			name:     "empty path",
			rootPath: "/tmp/test",
			input:    "",
			wantAbs:  true,
		},
		{
			name:     "path with trailing slash",
			rootPath: "/tmp/test",
			input:    "dir/",
			wantAbs:  true,
		},
		{
			name:     "path with multiple slashes",
			rootPath: "/tmp/test",
			input:    "dir///file.txt",
			wantAbs:  true,
		},
		{
			name:     "path with current dir in middle",
			rootPath: "/tmp/test",
			input:    "dir/./file.txt",
			wantAbs:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := NewManager(tt.rootPath)
			if err != nil {
				t.Fatalf("NewManager() failed = %v", err)
			}
			defer os.RemoveAll(tt.rootPath)

			result := m.sanitizePath(tt.input)

			// Verify result is absolute
			if !filepath.IsAbs(result) {
				t.Errorf("sanitizePath() result %s is not absolute", result)
			}
		})
	}
}

// TestWriteFileSizeLimit tests that file size limit is enforced.
func TestWriteFileSizeLimit(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "test-file-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	m, err := NewManager(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	// Create data exactly at the limit
	dataAtLimit := make([]byte, MaxFileSize)
	dataOverLimit := make([]byte, MaxFileSize+1)

	tests := []struct {
		name    string
		data    []byte
		wantErr error
	}{
		{
			name:    "file at size limit",
			data:    dataAtLimit,
			wantErr: nil,
		},
		{
			name:    "file over size limit",
			data:    dataOverLimit,
			wantErr: ErrFileTooLarge,
		},
		{
			name:    "small file",
			data:    []byte("hello"),
			wantErr: nil,
		},
		{
			name:    "empty file",
			data:    []byte{},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := m.WriteFile("test.txt", tt.data, 0644)
			if tt.wantErr != nil {
				if err == nil {
					t.Errorf("WriteFile() expected error %v, got nil", tt.wantErr)
				} else if err != tt.wantErr {
					t.Errorf("WriteFile() error = %v, want %v", err, tt.wantErr)
				}
			} else if err != nil {
				t.Errorf("WriteFile() unexpected error = %v", err)
			}
		})
	}
}

// TestWriteFileExecutablePermission tests that executable permission is controlled.
func TestWriteFileExecutablePermission(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "test-file-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	tests := []struct {
		name            string
		allowExecutable bool
		perm            os.FileMode
		wantErr         error
	}{
		{
			name:            "executable file when allowed",
			allowExecutable: true,
			perm:            0755,
			wantErr:         nil,
		},
		{
			name:            "executable file when not allowed",
			allowExecutable: false,
			perm:            0755,
			wantErr:         ErrPermissionDenied,
		},
		{
			name:            "non-executable file when not allowed",
			allowExecutable: false,
			perm:            0644,
			wantErr:         nil,
		},
		{
			name:            "partially executable file when not allowed",
			allowExecutable: false,
			perm:            0744,
			wantErr:         ErrPermissionDenied,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := NewManager(tempDir)
			if err != nil {
				t.Fatal(err)
			}
			m.allowExecutable = tt.allowExecutable
			defer m.Close()

			err = m.WriteFile("test.sh", []byte("#!/bin/bash\necho test"), tt.perm)
			if tt.wantErr != nil {
				if err == nil {
					t.Errorf("WriteFile() expected error %v, got nil", tt.wantErr)
				} else if err != tt.wantErr {
					t.Errorf("WriteFile() error = %v, want %v", err, tt.wantErr)
				}
			} else if err != nil {
				t.Errorf("WriteFile() unexpected error = %v", err)
			}
		})
	}
}

// TestWriteFileAtomic tests that writes are atomic (using temp file + rename).
func TestWriteFileAtomic(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "test-file-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	m, err := NewManager(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	filename := "test.txt"
	initialData := []byte("initial data")
	updatedData := []byte("updated data")

	// Write initial data
	err = m.WriteFile(filename, initialData, 0644)
	if err != nil {
		t.Fatalf("WriteFile() failed = %v", err)
	}

	// Read to verify
	data, err := m.ReadFile(filename)
	if err != nil {
		t.Fatalf("ReadFile() failed = %v", err)
	}
	if string(data) != string(initialData) {
		t.Fatalf("ReadFile() data = %s, want %s", string(data), string(initialData))
	}

	// Write updated data
	err = m.WriteFile(filename, updatedData, 0644)
	if err != nil {
		t.Fatalf("WriteFile() failed = %v", err)
	}

	// Read to verify
	data, err = m.ReadFile(filename)
	if err != nil {
		t.Fatalf("ReadFile() failed = %v", err)
	}
	if string(data) != string(updatedData) {
		t.Errorf("ReadFile() data = %s, want %s", string(data), string(updatedData))
	}

	// Verify .tmp file doesn't exist after successful write
	tmpPath := filepath.Join(tempDir, filename+".tmp")
	if _, err := os.Stat(tmpPath); err == nil {
		t.Errorf("Temporary file still exists: %s", tmpPath)
	} else if !os.IsNotExist(err) {
		t.Errorf("os.Stat() unexpected error = %v", err)
	}
}

// TestMakeDirValidPaths tests directory creation with various paths.
func TestMakeDirValidPaths(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "test-file-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	m, err := NewManager(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	tests := []struct {
		name      string
		path      string
		recursive bool
		wantErr   bool
	}{
		{
			name:      "valid directory",
			path:      "valid_dir",
			recursive: false,
			wantErr:   false,
		},
		{
			name:      "nested directory",
			path:      "parent/child",
			recursive: true,
			wantErr:   false,
		},
		{
			name:      "path with double dot stays within temp",
			path:      "subdir/../safe_dir",
			recursive: true,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := m.MakeDir(tt.path, 0755, tt.recursive)
			if (err != nil) != tt.wantErr {
				t.Errorf("MakeDir() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestStatSymlink tests that Stat properly handles symlinks.
func TestStatSymlink(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "test-file-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	m, err := NewManager(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	// Create a regular file
	err = m.WriteFile("target.txt", []byte("target content"), 0644)
	if err != nil {
		t.Fatalf("WriteFile() failed = %v", err)
	}

	// Create a symlink
	linkPath := filepath.Join(tempDir, "link.txt")
	err = os.Symlink("target.txt", linkPath)
	if err != nil {
		t.Fatalf("os.Symlink() failed = %v", err)
	}

	// Stat the symlink
	info, err := m.Stat("link.txt")
	if err != nil {
		t.Fatalf("Stat() failed = %v", err)
	}

	if info.Type != FileTypeSymlink {
		t.Errorf("Stat() Type = %s, want %s", info.Type, FileTypeSymlink)
	}
	if !info.IsLink {
		t.Errorf("Stat() IsLink = false, want true")
	}
	if info.LinkTarget != "target.txt" {
		t.Errorf("Stat() LinkTarget = %s, want target.txt", info.LinkTarget)
	}
}

// TestListDir tests directory listing.
func TestListDir(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "test-file-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	m, err := NewManager(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	// Create test structure
	os.MkdirAll(filepath.Join(tempDir, "subdir"), 0755)
	m.WriteFile("file1.txt", []byte("content1"), 0644)
	m.WriteFile("file2.txt", []byte("content2"), 0644)

	// List root directory
	entries, err := m.ListDir(".")
	if err != nil {
		t.Fatalf("ListDir() failed = %v", err)
	}

	if len(entries) != 3 {
		t.Errorf("ListDir() returned %d entries, want 3", len(entries))
	}

	// Check that we have the expected entries
	names := make(map[string]bool)
	for _, entry := range entries {
		names[entry.Name] = true
	}

	if !names["file1.txt"] || !names["file2.txt"] || !names["subdir"] {
		t.Errorf("ListDir() missing expected entries, got: %v", names)
	}
}

// TestGetRootPath tests GetRootPath returns the configured root.
func TestGetRootPath(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "test-file-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	m, err := NewManager(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	if m.GetRootPath() != tempDir {
		t.Errorf("GetRootPath() = %s, want %s", m.GetRootPath(), tempDir)
	}
}

// TestMoveFile tests file and directory move operations.
func TestMoveFile(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "test-file-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	m, err := NewManager(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	tests := []struct {
		name    string
		src     string
		dst     string
		setup   func() error
		verify  func() error
		wantErr bool
	}{
		{
			name: "move file to new name",
			src:  "file1.txt",
			dst:  "file2.txt",
			setup: func() error {
				return m.WriteFile("file1.txt", []byte("content"), 0644)
			},
			verify: func() error {
				data, err := m.ReadFile("file2.txt")
				if err != nil {
					return err
				}
				if string(data) != "content" {
					t.Errorf("content = %s, want content", string(data))
				}
				// Source should be gone
				_, err = m.ReadFile("file1.txt")
				if err == nil {
					t.Error("source file still exists")
				}
				return nil
			},
			wantErr: false,
		},
		{
			name: "move file to subdirectory",
			src:  "file.txt",
			dst:  "subdir/file.txt",
			setup: func() error {
				return m.WriteFile("file.txt", []byte("content"), 0644)
			},
			verify: func() error {
				data, err := m.ReadFile("subdir/file.txt")
				if err != nil {
					return err
				}
				if string(data) != "content" {
					t.Errorf("content = %s, want content", string(data))
				}
				return nil
			},
			wantErr: false,
		},
		{
			name:    "move non-existent file",
			src:     "nonexistent.txt",
			dst:     "dst.txt",
			setup:   func() error { return nil },
			verify:  func() error { return nil },
			wantErr: true,
		},
		{
			name: "move directory",
			src:  "dir1",
			dst:  "dir2",
			setup: func() error {
				return m.MakeDir("dir1", 0755, true)
			},
			verify: func() error {
				info, err := m.Stat("dir2")
				if err != nil {
					return err
				}
				if info.Type != FileTypeDir {
					t.Errorf("Type = %s, want dir", info.Type)
				}
				return nil
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clean up from previous test
			os.RemoveAll(tempDir)
			os.MkdirAll(tempDir, 0755)

			if err := tt.setup(); err != nil {
				t.Fatalf("setup() failed = %v", err)
			}

			err := m.Move(tt.src, tt.dst)
			if (err != nil) != tt.wantErr {
				t.Errorf("Move() error = %v, wantErr %v", err, tt.wantErr)
			}

			if err == nil && tt.verify != nil {
				if err := tt.verify(); err != nil {
					t.Errorf("verify() failed = %v", err)
				}
			}
		})
	}
}

// TestRemove tests file and directory removal.
func TestRemove(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "test-file-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	m, err := NewManager(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	tests := []struct {
		name    string
		path    string
		setup   func() error
		wantErr bool
	}{
		{
			name: "remove file",
			path: "file.txt",
			setup: func() error {
				return m.WriteFile("file.txt", []byte("content"), 0644)
			},
			wantErr: false,
		},
		{
			name: "remove directory",
			path: "dir",
			setup: func() error {
				m.WriteFile("dir/file1.txt", []byte("content1"), 0644)
				m.WriteFile("dir/file2.txt", []byte("content2"), 0644)
				return nil
			},
			wantErr: false,
		},
		{
			name: "remove nested directory",
			path: "parent/child",
			setup: func() error {
				return m.MakeDir("parent/child", 0755, true)
			},
			wantErr: false,
		},
		{
			name:    "remove non-existent path",
			path:    "nonexistent",
			setup:   func() error { return nil },
			wantErr: false, // RemoveAll doesn't error on non-existent
		},
		{
			name: "remove directory with special characters",
			path: "dir with spaces",
			setup: func() error {
				return m.WriteFile("dir with spaces/file.txt", []byte("content"), 0644)
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clean up from previous test
			os.RemoveAll(tempDir)
			os.MkdirAll(tempDir, 0755)

			if err := tt.setup(); err != nil {
				t.Fatalf("setup() failed = %v", err)
			}

			err := m.Remove(tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("Remove() error = %v, wantErr %v", err, tt.wantErr)
			}

			// Verify the path is gone
			_, err = m.Stat(tt.path)
			if err == nil {
				t.Error("Remove() path still exists")
			}
		})
	}
}

// TestReadFileNotFound tests reading a non-existent file.
func TestReadFileNotFound(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "test-file-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	m, err := NewManager(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	_, err = m.ReadFile("nonexistent.txt")
	if err != ErrFileNotFound {
		t.Errorf("ReadFile() error = %v, want %v", err, ErrFileNotFound)
	}
}

// TestStatNotFound tests stating a non-existent file.
func TestStatNotFound(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "test-file-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	m, err := NewManager(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	_, err = m.Stat("nonexistent.txt")
	if err != ErrFileNotFound {
		t.Errorf("Stat() error = %v, want %v", err, ErrFileNotFound)
	}
}

// TestListDirNotFound tests listing a non-existent directory.
func TestListDirNotFound(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "test-file-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	m, err := NewManager(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	_, err = m.ListDir("nonexistent")
	if err != ErrDirNotFound {
		t.Errorf("ListDir() error = %v, want %v", err, ErrDirNotFound)
	}
}

// TestConcurrentFileOperations tests concurrent file operations.
func TestConcurrentFileOperations(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "test-file-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	m, err := NewManager(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	const numGoroutines = 50
	const numOps = 20

	done := make(chan struct{})

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			for j := 0; j < numOps; j++ {
				select {
				case <-done:
					return
				default:
					filename := string(rune('a'+idx%26)) + ".txt"
					m.WriteFile(filename, []byte("data"), 0644)
				}
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			for j := 0; j < numOps; j++ {
				select {
				case <-done:
					return
				default:
					filename := string(rune('a'+idx%26)) + ".txt"
					m.ReadFile(filename)
					m.Stat(filename)
				}
			}
		}(i)
	}

	// Concurrent listings
	for i := 0; i < numGoroutines/2; i++ {
		go func() {
			for j := 0; j < numOps; j++ {
				select {
				case <-done:
					return
				default:
					m.ListDir(".")
				}
			}
		}()
	}

	// Let it run
	time.Sleep(100 * time.Millisecond)
	close(done)

	// Verify manager is still functional
	files, err := m.ListDir(".")
	if err != nil {
		t.Errorf("ListDir() after concurrent ops failed = %v", err)
	}
	_ = files // Just verify it doesn't panic
}

// TestWriteFileInSubdirectory tests writing to a nested path.
func TestWriteFileInSubdirectory(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "test-file-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	m, err := NewManager(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	// Write to nested path - should create parent directories
	err = m.WriteFile("parent/child/grandchild/file.txt", []byte("nested content"), 0644)
	if err != nil {
		t.Fatalf("WriteFile() failed = %v", err)
	}

	// Verify file exists
	data, err := m.ReadFile("parent/child/grandchild/file.txt")
	if err != nil {
		t.Fatalf("ReadFile() failed = %v", err)
	}
	if string(data) != "nested content" {
		t.Errorf("content = %s, want nested content", string(data))
	}

	// Verify intermediate directories exist
	for _, path := range []string{"parent", "parent/child", "parent/child/grandchild"} {
		info, err := m.Stat(path)
		if err != nil {
			t.Errorf("Stat(%s) failed = %v", path, err)
		}
		if info.Type != FileTypeDir {
			t.Errorf("Stat(%s) Type = %s, want dir", path, info.Type)
		}
	}
}

// TestStatFileModes tests that file modes are correctly reported.
func TestStatFileModes(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "test-file-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	m, err := NewManager(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	tests := []struct {
		name     string
		perm     os.FileMode
		wantMode string
	}{
		{
			name:     "0644 file",
			perm:     0644,
			wantMode: "0644",
		},
		{
			name:     "0755 file",
			perm:     0755,
			wantMode: "0755",
		},
		{
			name:     "0600 file",
			perm:     0600,
			wantMode: "0600",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filename := "test.txt"
			err := m.WriteFile(filename, []byte("content"), tt.perm)
			if err != nil {
				t.Fatalf("WriteFile() failed = %v", err)
			}

			info, err := m.Stat(filename)
			if err != nil {
				t.Fatalf("Stat() failed = %v", err)
			}

			if info.Mode != tt.wantMode {
				t.Errorf("Stat() Mode = %s, want %s", info.Mode, tt.wantMode)
			}
		})
	}
}

// TestFileInfoFields tests FileInfo field values.
func TestFileInfoFields(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "test-file-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	m, err := NewManager(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	// Test file
	err = m.WriteFile("test.txt", []byte("hello"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	info, err := m.Stat("test.txt")
	if err != nil {
		t.Fatal(err)
	}

	if info.Name != "test.txt" {
		t.Errorf("Name = %s, want test.txt", info.Name)
	}

	if info.Path != "test.txt" {
		t.Errorf("Path = %s, want test.txt", info.Path)
	}

	if info.Type != FileTypeFile {
		t.Errorf("Type = %s, want %s", info.Type, FileTypeFile)
	}

	if info.Size != 5 {
		t.Errorf("Size = %d, want 5", info.Size)
	}

	if info.ModTime.IsZero() {
		t.Error("ModTime is zero")
	}

	if info.IsLink {
		t.Error("IsLink = true, want false")
	}

	// Test directory
	err = m.MakeDir("testdir", 0755, false)
	if err != nil {
		t.Fatal(err)
	}

	dirInfo, err := m.Stat("testdir")
	if err != nil {
		t.Fatal(err)
	}

	if dirInfo.Type != FileTypeDir {
		t.Errorf("Type = %s, want %s", dirInfo.Type, FileTypeDir)
	}
}

// TestErrorDefinitions tests that error variables are properly defined.
func TestErrorDefinitions(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "ErrFileNotFound",
			err:  ErrFileNotFound,
			want: "file not found",
		},
		{
			name: "ErrDirNotFound",
			err:  ErrDirNotFound,
			want: "directory not found",
		},
		{
			name: "ErrFileTooLarge",
			err:  ErrFileTooLarge,
			want: "file too large",
		},
		{
			name: "ErrPermissionDenied",
			err:  ErrPermissionDenied,
			want: "permission denied",
		},
		{
			name: "ErrWatcherNotFound",
			err:  ErrWatcherNotFound,
			want: "watcher not found",
		},
		{
			name: "ErrWatcherClosed",
			err:  ErrWatcherClosed,
			want: "watcher manager closed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err == nil {
				t.Fatal("Error variable is nil")
			}
			if tt.err.Error() != tt.want {
				t.Errorf("Error() = %s, want %s", tt.err.Error(), tt.want)
			}
		})
	}
}

// TestFileTypeValues tests FileType constant values.
func TestFileTypeValues(t *testing.T) {
	tests := []struct {
		value    FileType
		expected string
	}{
		{FileTypeFile, "file"},
		{FileTypeDir, "dir"},
		{FileTypeSymlink, "symlink"},
	}

	for _, tt := range tests {
		if string(tt.value) != tt.expected {
			t.Errorf("FileType = %s, want %s", tt.value, tt.expected)
		}
	}
}
