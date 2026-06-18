package handlers

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	filepkg "github.com/sandbox0-ai/sandbox0/manager/procd/pkg/file"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"go.uber.org/zap"
)

func TestListFilesReturnsEmptyArray(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "test-file-handler-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	manager, err := filepkg.NewManager(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	handler := NewFileHandler(manager, zap.NewNop())
	req := httptest.NewRequest(http.MethodGet, "/files/list?path=.", nil)
	rec := httptest.NewRecorder()

	handler.List(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	resp, apiErr, err := spec.DecodeResponse[struct {
		Entries []*filepkg.FileInfo `json:"entries"`
	}](rec.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected api error: %+v", apiErr)
	}
	if resp.Entries == nil {
		t.Fatal("entries slice is nil, want empty array")
	}
	if len(resp.Entries) != 0 {
		t.Fatalf("entries = %d, want 0", len(resp.Entries))
	}
}

func TestFileHandlerMapsPathTypeMismatchesToConflict(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "test-file-handler-type-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	manager, err := filepkg.NewManager(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	if err := manager.MakeDir("dir", 0755, false); err != nil {
		t.Fatalf("MakeDir() failed = %v", err)
	}
	if err := manager.WriteFile("file.txt", []byte("hello"), 0644); err != nil {
		t.Fatalf("WriteFile() failed = %v", err)
	}

	handler := NewFileHandler(manager, zap.NewNop())
	cases := []struct {
		name     string
		req      *http.Request
		call     func(http.ResponseWriter, *http.Request)
		wantCode string
	}{
		{
			name:     "read directory",
			req:      httptest.NewRequest(http.MethodGet, "/files?path=dir", nil),
			call:     handler.Handle,
			wantCode: "path_not_file",
		},
		{
			name:     "list file",
			req:      httptest.NewRequest(http.MethodGet, "/files/list?path=file.txt", nil),
			call:     handler.List,
			wantCode: "path_not_directory",
		},
		{
			name:     "write directory",
			req:      httptest.NewRequest(http.MethodPost, "/files?path=dir", bytes.NewReader([]byte("replace"))),
			call:     handler.Handle,
			wantCode: "path_not_file",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()

			tc.call(rec, tc.req)

			if rec.Code != http.StatusConflict {
				t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusConflict, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), tc.wantCode) {
				t.Fatalf("body = %s, want error code %s", rec.Body.String(), tc.wantCode)
			}
		})
	}
}
