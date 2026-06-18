package handlers

import (
	"net/http"
	"net/http/httptest"
	"os"
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
