package volume

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
)

func TestHTTPCtldVolumeClientAttachAndDetach(t *testing.T) {
	var attachReq ctldapi.VolumeAttachRequest
	var detachReq ctldapi.VolumeDetachRequest
	var authHeader string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("x-internal-token")
		switch r.URL.Path {
		case "/api/v1/sandboxes/sandbox-1/volumes/attach":
			if r.Method != http.MethodPost {
				t.Fatalf("attach method = %s, want POST", r.Method)
			}
			if err := json.NewDecoder(r.Body).Decode(&attachReq); err != nil {
				t.Fatalf("decode attach request: %v", err)
			}
			w.Header().Set("content-type", "application/json")
			_, _ = w.Write([]byte(`{"attached":true,"attachment_id":"attach-1","mount_session_id":"session-1"}`))
		case "/api/v1/sandboxes/sandbox-1/volumes/detach":
			if r.Method != http.MethodPost {
				t.Fatalf("detach method = %s, want POST", r.Method)
			}
			if err := json.NewDecoder(r.Body).Decode(&detachReq); err != nil {
				t.Fatalf("decode detach request: %v", err)
			}
			w.Header().Set("content-type", "application/json")
			_, _ = w.Write([]byte(`{"detached":true}`))
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	client, err := NewHTTPCtldVolumeClient(server.URL, time.Second, staticTokenProvider{})
	if err != nil {
		t.Fatalf("NewHTTPCtldVolumeClient() error = %v", err)
	}

	resp, err := client.Attach(t.Context(), &ctldapi.VolumeAttachRequest{
		SandboxID:       "sandbox-1",
		SandboxVolumeID: "vol-1",
		MountPoint:      "/workspace/data",
	})
	if err != nil {
		t.Fatalf("Attach() error = %v", err)
	}
	if resp.AttachmentID != "attach-1" || resp.MountSessionID != "session-1" {
		t.Fatalf("Attach() response = %+v", resp)
	}
	if authHeader != "token" {
		t.Fatalf("x-internal-token = %q, want token", authHeader)
	}
	if attachReq.SandboxID != "sandbox-1" || attachReq.SandboxVolumeID != "vol-1" || attachReq.MountPoint != "/workspace/data" {
		t.Fatalf("attach request = %+v", attachReq)
	}

	if err := client.Detach(t.Context(), &ctldapi.VolumeDetachRequest{
		SandboxID:       "sandbox-1",
		SandboxVolumeID: "vol-1",
		MountPoint:      "/workspace/data",
		AttachmentID:    "attach-1",
		MountSessionID:  "session-1",
	}); err != nil {
		t.Fatalf("Detach() error = %v", err)
	}
	if detachReq.AttachmentID != "attach-1" || detachReq.MountSessionID != "session-1" {
		t.Fatalf("detach request = %+v", detachReq)
	}
}

func TestHTTPCtldVolumeClientRequiresAttachmentID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"attached":true,"mount_session_id":"session-1"}`))
	}))
	t.Cleanup(server.Close)

	client, err := NewHTTPCtldVolumeClient(server.URL, time.Second, nil)
	if err != nil {
		t.Fatalf("NewHTTPCtldVolumeClient() error = %v", err)
	}
	if _, err := client.Attach(t.Context(), &ctldapi.VolumeAttachRequest{SandboxID: "sandbox-1", SandboxVolumeID: "vol-1", MountPoint: "/mnt"}); err == nil {
		t.Fatalf("Attach() expected missing attachment_id error")
	}
}
