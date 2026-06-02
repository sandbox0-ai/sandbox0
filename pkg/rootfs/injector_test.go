package rootfs

import (
	"context"
	"errors"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
)

func TestPrepareAndRewriteOverlayMountsNoopsWithoutRootFS(t *testing.T) {
	mounts := []Mount{{
		Type:    "overlay",
		Source:  "overlay",
		Options: []string{"lowerdir=/lower", "upperdir=/old-upper", "workdir=/old-work"},
	}}
	result, err := PrepareAndRewriteOverlayMounts(context.Background(), nil, "", Metadata{}, mounts)
	if err != nil {
		t.Fatalf("PrepareAndRewriteOverlayMounts() error = %v", err)
	}
	if result.Rewritten {
		t.Fatal("Rewritten = true, want false")
	}
	if result.Mounts[0].Options[1] != "upperdir=/old-upper" {
		t.Fatalf("mounts = %#v, want unchanged copy", result.Mounts)
	}
	mounts[0].Options[1] = "upperdir=/mutated"
	if result.Mounts[0].Options[1] != "upperdir=/old-upper" {
		t.Fatalf("result changed after source mutation: %#v", result.Mounts)
	}
}

func TestPrepareAndRewriteOverlayMountsPreparesAndRewritesOverlay(t *testing.T) {
	client := &recordingPrepareClient{
		response: &ctldapi.PrepareRootFSResponse{
			Prepared:       true,
			SandboxID:      "sandbox-a",
			RootFSVolumeID: "rootfs-a",
			UpperDir:       "/s0fs/upper",
			WorkDir:        "/s0fs/work",
		},
	}
	result, err := PrepareAndRewriteOverlayMounts(context.Background(), client, "", Metadata{
		SandboxID: "sandbox-a",
		TeamID:    "team-a",
		Mode:      ModeS0FSUpperdir,
		VolumeID:  "rootfs-a",
		CtldPort:  8095,
	}, []Mount{{
		Type:    "overlay",
		Source:  "overlay",
		Options: []string{"lowerdir=/lower", "upperdir=/old-upper", "workdir=/old-work"},
	}})
	if err != nil {
		t.Fatalf("PrepareAndRewriteOverlayMounts() error = %v", err)
	}
	if !result.Rewritten || result.RootFS == nil {
		t.Fatalf("result = %+v, want rewritten rootfs", result)
	}
	if client.address != "http://127.0.0.1:8095" {
		t.Fatalf("ctld address = %q, want local ctld address", client.address)
	}
	if client.request.SandboxID != "sandbox-a" || client.request.TeamID != "team-a" || client.request.RootFSVolumeID != "rootfs-a" {
		t.Fatalf("prepare request = %+v, want sandbox-a team-a rootfs-a", client.request)
	}
	if got := result.Mounts[0].Options; got[1] != "upperdir=/s0fs/upper" || got[2] != "workdir=/s0fs/work" {
		t.Fatalf("mount options = %#v, want s0fs upper/work", got)
	}
}

func TestPrepareAndRewriteOverlayMountsUsesExplicitCtldAddress(t *testing.T) {
	client := &recordingPrepareClient{
		response: &ctldapi.PrepareRootFSResponse{
			Prepared: true,
			UpperDir: "/s0fs/upper",
			WorkDir:  "/s0fs/work",
		},
	}
	_, err := PrepareAndRewriteOverlayMounts(context.Background(), client, "http://ctld-node:8095", Metadata{
		SandboxID: "sandbox-a",
		TeamID:    "team-a",
		Mode:      ModeS0FSUpperdir,
		VolumeID:  "rootfs-a",
	}, []Mount{{Type: "overlay"}})
	if err != nil {
		t.Fatalf("PrepareAndRewriteOverlayMounts() error = %v", err)
	}
	if client.address != "http://ctld-node:8095" {
		t.Fatalf("ctld address = %q, want explicit address", client.address)
	}
}

func TestPrepareAndRewriteOverlayMountsRequiresOverlay(t *testing.T) {
	client := &recordingPrepareClient{
		response: &ctldapi.PrepareRootFSResponse{Prepared: true, UpperDir: "/s0fs/upper", WorkDir: "/s0fs/work"},
	}
	_, err := PrepareAndRewriteOverlayMounts(context.Background(), client, "http://ctld", Metadata{
		SandboxID: "sandbox-a",
		TeamID:    "team-a",
		Mode:      ModeS0FSUpperdir,
		VolumeID:  "rootfs-a",
	}, []Mount{{Type: "bind"}})
	if err == nil {
		t.Fatal("PrepareAndRewriteOverlayMounts() error = nil, want overlay error")
	}
}

func TestPrepareAndRewriteOverlayMountsRejectsInvalidPrepareResponse(t *testing.T) {
	client := &recordingPrepareClient{
		response: &ctldapi.PrepareRootFSResponse{Prepared: true, UpperDir: "/s0fs/upper"},
	}
	_, err := PrepareAndRewriteOverlayMounts(context.Background(), client, "http://ctld", Metadata{
		SandboxID: "sandbox-a",
		TeamID:    "team-a",
		Mode:      ModeS0FSUpperdir,
		VolumeID:  "rootfs-a",
	}, []Mount{{Type: "overlay"}})
	if err == nil {
		t.Fatal("PrepareAndRewriteOverlayMounts() error = nil, want invalid response error")
	}
}

type recordingPrepareClient struct {
	address  string
	request  ctldapi.PrepareRootFSRequest
	response *ctldapi.PrepareRootFSResponse
	err      error
}

func (c *recordingPrepareClient) PrepareRootFS(_ context.Context, address string, req ctldapi.PrepareRootFSRequest) (*ctldapi.PrepareRootFSResponse, error) {
	c.address = address
	c.request = req
	if c.err != nil {
		return nil, c.err
	}
	if c.response == nil {
		return nil, errors.New("missing response")
	}
	return c.response, nil
}
