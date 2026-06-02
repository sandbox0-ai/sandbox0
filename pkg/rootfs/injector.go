package rootfs

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
)

const localCtldHost = "127.0.0.1"

// Mount is the minimal runtime mount shape needed to rewrite an overlay rootfs.
type Mount struct {
	Type    string
	Source  string
	Options []string
}

type PrepareClient interface {
	PrepareRootFS(ctx context.Context, ctldAddress string, req ctldapi.PrepareRootFSRequest) (*ctldapi.PrepareRootFSResponse, error)
}

type PrepareOverlayResult struct {
	Mounts    []Mount
	RootFS    *ctldapi.PrepareRootFSResponse
	Rewritten bool
}

func LocalCtldAddress(port int) (string, error) {
	if port <= 0 {
		return "", fmt.Errorf("ctld port is required")
	}
	return "http://" + net.JoinHostPort(localCtldHost, fmt.Sprintf("%d", port)), nil
}

func PrepareAndRewriteOverlayMounts(
	ctx context.Context,
	client PrepareClient,
	ctldAddress string,
	meta Metadata,
	mounts []Mount,
) (PrepareOverlayResult, error) {
	result := PrepareOverlayResult{Mounts: cloneMounts(mounts)}
	if !meta.UsesS0FSUpperdir() {
		return result, nil
	}
	if client == nil {
		return result, fmt.Errorf("rootfs prepare client is required")
	}
	req, err := prepareRequestFromMetadata(meta)
	if err != nil {
		return result, err
	}
	ctldAddress = strings.TrimSpace(ctldAddress)
	if ctldAddress == "" {
		ctldAddress, err = LocalCtldAddress(meta.CtldPort)
		if err != nil {
			return result, err
		}
	}
	resp, err := client.PrepareRootFS(ctx, ctldAddress, req)
	if err != nil {
		return result, err
	}
	if resp == nil || !resp.Prepared || strings.TrimSpace(resp.UpperDir) == "" || strings.TrimSpace(resp.WorkDir) == "" {
		return result, fmt.Errorf("ctld returned an invalid rootfs prepare response")
	}
	for i := range result.Mounts {
		if strings.TrimSpace(result.Mounts[i].Type) != "overlay" {
			continue
		}
		options, err := RewriteOverlayUpperWorkOptions(result.Mounts[i].Options, resp.UpperDir, resp.WorkDir)
		if err != nil {
			return result, err
		}
		result.Mounts[i].Options = options
		result.RootFS = resp
		result.Rewritten = true
		return result, nil
	}
	return result, fmt.Errorf("overlay rootfs mount is required")
}

func prepareRequestFromMetadata(meta Metadata) (ctldapi.PrepareRootFSRequest, error) {
	if !meta.UsesS0FSUpperdir() {
		return ctldapi.PrepareRootFSRequest{}, fmt.Errorf("metadata does not request s0fs upperdir")
	}
	sandboxID := strings.TrimSpace(meta.SandboxID)
	teamID := strings.TrimSpace(meta.TeamID)
	volumeID := strings.TrimSpace(meta.VolumeID)
	if sandboxID == "" || teamID == "" || volumeID == "" {
		return ctldapi.PrepareRootFSRequest{}, fmt.Errorf("sandbox_id, team_id and rootfs_volume_id are required")
	}
	return ctldapi.PrepareRootFSRequest{
		SandboxID:      sandboxID,
		TeamID:         teamID,
		RootFSVolumeID: volumeID,
	}, nil
}

func cloneMounts(mounts []Mount) []Mount {
	if len(mounts) == 0 {
		return nil
	}
	cloned := make([]Mount, len(mounts))
	for i := range mounts {
		cloned[i] = mounts[i]
		if mounts[i].Options != nil {
			cloned[i].Options = append([]string(nil), mounts[i].Options...)
		}
	}
	return cloned
}
