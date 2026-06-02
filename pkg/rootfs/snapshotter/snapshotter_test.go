package snapshotter

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfs"
	"google.golang.org/grpc/metadata"
)

func TestMountsNoopsWhenMetadataIsUnavailable(t *testing.T) {
	base := &fakeSnapshotter{
		mounts: []mount.Mount{{
			Type:    "overlay",
			Source:  "overlay",
			Target:  "/",
			Options: []string{"lowerdir=/lower", "upperdir=/containerd/upper", "workdir=/containerd/work"},
		}},
	}
	sn, err := New(base, fakeResolver{}, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	mounts, err := sn.Mounts(context.Background(), "container-a")
	if err != nil {
		t.Fatalf("Mounts() error = %v", err)
	}
	if got := optionValue(mounts[0].Options, "upperdir="); got != "/containerd/upper" {
		t.Fatalf("upperdir = %q, want /containerd/upper", got)
	}
	if got := base.mountsCalls; got != 1 {
		t.Fatalf("base Mounts calls = %d, want 1", got)
	}
}

func TestMountsRewritesOverlayUpperAndWorkDirs(t *testing.T) {
	base := &fakeSnapshotter{
		mounts: []mount.Mount{{
			Type:    "overlay",
			Source:  "overlay",
			Target:  "/",
			Options: []string{"lowerdir=/lower", "upperdir=/containerd/upper", "workdir=/containerd/work", "volatile"},
		}},
	}
	client := &fakePrepareClient{
		response: &ctldapi.PrepareRootFSResponse{
			Prepared:       true,
			SandboxID:      "sandbox-a",
			RootFSVolumeID: "rootfs-a",
			UpperDir:       "/s0fs/upper",
			WorkDir:        "/s0fs/work",
		},
	}
	sn, err := New(base, fakeResolver{
		ok: true,
		meta: rootfs.Metadata{
			SandboxID: "sandbox-a",
			TeamID:    "team-a",
			Mode:      rootfs.ModeS0FSUpperdir,
			VolumeID:  "rootfs-a",
			CtldPort:  8095,
		},
	}, client)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	mounts, err := sn.Mounts(context.Background(), "container-a")
	if err != nil {
		t.Fatalf("Mounts() error = %v", err)
	}
	if got := mounts[0].Target; got != "/" {
		t.Fatalf("target = %q, want /", got)
	}
	if got := optionValue(mounts[0].Options, "upperdir="); got != "/s0fs/upper" {
		t.Fatalf("upperdir = %q, want /s0fs/upper", got)
	}
	if got := optionValue(mounts[0].Options, "workdir="); got != "/s0fs/work" {
		t.Fatalf("workdir = %q, want /s0fs/work", got)
	}
	if !hasOption(mounts[0].Options, "volatile") {
		t.Fatalf("options = %#v, want volatile preserved", mounts[0].Options)
	}
	if client.address != "http://127.0.0.1:8095" {
		t.Fatalf("ctld address = %q, want local ctld address", client.address)
	}
	if client.request.SandboxID != "sandbox-a" || client.request.TeamID != "team-a" || client.request.RootFSVolumeID != "rootfs-a" {
		t.Fatalf("prepare request = %+v, want sandbox-a team-a rootfs-a", client.request)
	}
}

func TestPrepareRewritesOverlayUpperAndWorkDirs(t *testing.T) {
	base := &fakeSnapshotter{
		mounts: []mount.Mount{{
			Type:    "overlay",
			Source:  "overlay",
			Target:  "/",
			Options: []string{"lowerdir=/lower", "upperdir=/containerd/upper", "workdir=/containerd/work"},
		}},
	}
	client := &fakePrepareClient{
		response: &ctldapi.PrepareRootFSResponse{
			Prepared:       true,
			SandboxID:      "sandbox-a",
			RootFSVolumeID: "rootfs-a",
			UpperDir:       "/s0fs/upper",
			WorkDir:        "/s0fs/work",
		},
	}
	sn, err := New(base, fakeResolver{
		ok: true,
		meta: rootfs.Metadata{
			SandboxID: "sandbox-a",
			TeamID:    "team-a",
			Mode:      rootfs.ModeS0FSUpperdir,
			VolumeID:  "rootfs-a",
			CtldPort:  8095,
		},
	}, client)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	mounts, err := sn.Prepare(context.Background(), "container-a", "parent-a")
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if got := mounts[0].Target; got != "/" {
		t.Fatalf("target = %q, want /", got)
	}
	if got := optionValue(mounts[0].Options, "upperdir="); got != "/s0fs/upper" {
		t.Fatalf("upperdir = %q, want /s0fs/upper", got)
	}
	if got := optionValue(mounts[0].Options, "workdir="); got != "/s0fs/work" {
		t.Fatalf("workdir = %q, want /s0fs/work", got)
	}
	if client.request.SandboxID != "sandbox-a" || client.request.TeamID != "team-a" || client.request.RootFSVolumeID != "rootfs-a" {
		t.Fatalf("prepare request = %+v, want sandbox-a team-a rootfs-a", client.request)
	}
}

func TestPrepareInjectsDefaultContainerdNamespace(t *testing.T) {
	base := &fakeSnapshotter{
		mounts: []mount.Mount{{
			Type:    "overlay",
			Source:  "overlay",
			Options: []string{"lowerdir=/lower", "upperdir=/containerd/upper", "workdir=/containerd/work"},
		}},
	}
	sn, err := New(base, fakeResolver{}, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if _, err := sn.Prepare(context.Background(), "extract-key", "parent"); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if base.namespace != defaultContainerdNamespace {
		t.Fatalf("base namespace = %q, want %s", base.namespace, defaultContainerdNamespace)
	}
}

func TestMountsPreservesExistingContainerdNamespace(t *testing.T) {
	base := &fakeSnapshotter{
		mounts: []mount.Mount{{
			Type:    "overlay",
			Source:  "overlay",
			Options: []string{"lowerdir=/lower", "upperdir=/containerd/upper", "workdir=/containerd/work"},
		}},
	}
	sn, err := New(base, fakeResolver{}, nil, WithNamespace("fallback"))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if _, err := sn.Mounts(namespaces.WithNamespace(context.Background(), "custom"), "container-a"); err != nil {
		t.Fatalf("Mounts() error = %v", err)
	}
	if base.namespace != "custom" {
		t.Fatalf("base namespace = %q, want custom", base.namespace)
	}
}

func TestPrepareNormalizesIncomingContainerdNamespaceForNestedCalls(t *testing.T) {
	base := &fakeSnapshotter{
		mounts: []mount.Mount{{
			Type:    "overlay",
			Source:  "overlay",
			Options: []string{"lowerdir=/lower", "upperdir=/containerd/upper", "workdir=/containerd/work"},
		}},
	}
	sn, err := New(base, fakeResolver{}, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(namespaces.GRPCHeader, "custom"))
	if _, err := sn.Prepare(ctx, "extract-key", "parent"); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if base.namespace != "custom" {
		t.Fatalf("base namespace = %q, want custom", base.namespace)
	}
	if base.outgoingNamespace != "custom" {
		t.Fatalf("base outgoing namespace = %q, want custom", base.outgoingNamespace)
	}
}

func TestMountsUsesExplicitCtldAddress(t *testing.T) {
	base := &fakeSnapshotter{mounts: []mount.Mount{{
		Type:    "overlay",
		Source:  "overlay",
		Options: []string{"lowerdir=/lower", "upperdir=/old-upper", "workdir=/old-work"},
	}}}
	client := &fakePrepareClient{
		response: &ctldapi.PrepareRootFSResponse{Prepared: true, UpperDir: "/upper", WorkDir: "/work"},
	}
	sn, err := New(base, fakeResolver{
		ok:   true,
		meta: rootfs.Metadata{SandboxID: "sandbox-a", TeamID: "team-a", Mode: rootfs.ModeS0FSUpperdir, VolumeID: "rootfs-a", CtldPort: 8095},
	}, client, WithCtldAddress("http://ctld-host:8095"))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if _, err := sn.Mounts(context.Background(), "container-a"); err != nil {
		t.Fatalf("Mounts() error = %v", err)
	}
	if client.address != "http://ctld-host:8095" {
		t.Fatalf("ctld address = %q, want explicit address", client.address)
	}
}

func TestMountsReturnsPrepareError(t *testing.T) {
	sn, err := New(
		&fakeSnapshotter{mounts: []mount.Mount{{Type: "overlay", Options: []string{"lowerdir=/lower", "upperdir=/old-upper", "workdir=/old-work"}}}},
		fakeResolver{ok: true, meta: rootfs.Metadata{SandboxID: "sandbox-a", TeamID: "team-a", Mode: rootfs.ModeS0FSUpperdir, VolumeID: "rootfs-a", CtldPort: 8095}},
		&fakePrepareClient{err: errors.New("ctld unavailable")},
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if _, err := sn.Mounts(context.Background(), "container-a"); err == nil || !strings.Contains(err.Error(), "ctld unavailable") {
		t.Fatalf("Mounts() error = %v, want ctld unavailable", err)
	}
}

func TestSnapshotterDelegatesCleanupWhenSupported(t *testing.T) {
	base := &fakeSnapshotter{}
	sn, err := New(base, nil, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := sn.Cleanup(context.Background()); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if !base.cleanupCalled {
		t.Fatal("base Cleanup was not called")
	}
}

type fakeResolver struct {
	meta rootfs.Metadata
	ok   bool
	err  error
}

func (r fakeResolver) ResolveRootFSMetadata(context.Context, string) (rootfs.Metadata, bool, error) {
	return r.meta, r.ok, r.err
}

type fakePrepareClient struct {
	address  string
	request  ctldapi.PrepareRootFSRequest
	response *ctldapi.PrepareRootFSResponse
	err      error
}

func (c *fakePrepareClient) PrepareRootFS(_ context.Context, address string, req ctldapi.PrepareRootFSRequest) (*ctldapi.PrepareRootFSResponse, error) {
	c.address = address
	c.request = req
	return c.response, c.err
}

type fakeSnapshotter struct {
	mounts            []mount.Mount
	mountsCalls       int
	cleanupCalled     bool
	namespace         string
	outgoingNamespace string
}

func (s *fakeSnapshotter) Stat(ctx context.Context, _ string) (snapshots.Info, error) {
	s.recordNamespace(ctx)
	return snapshots.Info{}, nil
}

func (s *fakeSnapshotter) Update(ctx context.Context, info snapshots.Info, _ ...string) (snapshots.Info, error) {
	s.recordNamespace(ctx)
	return info, nil
}

func (s *fakeSnapshotter) Usage(ctx context.Context, _ string) (snapshots.Usage, error) {
	s.recordNamespace(ctx)
	return snapshots.Usage{}, nil
}

func (s *fakeSnapshotter) Prepare(ctx context.Context, _, _ string, _ ...snapshots.Opt) ([]mount.Mount, error) {
	s.recordNamespace(ctx)
	return cloneContainerdMounts(s.mounts), nil
}

func (s *fakeSnapshotter) View(ctx context.Context, _, _ string, _ ...snapshots.Opt) ([]mount.Mount, error) {
	s.recordNamespace(ctx)
	return cloneContainerdMounts(s.mounts), nil
}

func (s *fakeSnapshotter) Mounts(ctx context.Context, _ string) ([]mount.Mount, error) {
	s.recordNamespace(ctx)
	s.mountsCalls++
	return cloneContainerdMounts(s.mounts), nil
}

func (s *fakeSnapshotter) Commit(ctx context.Context, _, _ string, _ ...snapshots.Opt) error {
	s.recordNamespace(ctx)
	return nil
}

func (s *fakeSnapshotter) Remove(ctx context.Context, _ string) error {
	s.recordNamespace(ctx)
	return nil
}

func (s *fakeSnapshotter) Walk(ctx context.Context, _ snapshots.WalkFunc, _ ...string) error {
	s.recordNamespace(ctx)
	return nil
}

func (s *fakeSnapshotter) Close() error {
	return nil
}

func (s *fakeSnapshotter) Cleanup(ctx context.Context) error {
	s.recordNamespace(ctx)
	s.cleanupCalled = true
	return nil
}

func (s *fakeSnapshotter) recordNamespace(ctx context.Context) {
	s.namespace, _ = namespaces.Namespace(ctx)
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		if values := md.Get(namespaces.GRPCHeader); len(values) > 0 {
			s.outgoingNamespace = values[0]
		}
	}
}

func cloneContainerdMounts(mounts []mount.Mount) []mount.Mount {
	if len(mounts) == 0 {
		return nil
	}
	out := make([]mount.Mount, len(mounts))
	for i := range mounts {
		out[i] = mounts[i]
		out[i].Options = append([]string(nil), mounts[i].Options...)
	}
	return out
}

func optionValue(options []string, prefix string) string {
	for _, option := range options {
		if strings.HasPrefix(option, prefix) {
			return strings.TrimPrefix(option, prefix)
		}
	}
	return ""
}

func hasOption(options []string, want string) bool {
	for _, option := range options {
		if option == want {
			return true
		}
	}
	return false
}
