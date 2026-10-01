package nomadruntime

import (
	"context"
	"os"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
)

func (s *Service) PreflightLiveStorage(ctx context.Context, handoff LiveRuntimeHandoff, files []*os.File) error {
	store, err := newRuntimeObjectStore(s.config, objectstore.Create)
	if err != nil {
		return err
	}
	if err := initializeRuntimeObjectStore(ctx, store); err != nil {
		return err
	}
	runtime, err := rootfssession.NewLinuxRuntime(rootfssession.LinuxRuntimeConfig{DevicePaths: s.config.RootFSNBDDevices, TransferableNBD: s.config.RootFSTransferableNBD})
	if err != nil {
		return err
	}
	return rootfssession.PreflightLiveHandoff(ctx, rootfssession.Config{
		BranchRoot: s.config.RootFSBranchRoot, MountRoot: s.config.RootFSMountRoot, Source: store, Runtime: runtime,
	}, handoff.Sessions, files)
}
