package portal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"
)

const nodeFSRequiredKernelCapabilities = fuse.CAP_HAS_RESEND | fuseCapabilityRecovery

type nodeFUSEServer interface {
	Serve()
	WaitMount() error
	Unmount() error
	ConnectionState() fuse.ConnectionState
}

type nodeFSConnectionFactory interface {
	New(fuse.RawFileSystem, string, *fuse.MountOptions) (nodeFUSEServer, error)
	Resume(fuse.RawFileSystem, string, int, fuse.ConnectionState, *fuse.MountOptions) (nodeFUSEServer, error)
	Recover(string) (int, uint64, error)
	CleanMount(string) error
}

type systemNodeFSConnectionFactory struct{}

func (systemNodeFSConnectionFactory) New(fs fuse.RawFileSystem, mountPath string, opts *fuse.MountOptions) (nodeFUSEServer, error) {
	return fuse.NewServer(fs, mountPath, opts)
}

func (systemNodeFSConnectionFactory) Resume(fs fuse.RawFileSystem, mountPath string, fd int, state fuse.ConnectionState, opts *fuse.MountOptions) (nodeFUSEServer, error) {
	return fuse.ResumeServerFromFD(fs, mountPath, fd, state, opts)
}

func (systemNodeFSConnectionFactory) Recover(tag string) (int, uint64, error) {
	return recoverFUSEConnection(tag)
}

func (systemNodeFSConnectionFactory) CleanMount(mountPath string) error {
	err := unix.Unmount(mountPath, unix.MNT_DETACH)
	if err == nil || errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOENT) {
		return nil
	}
	return fmt.Errorf("detach stale nodefs shard mount %s: %w", mountPath, err)
}

type nodeFSConnection struct {
	server nodeFUSEServer
	done   chan struct{}
}

func (c *nodeFSConnection) serve() error {
	if c == nil || c.server == nil {
		return fmt.Errorf("nodefs FUSE server is not initialized")
	}
	c.done = make(chan struct{})
	go func() {
		defer close(c.done)
		c.server.Serve()
	}()
	if err := c.server.WaitMount(); err != nil {
		return fmt.Errorf("wait for nodefs shard mount: %w", err)
	}
	return nil
}

func nodeFSMountOptions(shard nodeFSShardState, requireRecovery bool) *fuse.MountOptions {
	opts := &fuse.MountOptions{
		FsName:                  "sandbox0-nodefs",
		Name:                    "sandbox0-nodefs",
		MaxBackground:           128,
		MaxInflightRequestBytes: 64 << 20,
		EnableLocks:             true,
		AllowOther:              os.Getuid() == 0,
		DirectMount:             true,
		MaxWrite:                256 * 1024,
	}
	if requireRecovery {
		opts.Options = append(opts.Options, "tag="+shard.Tag, "rescue_uid="+strconv.Itoa(os.Getuid()))
		opts.ExtraCapabilities |= nodeFSRequiredKernelCapabilities
	}
	return opts
}

func startNodeFSConnection(
	journal *nodeFSJournalStore,
	journalState nodeFSJournal,
	shard nodeFSShardState,
	portalCount int,
	fs fuse.RawFileSystem,
	factory nodeFSConnectionFactory,
) (*nodeFSConnection, bool, error) {
	if journal == nil || fs == nil || factory == nil {
		return nil, false, fmt.Errorf("nodefs connection dependencies are required")
	}
	if err := os.MkdirAll(shard.MountPath, 0o700); err != nil {
		return nil, false, fmt.Errorf("create nodefs shard mount path: %w", err)
	}
	opts := nodeFSMountOptions(shard, journalState.RecoveryRequired)

	if len(shard.SessionState) > 0 && journalState.RecoveryRequired {
		connection, err := resumeNodeFSConnection(shard, fs, opts, factory)
		if err == nil {
			if err := connection.serve(); err != nil {
				return nil, false, err
			}
			return connection, true, nil
		}
		if portalCount > 0 {
			return nil, false, fmt.Errorf("recover nodefs shard %d with %d portals: %w", shard.Index, portalCount, err)
		}
		if clearErr := journal.ClearShardSession(shard.Index); clearErr != nil {
			return nil, false, errors.Join(err, clearErr)
		}
		shard.SessionState = nil
	}

	if len(shard.SessionState) > 0 {
		if portalCount > 0 {
			return nil, false, fmt.Errorf("nodefs shard %d has active portals but recovery is disabled", shard.Index)
		}
		if err := journal.ClearShardSession(shard.Index); err != nil {
			return nil, false, err
		}
		shard.SessionState = nil
	}
	if err := factory.CleanMount(shard.MountPath); err != nil {
		return nil, false, err
	}

	server, err := factory.New(fs, shard.MountPath, opts)
	if err != nil {
		return nil, false, fmt.Errorf("create nodefs shard %d: %w", shard.Index, err)
	}
	state := server.ConnectionState()
	if journalState.RecoveryRequired && state.InitResponse.Flags64()&nodeFSRequiredKernelCapabilities != nodeFSRequiredKernelCapabilities {
		_ = server.Unmount()
		return nil, false, fmt.Errorf("nodefs shard %d kernel did not negotiate FUSE recovery and resend", shard.Index)
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		_ = server.Unmount()
		return nil, false, fmt.Errorf("encode nodefs shard connection state: %w", err)
	}
	if err := journal.CommitShardSession(shard.Index, encoded); err != nil {
		_ = server.Unmount()
		return nil, false, fmt.Errorf("commit nodefs shard connection state: %w", err)
	}
	connection := &nodeFSConnection{server: server}
	if err := connection.serve(); err != nil {
		return nil, false, err
	}
	return connection, false, nil
}

func resumeNodeFSConnection(shard nodeFSShardState, fs fuse.RawFileSystem, opts *fuse.MountOptions, factory nodeFSConnectionFactory) (*nodeFSConnection, error) {
	var state fuse.ConnectionState
	if err := json.Unmarshal(shard.SessionState, &state); err != nil {
		return nil, fmt.Errorf("decode nodefs shard %d connection state: %w", shard.Index, err)
	}
	if state.InitResponse.Flags64()&nodeFSRequiredKernelCapabilities != nodeFSRequiredKernelCapabilities {
		return nil, fmt.Errorf("nodefs shard %d saved connection lacks recovery capabilities", shard.Index)
	}
	fd, _, err := factory.Recover(shard.Tag)
	if err != nil {
		return nil, err
	}
	server, err := factory.Resume(fs, shard.MountPath, fd, state, opts)
	if err != nil {
		return nil, fmt.Errorf("resume nodefs shard %d server: %w", shard.Index, err)
	}
	return &nodeFSConnection{server: server}, nil
}
