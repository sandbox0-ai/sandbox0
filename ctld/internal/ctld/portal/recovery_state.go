package portal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const portalRecoveryVersion = 1

// RecoveryCapabilityS0FSHandleJournal identifies peers that replay incremental
// S0FS file-handle recovery events in addition to legacy snapshot files.
const RecoveryCapabilityS0FSHandleJournal = "s0fs_handle_journal_v1"

// RecoveryManifest contains the durable and replicated state needed to attach
// a standby process to an existing portal's kernel FUSE connection.
type RecoveryManifest struct {
	Version           int       `json:"version"`
	Key               string    `json:"key"`
	Namespace         string    `json:"namespace,omitempty"`
	PodName           string    `json:"pod_name,omitempty"`
	PodUID            string    `json:"pod_uid"`
	Name              string    `json:"name"`
	MountPath         string    `json:"mount_path,omitempty"`
	TargetPath        string    `json:"target_path"`
	RootFSBackingPath string    `json:"rootfs_backing_path"`
	RootFSStatePath   string    `json:"rootfs_state_path"`
	VolumeStatePath   string    `json:"volume_state_path,omitempty"`
	VolumeID          string    `json:"volume_id,omitempty"`
	TeamID            string    `json:"team_id,omitempty"`
	AccessMode        string    `json:"access_mode,omitempty"`
	MountedAt         time.Time `json:"mounted_at,omitempty"`
	InitRequest       []byte    `json:"init_request"`
}

// PortalReplicator synchronizes recovery manifests and FUSE channels to the
// passive ctld process. Publish duplicates channel through the transport; the
// caller retains ownership of the supplied descriptor.
type PortalReplicator interface {
	Ready() bool
	Publish(context.Context, RecoveryManifest, *os.File) error
	Update(context.Context, RecoveryManifest) error
	Remove(context.Context, string) error
}

// PortalRecoveryCapabilityProvider allows compatible peers to opt into recovery
// formats without changing the wire protocol used during rolling upgrades.
type PortalRecoveryCapabilityProvider interface {
	SupportsRecoveryCapability(string) bool
}

func supportsRecoveryCapability(replicator PortalReplicator, capability string) bool {
	provider, ok := replicator.(PortalRecoveryCapabilityProvider)
	return ok && provider.SupportsRecoveryCapability(capability)
}

type portalRecoveryStore struct {
	dir      string
	observer *Observer
}

func newPortalRecoveryStore(rootDir string, observers ...*Observer) *portalRecoveryStore {
	store := &portalRecoveryStore{dir: filepath.Join(rootDir, "ha", "portals")}
	if len(observers) > 0 {
		store.observer = observers[0]
	}
	return store
}

func (s *portalRecoveryStore) Put(manifest RecoveryManifest) error {
	return s.put(manifest)
}

func (s *portalRecoveryStore) put(manifest RecoveryManifest) error {
	if s == nil {
		return nil
	}
	manifest.Version = portalRecoveryVersion
	if err := validateRecoveryManifest(manifest); err != nil {
		return err
	}
	started := time.Now()
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		s.observePhase("local_prepare", manifest.VolumeID, started, err)
		return fmt.Errorf("create portal recovery directory: %w", err)
	}
	s.observePhase("local_prepare", manifest.VolumeID, started, nil)
	started = time.Now()
	payload, err := json.Marshal(manifest)
	s.observePhase("local_encode", manifest.VolumeID, started, err)
	if err != nil {
		return fmt.Errorf("marshal portal recovery manifest: %w", err)
	}
	started = time.Now()
	path := s.path(manifest.Key)
	tmp, err := os.CreateTemp(s.dir, ".portal-*.tmp")
	if err != nil {
		s.observePhase("local_write", manifest.VolumeID, started, err)
		return fmt.Errorf("create portal recovery manifest: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		s.observePhase("local_write", manifest.VolumeID, started, err)
		return fmt.Errorf("chmod portal recovery manifest: %w", err)
	}
	if _, err := tmp.Write(payload); err != nil {
		_ = tmp.Close()
		s.observePhase("local_write", manifest.VolumeID, started, err)
		return fmt.Errorf("write portal recovery manifest: %w", err)
	}
	s.observePhase("local_write", manifest.VolumeID, started, nil)
	started = time.Now()
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		s.observePhase("local_fsync", manifest.VolumeID, started, err)
		return fmt.Errorf("sync portal recovery manifest: %w", err)
	}
	s.observePhase("local_fsync", manifest.VolumeID, started, nil)
	started = time.Now()
	if err := tmp.Close(); err != nil {
		s.observePhase("local_close", manifest.VolumeID, started, err)
		return fmt.Errorf("close portal recovery manifest: %w", err)
	}
	s.observePhase("local_close", manifest.VolumeID, started, nil)
	started = time.Now()
	if err := os.Rename(tmpPath, path); err != nil {
		s.observePhase("local_rename", manifest.VolumeID, started, err)
		return fmt.Errorf("replace portal recovery manifest: %w", err)
	}
	s.observePhase("local_rename", manifest.VolumeID, started, nil)
	return nil
}

func (s *portalRecoveryStore) observePhase(phase, volumeID string, started time.Time, err error) {
	if s != nil && s.observer != nil {
		s.observer.ObservePhase("recovery", phase, "local", 0, volumeID, started, err)
	}
}

func (s *portalRecoveryStore) syncDirectory() error {
	if s == nil {
		return nil
	}
	dir, err := os.Open(s.dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open portal recovery directory: %w", err)
	}
	defer dir.Close()
	started := time.Now()
	if err := dir.Sync(); err != nil {
		s.observePhase("local_dir_fsync", "", started, err)
		return fmt.Errorf("sync portal recovery directory: %w", err)
	}
	s.observePhase("local_dir_fsync", "", started, nil)
	return nil
}

func (s *portalRecoveryStore) Delete(key string) error {
	return s.delete(key)
}

func (s *portalRecoveryStore) delete(key string) error {
	if s == nil || strings.TrimSpace(key) == "" {
		return nil
	}
	if err := os.Remove(s.path(key)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("delete portal recovery manifest: %w", err)
	}
	return nil
}

func (s *portalRecoveryStore) List() ([]RecoveryManifest, error) {
	if s == nil {
		return nil, nil
	}
	entries, err := os.ReadDir(s.dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list portal recovery manifests: %w", err)
	}
	manifests := make([]RecoveryManifest, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		payload, err := os.ReadFile(filepath.Join(s.dir, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read portal recovery manifest %s: %w", entry.Name(), err)
		}
		var manifest RecoveryManifest
		if err := json.Unmarshal(payload, &manifest); err != nil {
			return nil, fmt.Errorf("decode portal recovery manifest %s: %w", entry.Name(), err)
		}
		if err := validateRecoveryManifest(manifest); err != nil {
			return nil, fmt.Errorf("validate portal recovery manifest %s: %w", entry.Name(), err)
		}
		manifests = append(manifests, manifest)
	}
	return manifests, nil
}

func (s *portalRecoveryStore) path(key string) string {
	digest := sha256.Sum256([]byte(key))
	return filepath.Join(s.dir, hex.EncodeToString(digest[:])+".json")
}

func validateRecoveryManifest(manifest RecoveryManifest) error {
	if manifest.Version != portalRecoveryVersion {
		return fmt.Errorf("unsupported portal recovery manifest version %d", manifest.Version)
	}
	if strings.TrimSpace(manifest.Key) == "" || strings.TrimSpace(manifest.PodUID) == "" || strings.TrimSpace(manifest.Name) == "" {
		return fmt.Errorf("portal recovery key, pod_uid, and name are required")
	}
	if strings.TrimSpace(manifest.TargetPath) == "" || strings.TrimSpace(manifest.RootFSBackingPath) == "" {
		return fmt.Errorf("portal recovery target and rootfs backing paths are required")
	}
	if len(manifest.InitRequest) == 0 {
		return fmt.Errorf("portal recovery FUSE INIT request is required")
	}
	return nil
}

func recoveryManifest(pm *portalMount) RecoveryManifest {
	if pm == nil {
		return RecoveryManifest{}
	}
	manifest := RecoveryManifest{
		Version:           portalRecoveryVersion,
		Key:               portalKey(pm.podUID, pm.name),
		Namespace:         pm.namespace,
		PodName:           pm.podName,
		PodUID:            pm.podUID,
		Name:              pm.name,
		MountPath:         pm.mountPath,
		TargetPath:        pm.targetPath,
		RootFSBackingPath: pm.rootfsBackingPath,
		RootFSStatePath:   pm.rootfsStatePath,
		VolumeStatePath:   pm.volumeStatePath,
		VolumeID:          pm.volumeID,
		TeamID:            pm.teamID,
		AccessMode:        string(pm.access),
		MountedAt:         pm.mountedAt,
	}
	if pm.server != nil {
		manifest.InitRequest = pm.server.InitRequest()
	}
	return manifest
}
