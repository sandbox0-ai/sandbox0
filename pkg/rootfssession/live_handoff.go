package session

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	bolt "go.etcd.io/bbolt"
)

// LiveHandoff is private node-local state, never a regional writer grant or a
// durable retirement proof. The socket files travel separately over SCM_RIGHTS.
type LiveHandoff struct {
	Version  int                  `json:"version"`
	Sessions []LiveHandoffSession `json:"sessions"`
}

type LiveHandoffSession struct {
	Parent       string          `json:"parent"`
	RecordDigest string          `json:"record_digest"`
	Sequence     uint64          `json:"sequence"`
	Record       json.RawMessage `json:"record"`
}

type transferableDevice interface {
	Device
	PrepareHandoff(context.Context) (*os.File, error)
	AbortHandoff()
	CommitHandoff() error
}

type liveDeviceAdopter interface {
	AdoptLiveDevice(context.Context, string, string, string, string, *os.File, rootfsblock.WritableBlockDevice) (Device, error)
}

// PreparedLiveHandoff retains source ownership until Commit. The caller must
// stop and join every control/reconciliation operation before Prepare, and keep
// them stopped until Abort or Commit. Writer renewal may continue independently.
type PreparedLiveHandoff struct {
	Manifest LiveHandoff
	Files    []*os.File
	manager  *Manager
	devices  []transferableDevice
	mu       sync.Mutex
	finished bool
}

func (m *Manager) PrepareLiveHandoff(ctx context.Context) (_ *PreparedLiveHandoff, result error) {
	m.mu.Lock()
	if m.closing || m.handoff {
		m.mu.Unlock()
		return nil, fmt.Errorf("RootFS owner is closing or already preparing")
	}
	m.handoff = true
	parents := make([]string, 0, len(m.live))
	for parent := range m.live {
		parents = append(parents, parent)
	}
	busy := len(m.captures) != 0 || len(m.rebaseAdmission) != 0
	m.mu.Unlock()
	prepared := &PreparedLiveHandoff{manager: m, Manifest: LiveHandoff{Version: 1}}
	defer func() {
		if result != nil {
			prepared.Abort()
		}
	}()
	if busy {
		return nil, fmt.Errorf("RootFS checkpoint or rebase is still active")
	}
	sort.Strings(parents)
	// Also reject ready records without an owner and transitional storage. They
	// must finish their existing recovery protocol before a planned transfer.
	ready, err := m.liveHandoffRecords()
	if err != nil {
		return nil, err
	}
	if len(ready) != len(parents) {
		return nil, fmt.Errorf("RootFS live owner set is incomplete")
	}
	for _, parent := range parents {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		current, err := m.load(parent)
		if err != nil || current.State != stateReady || current.Stage == nil || current.Consumer == nil {
			return nil, fmt.Errorf("RootFS session %s is not transferable: %w", parent, err)
		}
		if current.Consumer.RenewalProtocol != 1 {
			return nil, fmt.Errorf("RootFS consumer %s cannot retry a live update", parent)
		}
		remaining, err := time.Parse(time.RFC3339Nano, current.Consumer.LeaseExpiresAt)
		if err != nil || time.Until(remaining) < 20*time.Second {
			return nil, fmt.Errorf("RootFS consumer %s needs a fresh proven lease", parent)
		}
		live := m.live[parent]
		device, ok := live.device.(transferableDevice)
		if !ok {
			return nil, fmt.Errorf("RootFS session %s has a legacy device", parent)
		}
		file, err := device.PrepareHandoff(ctx)
		if err != nil {
			return nil, fmt.Errorf("prepare RootFS session %s: %w", parent, err)
		}
		prepared.devices = append(prepared.devices, device)
		prepared.Files = append(prepared.Files, file)
		if err := live.branch.Flush(); err != nil {
			return nil, err
		}
		index, err := live.branch.ExportLiveIndex()
		if err != nil {
			return nil, err
		}
		prepared.Files = append(prepared.Files, index)
		sequence, err := live.branch.DurableSequence()
		if err != nil {
			return nil, err
		}
		var payload []byte
		if err := m.db.View(func(tx *bolt.Tx) error {
			payload = append([]byte(nil), tx.Bucket(sessionBucket).Get([]byte(parent))...)
			return nil
		}); err != nil {
			return nil, err
		}
		prepared.Manifest.Sessions = append(prepared.Manifest.Sessions, LiveHandoffSession{Parent: parent, RecordDigest: ready[parent], Sequence: sequence, Record: payload})
	}
	return prepared, nil
}

func (p *PreparedLiveHandoff) Abort() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finished {
		return
	}
	for _, device := range p.devices {
		device.AbortHandoff()
	}
	for _, file := range p.Files {
		_ = file.Close()
	}
	p.manager.mu.Lock()
	p.manager.handoff = false
	p.manager.mu.Unlock()
	p.finished = true
}

// Commit removes only userspace owners. It never disconnects NBD, unmounts,
// releases reservations, retires writers, or changes the session journal.
// A failure after the first detach is terminal for this source; retain its HA
// fence and files until recovery, rather than invoking ordinary shutdown.
func (p *PreparedLiveHandoff) Commit() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finished {
		return fmt.Errorf("RootFS handoff already finished")
	}
	// Even a partial detach is irreversible. Abort must never reopen source
	// admission after a failed commit.
	p.finished = true
	for _, device := range p.devices {
		if err := device.CommitHandoff(); err != nil {
			return err
		}
	}
	m := p.manager
	m.mu.Lock()
	m.closing = true
	m.mu.Unlock()
	var result error
	for _, parent := range p.Manifest.Sessions {
		live := m.live[parent.Parent]
		result = errors.Join(result, live.device.Close(), live.branch.Close())
	}
	m.live = make(map[string]*liveSession)
	m.cancel()
	result = errors.Join(result, m.readCache.Close(), m.db.Close())
	p.finished = true
	return result
}

func (m *Manager) liveHandoffRecords() (map[string]string, error) {
	result := make(map[string]string)
	err := m.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(sessionBucket).ForEach(func(key, value []byte) error {
			var current record
			if err := json.Unmarshal(value, &current); err != nil {
				return err
			}
			switch current.State {
			case stateReady:
				digest := sha256.Sum256(value)
				result[string(key)] = hex.EncodeToString(digest[:])
			case stateTombstoned, stateReserved, stateDeviceReserved:
			default:
				return fmt.Errorf("RootFS session %s is in transitional state %s", key, current.State)
			}
			return nil
		})
	})
	return result, err
}

// AdoptLiveHandoff must run before any startup reconciliation. Only the exact
// complete durable session set can be imported. Caller supplies an already
// committed transfer under the inherited primary lock; replay is forbidden.
func (m *Manager) AdoptLiveHandoff(ctx context.Context, manifest LiveHandoff, files []*os.File) error {
	if manifest.Version != 1 || 2*len(manifest.Sessions) != len(files) || len(m.live) != 0 {
		return fmt.Errorf("invalid RootFS handoff envelope")
	}
	adopter, ok := m.runtime.(liveDeviceAdopter)
	if !ok {
		return fmt.Errorf("host runtime cannot adopt connected RootFS devices")
	}
	records, err := m.liveHandoffRecords()
	if err != nil {
		return err
	}
	if len(records) != len(manifest.Sessions) {
		return fmt.Errorf("RootFS handoff omits a ready session")
	}
	branches := make([]*rootfsblock.Branch, len(manifest.Sessions))
	currents := make([]record, len(manifest.Sessions))
	defer func() {
		for _, branch := range branches {
			if branch != nil {
				_ = branch.Close()
			}
		}
	}()
	for i, entry := range manifest.Sessions {
		if records[entry.Parent] == "" || records[entry.Parent] != entry.RecordDigest {
			return fmt.Errorf("RootFS handoff session %s changed or was repeated", entry.Parent)
		}
		delete(records, entry.Parent)
		current, err := m.load(entry.Parent)
		if err != nil {
			return err
		}
		if current.Consumer == nil || current.Stage == nil {
			return fmt.Errorf("RootFS handoff lacks consumer binding")
		}
		branch, err := m.reopenBranchWithIndex(current, files[2*i+1])
		if err != nil {
			return err
		}
		branches[i], currents[i] = branch, current
		sequence, err := branch.DurableSequence()
		if err != nil || sequence != entry.Sequence {
			return fmt.Errorf("RootFS handoff journal cut changed: %w", err)
		}
	}
	for i, entry := range manifest.Sessions {
		current := currents[i]
		if err := ctx.Err(); err != nil {
			return err
		}
		device, err := adopter.AdoptLiveDevice(m.lifetime, current.DevicePath, current.DeviceAllocationID, current.XFSRoot, current.MergedRoot, files[2*i], branches[i])
		if err != nil {
			return err
		}
		m.live[entry.Parent] = &liveSession{branch: branches[i], device: device}
		branches[i] = nil
	}
	return nil
}
