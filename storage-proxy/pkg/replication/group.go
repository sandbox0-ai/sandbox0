package replication

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/router"
)

var (
	ErrGroupNotFound     = errors.New("replication group not found")
	ErrNotLeader         = errors.New("replication leader mismatch")
	ErrLeaseExpired      = errors.New("replication lease expired")
	ErrQuorumUnavailable = errors.New("replication quorum unavailable")
	ErrReplicaNotMember  = errors.New("replica is not a group member")
	ErrInvalidSnapshot   = errors.New("snapshot index is behind the committed index")
)

type Member struct {
	ID   string
	Addr string
}

type Lease struct {
	VolumeID  string
	LeaderID  string
	Term      uint64
	Epoch     uint64
	Committed uint64
}

type Entry struct {
	Index uint64
	Term  uint64
	Data  []byte
}

type Snapshot struct {
	Index uint64
	Term  uint64
	Data  []byte
}

type Replica interface {
	ID() string
	Append(ctx context.Context, volumeID string, term uint64, entry Entry, committed uint64) error
	InstallSnapshot(ctx context.Context, volumeID string, snapshot Snapshot) error
}

type Progress struct {
	MatchIndex uint64
	Healthy    bool
	LastError  string
}

type Status struct {
	VolumeID    string
	LeaderID    string
	Term        uint64
	Epoch       uint64
	LastIndex   uint64
	CommitIndex uint64
	Members     []Member
	Progress    map[string]Progress
}

type Manager struct {
	mu         sync.RWMutex
	localID    string
	volumeAddr string
	router     *router.VolumeRouter
	groups     map[string]*Group
}

func NewManager(localID, volumeAddr string, volumeRouter *router.VolumeRouter) *Manager {
	if volumeRouter == nil {
		volumeRouter = router.NewVolumeRouter()
	}
	return &Manager{
		localID:    localID,
		volumeAddr: volumeAddr,
		router:     volumeRouter,
		groups:     make(map[string]*Group),
	}
}

func (m *Manager) Router() *router.VolumeRouter {
	if m == nil {
		return nil
	}
	return m.router
}

func (m *Manager) EnsureGroup(volumeID string, members []Member) *Group {
	m.mu.Lock()
	defer m.mu.Unlock()

	group, ok := m.groups[volumeID]
	if ok {
		group.setMembers(members)
		return group
	}

	group = newGroup(volumeID, m.localID, m.volumeAddr, m.router, members)
	m.groups[volumeID] = group
	return group
}

func (m *Manager) Group(volumeID string) (*Group, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	group, ok := m.groups[volumeID]
	return group, ok
}

type Group struct {
	mu         sync.Mutex
	volumeID   string
	localID    string
	localAddr  string
	router     *router.VolumeRouter
	members    []Member
	memberAddr map[string]string
	replicas   map[string]Replica
	progress   map[string]*Progress

	leaderID    string
	term        uint64
	epoch       uint64
	lastIndex   uint64
	commitIndex uint64
}

func newGroup(volumeID, localID, localAddr string, volumeRouter *router.VolumeRouter, members []Member) *Group {
	group := &Group{
		volumeID:   volumeID,
		localID:    localID,
		localAddr:  localAddr,
		router:     volumeRouter,
		replicas:   make(map[string]Replica),
		progress:   make(map[string]*Progress),
		memberAddr: make(map[string]string),
	}
	group.setMembers(members)
	return group
}

func (g *Group) setMembers(members []Member) {
	g.members = append(g.members[:0], members...)
	if len(g.members) == 0 && g.localID != "" {
		g.members = []Member{{ID: g.localID, Addr: g.localAddr}}
	}
	g.memberAddr = make(map[string]string, len(g.members))
	for _, member := range g.members {
		g.memberAddr[member.ID] = member.Addr
		if _, ok := g.progress[member.ID]; !ok {
			g.progress[member.ID] = &Progress{}
		}
	}
	for replicaID := range g.progress {
		if !g.isMember(replicaID) {
			delete(g.progress, replicaID)
		}
	}
}

func (g *Group) RegisterReplica(replica Replica) error {
	if g == nil || replica == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.isMember(replica.ID()) {
		return ErrReplicaNotMember
	}
	g.replicas[replica.ID()] = replica
	return nil
}

func (g *Group) Campaign(leaderID string) (Lease, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.isMember(leaderID) {
		return Lease{}, ErrReplicaNotMember
	}
	g.term++
	g.epoch++
	g.leaderID = leaderID
	g.publishRouteLocked()
	return g.leaseLocked(), nil
}

func (g *Group) Append(ctx context.Context, leaderID string, payload []byte) (Entry, Lease, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.leaderID == "" {
		return Entry{}, Lease{}, ErrNotLeader
	}
	if g.leaderID != leaderID {
		return Entry{}, Lease{}, ErrNotLeader
	}

	entry := Entry{
		Index: g.lastIndex + 1,
		Term:  g.term,
		Data:  slices.Clone(payload),
	}
	acks := 1
	g.progress[leaderID].MatchIndex = entry.Index
	g.progress[leaderID].Healthy = true
	g.progress[leaderID].LastError = ""

	for _, member := range g.members {
		if member.ID == leaderID {
			continue
		}
		progress := g.progress[member.ID]
		progress.Healthy = false
		progress.LastError = ""
		replica := g.replicas[member.ID]
		if replica == nil {
			progress.LastError = "replica unavailable"
			continue
		}
		if err := replica.Append(ctx, g.volumeID, g.term, entry, g.commitIndex); err != nil {
			progress.LastError = err.Error()
			continue
		}
		progress.MatchIndex = entry.Index
		progress.Healthy = true
		acks++
	}

	if acks < g.quorumLocked() {
		return Entry{}, g.leaseLocked(), ErrQuorumUnavailable
	}

	g.lastIndex = entry.Index
	g.commitIndex = entry.Index
	g.publishRouteLocked()
	return entry, g.leaseLocked(), nil
}

func (g *Group) InstallSnapshot(ctx context.Context, leaderID string, snapshot Snapshot) (Lease, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.leaderID != leaderID {
		return Lease{}, ErrNotLeader
	}
	if snapshot.Index < g.commitIndex {
		return Lease{}, ErrInvalidSnapshot
	}

	acks := 1
	for _, member := range g.members {
		if member.ID == leaderID {
			continue
		}
		progress := g.progress[member.ID]
		progress.Healthy = false
		progress.LastError = ""
		replica := g.replicas[member.ID]
		if replica == nil {
			progress.LastError = "replica unavailable"
			continue
		}
		if err := replica.InstallSnapshot(ctx, g.volumeID, snapshot); err != nil {
			progress.LastError = err.Error()
			continue
		}
		progress.MatchIndex = snapshot.Index
		progress.Healthy = true
		acks++
	}
	if acks < g.quorumLocked() {
		return Lease{}, ErrQuorumUnavailable
	}

	if snapshot.Index > g.lastIndex {
		g.lastIndex = snapshot.Index
	}
	if snapshot.Index > g.commitIndex {
		g.commitIndex = snapshot.Index
	}
	g.publishRouteLocked()
	return g.leaseLocked(), nil
}

func (g *Group) ValidateLease(lease Lease) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if lease.VolumeID != g.volumeID || lease.LeaderID != g.leaderID || lease.Term != g.term || lease.Epoch != g.epoch {
		return ErrLeaseExpired
	}
	return nil
}

func (g *Group) Status() Status {
	g.mu.Lock()
	defer g.mu.Unlock()

	progress := make(map[string]Progress, len(g.progress))
	for replicaID, state := range g.progress {
		if state == nil {
			continue
		}
		progress[replicaID] = *state
	}

	members := make([]Member, len(g.members))
	copy(members, g.members)

	return Status{
		VolumeID:    g.volumeID,
		LeaderID:    g.leaderID,
		Term:        g.term,
		Epoch:       g.epoch,
		LastIndex:   g.lastIndex,
		CommitIndex: g.commitIndex,
		Members:     members,
		Progress:    progress,
	}
}

func (g *Group) leaseLocked() Lease {
	return Lease{
		VolumeID:  g.volumeID,
		LeaderID:  g.leaderID,
		Term:      g.term,
		Epoch:     g.epoch,
		Committed: g.commitIndex,
	}
}

func (g *Group) publishRouteLocked() {
	if g.router == nil || g.leaderID == "" {
		return
	}
	route := router.Route{
		VolumeID:      g.volumeID,
		PrimaryNodeID: g.leaderID,
		Epoch:         g.epoch,
		LocalPrimary:  g.leaderID == g.localID,
	}
	if !route.LocalPrimary {
		route.PrimaryAddr = g.memberAddr[g.leaderID]
	}
	g.router.SetRoute(route)
}

func (g *Group) quorumLocked() int {
	return (len(g.members) / 2) + 1
}

func (g *Group) isMember(replicaID string) bool {
	_, ok := g.memberAddr[replicaID]
	return ok
}

func (g *Group) String() string {
	status := g.Status()
	return fmt.Sprintf("group(volume=%s leader=%s term=%d epoch=%d commit=%d)", status.VolumeID, status.LeaderID, status.Term, status.Epoch, status.CommitIndex)
}
