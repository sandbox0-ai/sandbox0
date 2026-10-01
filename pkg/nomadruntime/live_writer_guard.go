package nomadruntime

import (
	"context"
	"fmt"
	"sync"
	"time"

	managerauthority "github.com/sandbox0-ai/sandbox0/manager/pkg/rootfswriterauthority"
)

// LiveWriterGuard starts before candidate acceptance. It keeps already
// consumed regional writers renewed while their new local indexes initialize.
// It cannot consume a grant, create a session, or revive an expired writer.
type LiveWriterGuard struct {
	runtime *rootfsRuntime
	mu      sync.Mutex
	failure error
	once    sync.Once
}

func (s *Service) StartLiveWriterGuard(ctx context.Context, handoff LiveRuntimeHandoff) (*LiveWriterGuard, error) {
	config := s.config
	authority, err := managerauthority.NewManagerClient(managerauthority.ManagerClientConfig{
		BaseURL: config.RootFSAuthorityURL, CAFile: config.RootFSAuthorityCAFile,
		ClientCertFile: config.RootFSAuthorityClientCertFile, ClientKeyFile: config.RootFSAuthorityClientKeyFile,
		TokenFile: config.RootFSAuthorityTokenFile, Timeout: 2 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	guard := &LiveWriterGuard{runtime: &rootfsRuntime{authority: authority, renewals: make(map[string]*rootfsRenewal), logger: newLogger(s.logger)}}
	seen := make(map[string]bool)
	for _, request := range handoff.Requests {
		if seen[request.Parent] || request.Validate() != nil {
			guard.Close()
			return nil, fmt.Errorf("invalid candidate writer set")
		}
		seen[request.Parent] = true
	}
	if len(seen) != len(handoff.Sessions.Sessions) {
		guard.Close()
		return nil, fmt.Errorf("candidate writers differ from sessions")
	}
	for _, session := range handoff.Sessions.Sessions {
		if !seen[session.Parent] {
			guard.Close()
			return nil, fmt.Errorf("candidate writer set omits a session")
		}
	}
	proofs, err := proveLiveWriters(ctx, authority, handoff.Requests)
	if err != nil {
		guard.Close()
		return nil, err
	}
	for _, proof := range proofs {
		guard.runtime.startRenewal(proof.request, proof.observation, proof.started, func(err error) { guard.mu.Lock(); guard.failure = err; guard.mu.Unlock() })
	}
	return guard, nil
}

func (g *LiveWriterGuard) Err() error {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.failure
}
func (g *LiveWriterGuard) Close() {
	if g != nil {
		g.once.Do(g.runtime.stopAllRenewals)
	}
}
