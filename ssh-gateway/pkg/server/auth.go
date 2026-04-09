package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	managerclient "github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/client"
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
	"go.uber.org/zap"
	"golang.org/x/crypto/ssh"
)

var (
	ErrInvalidSSHUsername       = errors.New("invalid ssh username")
	ErrUnauthorizedSSHPublicKey = errors.New("ssh public key is not authorized")
	ErrSandboxAccessDenied      = errors.New("sandbox access denied")
	ErrSandboxUnavailable       = errors.New("sandbox unavailable")
)

// SessionTarget carries the resolved sandbox and identity information for one SSH connection.
type SessionTarget struct {
	Username  string
	SandboxID string
	UserID    string
	TeamID    string
	ProcdURL  string
}

// SessionAuthorizer authenticates an SSH public key and resolves the sandbox target.
type SessionAuthorizer interface {
	Authenticate(ctx context.Context, username string, key ssh.PublicKey) (*SessionTarget, error)
}

type identityStore interface {
	GetUserSSHPublicKeyByFingerprint(ctx context.Context, fingerprint string) (*identity.UserSSHPublicKey, error)
	GetTeamMember(ctx context.Context, teamID, userID string) (*identity.TeamMember, error)
}

type sandboxStore interface {
	GetSandboxInternal(ctx context.Context, sandboxID string) (*mgr.Sandbox, error)
	ResumeSandbox(ctx context.Context, sandboxID, userID, teamID string) error
}

// Authenticator resolves the uploaded SSH public key to a user and authorizes access to the requested sandbox.
type Authenticator struct {
	repo          identityStore
	manager       sandboxStore
	resumeTimeout time.Duration
	pollInterval  time.Duration
	logger        *zap.Logger
}

// NewAuthenticator creates a new SSH authenticator.
func NewAuthenticator(repo identityStore, manager sandboxStore, resumeTimeout, pollInterval time.Duration, logger *zap.Logger) *Authenticator {
	if resumeTimeout <= 0 {
		resumeTimeout = 30 * time.Second
	}
	if pollInterval <= 0 {
		pollInterval = 500 * time.Millisecond
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Authenticator{repo: repo, manager: manager, resumeTimeout: resumeTimeout, pollInterval: pollInterval, logger: logger}
}

// ParseSSHUsername extracts the sandbox ID from the SSH username.
// V1 only supports <sandbox_id> or <sandbox_id>+main.
func ParseSSHUsername(username string) (string, error) {
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return "", ErrInvalidSSHUsername
	}
	if strings.ContainsAny(trimmed, " /:@") {
		return "", ErrInvalidSSHUsername
	}
	base := trimmed
	if plus := strings.Index(trimmed, "+"); plus >= 0 {
		base = trimmed[:plus]
		target := trimmed[plus+1:]
		if target != "main" {
			return "", ErrInvalidSSHUsername
		}
	}
	if base == "" {
		return "", ErrInvalidSSHUsername
	}
	return base, nil
}

// Authenticate resolves the SSH public key to a user, authorizes the sandbox, and ensures procd is reachable.
func (a *Authenticator) Authenticate(ctx context.Context, username string, key ssh.PublicKey) (*SessionTarget, error) {
	sandboxID, err := ParseSSHUsername(username)
	if err != nil {
		return nil, err
	}

	storedKey, err := a.repo.GetUserSSHPublicKeyByFingerprint(ctx, ssh.FingerprintSHA256(key))
	if err != nil {
		if errors.Is(err, identity.ErrSSHPublicKeyNotFound) {
			return nil, ErrUnauthorizedSSHPublicKey
		}
		return nil, fmt.Errorf("lookup ssh public key: %w", err)
	}

	sandbox, err := a.manager.GetSandboxInternal(ctx, sandboxID)
	if err != nil {
		if errors.Is(err, managerclient.ErrSandboxNotFound) {
			return nil, ErrSandboxUnavailable
		}
		return nil, fmt.Errorf("get sandbox: %w", err)
	}

	if _, err := a.repo.GetTeamMember(ctx, sandbox.TeamID, storedKey.UserID); err != nil {
		if errors.Is(err, identity.ErrMemberNotFound) {
			return nil, ErrSandboxAccessDenied
		}
		return nil, fmt.Errorf("authorize sandbox membership: %w", err)
	}

	procdURL, err := a.ensureSandboxReady(ctx, sandboxID, storedKey.UserID, sandbox.TeamID, sandbox)
	if err != nil {
		return nil, err
	}

	return &SessionTarget{
		Username:  username,
		SandboxID: sandboxID,
		UserID:    storedKey.UserID,
		TeamID:    sandbox.TeamID,
		ProcdURL:  procdURL,
	}, nil
}

func (a *Authenticator) ensureSandboxReady(ctx context.Context, sandboxID, userID, teamID string, sandbox *mgr.Sandbox) (string, error) {
	if sandbox != nil && !sandboxWantsPaused(sandbox) && strings.TrimSpace(sandbox.InternalAddr) != "" {
		return sandbox.InternalAddr, nil
	}

	resumeCtx, cancel := context.WithTimeout(ctx, a.resumeTimeout)
	defer cancel()
	if err := a.manager.ResumeSandbox(resumeCtx, sandboxID, userID, teamID); err != nil {
		return "", fmt.Errorf("resume sandbox: %w", err)
	}

	ticker := time.NewTicker(a.pollInterval)
	defer ticker.Stop()

	for {
		current, err := a.manager.GetSandboxInternal(resumeCtx, sandboxID)
		if err != nil {
			if resumeCtx.Err() != nil {
				break
			}
			a.logger.Debug("Failed to refresh sandbox during SSH resume wait",
				zap.String("sandbox_id", sandboxID),
				zap.Error(err),
			)
		} else if !sandboxWantsPaused(current) && strings.TrimSpace(current.InternalAddr) != "" {
			return current.InternalAddr, nil
		}

		select {
		case <-resumeCtx.Done():
			return "", fmt.Errorf("%w: timed out waiting for sandbox to become reachable", ErrSandboxUnavailable)
		case <-ticker.C:
		}
	}

	return "", fmt.Errorf("%w: timed out waiting for sandbox to become reachable", ErrSandboxUnavailable)
}

func sandboxWantsPaused(sandbox *mgr.Sandbox) bool {
	if sandbox == nil {
		return false
	}
	if sandbox.PowerState.Desired == mgr.SandboxPowerStatePaused {
		return true
	}
	return sandbox.Paused
}
