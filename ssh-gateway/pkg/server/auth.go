package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
	sharedssh "github.com/sandbox0-ai/sandbox0/pkg/sshgateway"
	"go.uber.org/zap"
	"golang.org/x/crypto/ssh"
)

var (
	ErrInvalidSSHUsername       = errors.New("invalid ssh username")
	ErrUnauthorizedSSHPublicKey = errors.New("ssh public key is not authorized")
	ErrSandboxAccessDenied      = errors.New("sandbox access denied")
	ErrSandboxUnavailable       = errors.New("sandbox unavailable")
	ErrSandboxWakingUp          = errors.New("sandbox is waking up")
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
}

type sandboxResolver interface {
	ResolveSandbox(ctx context.Context, sandboxID, userID string) (*sharedssh.ResolvedTarget, error)
}

// Authenticator resolves the uploaded SSH public key to a user and asks the
// regional routing layer to authorize and resolve the sandbox runtime target.
type Authenticator struct {
	repo          identityStore
	resolver      sandboxResolver
	resumeTimeout time.Duration
	pollInterval  time.Duration
	logger        *zap.Logger
}

// NewAuthenticator creates a new SSH authenticator.
func NewAuthenticator(repo identityStore, resolver sandboxResolver, resumeTimeout, pollInterval time.Duration, logger *zap.Logger) *Authenticator {
	if resumeTimeout <= 0 {
		resumeTimeout = 30 * time.Second
	}
	if pollInterval <= 0 {
		pollInterval = 500 * time.Millisecond
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Authenticator{
		repo:          repo,
		resolver:      resolver,
		resumeTimeout: resumeTimeout,
		pollInterval:  pollInterval,
		logger:        logger,
	}
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

// Authenticate resolves the SSH public key to a user, authorizes the sandbox,
// and waits for the regional routing layer to report a reachable procd target.
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

	resolveCtx, cancel := context.WithTimeout(ctx, a.resumeTimeout)
	defer cancel()

	ticker := time.NewTicker(a.pollInterval)
	defer ticker.Stop()

	for {
		target, err := a.resolver.ResolveSandbox(resolveCtx, sandboxID, storedKey.UserID)
		if err == nil {
			if target == nil || strings.TrimSpace(target.ProcdURL) == "" || strings.TrimSpace(target.TeamID) == "" {
				return nil, fmt.Errorf("regional sandbox resolver returned incomplete target")
			}
			return &SessionTarget{
				Username:  username,
				SandboxID: sandboxID,
				UserID:    storedKey.UserID,
				TeamID:    target.TeamID,
				ProcdURL:  target.ProcdURL,
			}, nil
		}

		if errors.Is(err, ErrSandboxWakingUp) {
			select {
			case <-resolveCtx.Done():
				return nil, fmt.Errorf("%w: timed out waiting for sandbox to become reachable", ErrSandboxUnavailable)
			case <-ticker.C:
				continue
			}
		}

		if errors.Is(err, ErrSandboxAccessDenied) || errors.Is(err, ErrSandboxUnavailable) {
			return nil, err
		}
		return nil, err
	}
}
