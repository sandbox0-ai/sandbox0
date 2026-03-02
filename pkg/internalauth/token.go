package internalauth

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Generator is responsible for generating internal authentication tokens.
type Generator struct {
	mu     sync.RWMutex
	config GeneratorConfig
}

// NewGenerator creates a new Generator with the given configuration.
func NewGenerator(config GeneratorConfig) *Generator {
	if config.Caller == "" {
		panic("internalauth: caller cannot be empty")
	}
	if config.PrivateKey == nil {
		panic("internalauth: private key cannot be empty")
	}
	if config.TTL == 0 {
		config.TTL = 30 * time.Second
	}
	return &Generator{
		config: config,
	}
}

// Generate creates a new JWT token for the specified target service.
//
// Parameters:
//   - target: The service being called (e.g., "storage-proxy", "procd")
//   - teamID: The team ID for authorization context
//   - userID: Optional user ID for audit logging
//   - opts: Optional parameters (permissions, custom TTL, etc.)
//
// Returns a signed JWT token string.
func (g *Generator) Generate(target, teamID, userID string, opts GenerateOptions) (string, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if target == "" {
		return "", errors.New("internalauth: target cannot be empty")
	}
	if teamID == "" {
		return "", errors.New("internalauth: teamID cannot be empty")
	}

	ttl := g.config.TTL
	if opts.TTL > 0 {
		ttl = opts.TTL
	}

	now := time.Now()
	if g.config.NowFunc != nil {
		now = g.config.NowFunc()
	}

	claims := &Claims{
		Issuer:      g.config.Caller,
		Subject:     teamID,
		Audience:    target,
		IssuedAt:    jwt.NewNumericDate(now),
		ExpiresAt:   jwt.NewNumericDate(now.Add(ttl)),
		ID:          generateJTI(),
		Caller:      g.config.Caller,
		Target:      target,
		TeamID:      teamID,
		UserID:      userID,
		SandboxID:   opts.SandboxID,
		Permissions: opts.Permissions,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	signed, err := token.SignedString(g.config.PrivateKey)
	if err != nil {
		return "", fmt.Errorf("failed to sign token: %w", err)
	}

	return signed, nil
}

// MustGenerate is like Generate but panics on error.
// Useful for initialization where failure is fatal.
func (g *Generator) MustGenerate(target, teamID, userID string, opts GenerateOptions) string {
	token, err := g.Generate(target, teamID, userID, opts)
	if err != nil {
		panic(err)
	}
	return token
}

// GenerateSystem creates a system-level JWT token for internal service communication.
// System tokens are not bound to a specific team and have full access.
//
// Parameters:
//   - target: The service being called (e.g., "manager", "storage-proxy")
//   - opts: Optional parameters (permissions, custom TTL, etc.)
//
// Returns a signed JWT token string.
func (g *Generator) GenerateSystem(target string, opts GenerateOptions) (string, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if target == "" {
		return "", errors.New("internalauth: target cannot be empty")
	}

	ttl := g.config.TTL
	if opts.TTL > 0 {
		ttl = opts.TTL
	}

	now := time.Now()
	if g.config.NowFunc != nil {
		now = g.config.NowFunc()
	}

	claims := &Claims{
		Issuer:      g.config.Caller,
		Subject:     "system",
		Audience:    target,
		IssuedAt:    jwt.NewNumericDate(now),
		ExpiresAt:   jwt.NewNumericDate(now.Add(ttl)),
		ID:          generateJTI(),
		Caller:      g.config.Caller,
		Target:      target,
		IsSystem:    true,
		Permissions: opts.Permissions,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	signed, err := token.SignedString(g.config.PrivateKey)
	if err != nil {
		return "", fmt.Errorf("failed to sign token: %w", err)
	}

	return signed, nil
}

// MustGenerateSystem is like GenerateSystem but panics on error.
func (g *Generator) MustGenerateSystem(target string, opts GenerateOptions) string {
	token, err := g.GenerateSystem(target, opts)
	if err != nil {
		panic(err)
	}
	return token
}

// Validator is responsible for validating internal authentication tokens.
type Validator struct {
	mu     sync.RWMutex
	config ValidatorConfig
	// jtiCache tracks used token IDs for replay detection.
	jtiCache map[string]time.Time
	jtiMu    sync.RWMutex
}

// NewValidator creates a new Validator with the given configuration.
func NewValidator(config ValidatorConfig) *Validator {
	if config.Target == "" {
		panic("internalauth: target cannot be empty")
	}
	if config.PublicKey == nil {
		panic("internalauth: public key cannot be empty")
	}
	return &Validator{
		config:   config,
		jtiCache: make(map[string]time.Time),
	}
}

// Validate validates a JWT token and returns the claims if valid.
//
// The token is validated against:
//   - Ed25519 signature
//   - Expiration time (with clock skew tolerance)
//   - Target service (audience)
//   - Caller (must be in allowed callers if specified)
//   - Replay attack (if enabled)
func (v *Validator) Validate(tokenString string) (*Claims, error) {
	return v.ValidateWithOptions(tokenString, ValidateOptions{})
}

// ValidateWithOptions validates a token with additional validation options.
func (v *Validator) ValidateWithOptions(tokenString string, opts ValidateOptions) (*Claims, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	now := time.Now()
	if v.config.NowFunc != nil {
		now = v.config.NowFunc()
	}

	// Parse and verify signature
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (any, error) {
		// Verify signing method is Ed25519
		if token.Method != jwt.SigningMethodEdDSA {
			return nil, ErrInvalidSignature
		}
		return v.config.PublicKey, nil
	})

	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, ErrTokenExpired
		}
		if errors.Is(err, jwt.ErrSignatureInvalid) || errors.Is(err, jwt.ErrTokenSignatureInvalid) {
			return nil, fmt.Errorf("%w: %v", ErrInvalidSignature, err)
		}
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}

	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, ErrInvalidToken
	}

	// Check expiration with clock skew tolerance
	expiresAt := claims.ExpiresAt.Time
	if now.After(expiresAt.Add(v.config.ClockSkewTolerance)) {
		return nil, ErrTokenExpired
	}

	// Validate target (audience)
	if !opts.SkipTargetCheck {
		if claims.Audience != v.config.Target {
			return nil, fmt.Errorf("%w: expected %s, got %s", ErrInvalidTarget, v.config.Target, claims.Audience)
		}
	}

	// Validate caller
	if !opts.SkipCallerCheck {
		if claims.Caller != claims.Issuer {
			return nil, fmt.Errorf("%w: caller mismatch", ErrInvalidCaller)
		}
		if len(v.config.AllowedCallers) > 0 {
			allowed := false
			for _, c := range v.config.AllowedCallers {
				if c == claims.Caller {
					allowed = true
					break
				}
			}
			if !allowed {
				return nil, fmt.Errorf("%w: caller %s not in allowed list", ErrInvalidCaller, claims.Caller)
			}
		}
	}

	// Validate required fields
	// System tokens bypass team ID requirement
	if opts.RequireTeamID && claims.TeamID == "" && !claims.IsSystem {
		return nil, errors.New("internalauth: team_id is required")
	}

	// Validate permissions
	if len(opts.RequirePermissions) > 0 {
		if !hasPermissions(claims.Permissions, opts.RequirePermissions) {
			return nil, fmt.Errorf("internalauth: missing required permissions: %v", opts.RequirePermissions)
		}
	}

	// Check for replay attack
	if v.config.ReplayDetectionEnabled {
		if v.isReplay(claims.ID) {
			return nil, ErrReplayAttack
		}
		v.recordJTI(claims.ID, expiresAt)
	}

	return claims, nil
}

// isReplay checks if the JTI has been used before.
func (v *Validator) isReplay(jti string) bool {
	v.jtiMu.RLock()
	defer v.jtiMu.RUnlock()
	_, exists := v.jtiCache[jti]
	return exists
}

// recordJTI records a JTI as used.
func (v *Validator) recordJTI(jti string, expiresAt time.Time) {
	v.jtiMu.Lock()
	v.jtiCache[jti] = expiresAt
	cacheSize := len(v.jtiCache)
	v.jtiMu.Unlock()

	// Only cleanup occasionally to avoid spawning too many goroutines
	if cacheSize%100 == 0 {
		go v.cleanupExpiredJTI(time.Now())
	}
}

// cleanupExpiredJTI removes JTIs that have expired.
func (v *Validator) cleanupExpiredJTI(now time.Time) {
	v.jtiMu.Lock()
	defer v.jtiMu.Unlock()
	for jti, expiresAt := range v.jtiCache {
		if now.After(expiresAt.Add(5 * time.Minute)) {
			delete(v.jtiCache, jti)
		}
	}
}

// hasPermissions checks if the claims have all required permissions.
func hasPermissions(have, require []string) bool {
	haveMap := make(map[string]string)
	for _, p := range have {
		haveMap[p] = p
	}
	for _, req := range require {
		if _, ok := haveMap[req]; !ok {
			// Check for wildcard permission
			if _, ok := haveMap["*"]; !ok {
				return false
			}
		}
	}
	return true
}
