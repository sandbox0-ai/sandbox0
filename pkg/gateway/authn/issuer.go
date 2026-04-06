package authn

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

// TeamGrant represents one team membership grant embedded into an access token.
type TeamGrant struct {
	TeamID       string `json:"team_id"`
	TeamRole     string `json:"team_role"`
	HomeRegionID string `json:"home_region_id,omitempty"`
}

var (
	ErrInvalidToken         = errors.New("invalid token")
	ErrTokenExpired         = errors.New("token expired")
	ErrInvalidSigningMethod = errors.New("invalid signing method")
	ErrJWTNotConfigured     = errors.New("JWT authentication not configured")
	ErrJWTSigningDisabled   = errors.New("JWT signing is not configured")
)

// Claims represents JWT claims for human session tokens.
type Claims struct {
	jwt.RegisteredClaims
	UserID     string      `json:"user_id"`
	Email      string      `json:"email"`
	Name       string      `json:"name"`
	TeamGrants []TeamGrant `json:"team_grants,omitempty"`
	IsAdmin    bool        `json:"is_admin"`
	TokenType  string      `json:"token_type"`
}

// FindTeamGrant returns the embedded grant for the requested team ID.
func (c *Claims) FindTeamGrant(teamID string) (TeamGrant, bool) {
	if c == nil {
		return TeamGrant{}, false
	}
	for _, grant := range c.TeamGrants {
		if grant.TeamID == teamID {
			return grant, true
		}
	}
	return TeamGrant{}, false
}

// Issuer handles JWT token creation and validation.
type Issuer struct {
	signingMethod   jwt.SigningMethod
	signingKey      any
	verificationKey any
	accessTokenTTL  time.Duration
	refreshTokenTTL time.Duration
	issuer          string
	now             func() time.Time
	newSessionID    func() (string, error)
}

// TokenPair represents an access/refresh token pair.
type TokenPair struct {
	AccessToken      string    `json:"access_token"`
	RefreshToken     string    `json:"refresh_token"`
	ExpiresAt        time.Time `json:"expires_at"`
	RefreshExpiresAt time.Time `json:"refresh_expires_at"`
}

// NewIssuer creates a new JWT issuer.
func NewIssuer(issuerName, secret string, accessTTL, refreshTTL time.Duration) *Issuer {
	secretBytes := []byte(secret)
	return newIssuerWithDeps(issuerName, jwt.SigningMethodHS256, secretBytes, secretBytes, accessTTL, refreshTTL, time.Now, generateSessionID)
}

// NewIssuerWithEd25519 creates a JWT issuer backed by an Ed25519 keypair.
func NewIssuerWithEd25519(issuerName string, privateKey ed25519.PrivateKey, accessTTL, refreshTTL time.Duration) *Issuer {
	var publicKey ed25519.PublicKey
	if privateKey != nil {
		if derived, ok := privateKey.Public().(ed25519.PublicKey); ok {
			publicKey = derived
		}
	}
	return newIssuerWithDeps(issuerName, jwt.SigningMethodEdDSA, privateKey, publicKey, accessTTL, refreshTTL, time.Now, generateSessionID)
}

// NewVerifierWithEd25519 creates a verifier-only JWT issuer backed by an Ed25519 public key.
func NewVerifierWithEd25519(issuerName string, publicKey ed25519.PublicKey) *Issuer {
	return newIssuerWithDeps(issuerName, jwt.SigningMethodEdDSA, nil, publicKey, 0, 0, time.Now, generateSessionID)
}

// NewIssuerFromConfig builds an issuer or verifier from gateway config values.
func NewIssuerFromConfig(issuerName, secret, privateKeyPEM, publicKeyPEM, privateKeyFile, publicKeyFile string, accessTTL, refreshTTL time.Duration) (*Issuer, error) {
	loadedPrivateKey, err := loadEd25519PrivateKey(privateKeyPEM, privateKeyFile)
	if err != nil {
		return nil, err
	}
	loadedPublicKey, err := loadEd25519PublicKey(publicKeyPEM, publicKeyFile)
	if err != nil {
		return nil, err
	}
	if loadedPrivateKey != nil {
		issuer := NewIssuerWithEd25519(issuerName, loadedPrivateKey, accessTTL, refreshTTL)
		if loadedPublicKey != nil {
			issuer.verificationKey = loadedPublicKey
		}
		return issuer, nil
	}
	if loadedPublicKey != nil {
		return NewVerifierWithEd25519(issuerName, loadedPublicKey), nil
	}
	if strings.TrimSpace(secret) != "" {
		return NewIssuer(issuerName, secret, accessTTL, refreshTTL), nil
	}
	return newIssuerWithDeps(issuerName, nil, nil, nil, accessTTL, refreshTTL, time.Now, generateSessionID), nil
}

func loadEd25519PrivateKey(privateKeyPEM, privateKeyFile string) (ed25519.PrivateKey, error) {
	if trimmed := strings.TrimSpace(privateKeyPEM); trimmed != "" {
		return internalauth.LoadEd25519PrivateKey([]byte(trimmed))
	}
	if trimmed := strings.TrimSpace(privateKeyFile); trimmed != "" {
		return internalauth.LoadEd25519PrivateKeyFromFile(trimmed)
	}
	return nil, nil
}

func loadEd25519PublicKey(publicKeyPEM, publicKeyFile string) (ed25519.PublicKey, error) {
	if trimmed := strings.TrimSpace(publicKeyPEM); trimmed != "" {
		return internalauth.LoadEd25519PublicKey([]byte(trimmed))
	}
	if trimmed := strings.TrimSpace(publicKeyFile); trimmed != "" {
		return internalauth.LoadEd25519PublicKeyFromFile(trimmed)
	}
	return nil, nil
}

func newIssuerWithDeps(
	issuerName string,
	signingMethod jwt.SigningMethod,
	signingKey any,
	verificationKey any,
	accessTTL, refreshTTL time.Duration,
	now func() time.Time,
	newSessionID func() (string, error),
) *Issuer {
	return &Issuer{
		signingMethod:   signingMethod,
		signingKey:      signingKey,
		verificationKey: verificationKey,
		accessTokenTTL:  accessTTL,
		refreshTokenTTL: refreshTTL,
		issuer:          issuerName,
		now:             now,
		newSessionID:    newSessionID,
	}
}

// IssuerName returns the configured JWT issuer identifier.
func (i *Issuer) IssuerName() string {
	if i == nil {
		return ""
	}
	return i.issuer
}

// IssueTokenPair issues both access and refresh tokens.
func (i *Issuer) IssueTokenPair(userID, email, name string, isAdmin bool, teamGrants []TeamGrant) (*TokenPair, error) {
	if i == nil || i.signingMethod == nil || i.signingKey == nil {
		return nil, ErrJWTSigningDisabled
	}

	now := time.Now
	if i.now != nil {
		now = i.now
	}
	sessionID := generateSessionID
	if i.newSessionID != nil {
		sessionID = i.newSessionID
	}

	sessionTokenID, err := sessionID()
	if err != nil {
		return nil, err
	}

	currentTime := now()
	accessExpiry := currentTime.Add(i.accessTokenTTL)
	refreshExpiry := currentTime.Add(i.refreshTokenTTL)

	accessClaims := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    i.issuer,
			Subject:   userID,
			ID:        sessionTokenID,
			ExpiresAt: jwt.NewNumericDate(accessExpiry),
			IssuedAt:  jwt.NewNumericDate(currentTime),
			NotBefore: jwt.NewNumericDate(currentTime),
		},
		UserID:     userID,
		Email:      email,
		Name:       name,
		TeamGrants: teamGrants,
		IsAdmin:    isAdmin,
		TokenType:  "access",
	}

	accessToken := jwt.NewWithClaims(i.signingMethod, accessClaims)
	accessTokenString, err := accessToken.SignedString(i.signingKey)
	if err != nil {
		return nil, err
	}

	refreshClaims := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    i.issuer,
			Subject:   userID,
			ID:        sessionTokenID,
			ExpiresAt: jwt.NewNumericDate(refreshExpiry),
			IssuedAt:  jwt.NewNumericDate(currentTime),
			NotBefore: jwt.NewNumericDate(currentTime),
		},
		UserID:    userID,
		TokenType: "refresh",
	}

	refreshToken := jwt.NewWithClaims(i.signingMethod, refreshClaims)
	refreshTokenString, err := refreshToken.SignedString(i.signingKey)
	if err != nil {
		return nil, err
	}

	return &TokenPair{
		AccessToken:      accessTokenString,
		RefreshToken:     refreshTokenString,
		ExpiresAt:        accessExpiry,
		RefreshExpiresAt: refreshExpiry,
	}, nil
}

func generateSessionID() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(bytes), nil
}

// ValidateAccessToken validates an access token and returns claims.
func (i *Issuer) ValidateAccessToken(tokenString string) (*Claims, error) {
	claims, err := i.validateToken(tokenString)
	if err != nil {
		return nil, err
	}
	if claims.TokenType != "access" {
		return nil, ErrInvalidToken
	}
	return claims, nil
}

// ValidateRefreshToken validates a refresh token.
func (i *Issuer) ValidateRefreshToken(tokenString string) (*Claims, error) {
	claims, err := i.validateToken(tokenString)
	if err != nil {
		return nil, err
	}
	if claims.TokenType != "refresh" {
		return nil, ErrInvalidToken
	}
	return claims, nil
}

func (i *Issuer) validateToken(tokenString string) (*Claims, error) {
	if i == nil || i.verificationKey == nil || i.signingMethod == nil {
		return nil, ErrJWTNotConfigured
	}

	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (any, error) {
		if token.Method.Alg() != i.signingMethod.Alg() {
			return nil, ErrInvalidSigningMethod
		}
		return i.verificationKey, nil
	})
	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, ErrTokenExpired
		}
		return nil, ErrInvalidToken
	}

	if !token.Valid {
		return nil, ErrInvalidToken
	}

	claims, ok := token.Claims.(*Claims)
	if !ok {
		return nil, ErrInvalidToken
	}
	return claims, nil
}

// GenerateRefreshTokenHash generates a random hash for DB storage.
func GenerateRefreshTokenHash() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(bytes), nil
}

// HashRefreshToken returns a deterministic hash for refresh token persistence/lookup.
func HashRefreshToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
