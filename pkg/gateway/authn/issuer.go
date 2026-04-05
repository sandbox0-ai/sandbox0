package authn

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"time"

	"github.com/golang-jwt/jwt/v5"
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
	secret          []byte
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
	return newIssuerWithDeps(issuerName, secret, accessTTL, refreshTTL, time.Now, generateSessionID)
}

func newIssuerWithDeps(
	issuerName, secret string,
	accessTTL, refreshTTL time.Duration,
	now func() time.Time,
	newSessionID func() (string, error),
) *Issuer {
	return &Issuer{
		secret:          []byte(secret),
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
	if len(i.secret) == 0 {
		return nil, ErrJWTNotConfigured
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

	accessToken := jwt.NewWithClaims(jwt.SigningMethodHS256, accessClaims)
	accessTokenString, err := accessToken.SignedString(i.secret)
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

	refreshToken := jwt.NewWithClaims(jwt.SigningMethodHS256, refreshClaims)
	refreshTokenString, err := refreshToken.SignedString(i.secret)
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
	if len(i.secret) == 0 {
		return nil, ErrJWTNotConfigured
	}

	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, ErrInvalidSigningMethod
		}
		return i.secret, nil
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
