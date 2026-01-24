package jwt

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var (
	ErrInvalidToken         = errors.New("invalid token")
	ErrTokenExpired         = errors.New("token expired")
	ErrInvalidSigningMethod = errors.New("invalid signing method")
)

// Claims represents JWT claims
type Claims struct {
	jwt.RegisteredClaims
	UserID    string `json:"user_id"`
	TeamID    string `json:"team_id"`
	Email     string `json:"email"`
	Name      string `json:"name"`
	TeamRole  string `json:"team_role"`
	IsAdmin   bool   `json:"is_admin"`
	TokenType string `json:"token_type"` // "access" or "refresh"
}

// Issuer handles JWT token creation and validation
type Issuer struct {
	secret          []byte
	accessTokenTTL  time.Duration
	refreshTokenTTL time.Duration
	issuer          string
}

// NewIssuer creates a new JWT issuer
func NewIssuer(issuerName, secret string, accessTTL, refreshTTL time.Duration) *Issuer {
	return &Issuer{
		secret:          []byte(secret),
		accessTokenTTL:  accessTTL,
		refreshTokenTTL: refreshTTL,
		issuer:          issuerName,
	}
}

// TokenPair represents an access/refresh token pair
type TokenPair struct {
	AccessToken      string    `json:"access_token"`
	RefreshToken     string    `json:"refresh_token"`
	ExpiresAt        time.Time `json:"expires_at"`
	RefreshExpiresAt time.Time `json:"refresh_expires_at"`
}

// IssueTokenPair issues both access and refresh tokens
func (i *Issuer) IssueTokenPair(userID, teamID, teamRole, email, name string, isAdmin bool) (*TokenPair, error) {
	now := time.Now()
	accessExpiry := now.Add(i.accessTokenTTL)
	refreshExpiry := now.Add(i.refreshTokenTTL)

	// Generate access token
	accessClaims := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    i.issuer,
			Subject:   userID,
			ExpiresAt: jwt.NewNumericDate(accessExpiry),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
		},
		UserID:    userID,
		TeamID:    teamID,
		Email:     email,
		Name:      name,
		TeamRole:  teamRole,
		IsAdmin:   isAdmin,
		TokenType: "access",
	}

	accessToken := jwt.NewWithClaims(jwt.SigningMethodHS256, accessClaims)
	accessTokenString, err := accessToken.SignedString(i.secret)
	if err != nil {
		return nil, err
	}

	// Generate refresh token (simpler, for rotation)
	refreshClaims := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    i.issuer,
			Subject:   userID,
			ExpiresAt: jwt.NewNumericDate(refreshExpiry),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
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

// ValidateAccessToken validates an access token and returns claims
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

// ValidateRefreshToken validates a refresh token
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

// validateToken validates a JWT token
func (i *Issuer) validateToken(tokenString string) (*Claims, error) {
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

// GenerateRefreshTokenHash generates a random hash for DB storage
func GenerateRefreshTokenHash() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(bytes), nil
}
