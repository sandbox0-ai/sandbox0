package db

import (
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound    = errors.New("not found")
	ErrInvalidKey  = errors.New("invalid api key")
	ErrExpiredKey  = errors.New("api key expired")
	ErrInactiveKey = errors.New("api key inactive")
)

// Repository provides database access for edge-gateway
type Repository struct {
	pool *pgxpool.Pool
}

// NewRepository creates a new database repository
func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// Pool returns the underlying connection pool
func (r *Repository) Pool() *pgxpool.Pool {
	return r.pool
}
