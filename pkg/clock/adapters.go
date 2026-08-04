package clock

import (
	"context"

	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)

// PGXQueryRower is implemented by pgx.Conn and pgxpool.Pool. Its concrete
// pgx.Row return type cannot directly satisfy DB because Go interfaces do not
// support covariant return types.
type PGXQueryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type pgxDB struct {
	queryRower PGXQueryRower
}

func (db pgxDB) QueryRow(ctx context.Context, sql string, args ...any) Row {
	return db.queryRower.QueryRow(ctx, sql, args...)
}

// NewPGX creates a synchronized clock backed by a pgx connection or pool.
func NewPGX(ctx context.Context, db PGXQueryRower, opts ...Option) (*Clock, error) {
	return New(ctx, pgxDB{queryRower: db}, opts...)
}

// WithZapLogger configures structured clock logs on a zap logger.
func WithZapLogger(logger *zap.Logger) Option {
	if logger == nil {
		return WithLogger(nil)
	}
	return WithLogger(zapLogger{logger: logger})
}

type zapLogger struct {
	logger *zap.Logger
}

func (logger zapLogger) Info(msg string, keysAndValues ...any) {
	logger.logger.Info(msg, zapFields(keysAndValues)...)
}

func (logger zapLogger) Warn(msg string, keysAndValues ...any) {
	logger.logger.Warn(msg, zapFields(keysAndValues)...)
}

func (logger zapLogger) Error(msg string, keysAndValues ...any) {
	logger.logger.Error(msg, zapFields(keysAndValues)...)
}

func zapFields(keysAndValues []any) []zap.Field {
	if len(keysAndValues)%2 != 0 {
		return []zap.Field{zap.Any("args", keysAndValues)}
	}

	fields := make([]zap.Field, 0, len(keysAndValues)/2)
	for i := 0; i < len(keysAndValues); i += 2 {
		key, ok := keysAndValues[i].(string)
		if !ok {
			continue
		}
		fields = append(fields, zap.Any(key, keysAndValues[i+1]))
	}
	return fields
}
