package dbpool

import "context"

type operationKey struct{}

// WithOperation tags a database call for pool diagnostics. Tracers must map
// these values to an explicit allowlist before using them as metric labels.
func WithOperation(ctx context.Context, operation string) context.Context {
	return context.WithValue(ctx, operationKey{}, operation)
}

// Operation returns the caller's tag; it contains no SQL or request identifiers.
func Operation(ctx context.Context) string {
	operation, _ := ctx.Value(operationKey{}).(string)
	return operation
}
