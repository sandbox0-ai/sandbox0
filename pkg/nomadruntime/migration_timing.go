package nomadruntime

import "time"

// Migration timings report completed node stages without serializing their
// authority payloads. Operation IDs stay in logs, never metric labels.
func (d *nodeRuntime) logMigrationTiming(operation, stage string, started time.Time, fields ...any) {
	if d.logger.base == nil {
		return
	}
	args := []any{"operation_id", operation, "stage", stage, "duration_us", time.Since(started).Microseconds()}
	d.logger.Info("Migration node stage completed", append(args, fields...)...)
}
