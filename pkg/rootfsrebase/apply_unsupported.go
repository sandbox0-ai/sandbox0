//go:build !linux

package rootfsrebase

import (
	"context"
	"fmt"
)

func Apply(context.Context, ApplyRequest) (*ApplyResult, error) {
	return nil, fmt.Errorf("RootFS rebase apply is only supported on Linux")
}
