package runtime

import (
	"context"
	"fmt"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/egressauthstore"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
)

type resolveResult struct {
	Response *egressauth.ResolveResponse
	TTL      time.Duration
}

type provider interface {
	Resolve(
		ctx context.Context,
		req *egressauth.ResolveRequest,
		binding *egressauthstore.CredentialBinding,
		source *egressauthstore.CredentialSourceVersion,
		defaultTTL time.Duration,
	) (*resolveResult, error)
}

// UnsupportedProviderError indicates the binding references a provider that is
// not available in this runtime resolver process.
type UnsupportedProviderError struct {
	Provider string
}

func (e *UnsupportedProviderError) Error() string {
	return fmt.Sprintf("credential binding provider %q is not supported", e.Provider)
}
