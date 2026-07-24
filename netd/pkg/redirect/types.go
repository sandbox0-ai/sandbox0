package redirect

import "context"

type Manager interface {
	Sync(ctx context.Context, sandboxIPs []string, bypassCIDRs []string) error
	ForceSync(ctx context.Context, sandboxIPs []string, bypassCIDRs []string) error
	Cleanup(ctx context.Context) error
}

type Config struct {
	PreferNFT      bool
	ProxyHTTPPort  int
	ProxyHTTPSPort int
}
