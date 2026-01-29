package proxy

import "net/http"

// RequestModifier mutates an outgoing request before proxying.
type RequestModifier func(*http.Request)

type Option func(*options)

type options struct {
	requestModifiers []RequestModifier
	httpClient       *http.Client
}

// WithRequestModifier registers a request modifier for proxy requests.
func WithRequestModifier(mod RequestModifier) Option {
	return func(o *options) {
		if mod == nil {
			return
		}
		o.requestModifiers = append(o.requestModifiers, mod)
	}
}

// WithHTTPClient sets a custom HTTP client for the proxy.
// This allows for custom transport configurations including observability.
func WithHTTPClient(client *http.Client) Option {
	return func(o *options) {
		o.httpClient = client
	}
}

func collectOptions(opts ...Option) options {
	var o options
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o
}

func applyRequestModifiers(req *http.Request, mods []RequestModifier) {
	for _, mod := range mods {
		if mod != nil {
			mod(req)
		}
	}
}
