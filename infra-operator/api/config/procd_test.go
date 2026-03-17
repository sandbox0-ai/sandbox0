package config

import "testing"

func TestProcdConfigEnvKeysIncludesStorageProxyBaseURL(t *testing.T) {
	keys := ProcdConfig{}.EnvKeys()
	set := map[string]struct{}{}
	for _, key := range keys {
		set[key] = struct{}{}
	}

	if _, ok := set["storage_proxy_base_url"]; !ok {
		t.Fatalf("expected storage_proxy_base_url to be an env key")
	}
}
