package registry

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

func TestSecretReaderReadRequiredUsesDefaultKey(t *testing.T) {
	t.Parallel()

	reader := secretReader{
		secretLister: newRegistrySecretLister(t, "sandbox0-system", &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "sandbox0-system",
				Name:      "registry-auth",
			},
			Data: map[string][]byte{
				"username": []byte("robot"),
			},
		}),
		namespace: "sandbox0-system",
	}

	got, err := reader.readRequired(context.Background(), "registry-auth", "", "username", "registry username")
	if err != nil {
		t.Fatalf("readRequired returned error: %v", err)
	}
	if got != "robot" {
		t.Fatalf("value = %q, want %q", got, "robot")
	}
}

func TestSecretReaderReadRequiredUsesConfiguredKey(t *testing.T) {
	t.Parallel()

	reader := secretReader{
		secretLister: newRegistrySecretLister(t, "sandbox0-system", &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "sandbox0-system",
				Name:      "registry-auth",
			},
			Data: map[string][]byte{
				"custom-password": []byte("secret-token"),
			},
		}),
		namespace: "sandbox0-system",
	}

	got, err := reader.readRequired(context.Background(), "registry-auth", "custom-password", "password", "registry password")
	if err != nil {
		t.Fatalf("readRequired returned error: %v", err)
	}
	if got != "secret-token" {
		t.Fatalf("value = %q, want %q", got, "secret-token")
	}
}

func TestSecretReaderReadRequiredWrapsReadError(t *testing.T) {
	t.Parallel()

	reader := secretReader{
		secretLister: newRegistrySecretLister(t, "sandbox0-system", &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "sandbox0-system",
				Name:      "registry-auth",
			},
		}),
		namespace: "sandbox0-system",
	}

	_, err := reader.readRequired(context.Background(), "registry-auth", "", "username", "registry username")
	if err == nil {
		t.Fatal("expected error")
	}
	want := `read registry username: secret "registry-auth" has no data`
	if err.Error() != want {
		t.Fatalf("error = %q, want %q", err.Error(), want)
	}
}

func newRegistrySecretLister(t *testing.T, namespace string, secrets ...*corev1.Secret) v1.SecretLister {
	t.Helper()

	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		cache.NamespaceIndex: cache.MetaNamespaceIndexFunc,
	})
	for _, secret := range secrets {
		if secret.Namespace == "" {
			secret.Namespace = namespace
		}
		if err := indexer.Add(secret); err != nil {
			t.Fatalf("add secret %q: %v", secret.Name, err)
		}
	}
	return v1.NewSecretLister(indexer)
}
