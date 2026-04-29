/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package common

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
)

const (
	ObjectEncryptionSecretSuffix = "object-encryption-key"
	ObjectEncryptionSecretKey    = "private.key"
	ObjectEncryptionKeyFilename  = "object_rsa_private.pem"
	ObjectEncryptionMountDir     = "/etc/storage-proxy/objectstore"
	ObjectEncryptionKeyPath      = ObjectEncryptionMountDir + "/" + ObjectEncryptionKeyFilename
)

// ObjectEncryptionSecretName returns the shared RSA key secret name for S0FS object encryption.
func ObjectEncryptionSecretName(infraName string) string {
	return fmt.Sprintf("%s-%s", infraName, ObjectEncryptionSecretSuffix)
}

// EnsureObjectEncryptionKeySecret creates or repairs the shared S0FS object encryption key secret.
func EnsureObjectEncryptionKeySecret(ctx context.Context, resources *ResourceManager, infra *infrav1alpha1.Sandbox0Infra) error {
	logger := log.FromContext(ctx)
	secretName := ObjectEncryptionSecretName(infra.Name)
	secret := &corev1.Secret{}
	err := resources.Client.Get(ctx, types.NamespacedName{Name: secretName, Namespace: infra.Namespace}, secret)
	if err == nil {
		if len(secret.Data[ObjectEncryptionSecretKey]) > 0 {
			return nil
		}
		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}
		privateKeyPEM, err := generateObjectEncryptionPrivateKey()
		if err != nil {
			return err
		}
		secret.Data[ObjectEncryptionSecretKey] = privateKeyPEM
		return resources.Client.Update(ctx, secret)
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	privateKeyPEM, err := generateObjectEncryptionPrivateKey()
	if err != nil {
		return err
	}
	secret = &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: infra.Namespace,
			Labels:    GetServiceLabels(infra.Name, "object-encryption"),
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			ObjectEncryptionSecretKey: privateKeyPEM,
		},
	}
	if err := NewObjectScope(infra).SetControllerReference(secret, resources.Scheme); err != nil {
		return err
	}
	logger.Info("Creating object encryption key secret", "secretName", secretName)
	if err := resources.Client.Create(ctx, secret); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return err
	}
	return nil
}

func generateObjectEncryptionPrivateKey() ([]byte, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("generate RSA private key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	}), nil
}
