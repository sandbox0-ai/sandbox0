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

package webhook

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

func (r *Reconciler) ReconcileCertSecret(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, name string, labels map[string]string, dnsNames []string) error {
	secret := &corev1.Secret{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, secret)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	secretExists := err == nil

	if secretExists {
		if validateTLSSecret(secret) == nil {
			return nil
		}
	}

	certPEM, keyPEM, err := generateSelfSignedTLSCert(dnsNames)
	if err != nil {
		return err
	}

	desired := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: infra.Namespace,
			Labels:    labels,
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{
			corev1.TLSCertKey:       certPEM,
			corev1.TLSPrivateKeyKey: keyPEM,
		},
	}
	if err := ctrl.SetControllerReference(infra, desired, r.Resources.Scheme); err != nil {
		return err
	}

	if !secretExists {
		if err := r.Resources.Client.Create(ctx, desired); err != nil {
			return fmt.Errorf("create tls secret %q: %w", desired.Name, err)
		}
		return nil
	}

	secret.Type = desired.Type
	secret.Data = desired.Data
	secret.Labels = desired.Labels
	secret.OwnerReferences = desired.OwnerReferences
	if err := r.Resources.Client.Update(ctx, secret); err != nil {
		return fmt.Errorf("update tls secret %q: %w", secret.Name, err)
	}
	return nil
}

func validateTLSSecret(secret *corev1.Secret) error {
	if secret.Type != corev1.SecretTypeTLS {
		return errors.NewBadRequest("secret is not TLS type")
	}
	certPEM := secret.Data[corev1.TLSCertKey]
	keyPEM := secret.Data[corev1.TLSPrivateKeyKey]
	if len(certPEM) == 0 || len(keyPEM) == 0 {
		return errors.NewBadRequest("missing TLS certificate data")
	}
	_, err := tls.X509KeyPair(certPEM, keyPEM)
	return err
}

func generateSelfSignedTLSCert(dnsNames []string) ([]byte, []byte, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, err
	}

	notBefore := time.Now().Add(-1 * time.Hour)
	notAfter := notBefore.Add(365 * 24 * time.Hour)

	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"sandbox0"},
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              uniqueDNSNames(dnsNames),
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return nil, nil, err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})

	return certPEM, keyPEM, nil
}

func uniqueDNSNames(names []string) []string {
	seen := make(map[string]struct{}, len(names))
	unique := make([]string, 0, len(names))
	for _, name := range names {
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		unique = append(unique, name)
	}
	return unique
}
