package netd

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
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
)

const (
	mitmCACertKey = "ca.crt"
	mitmCAKeyKey  = "ca.key"
)

func managedMITMCASecretName(infra *infrav1alpha1.Sandbox0Infra) string {
	return fmt.Sprintf("%s-netd-mitm-ca", infra.Name)
}

func ResolveMITMCASecretName(infra *infrav1alpha1.Sandbox0Infra) string {
	if infra == nil {
		return ""
	}
	if infra.Spec.Services != nil && infra.Spec.Services.Netd != nil {
		if secretName := strings.TrimSpace(infra.Spec.Services.Netd.MITMCASecretName); secretName != "" {
			return secretName
		}
	}
	if infra.Name == "" {
		return ""
	}
	return managedMITMCASecretName(infra)
}

func EnsureMITMCASecret(ctx context.Context, resources *common.ResourceManager, infra *infrav1alpha1.Sandbox0Infra, labels map[string]string) (string, error) {
	if infra == nil || infra.Spec.Services == nil || infra.Spec.Services.Netd == nil {
		return "", nil
	}
	if resources == nil || resources.Client == nil || resources.Scheme == nil {
		return "", fmt.Errorf("resource manager with client and scheme is required")
	}

	if secretName := strings.TrimSpace(infra.Spec.Services.Netd.MITMCASecretName); secretName != "" {
		if err := validateExistingMITMCASecret(ctx, resources.Client, infra.Namespace, secretName); err != nil {
			return "", err
		}
		return secretName, nil
	}

	if infra.Name == "" {
		return "", nil
	}
	secretName := managedMITMCASecretName(infra)
	if err := reconcileManagedMITMCASecret(ctx, resources.Client, resources.Scheme, infra, secretName, labels); err != nil {
		return "", err
	}
	return secretName, nil
}

func reconcileManagedMITMCASecret(ctx context.Context, client ctrlclient.Client, scheme *runtime.Scheme, infra *infrav1alpha1.Sandbox0Infra, name string, labels map[string]string) error {
	secret := &corev1.Secret{}
	getErr := client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, secret)
	if getErr != nil && !apierrors.IsNotFound(getErr) {
		return getErr
	}

	if getErr == nil && validateMITMCASecret(secret) == nil {
		return nil
	}

	certPEM, keyPEM, err := generateManagedMITMCA(infra)
	if err != nil {
		return err
	}

	desired := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: infra.Namespace,
			Labels:    labels,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			mitmCACertKey: certPEM,
			mitmCAKeyKey:  keyPEM,
		},
	}
	if err := ctrl.SetControllerReference(infra, desired, scheme); err != nil {
		return err
	}

	if apierrors.IsNotFound(getErr) {
		return client.Create(ctx, desired)
	}

	secret.Type = desired.Type
	secret.Data = desired.Data
	secret.Labels = desired.Labels
	secret.OwnerReferences = desired.OwnerReferences
	return client.Update(ctx, secret)
}

func validateExistingMITMCASecret(ctx context.Context, client ctrlclient.Client, namespace, name string) error {
	secret := &corev1.Secret{}
	if err := client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, secret); err != nil {
		return fmt.Errorf("get MITM CA secret %s/%s: %w", namespace, name, err)
	}
	if err := validateMITMCASecret(secret); err != nil {
		return fmt.Errorf("validate MITM CA secret %s/%s: %w", namespace, name, err)
	}
	return nil
}

func validateMITMCASecret(secret *corev1.Secret) error {
	certPEM := secret.Data[mitmCACertKey]
	keyPEM := secret.Data[mitmCAKeyKey]
	if len(certPEM) == 0 || len(keyPEM) == 0 {
		return fmt.Errorf("missing MITM CA data")
	}
	if _, err := tls.X509KeyPair(certPEM, keyPEM); err != nil {
		return fmt.Errorf("load MITM CA key pair: %w", err)
	}

	block, _ := pem.Decode(certPEM)
	if block == nil {
		return fmt.Errorf("decode MITM CA cert PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return fmt.Errorf("parse MITM CA cert: %w", err)
	}
	if !cert.IsCA {
		return fmt.Errorf("MITM certificate is not a CA")
	}
	return nil
}

func generateManagedMITMCA(infra *infrav1alpha1.Sandbox0Infra) ([]byte, []byte, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, fmt.Errorf("generate MITM CA key: %w", err)
	}

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, fmt.Errorf("generate MITM CA serial: %w", err)
	}

	commonName := "sandbox0-netd-mitm-ca"
	if infra != nil && infra.Name != "" {
		commonName = fmt.Sprintf("sandbox0-netd-mitm-ca.%s", infra.Name)
	}
	now := time.Now().UTC()
	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName:   commonName,
			Organization: []string{"sandbox0"},
		},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.AddDate(10, 0, 0),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return nil, nil, fmt.Errorf("create MITM CA cert: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	return certPEM, keyPEM, nil
}
