package common

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
)

const (
	// EnterpriseLicenseSecretKey is the key in Secret data that stores the signed enterprise license.
	EnterpriseLicenseSecretKey = "license.lic"
	// EnterpriseLicenseDefaultPath is the default in-container file path for enterprise license.
	EnterpriseLicenseDefaultPath = "/licenses/license.lic"
)

// EnterpriseLicenseSecretName returns the default Secret name that stores enterprise license.
func EnterpriseLicenseSecretName(infraName string) string {
	return fmt.Sprintf("%s-enterprise-license", infraName)
}

func EnsureEnterpriseLicense(
	ctx context.Context,
	resources *ResourceManager,
	infra *infrav1alpha1.Sandbox0Infra,
	licenseFile *string,
	required bool,
	reason string,
) error {
	if !required {
		return nil
	}

	if strings.TrimSpace(*licenseFile) == "" {
		*licenseFile = EnterpriseLicenseDefaultPath
	}

	_, err := GetSecretValue(ctx, resources.Client, infra.Namespace, infrav1alpha1.SecretKeyRef{
		Name: EnterpriseLicenseSecretName(infra.Name),
		Key:  EnterpriseLicenseSecretKey,
	})
	if err != nil {
		return fmt.Errorf("enterprise license secret is required for %s: %w", reason, err)
	}
	return nil
}

func NormalizeEnterpriseLicenseFile(licenseFile *string, required bool) {
	if !required || licenseFile == nil {
		return
	}
	if strings.TrimSpace(*licenseFile) == "" {
		*licenseFile = EnterpriseLicenseDefaultPath
	}
}

func AppendEnterpriseLicenseVolume(
	infraName string,
	licenseFile string,
	volumeMounts []corev1.VolumeMount,
	volumes []corev1.Volume,
) ([]corev1.VolumeMount, []corev1.Volume) {
	mountPath := strings.TrimSpace(licenseFile)
	if mountPath == "" {
		mountPath = EnterpriseLicenseDefaultPath
	}

	volumeMounts = append(volumeMounts, corev1.VolumeMount{
		Name:      "enterprise-license",
		MountPath: mountPath,
		SubPath:   EnterpriseLicenseSecretKey,
		ReadOnly:  true,
	})
	volumes = append(volumes, corev1.Volume{
		Name: "enterprise-license",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: EnterpriseLicenseSecretName(infraName),
				Items: []corev1.KeyToPath{
					{
						Key:  EnterpriseLicenseSecretKey,
						Path: EnterpriseLicenseSecretKey,
					},
				},
			},
		},
	})

	return volumeMounts, volumes
}
