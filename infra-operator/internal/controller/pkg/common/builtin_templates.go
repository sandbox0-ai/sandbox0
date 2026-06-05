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
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/log"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	templatev1alpha1 "github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	templmigrations "github.com/sandbox0-ai/sandbox0/pkg/template/migrations"
	templstorepg "github.com/sandbox0-ai/sandbox0/pkg/template/store/pg"
)

// BuiltinTemplateOptions controls builtin template synchronization.
type BuiltinTemplateOptions struct {
	DatabaseURL          string
	DatabaseMaxConns     int32
	DatabaseMinConns     int32
	TemplateStoreEnabled bool
	Owner                string
	MemoryPerCPU         resource.Quantity
}

// EnsureBuiltinTemplates creates or updates builtin templates in the template store.
func EnsureBuiltinTemplates(ctx context.Context, builtins []infrav1alpha1.BuiltinTemplateConfig, opts BuiltinTemplateOptions) error {
	logger := log.FromContext(ctx)
	if len(builtins) == 0 {
		return nil
	}
	if !opts.TemplateStoreEnabled {
		logger.Info("Template store disabled; skipping builtin template sync")
		return nil
	}
	if opts.DatabaseURL == "" {
		return fmt.Errorf("database_url is required to sync builtin templates")
	}
	if opts.Owner == "" {
		opts.Owner = "infra-operator"
	}

	pool, err := dbpool.New(ctx, dbpool.Options{
		DatabaseURL:     opts.DatabaseURL,
		MaxConns:        opts.DatabaseMaxConns,
		MinConns:        opts.DatabaseMinConns,
		DefaultMaxConns: 10,
		DefaultMinConns: 2,
		Schema:          "scheduler",
	})
	if err != nil {
		return fmt.Errorf("init template store database: %w", err)
	}
	defer pool.Close()

	if err := migrate.Up(ctx, pool, ".",
		migrate.WithBaseFS(templmigrations.FS),
		migrate.WithSchema("scheduler"),
	); err != nil {
		return fmt.Errorf("migrate template store: %w", err)
	}

	store := templstorepg.NewStore(pool)

	for _, builtin := range builtins {
		templateID, err := naming.CanonicalTemplateID(builtin.TemplateID)
		if err != nil {
			return fmt.Errorf("builtin template_id is invalid: %w", err)
		}

		spec := buildBuiltinTemplateSpec(templateID, builtin)
		if err := template.ValidateResourceRatio(spec, builtinTemplateMemoryPerCPU(opts), "builtin template "+templateID); err != nil {
			return fmt.Errorf("validate builtin template %s: %w", templateID, err)
		}

		tpl := &template.Template{
			TemplateID: templateID,
			Scope:      naming.ScopePublic,
			TeamID:     "",
			UserID:     opts.Owner,
			Spec:       spec,
		}

		existing, err := store.GetTemplate(ctx, tpl.Scope, tpl.TeamID, tpl.TemplateID)
		if err != nil {
			return fmt.Errorf("get builtin template %s: %w", templateID, err)
		}
		if existing == nil {
			if err := store.CreateTemplate(ctx, tpl); err != nil {
				return fmt.Errorf("create builtin template %s: %w", templateID, err)
			}
			logger.Info("Builtin template created in store", "template_id", templateID)
			continue
		}

		if err := store.UpdateTemplate(ctx, tpl); err != nil {
			return fmt.Errorf("update builtin template %s: %w", templateID, err)
		}
		logger.Info("Builtin template updated in store", "template_id", templateID)
	}
	return nil
}

func builtinTemplateMemoryPerCPU(opts BuiltinTemplateOptions) resource.Quantity {
	if opts.MemoryPerCPU.Sign() <= 0 {
		return template.MemoryPerCPUOrDefault("")
	}
	return opts.MemoryPerCPU
}

// TemplateMemoryPerCPUFromManagerConfig resolves the template resource shape used by manager API validation.
func TemplateMemoryPerCPUFromManagerConfig(cfg *apiconfig.ManagerConfig) resource.Quantity {
	if cfg == nil {
		return template.MemoryPerCPUOrDefault("")
	}
	return template.MemoryPerCPUOrDefault(cfg.TeamTemplateMemoryPerCPU)
}

func buildBuiltinTemplateSpec(templateID string, builtin infrav1alpha1.BuiltinTemplateConfig) templatev1alpha1.SandboxTemplateSpec {
	var spec templatev1alpha1.SandboxTemplateSpec
	if builtin.Spec != nil {
		spec = *builtin.Spec.DeepCopy()
	} else if templateID == template.DockerInSandboxTemplateID {
		spec = dockerInSandboxTemplateSpec()
	} else {
		spec = defaultBuiltinTemplateSpec(templateID)
	}

	applyBuiltinTemplateConfig(&spec, templateID, builtin)
	return spec
}

func defaultBuiltinTemplateSpec(templateID string) templatev1alpha1.SandboxTemplateSpec {
	displayName := templateID
	if templateID == template.DefaultTemplateID {
		displayName = template.DefaultTemplateDisplayName
	}
	return templatev1alpha1.SandboxTemplateSpec{
		DisplayName: displayName,
		Description: fmt.Sprintf("Builtin template %s installed by infra-operator.", templateID),
		MainContainer: templatev1alpha1.ContainerSpec{
			Image: template.DefaultTemplateImage,
			Resources: templatev1alpha1.ResourceQuota{
				CPU:              resource.MustParse(template.DefaultTemplateCPU),
				Memory:           resource.MustParse(template.DefaultTemplateMemory),
				EphemeralStorage: resource.MustParse(template.DefaultTemplateEphemeralStorage),
			},
		},
		Pool: defaultBuiltinTemplatePool(),
		Network: &templatev1alpha1.SandboxNetworkPolicy{
			Mode: templatev1alpha1.NetworkModeAllowAll,
		},
	}
}

func dockerInSandboxTemplateSpec() templatev1alpha1.SandboxTemplateSpec {
	sizeLimit := resource.MustParse(template.DockerInSandboxDockerRootSizeLimit)
	return templatev1alpha1.SandboxTemplateSpec{
		DisplayName: template.DockerInSandboxTemplateDisplayName,
		Description: template.DockerInSandboxTemplateDescription,
		MainContainer: templatev1alpha1.ContainerSpec{
			Image: template.DefaultTemplateImage,
			Resources: templatev1alpha1.ResourceQuota{
				CPU:              resource.MustParse(template.DockerInSandboxCPU),
				Memory:           resource.MustParse(template.DockerInSandboxMemory),
				EphemeralStorage: resource.MustParse(template.DockerInSandboxEphemeralStorage),
			},
			SecurityContext: &templatev1alpha1.SecurityContext{
				Privileged:               BoolPtr(true),
				AllowPrivilegeEscalation: BoolPtr(true),
			},
		},
		Pod: &templatev1alpha1.PodSpecOverride{
			EmptyDirMounts: []templatev1alpha1.EmptyDirMountSpec{{
				MountPath: template.DockerInSandboxDockerRoot,
				SizeLimit: &sizeLimit,
			}},
		},
		Pool: defaultBuiltinTemplatePool(),
		Network: &templatev1alpha1.SandboxNetworkPolicy{
			Mode: templatev1alpha1.NetworkModeAllowAll,
		},
	}
}

func applyBuiltinTemplateConfig(spec *templatev1alpha1.SandboxTemplateSpec, templateID string, builtin infrav1alpha1.BuiltinTemplateConfig) {
	if image := strings.TrimSpace(builtin.Image); image != "" {
		spec.MainContainer.Image = image
	}
	if spec.MainContainer.Image == "" {
		spec.MainContainer.Image = template.DefaultTemplateImage
	}
	if spec.MainContainer.Resources.CPU.Sign() == 0 {
		spec.MainContainer.Resources.CPU = resource.MustParse(template.DefaultTemplateCPU)
	}
	if spec.MainContainer.Resources.Memory.Sign() == 0 {
		spec.MainContainer.Resources.Memory = resource.MustParse(template.DefaultTemplateMemory)
	}
	if spec.MainContainer.Resources.EphemeralStorage.Sign() == 0 {
		spec.MainContainer.Resources.EphemeralStorage = resource.MustParse(template.DefaultTemplateEphemeralStorage)
	}

	if displayName := strings.TrimSpace(builtin.DisplayName); displayName != "" {
		spec.DisplayName = displayName
	}
	if spec.DisplayName == "" {
		spec.DisplayName = templateID
	}
	if description := strings.TrimSpace(builtin.Description); description != "" {
		spec.Description = description
	}
	if spec.Description == "" {
		spec.Description = fmt.Sprintf("Builtin template %s installed by infra-operator.", templateID)
	}

	if builtin.Pool.MinIdle != 0 || builtin.Pool.MaxIdle != 0 || (spec.Pool.MinIdle == 0 && spec.Pool.MaxIdle == 0) {
		poolCfg := applyBuiltinTemplatePool(builtin.Pool)
		spec.Pool = templatev1alpha1.PoolStrategy{
			MinIdle: poolCfg.MinIdle,
			MaxIdle: poolCfg.MaxIdle,
		}
	}
	if spec.Network == nil {
		spec.Network = &templatev1alpha1.SandboxNetworkPolicy{
			Mode: templatev1alpha1.NetworkModeAllowAll,
		}
	}
}

func defaultBuiltinTemplatePool() templatev1alpha1.PoolStrategy {
	pool := applyBuiltinTemplatePool(infrav1alpha1.BuiltinTemplatePoolConfig{})
	return templatev1alpha1.PoolStrategy{
		MinIdle: pool.MinIdle,
		MaxIdle: pool.MaxIdle,
	}
}

func applyBuiltinTemplatePool(pool infrav1alpha1.BuiltinTemplatePoolConfig) infrav1alpha1.BuiltinTemplatePoolConfig {
	minIdle, maxIdle := template.ApplyDefaultPool(pool.MinIdle, pool.MaxIdle)
	pool.MinIdle = minIdle
	pool.MaxIdle = maxIdle
	return pool
}
