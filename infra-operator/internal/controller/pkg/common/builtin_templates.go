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
}

// EnsureBuiltinTemplates creates or updates builtin templates in the template store.
func EnsureBuiltinTemplates(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, opts BuiltinTemplateOptions) error {
	logger := log.FromContext(ctx)
	if infra == nil || len(infra.Spec.BuiltinTemplates) == 0 {
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

	for _, builtin := range infra.Spec.BuiltinTemplates {
		templateID, err := naming.CanonicalTemplateID(builtin.TemplateID)
		if err != nil {
			return fmt.Errorf("builtin template_id is invalid: %w", err)
		}

		image := strings.TrimSpace(builtin.Image)
		if image == "" {
			image = template.DefaultTemplateImage
		}
		poolCfg := applyBuiltinTemplatePool(builtin.Pool)
		displayName := strings.TrimSpace(builtin.DisplayName)
		if displayName == "" {
			displayName = templateID
		}
		description := strings.TrimSpace(builtin.Description)
		if description == "" {
			description = fmt.Sprintf("Builtin template %s installed by infra-operator.", templateID)
		}

		spec := templatev1alpha1.SandboxTemplateSpec{
			DisplayName: displayName,
			Description: description,
			MainContainer: templatev1alpha1.ContainerSpec{
				Image: image,
				Resources: templatev1alpha1.ResourceQuota{
					CPU:    resource.MustParse(template.DefaultTemplateCPU),
					Memory: resource.MustParse(template.DefaultTemplateMemory),
				},
			},
			Pool: templatev1alpha1.PoolStrategy{
				MinIdle: poolCfg.MinIdle,
				MaxIdle: poolCfg.MaxIdle,
			},
			Network: &templatev1alpha1.SandboxNetworkPolicy{
				Mode: templatev1alpha1.NetworkModeAllowAll,
			},
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

func applyBuiltinTemplatePool(pool infrav1alpha1.BuiltinTemplatePoolConfig) infrav1alpha1.BuiltinTemplatePoolConfig {
	minIdle, maxIdle := template.ApplyDefaultPool(pool.MinIdle, pool.MaxIdle)
	pool.MinIdle = minIdle
	pool.MaxIdle = maxIdle
	return pool
}
