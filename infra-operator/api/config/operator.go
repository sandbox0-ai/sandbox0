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

package config

import (
	"fmt"
	"os"

	"sigs.k8s.io/yaml"
)

const (
	DefaultConfigPath = "/etc/infra-operator/config.yaml"
	DefaultImageRepo  = "sandbox0ai/infra"
)

type OperatorConfig struct {
	ImageRepo string `json:"imageRepo" yaml:"imageRepo"`
}

func LoadOperatorConfig(configPath string) (OperatorConfig, error) {
	config := OperatorConfig{
		ImageRepo: DefaultImageRepo,
	}

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return config, nil
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return config, fmt.Errorf("failed to read config file: %w", err)
	}

	if err := yaml.Unmarshal(data, &config); err != nil {
		return config, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	if config.ImageRepo == "" {
		config.ImageRepo = DefaultImageRepo
	}

	return config, nil
}
