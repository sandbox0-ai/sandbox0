package config

import (
	"encoding/json"
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration is a transport-neutral configuration duration encoded as a string.
type Duration struct {
	time.Duration `json:"-" yaml:"-"`
}

// UnmarshalText parses a duration such as 500ms or 2m.
func (d *Duration) UnmarshalText(value []byte) error {
	if d == nil {
		return fmt.Errorf("duration target is nil")
	}
	parsed, err := time.ParseDuration(string(value))
	if err != nil {
		return err
	}
	d.Duration = parsed
	return nil
}

// MarshalText returns the canonical duration string.
func (d Duration) MarshalText() ([]byte, error) {
	return []byte(d.Duration.String()), nil
}

// UnmarshalYAML accepts only a scalar duration string.
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	if value == nil || value.Kind != yaml.ScalarNode {
		return fmt.Errorf("duration must be a scalar string")
	}
	return d.UnmarshalText([]byte(value.Value))
}

// MarshalYAML emits a scalar duration string.
func (d Duration) MarshalYAML() (any, error) {
	return d.Duration.String(), nil
}

// UnmarshalJSON parses a JSON duration string.
func (d *Duration) UnmarshalJSON(value []byte) error {
	var raw string
	if err := json.Unmarshal(value, &raw); err != nil {
		return fmt.Errorf("duration must be a string: %w", err)
	}
	return d.UnmarshalText([]byte(raw))
}

// MarshalJSON emits a JSON duration string.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Duration.String())
}
