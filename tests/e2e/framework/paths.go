package framework

import (
	"fmt"
	"os"
	"path/filepath"
)

// FindInfraRoot locates the infra module root by searching for go.mod.
func FindInfraRoot() (string, error) {
	fmt.Printf("Finding infra root...\n")
	current, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("get working directory: %w", err)
	}

	for {
		goMod := filepath.Join(current, "go.mod")
		if _, err := os.Stat(goMod); err == nil {
			fmt.Printf("Infra root found at %q...\n", current)
			return current, nil
		}

		parent := filepath.Dir(current)
		if parent == current {
			fmt.Printf("Infra root not found, reached root directory...\n")
			return "", fmt.Errorf("infra root not found")
		}
		current = parent
	}
}
