//go:build !linux

package processidentity

import "fmt"

func Current() (string, error)     { return "", fmt.Errorf("host process identity requires Linux") }
func Alive(_ string) (bool, error) { return false, fmt.Errorf("host process identity requires Linux") }
