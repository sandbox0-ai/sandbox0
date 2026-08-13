// Package carrier defines the stable contract between manager-owned carrier
// Pods and the node-local ctld activation runtime.
package carrier

import (
	"fmt"
	"regexp"
	"strings"
)

const (
	LabelPool       = "sandbox0.ai/carrier-pool"
	LabelGeneration = "sandbox0.ai/carrier-generation"
	AnnotationSlot  = "sandbox0.ai/carrier-slot"
	AnnotationState = "sandbox0.ai/carrier-state"

	StateReady    = "ready"
	StateReserved = "reserved"
	StateDraining = "draining"

	GateVolumeName  = "sandbox0-carrier-gate"
	GateMountPath   = "/var/run/sandbox0/carrier"
	GateReleaseFile = "release"

	MarkerRepository = "sandbox0.local/rootfs-heads"
)

var slotPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{7,62}$`)

// ValidateSlot rejects aliases that could escape the platform-reserved marker
// namespace or be confused with a registry-supplied image reference.
func ValidateSlot(slot string) error {
	slot = strings.TrimSpace(slot)
	if !slotPattern.MatchString(slot) {
		return fmt.Errorf("invalid carrier slot %q", slot)
	}
	return nil
}

// MarkerImage returns the unique PullNever image reference predeclared by a carrier Pod.
func MarkerImage(slot string) (string, error) {
	if err := ValidateSlot(slot); err != nil {
		return "", err
	}
	return MarkerRepository + ":" + strings.TrimSpace(slot), nil
}

// ValidateMarkerImage ensures a materialization target is exactly the slot's
// platform-reserved node-local alias.
func ValidateMarkerImage(slot, image string) error {
	expected, err := MarkerImage(slot)
	if err != nil {
		return err
	}
	if strings.TrimSpace(image) != expected {
		return fmt.Errorf("carrier marker image %q does not match slot %q", image, slot)
	}
	return nil
}
