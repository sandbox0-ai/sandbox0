// Package netdiscovery provides network device discovery for sandbox containers.
// It supports both standard containers (veth devices) and Kata Containers (tap devices).
package netdiscovery

import (
	"fmt"
	"net"
	"os/exec"
	"strings"

	"github.com/vishvananda/netlink"
)

// DeviceInfo contains information about a discovered network device
type DeviceInfo struct {
	Name      string
	Type      string // "veth", "tap", "other"
	Index     int
	SandboxID string
}

// FindDeviceByIP finds the network device for a pod by querying the routing table.
// This is more reliable than checking device addresses, as CNI networks often
// don't assign the pod IP directly to the veth/tap interface.
func FindDeviceByIP(podIP string) (*DeviceInfo, error) {
	ip := net.ParseIP(podIP)
	if ip == nil {
		return nil, fmt.Errorf("invalid IP address: %s", podIP)
	}

	// Query routing table and find the route that matches this IP
	// Use 0 to list all families
	routes, err := netlink.RouteList(nil, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to list routes: %w", err)
	}

	// Find the most specific route that contains the pod IP
	var bestRoute *netlink.Route
	var bestPrefixLen int

	for _, route := range routes {
		// Skip routes with no output link
		if route.LinkIndex == 0 {
			continue
		}

		// Check if this route covers the pod IP
		if route.Dst != nil && route.Dst.Contains(ip) {
			prefixLen, _ := route.Dst.Mask.Size()
			// Prefer more specific routes (longer prefix)
			if prefixLen > bestPrefixLen {
				bestRoute = &route
				bestPrefixLen = prefixLen
			}
		}
	}

	if bestRoute == nil {
		// Try using kernel's route resolution via ip command as fallback
		return findDeviceByIPFallback(podIP)
	}

	// Get device details from index
	link, err := netlink.LinkByIndex(bestRoute.LinkIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to get link by index %d: %w", bestRoute.LinkIndex, err)
	}

	deviceName := link.Attrs().Name
	deviceType := classifyDevice(deviceName)

	return &DeviceInfo{
		Name:  deviceName,
		Type:  deviceType,
		Index: bestRoute.LinkIndex,
	}, nil
}

// findDeviceByIPFallback uses ip route get as a fallback mechanism
func findDeviceByIPFallback(podIP string) (*DeviceInfo, error) {
	// Execute "ip route get" command to query kernel routing table
	output, err := exec.Command("ip", "route", "get", podIP).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("failed to execute 'ip route get': %w", err)
	}

	// Parse output to find device name
	// Output format example:
	// 10.244.1.5 dev veth1234 src 10.244.1.1 uid 0
	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		if strings.Contains(line, "dev ") {
			parts := strings.Fields(line)
			for i, part := range parts {
				if part == "dev" && i+1 < len(parts) {
					deviceName := parts[i+1]
					// Get link index
					link, err := netlink.LinkByName(deviceName)
					if err != nil {
						return nil, fmt.Errorf("failed to get link by name %s: %w", deviceName, err)
					}
					deviceType := classifyDevice(deviceName)
					return &DeviceInfo{
						Name:  deviceName,
						Type:  deviceType,
						Index: link.Attrs().Index,
					}, nil
				}
			}
		}
	}

	return nil, fmt.Errorf("could not parse device name from ip route get output")
}

// FindDeviceBySandboxID attempts to find a device using multiple strategies.
// Strategy 1: Try veth naming convention (runc/gVisor)
// Strategy 2: Try tap naming convention (Kata)
// Strategy 3: Fall back to IP-based discovery
func FindDeviceBySandboxID(sandboxID, podIP string) (*DeviceInfo, error) {
	if podIP == "" {
		return nil, fmt.Errorf("pod IP is required")
	}

	// Strategy 1: Try veth naming convention (runc, gVisor)
	// Standard veth devices are typically named like "vethXXXX" where XXXX is a hash
	// This is faster than RouteGet but may not work for all CNIs
	vethName := "veth" + sandboxID[:min(8, len(sandboxID))]
	if _, err := netlink.LinkByName(vethName); err == nil {
		return &DeviceInfo{
			Name:      vethName,
			Type:      "veth",
			SandboxID: sandboxID,
		}, nil
	}

	// Strategy 2: Try tap naming convention (Kata Containers)
	// Kata devices may be named like "tapXXXX" or follow other patterns
	// Use IP-based discovery as the reliable fallback
	deviceInfo, err := FindDeviceByIP(podIP)
	if err != nil {
		return nil, fmt.Errorf("failed to find device for sandbox %s (IP: %s): %w", sandboxID, podIP, err)
	}

	deviceInfo.SandboxID = sandboxID
	return deviceInfo, nil
}

// classifyDevice determines the type of network device based on its name
func classifyDevice(name string) string {
	if len(name) >= 4 && name[:4] == "veth" {
		return "veth"
	}
	if len(name) >= 3 && name[:3] == "tap" {
		return "tap"
	}
	if len(name) >= 3 && name[:3] == "vtap" {
		return "tap" // vtap is also used by Kata in some configurations
	}
	if len(name) >= 5 && name[:5] == "eth0_" {
		return "other" // CNI bridge interfaces
	}
	return "other"
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
