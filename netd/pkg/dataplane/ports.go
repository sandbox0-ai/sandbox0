package dataplane

import (
	"fmt"

	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
)

func (dp *DataPlane) applyEgressPortRule(podIP string, portSpec v1alpha1.PortSpec, action, comment string) error {
	protocol := portSpec.Protocol
	if protocol == "" {
		protocol = "tcp"
	}

	args := []string{
		"-t", "filter", "-A", "NETD-EGRESS",
		"-s", podIP,
		"-p", protocol,
	}
	if portSpec.EndPort != nil && *portSpec.EndPort >= portSpec.Port {
		args = append(args, "--dport", fmt.Sprintf("%d:%d", portSpec.Port, *portSpec.EndPort))
	} else {
		args = append(args, "--dport", fmt.Sprintf("%d", portSpec.Port))
	}
	args = append(args,
		"-m", "comment", "--comment", comment,
		"-j", action,
	)

	return dp.runIPTables(args...)
}

func (dp *DataPlane) applyEgressPortRuleWithSet(podIP string, portSpec v1alpha1.PortSpec, setName, action, comment string) error {
	protocol := portSpec.Protocol
	if protocol == "" {
		protocol = "tcp"
	}

	args := []string{
		"-t", "filter", "-A", "NETD-EGRESS",
		"-s", podIP,
		"-m", "set", "--match-set", setName, "dst",
		"-p", protocol,
	}
	if portSpec.EndPort != nil && *portSpec.EndPort >= portSpec.Port {
		args = append(args, "--dport", fmt.Sprintf("%d:%d", portSpec.Port, *portSpec.EndPort))
	} else {
		args = append(args, "--dport", fmt.Sprintf("%d", portSpec.Port))
	}
	args = append(args,
		"-m", "comment", "--comment", comment,
		"-j", action,
	)

	return dp.runIPTables(args...)
}

func (dp *DataPlane) applyEgressPortRuleWithCIDR(podIP, cidr string, portSpec v1alpha1.PortSpec, action, comment string) error {
	protocol := portSpec.Protocol
	if protocol == "" {
		protocol = "tcp"
	}

	args := []string{
		"-t", "filter", "-A", "NETD-EGRESS",
		"-s", podIP, "-d", cidr,
		"-p", protocol,
	}
	if portSpec.EndPort != nil && *portSpec.EndPort >= portSpec.Port {
		args = append(args, "--dport", fmt.Sprintf("%d:%d", portSpec.Port, *portSpec.EndPort))
	} else {
		args = append(args, "--dport", fmt.Sprintf("%d", portSpec.Port))
	}
	args = append(args,
		"-m", "comment", "--comment", comment,
		"-j", action,
	)

	return dp.runIPTables(args...)
}

func (dp *DataPlane) applyIngressPortRule(podIP string, portSpec v1alpha1.PortSpec, action, comment string) error {
	protocol := portSpec.Protocol
	if protocol == "" {
		protocol = "tcp"
	}

	args := []string{
		"-t", "filter", "-A", "NETD-INGRESS",
		"-d", podIP,
		"-p", protocol,
	}
	if portSpec.EndPort != nil && *portSpec.EndPort >= portSpec.Port {
		args = append(args, "--dport", fmt.Sprintf("%d:%d", portSpec.Port, *portSpec.EndPort))
	} else {
		args = append(args, "--dport", fmt.Sprintf("%d", portSpec.Port))
	}
	args = append(args,
		"-m", "comment", "--comment", comment,
		"-j", action,
	)

	return dp.runIPTables(args...)
}
