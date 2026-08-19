// Copyright 2026 Sandbox0 Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package driver

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
)

const (
	networkPolicyBlockAll = "block-all"
	networkPolicyAllowAll = "allow-all"
)

// NetworkPolicy is the deliberately small L3/L4 policy language used by the
// Nomad PoC. Production moves compilation and application to ctld.
type NetworkPolicy struct {
	Mode  string             `json:"mode"`
	Allow []NetworkAllowRule `json:"allow,omitempty"`
}

type NetworkAllowRule struct {
	Protocol string `json:"protocol"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
}

func (p NetworkPolicy) Validate() error {
	switch p.Mode {
	case "", networkPolicyBlockAll, networkPolicyAllowAll:
	default:
		return fmt.Errorf("unsupported network policy mode %q", p.Mode)
	}
	for index, rule := range p.Allow {
		rule.Protocol = strings.ToLower(strings.TrimSpace(rule.Protocol))
		if rule.Protocol != "tcp" && rule.Protocol != "udp" {
			return fmt.Errorf("allow[%d].protocol must be tcp or udp", index)
		}
		if strings.TrimSpace(rule.Host) == "" {
			return fmt.Errorf("allow[%d].host is required", index)
		}
		if rule.Port <= 0 || rule.Port > 65535 {
			return fmt.Errorf("allow[%d].port is invalid", index)
		}
	}
	return nil
}

// NetworkRuntime applies policy in one Nomad allocation network namespace.
type NetworkRuntime interface {
	Apply(ctx context.Context, netnsPath, chain string, policy NetworkPolicy) error
	Cleanup(ctx context.Context, netnsPath, chain string) error
}

type commandNetworkRuntime struct{}

func networkRuntime(config *PluginConfig) NetworkRuntime {
	if config == nil || !config.NetworkPolicyEnabled {
		return nil
	}
	return commandNetworkRuntime{}
}

func networkChainName(containerID string) string {
	value := strings.TrimPrefix(containerID, "s0-")
	if len(value) > 12 {
		value = value[:12]
	}
	return "S0-NET-" + value
}

func (commandNetworkRuntime) Apply(ctx context.Context, netnsPath, chain string, policy NetworkPolicy) error {
	if strings.TrimSpace(netnsPath) == "" {
		return fmt.Errorf("Nomad network namespace path is required")
	}
	if err := policy.Validate(); err != nil {
		return err
	}
	var runtime commandNetworkRuntime
	if err := runtime.reset(ctx, netnsPath, chain); err != nil {
		return err
	}
	if err := nsenter(ctx, netnsPath, "iptables", "-w", "-N", chain); err != nil {
		return err
	}
	if err := nsenter(ctx, netnsPath, "iptables", "-w", "-A", chain, "-o", "lo", "-j", "RETURN"); err != nil {
		return err
	}
	if err := nsenter(ctx, netnsPath, "iptables", "-w", "-A", chain,
		"-m", "conntrack", "--ctstate", "ESTABLISHED,RELATED", "-j", "RETURN"); err != nil {
		return err
	}
	if policy.Mode == networkPolicyAllowAll {
		if err := nsenter(ctx, netnsPath, "iptables", "-w", "-A", chain, "-j", "RETURN"); err != nil {
			return err
		}
	} else {
		for _, rule := range policy.Allow {
			protocol := strings.ToLower(strings.TrimSpace(rule.Protocol))
			if err := nsenter(ctx, netnsPath, "iptables", "-w", "-A", chain,
				"-p", protocol, "-d", strings.TrimSpace(rule.Host),
				"--dport", fmt.Sprintf("%d", rule.Port), "-j", "RETURN"); err != nil {
				return err
			}
		}
	}
	if err := nsenter(ctx, netnsPath, "iptables", "-w", "-A", chain, "-j", "REJECT"); err != nil {
		return err
	}
	return nsenter(ctx, netnsPath, "iptables", "-w", "-I", "OUTPUT", "1", "-j", chain)
}

func (commandNetworkRuntime) Cleanup(ctx context.Context, netnsPath, chain string) error {
	if strings.TrimSpace(netnsPath) == "" {
		return nil
	}
	_ = nsenter(ctx, netnsPath, "iptables", "-w", "-D", "OUTPUT", "-j", chain)
	_ = nsenter(ctx, netnsPath, "iptables", "-w", "-F", chain)
	err := nsenter(ctx, netnsPath, "iptables", "-w", "-X", chain)
	if err != nil && !networkCleanupAlreadyComplete(err) {
		return err
	}
	return nil
}

func networkCleanupAlreadyComplete(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "no chain/target/match by that name") ||
		(strings.Contains(message, "nsenter: cannot open ") && strings.Contains(message, "no such file or directory"))
}

func (commandNetworkRuntime) reset(ctx context.Context, netnsPath, chain string) error {
	_ = nsenter(ctx, netnsPath, "iptables", "-w", "-D", "OUTPUT", "-j", chain)
	_ = nsenter(ctx, netnsPath, "iptables", "-w", "-F", chain)
	_ = nsenter(ctx, netnsPath, "iptables", "-w", "-X", chain)
	return nil
}

func nsenter(ctx context.Context, netnsPath string, command string, args ...string) error {
	allArgs := append([]string{"--net=" + netnsPath, "--", command}, args...)
	cmd := exec.CommandContext(ctx, "nsenter", allArgs...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		message := strings.TrimSpace(string(output))
		if message == "" {
			return fmt.Errorf("%s %s: %w", command, strings.Join(args, " "), err)
		}
		return fmt.Errorf("%s %s: %s: %w", command, strings.Join(args, " "), message, err)
	}
	return nil
}
