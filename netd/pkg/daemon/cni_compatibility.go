package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	cniConfigNamespace           = "kube-system"
	ciliumConfigMapName          = "cilium-config"
	terwayConfigMapName          = "eni-config"
	terwayCNIConfigKey           = "10-terway.conf"
	ciliumHostLegacyRoutingKey   = "enable-host-legacy-routing"
	cniCompatibilityCheckTimeout = 5 * time.Second
)

type ciliumHostRoutingStatus struct {
	CNI               string
	ConfigMap         string
	HostLegacyRouting bool
}

type terwayCNIConfig struct {
	Type                  string `json:"type"`
	NetworkPolicyProvider string `json:"network_policy_provider"`
	ENIIPVirtualType      string `json:"eniip_virtual_type"`
	CiliumArgs            string `json:"cilium_args"`
}

// warnIfCiliumMayBypassNetd emits one startup warning when the installed CNI
// configuration can route sandbox traffic around netd's host-netfilter rules.
func warnIfCiliumMayBypassNetd(ctx context.Context, client kubernetes.Interface, nodeName string, logger *zap.Logger) {
	if logger == nil {
		logger = zap.NewNop()
	}
	checkCtx, cancel := context.WithTimeout(ctx, cniCompatibilityCheckTimeout)
	defer cancel()

	status, err := detectCiliumHostRouting(checkCtx, client)
	if err != nil {
		logger.Warn("Unable to verify Cilium host-routing compatibility",
			zap.String("node", nodeName),
			zap.Error(err),
		)
		return
	}
	if status == nil || status.HostLegacyRouting {
		return
	}

	logger.Warn("Cilium is configured without host legacy routing; netd iptables interception may be bypassed",
		zap.String("node", nodeName),
		zap.String("cni", status.CNI),
		zap.String("config_map", status.ConfigMap),
		zap.Bool("host_legacy_routing", false),
		zap.String("required_setting", ciliumHostLegacyRoutingKey+"=true"),
		zap.String("impact", "sandbox network policy and audit may be incomplete"),
	)
}

// detectCiliumHostRouting reads the standard Cilium and ACK Terway config maps.
// A missing host-legacy setting means Cilium's default BPF host-routing mode.
func detectCiliumHostRouting(ctx context.Context, client kubernetes.Interface) (*ciliumHostRoutingStatus, error) {
	if client == nil {
		return nil, fmt.Errorf("kubernetes client is nil")
	}
	configMaps := client.CoreV1().ConfigMaps(cniConfigNamespace)

	ciliumConfig, err := configMaps.Get(ctx, ciliumConfigMapName, metav1.GetOptions{})
	if err == nil {
		return standardCiliumHostRouting(ciliumConfig)
	}
	if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("get configmap %s/%s: %w", cniConfigNamespace, ciliumConfigMapName, err)
	}

	terwayConfig, err := configMaps.Get(ctx, terwayConfigMapName, metav1.GetOptions{})
	if err == nil {
		return terwayCiliumHostRouting(terwayConfig)
	}
	if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("get configmap %s/%s: %w", cniConfigNamespace, terwayConfigMapName, err)
	}

	return nil, nil
}

func standardCiliumHostRouting(configMap *corev1.ConfigMap) (*ciliumHostRoutingStatus, error) {
	hostLegacyRouting := false
	if raw, ok := configMap.Data[ciliumHostLegacyRoutingKey]; ok {
		parsed, err := strconv.ParseBool(strings.TrimSpace(raw))
		if err != nil {
			return nil, fmt.Errorf("parse %s in configmap %s/%s: %w", ciliumHostLegacyRoutingKey, configMap.Namespace, configMap.Name, err)
		}
		hostLegacyRouting = parsed
	}
	return &ciliumHostRoutingStatus{
		CNI:               "cilium",
		ConfigMap:         configMap.Namespace + "/" + configMap.Name,
		HostLegacyRouting: hostLegacyRouting,
	}, nil
}

func terwayCiliumHostRouting(configMap *corev1.ConfigMap) (*ciliumHostRoutingStatus, error) {
	raw, ok := configMap.Data[terwayCNIConfigKey]
	if !ok {
		return nil, nil
	}

	var config terwayCNIConfig
	if err := json.Unmarshal([]byte(raw), &config); err != nil {
		return nil, fmt.Errorf("parse %s in configmap %s/%s: %w", terwayCNIConfigKey, configMap.Namespace, configMap.Name, err)
	}
	if config.Type != "terway" {
		return nil, nil
	}

	hostLegacyRouting, hostLegacyConfigured, err := parseCiliumBoolArg(config.CiliumArgs, ciliumHostLegacyRoutingKey)
	if err != nil {
		return nil, fmt.Errorf("parse cilium_args in configmap %s/%s: %w", configMap.Namespace, configMap.Name, err)
	}
	usesCilium := config.NetworkPolicyProvider == "ebpf" || config.ENIIPVirtualType == "datapathv2" || hostLegacyConfigured
	if !usesCilium {
		return nil, nil
	}

	return &ciliumHostRoutingStatus{
		CNI:               "terway-cilium",
		ConfigMap:         configMap.Namespace + "/" + configMap.Name,
		HostLegacyRouting: hostLegacyRouting,
	}, nil
}

func parseCiliumBoolArg(args, name string) (value bool, found bool, err error) {
	for _, part := range strings.Split(args, "--") {
		fields := strings.Fields(part)
		if len(fields) == 0 {
			continue
		}
		arg := fields[0]
		if arg == name {
			value = true
			found = true
			continue
		}
		prefix := name + "="
		if !strings.HasPrefix(arg, prefix) {
			continue
		}
		parsed, parseErr := strconv.ParseBool(strings.TrimPrefix(arg, prefix))
		if parseErr != nil {
			return false, true, parseErr
		}
		value = parsed
		found = true
	}
	return value, found, nil
}
