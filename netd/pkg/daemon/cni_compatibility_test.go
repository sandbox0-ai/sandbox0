package daemon

import (
	"context"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func TestDetectCiliumHostRouting(t *testing.T) {
	tests := []struct {
		name       string
		objects    []runtime.Object
		wantCNI    string
		wantLegacy bool
		wantNil    bool
		wantErr    bool
	}{
		{
			name: "standard Cilium defaults to BPF host routing",
			objects: []runtime.Object{cniConfigMap(ciliumConfigMapName, map[string]string{
				"routing-mode": "native",
			})},
			wantCNI: "cilium",
		},
		{
			name: "standard Cilium enables host legacy routing",
			objects: []runtime.Object{cniConfigMap(ciliumConfigMapName, map[string]string{
				ciliumHostLegacyRoutingKey: "true",
			})},
			wantCNI:    "cilium",
			wantLegacy: true,
		},
		{
			name: "Terway datapath v2 defaults to BPF host routing",
			objects: []runtime.Object{cniConfigMap(terwayConfigMapName, map[string]string{
				terwayCNIConfigKey: `{"type":"terway","network_policy_provider":"ebpf","eniip_virtual_type":"datapathv2"}`,
			})},
			wantCNI: "terway-cilium",
		},
		{
			name: "Terway preserves an enabled host legacy argument",
			objects: []runtime.Object{cniConfigMap(terwayConfigMapName, map[string]string{
				terwayCNIConfigKey: `{"type":"terway","network_policy_provider":"ebpf","eniip_virtual_type":"datapathv2","cilium_args":"--enable-hubble=true --enable-host-legacy-routing=true"}`,
			})},
			wantCNI:    "terway-cilium",
			wantLegacy: true,
		},
		{
			name: "Terway without Cilium is ignored",
			objects: []runtime.Object{cniConfigMap(terwayConfigMapName, map[string]string{
				terwayCNIConfigKey: `{"type":"terway","network_policy_provider":"calico","eniip_virtual_type":"veth"}`,
			})},
			wantNil: true,
		},
		{
			name:    "non-Cilium cluster is ignored",
			wantNil: true,
		},
		{
			name: "invalid Cilium boolean is reported",
			objects: []runtime.Object{cniConfigMap(ciliumConfigMapName, map[string]string{
				ciliumHostLegacyRoutingKey: "sometimes",
			})},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, err := detectCiliumHostRouting(context.Background(), fake.NewSimpleClientset(tt.objects...))
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("detect Cilium host routing: %v", err)
			}
			if tt.wantNil {
				if status != nil {
					t.Fatalf("status = %+v, want nil", status)
				}
				return
			}
			if status == nil {
				t.Fatal("status is nil")
			}
			if status.CNI != tt.wantCNI || status.HostLegacyRouting != tt.wantLegacy {
				t.Fatalf("status = %+v, want CNI %q legacy=%t", status, tt.wantCNI, tt.wantLegacy)
			}
		})
	}
}

func TestWarnIfCiliumMayBypassNetd(t *testing.T) {
	client := fake.NewSimpleClientset(cniConfigMap(terwayConfigMapName, map[string]string{
		terwayCNIConfigKey: `{"type":"terway","network_policy_provider":"ebpf","eniip_virtual_type":"datapathv2"}`,
	}))
	core, logs := observer.New(zap.WarnLevel)

	warnIfCiliumMayBypassNetd(context.Background(), client, "sandbox-node-a", zap.New(core))

	entries := logs.All()
	if len(entries) != 1 {
		t.Fatalf("warning count = %d, want 1", len(entries))
	}
	if entries[0].Message != "Cilium is configured without host legacy routing; netd iptables interception may be bypassed" {
		t.Fatalf("warning message = %q", entries[0].Message)
	}
	fields := entries[0].ContextMap()
	if fields["node"] != "sandbox-node-a" || fields["cni"] != "terway-cilium" {
		t.Fatalf("warning fields = %#v", fields)
	}
	if fields["config_map"] != "kube-system/eni-config" || fields["host_legacy_routing"] != false {
		t.Fatalf("routing fields = %#v", fields)
	}
	if fields["required_setting"] != "enable-host-legacy-routing=true" {
		t.Fatalf("required_setting = %v", fields["required_setting"])
	}
	if fields["impact"] != "sandbox network policy and audit may be incomplete" {
		t.Fatalf("impact = %v", fields["impact"])
	}
}

func TestWarnIfCiliumMayBypassNetdStaysQuietForHostLegacyRouting(t *testing.T) {
	client := fake.NewSimpleClientset(cniConfigMap(ciliumConfigMapName, map[string]string{
		ciliumHostLegacyRoutingKey: "true",
	}))
	core, logs := observer.New(zap.WarnLevel)

	warnIfCiliumMayBypassNetd(context.Background(), client, "sandbox-node-a", zap.New(core))

	if logs.Len() != 0 {
		t.Fatalf("warning count = %d, want 0", logs.Len())
	}
}

func TestParseCiliumBoolArgUsesLastValue(t *testing.T) {
	value, found, err := parseCiliumBoolArg(
		"--enable-host-legacy-routing=false--debug=true --enable-host-legacy-routing=true",
		ciliumHostLegacyRoutingKey,
	)
	if err != nil {
		t.Fatalf("parse Cilium bool argument: %v", err)
	}
	if !found || !value {
		t.Fatalf("value=%t found=%t, want true true", value, found)
	}
}

func cniConfigMap(name string, data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cniConfigNamespace},
		Data:       data,
	}
}
