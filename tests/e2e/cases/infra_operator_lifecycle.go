package cases

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/framework"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// RegisterInfraOperatorLifecycleSuite validates lifecycle transitions after the
// normal sandbox0 operator and API scenarios have completed.
func RegisterInfraOperatorLifecycleSuite(envProvider func() *framework.ScenarioEnv) {
	Describe("Infra operator lifecycle transitions", Ordered, func() {
		It("disables builtin registry and retains the builtin registry PVC", func() {
			env := shouldRunApiScenario(envProvider, "fullmode")

			ctx := env.TestCtx.Context
			kubeconfig := env.Config.Kubeconfig
			namespace := env.Infra.Namespace
			infraName := env.Infra.Name

			Expect(framework.Kubectl(ctx, kubeconfig, "get", "deployment", infraName+"-registry", "--namespace", namespace)).To(Succeed())
			Expect(framework.Kubectl(ctx, kubeconfig, "get", "service", infraName+"-registry", "--namespace", namespace)).To(Succeed())
			Expect(framework.Kubectl(ctx, kubeconfig, "get", "secret", infraName+"-registry-auth", "--namespace", namespace)).To(Succeed())
			Expect(framework.Kubectl(ctx, kubeconfig, "get", "secret", infraName+"-registry-pull", "--namespace", namespace)).To(Succeed())
			Expect(framework.Kubectl(ctx, kubeconfig, "get", "pvc", infraName+"-registry-data", "--namespace", namespace)).To(Succeed())

			patch := `{"spec":{"registry":{"provider":"builtin","builtin":{"enabled":false,"statefulResourcePolicy":"Retain"}}}}`
			Expect(framework.KubectlPatch(ctx, kubeconfig, namespace, "sandbox0infra", infraName, patch)).To(Succeed())

			expectResourceAbsentEventually(ctx, kubeconfig, namespace, "deployment", infraName+"-registry")
			expectResourceAbsentEventually(ctx, kubeconfig, namespace, "service", infraName+"-registry")
			expectResourceAbsentEventually(ctx, kubeconfig, namespace, "secret", infraName+"-registry-auth")
			expectResourceAbsentEventually(ctx, kubeconfig, namespace, "secret", infraName+"-registry-pull")
			expectResourcePresentEventually(ctx, kubeconfig, namespace, "pvc", infraName+"-registry-data")

			expectRetainedResourcesEventually(ctx, kubeconfig, namespace, infraName, []retainedResourceExpectation{
				{Component: "registry", Kind: "PersistentVolumeClaim", Name: infraName + "-registry-data"},
			})
		})

		It("deletes the retained builtin registry PVC when policy changes to Delete", func() {
			env := shouldRunApiScenario(envProvider, "fullmode")

			ctx := env.TestCtx.Context
			kubeconfig := env.Config.Kubeconfig
			namespace := env.Infra.Namespace
			infraName := env.Infra.Name

			Expect(framework.Kubectl(ctx, kubeconfig, "get", "pvc", infraName+"-registry-data", "--namespace", namespace)).To(Succeed())

			patch := `{"spec":{"registry":{"provider":"builtin","builtin":{"enabled":false,"statefulResourcePolicy":"Delete"}}}}`
			Expect(framework.KubectlPatch(ctx, kubeconfig, namespace, "sandbox0infra", infraName, patch)).To(Succeed())

			expectResourceAbsentEventually(ctx, kubeconfig, namespace, "pvc", infraName+"-registry-data")
			expectRetainedResourcesEventually(ctx, kubeconfig, namespace, infraName, nil)
		})

		It("disables builtin storage and retains builtin storage stateful resources", func() {
			env := shouldRunApiScenario(envProvider, "volumes")

			ctx := env.TestCtx.Context
			kubeconfig := env.Config.Kubeconfig
			namespace := env.Infra.Namespace
			infraName := env.Infra.Name

			Expect(framework.Kubectl(ctx, kubeconfig, "get", "statefulset", infraName+"-rustfs", "--namespace", namespace)).To(Succeed())
			Expect(framework.Kubectl(ctx, kubeconfig, "get", "service", infraName+"-rustfs", "--namespace", namespace)).To(Succeed())
			Expect(framework.Kubectl(ctx, kubeconfig, "get", "secret", infraName+"-sandbox0-rustfs-credentials", "--namespace", namespace)).To(Succeed())
			Expect(framework.Kubectl(ctx, kubeconfig, "get", "pvc", infraName+"-rustfs-data", "--namespace", namespace)).To(Succeed())

			patch := `{"spec":{"storage":{"type":"builtin","builtin":{"enabled":false,"statefulResourcePolicy":"Retain"}}}}`
			Expect(framework.KubectlPatch(ctx, kubeconfig, namespace, "sandbox0infra", infraName, patch)).To(Succeed())

			expectResourceAbsentEventually(ctx, kubeconfig, namespace, "statefulset", infraName+"-rustfs")
			expectResourceAbsentEventually(ctx, kubeconfig, namespace, "service", infraName+"-rustfs")
			expectResourcePresentEventually(ctx, kubeconfig, namespace, "secret", infraName+"-sandbox0-rustfs-credentials")
			expectResourcePresentEventually(ctx, kubeconfig, namespace, "pvc", infraName+"-rustfs-data")

			expectRetainedResourcesEventually(ctx, kubeconfig, namespace, infraName, []retainedResourceExpectation{
				{Component: "storage", Kind: "Secret", Name: infraName + "-sandbox0-rustfs-credentials"},
				{Component: "storage", Kind: "PersistentVolumeClaim", Name: infraName + "-rustfs-data"},
			})
		})

		It("deletes retained builtin storage stateful resources when policy changes to Delete", func() {
			env := shouldRunApiScenario(envProvider, "volumes")

			ctx := env.TestCtx.Context
			kubeconfig := env.Config.Kubeconfig
			namespace := env.Infra.Namespace
			infraName := env.Infra.Name

			Expect(framework.Kubectl(ctx, kubeconfig, "get", "secret", infraName+"-sandbox0-rustfs-credentials", "--namespace", namespace)).To(Succeed())
			Expect(framework.Kubectl(ctx, kubeconfig, "get", "pvc", infraName+"-rustfs-data", "--namespace", namespace)).To(Succeed())

			patch := `{"spec":{"storage":{"type":"builtin","builtin":{"enabled":false,"statefulResourcePolicy":"Delete"}}}}`
			Expect(framework.KubectlPatch(ctx, kubeconfig, namespace, "sandbox0infra", infraName, patch)).To(Succeed())

			expectResourceAbsentEventually(ctx, kubeconfig, namespace, "secret", infraName+"-sandbox0-rustfs-credentials")
			expectResourceAbsentEventually(ctx, kubeconfig, namespace, "pvc", infraName+"-rustfs-data")
			expectRetainedResourcesEventually(ctx, kubeconfig, namespace, infraName, nil)
		})

		It("disables builtin database and retains builtin database stateful resources", func() {
			env := shouldRunApiScenario(envProvider, "minimal")

			ctx := env.TestCtx.Context
			kubeconfig := env.Config.Kubeconfig
			namespace := env.Infra.Namespace
			infraName := env.Infra.Name

			Expect(framework.Kubectl(ctx, kubeconfig, "get", "statefulset", infraName+"-postgres", "--namespace", namespace)).To(Succeed())
			Expect(framework.Kubectl(ctx, kubeconfig, "get", "service", infraName+"-postgres", "--namespace", namespace)).To(Succeed())
			Expect(framework.Kubectl(ctx, kubeconfig, "get", "secret", infraName+"-sandbox0-database-credentials", "--namespace", namespace)).To(Succeed())
			Expect(framework.Kubectl(ctx, kubeconfig, "get", "pvc", infraName+"-postgres-data", "--namespace", namespace)).To(Succeed())

			patch := `{"spec":{"database":{"type":"builtin","builtin":{"enabled":false,"statefulResourcePolicy":"Retain"}}}}`
			Expect(framework.KubectlPatch(ctx, kubeconfig, namespace, "sandbox0infra", infraName, patch)).To(Succeed())

			expectResourceAbsentEventually(ctx, kubeconfig, namespace, "statefulset", infraName+"-postgres")
			expectResourceAbsentEventually(ctx, kubeconfig, namespace, "service", infraName+"-postgres")
			expectResourcePresentEventually(ctx, kubeconfig, namespace, "secret", infraName+"-sandbox0-database-credentials")
			expectResourcePresentEventually(ctx, kubeconfig, namespace, "pvc", infraName+"-postgres-data")

			expectRetainedResourcesEventually(ctx, kubeconfig, namespace, infraName, []retainedResourceExpectation{
				{Component: "database", Kind: "Secret", Name: infraName + "-sandbox0-database-credentials"},
				{Component: "database", Kind: "PersistentVolumeClaim", Name: infraName + "-postgres-data"},
			})
		})

		It("disables builtin database and deletes builtin database stateful resources", func() {
			env := shouldRunApiScenario(envProvider, "network-policy")

			ctx := env.TestCtx.Context
			kubeconfig := env.Config.Kubeconfig
			namespace := env.Infra.Namespace
			infraName := env.Infra.Name

			Expect(framework.Kubectl(ctx, kubeconfig, "get", "statefulset", infraName+"-postgres", "--namespace", namespace)).To(Succeed())
			Expect(framework.Kubectl(ctx, kubeconfig, "get", "service", infraName+"-postgres", "--namespace", namespace)).To(Succeed())
			Expect(framework.Kubectl(ctx, kubeconfig, "get", "secret", infraName+"-sandbox0-database-credentials", "--namespace", namespace)).To(Succeed())
			Expect(framework.Kubectl(ctx, kubeconfig, "get", "pvc", infraName+"-postgres-data", "--namespace", namespace)).To(Succeed())

			patch := `{"spec":{"database":{"type":"builtin","builtin":{"enabled":false,"statefulResourcePolicy":"Delete"}}}}`
			Expect(framework.KubectlPatch(ctx, kubeconfig, namespace, "sandbox0infra", infraName, patch)).To(Succeed())

			expectResourceAbsentEventually(ctx, kubeconfig, namespace, "statefulset", infraName+"-postgres")
			expectResourceAbsentEventually(ctx, kubeconfig, namespace, "service", infraName+"-postgres")
			expectResourceAbsentEventually(ctx, kubeconfig, namespace, "secret", infraName+"-sandbox0-database-credentials")
			expectResourceAbsentEventually(ctx, kubeconfig, namespace, "pvc", infraName+"-postgres-data")

			expectRetainedResourcesEventually(ctx, kubeconfig, namespace, infraName, nil)
		})
	})
}

type retainedResourceExpectation struct {
	Component string
	Kind      string
	Name      string
}

func fetchSandbox0Infra(ctx context.Context, kubeconfig, namespace, name string) (*infrav1alpha1.Sandbox0Infra, error) {
	output, err := framework.KubectlOutput(ctx, kubeconfig, "get", "sandbox0infra", name, "--namespace", namespace, "-o", "json")
	if err != nil {
		return nil, err
	}
	var infra infrav1alpha1.Sandbox0Infra
	if err := json.Unmarshal([]byte(output), &infra); err != nil {
		return nil, err
	}
	return &infra, nil
}

func kubectlGetMustBeAbsent(ctx context.Context, kubeconfig, namespace, resource, name string) error {
	err := framework.Kubectl(ctx, kubeconfig, "get", resource, name, "--namespace", namespace)
	if err == nil {
		return fmt.Errorf("%s/%s still exists", resource, name)
	}
	if strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "not found") {
		return nil
	}
	return err
}

func expectResourceAbsentEventually(ctx context.Context, kubeconfig, namespace, resource, name string) {
	Eventually(func() error {
		return kubectlGetMustBeAbsent(ctx, kubeconfig, namespace, resource, name)
	}).WithTimeout(2 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())
}

func expectResourcePresentEventually(ctx context.Context, kubeconfig, namespace, resource, name string) {
	Eventually(func() error {
		return framework.Kubectl(ctx, kubeconfig, "get", resource, name, "--namespace", namespace)
	}).WithTimeout(2 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())
}

func expectRetainedResourcesEventually(ctx context.Context, kubeconfig, namespace, infraName string, expected []retainedResourceExpectation) {
	Eventually(func() error {
		infra, err := fetchSandbox0Infra(ctx, kubeconfig, namespace, infraName)
		if err != nil {
			return err
		}
		if len(infra.Status.RetainedResources) != len(expected) {
			return fmt.Errorf("expected %d retained resources, got %d", len(expected), len(infra.Status.RetainedResources))
		}

		actual := make(map[string]struct{}, len(infra.Status.RetainedResources))
		for _, retained := range infra.Status.RetainedResources {
			key := retained.Component + "/" + retained.Kind + "/" + retained.Name
			actual[key] = struct{}{}
		}
		for _, want := range expected {
			key := want.Component + "/" + want.Kind + "/" + want.Name
			if _, ok := actual[key]; !ok {
				return fmt.Errorf("missing retained resource %s in %#v", key, infra.Status.RetainedResources)
			}
		}
		return nil
	}).WithTimeout(2 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())
}
