package api

import (
	"fmt"
	"testing"

	"github.com/sandbox0-ai/infra/tests/e2e/framework"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var (
	cfg     framework.Config
	testCtx *framework.TestContext
)

func TestApi(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "API E2E Suite")
}

var _ = BeforeSuite(func() {
	// var err error

	// cfg, err = framework.LoadConfig()
	// Expect(err).NotTo(HaveOccurred())

	// cluster := framework.NewCluster(cfg.ClusterName)
	// testCtx = framework.NewTestContext(cluster)

	// lockRelease, err := framework.AcquireE2ELock(testCtx.Context, cfg.ClusterName, 0)
	// Expect(err).NotTo(HaveOccurred())
	// DeferCleanup(lockRelease)

	// if !cfg.UseExistingCluster {
	// 	fmt.Printf("Setting up Kind cluster %q...\n", cfg.ClusterName)
	// 	err = cluster.CreateKind(testCtx.Context, cfg.KindConfigPath)
	// 	Expect(err).NotTo(HaveOccurred())

	// 	err = cluster.LoadDockerImage(testCtx.Context, "sandbox0ai/infra:latest")
	// 	Expect(err).NotTo(HaveOccurred())

	// 	if !cfg.SkipClusterDelete {
	// 		DeferCleanup(func() {
	// 			fmt.Printf("Deleting Kind cluster %q...\n", cfg.ClusterName)
	// 			_ = testCtx.Cluster.DeleteKind(testCtx.Context)
	// 		})
	// 	}
	// }

	// if cfg.Kubeconfig == "" {
	// 	kubeconfig, err := cluster.ExportKubeconfig(testCtx.Context)
	// 	Expect(err).NotTo(HaveOccurred())
	// 	cfg.Kubeconfig = kubeconfig
	// 	DeferCleanup(func() {
	// 		fmt.Printf("Cleaning up temporary kubeconfig %q...\n", kubeconfig)
	// 		_ = os.Remove(kubeconfig)
	// 	})
	// }

	// if !cfg.SkipOperatorInstall {
	// 	fmt.Printf("Installing infra-operator...\n")
	// 	err = framework.InstallOperator(testCtx.Context, cfg)
	// 	Expect(err).NotTo(HaveOccurred())

	// 	err = framework.WaitForOperatorReady(testCtx.Context, cfg, "5m")
	// 	Expect(err).NotTo(HaveOccurred())

	// 	if !cfg.SkipOperatorUninstall {
	// 		DeferCleanup(func() {
	// 			fmt.Printf("Uninstalling infra-operator...\n")
	// 			_ = framework.UninstallOperator(testCtx.Context, cfg)
	// 			time.Sleep(2 * time.Second)
	// 		})
	// 	}
	// }
})

var _ = AfterSuite(func() {
	fmt.Printf("API suite teardown completed.\n")
})
