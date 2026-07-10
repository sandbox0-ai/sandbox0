package cases

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
	"github.com/sandbox0-ai/sandbox0/pkg/framework"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	e2eutils "github.com/sandbox0-ai/sandbox0/tests/e2e/utils"
	corev1 "k8s.io/api/core/v1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func nodeFSRecoveryEnabled(env *framework.ScenarioEnv) (bool, error) {
	args, err := framework.KubectlGetJSONPath(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		env.Infra.Namespace,
		"daemonset",
		env.Infra.Name+"-ctld",
		`{.spec.template.spec.containers[?(@.name=="ctld")].args}`,
	)
	if err != nil {
		return false, err
	}
	return strings.Contains(args, "-nodefs-shards=") &&
		strings.Contains(args, "-nodefs-require-recovery=true"), nil
}

func assertNodeFSRecoveryAcrossCtldRestart(env *framework.ScenarioEnv, session *e2eutils.Session) {
	volume, status, err := session.CreateSandboxVolume(
		env.TestCtx.Context,
		GinkgoT(),
		apispec.CreateSandboxVolumeRequest{},
	)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusCreated))
	Expect(volume).NotTo(BeNil())
	volumeID := expectStringPtr(volume.Id, "nodefs recovery volume id")
	DeferCleanup(func() {
		deleteSandboxVolumeForCleanup(env, session, volumeID)
	})

	mountPoint := "/workspace/nodefs-recovery"
	templateID := createVolumePortalTemplate(env, session, mountPoint)
	template, err := session.GetTemplate(env.TestCtx.Context, GinkgoT(), templateID)
	Expect(err).NotTo(HaveOccurred())
	templateNamespace, err := naming.TemplateNamespaceForTeam(expectStringPtr(template.TeamId, "team id"))
	Expect(err).NotTo(HaveOccurred())

	claim, err := session.ClaimSandboxWithRequest(
		env.TestCtx.Context,
		GinkgoT(),
		apispec.ClaimRequest{
			Template: &templateID,
			Mounts: &[]apispec.ClaimMountRequest{{
				SandboxvolumeId: volumeID,
				MountPoint:      mountPoint,
			}},
		},
	)
	Expect(err).NotTo(HaveOccurred())
	Expect(claim).NotTo(BeNil())
	sandboxID := claim.SandboxId
	DeferCleanup(func() {
		_ = session.DeleteSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
	})

	sandbox := waitForSandboxPodReadyEventually(env, session, sandboxID, templateNamespace)
	podUID, err := framework.KubectlGetJSONPath(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		templateNamespace,
		"pod",
		sandbox.PodName,
		"{.metadata.uid}",
	)
	Expect(err).NotTo(HaveOccurred())
	nodeName, err := framework.KubectlGetJSONPath(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		templateNamespace,
		"pod",
		sandbox.PodName,
		"{.spec.nodeName}",
	)
	Expect(err).NotTo(HaveOccurred())
	Expect(podUID).NotTo(BeEmpty())
	Expect(nodeName).NotTo(BeEmpty())

	rootDir := "/tmp/ctld-nodefs-recovery-e2e"
	volumeDir := mountPoint + "/ctld-nodefs-recovery-e2e"
	mutationScript := fmt.Sprintf(`set -eu
rm -rf %[1]s %[2]s
mkdir -p %[1]s %[2]s
printf ready > %[1]s/ready
printf ready > %[2]s/ready
nohup /bin/sh -c '
set -eu
i=0
while [ "$i" -lt 2000 ]; do
    value=$(printf "iteration=%%08d" "$i")
    printf "%%s" "$value" > %[1]s/state.tmp
    mv %[1]s/state.tmp %[1]s/state
    test "$(cat %[1]s/state)" = "$value"
    printf "%%s" "$value" > %[2]s/state.tmp
    mv %[2]s/state.tmp %[2]s/state
    test "$(cat %[2]s/state)" = "$value"
    i=$((i + 1))
    sleep 0.01
done
sync
printf done > %[1]s/done
printf done > %[2]s/done
' > %[1]s/mutation.log 2>&1 &
`, rootDir, volumeDir)
	_, err = execInSandboxPod(env, templateNamespace, sandbox.PodName, mutationScript)
	Expect(err).NotTo(HaveOccurred())

	Eventually(func() error {
		_, execErr := execInSandboxPod(
			env,
			templateNamespace,
			sandbox.PodName,
			fmt.Sprintf("test -s %s/state && test -s %s/state", shellQuote(rootDir), shellQuote(volumeDir)),
		)
		return execErr
	}).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())

	oldCtld, err := readyCtldPodOnNode(env, nodeName, "")
	Expect(err).NotTo(HaveOccurred())
	Expect(oldCtld).NotTo(BeNil())
	Expect(framework.Kubectl(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		"delete",
		"pod",
		oldCtld.Name,
		"--namespace",
		env.Infra.Namespace,
		"--grace-period=0",
		"--force",
		"--wait=false",
	)).To(Succeed())

	var newCtld *nodeFSRecoveryPod
	Eventually(func() error {
		var lookupErr error
		newCtld, lookupErr = readyCtldPodOnNode(env, nodeName, oldCtld.UID)
		return lookupErr
	}).WithTimeout(4 * time.Minute).WithPolling(2 * time.Second).Should(Succeed())
	Expect(newCtld).NotTo(BeNil())
	Expect(newCtld.UID).NotTo(Equal(oldCtld.UID))

	currentUID, err := framework.KubectlGetJSONPath(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		templateNamespace,
		"pod",
		sandbox.PodName,
		"{.metadata.uid}",
	)
	Expect(err).NotTo(HaveOccurred())
	Expect(currentUID).To(Equal(podUID))
	Expect(framework.KubectlWaitForCondition(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		templateNamespace,
		"pod",
		sandbox.PodName,
		"Ready",
		"2m",
	)).To(Succeed())

	Eventually(func() (string, error) {
		return execInSandboxPod(
			env,
			templateNamespace,
			sandbox.PodName,
			fmt.Sprintf(
				"test -f %s/done; test -f %s/done; printf '%%s %%s' \"$(cat %s/state)\" \"$(cat %s/state)\"",
				shellQuote(rootDir),
				shellQuote(volumeDir),
				shellQuote(rootDir),
				shellQuote(volumeDir),
			),
		)
	}).WithTimeout(4 * time.Minute).WithPolling(2 * time.Second).Should(Equal("iteration=00001999 iteration=00001999"))

	expected := []byte("iteration=00001999")
	Eventually(func() ([]byte, error) {
		body, _, readErr := session.ReadFile(
			env.TestCtx.Context,
			GinkgoT(),
			sandboxID,
			rootDir+"/state",
		)
		return body, readErr
	}).WithTimeout(time.Minute).WithPolling(2 * time.Second).Should(Equal(expected))
	Eventually(func() ([]byte, error) {
		body, _, readErr := session.ReadVolumeFile(
			env.TestCtx.Context,
			GinkgoT(),
			volumeID,
			"/ctld-nodefs-recovery-e2e/state",
		)
		return body, readErr
	}).WithTimeout(time.Minute).WithPolling(2 * time.Second).Should(Equal(expected))
}

type nodeFSRecoveryPod struct {
	Name string
	UID  string
}

func readyCtldPodOnNode(env *framework.ScenarioEnv, nodeName, excludedUID string) (*nodeFSRecoveryPod, error) {
	output, err := framework.KubectlOutput(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		"get",
		"pods",
		"--namespace",
		env.Infra.Namespace,
		"--selector",
		fmt.Sprintf("app.kubernetes.io/name=ctld,app.kubernetes.io/instance=%s", env.Infra.Name),
		"--field-selector",
		"spec.nodeName="+nodeName,
		"-o",
		"json",
	)
	if err != nil {
		return nil, err
	}
	var pods corev1.PodList
	if err := json.Unmarshal([]byte(output), &pods); err != nil {
		return nil, fmt.Errorf("decode ctld pod list: %w", err)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if string(pod.UID) == excludedUID || pod.DeletionTimestamp != nil || !podReady(pod) {
			continue
		}
		return &nodeFSRecoveryPod{Name: pod.Name, UID: string(pod.UID)}, nil
	}
	return nil, fmt.Errorf("ready ctld pod on node %s was not found", nodeName)
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
