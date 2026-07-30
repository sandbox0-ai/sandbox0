package cases

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	mgrv1alpha1 "github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
	"github.com/sandbox0-ai/sandbox0/pkg/framework"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	e2eutils "github.com/sandbox0-ai/sandbox0/tests/e2e/utils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	runtimeFailureStabilityWindow = 100 * time.Second
	runtimeRecoveryPolling        = 2 * time.Second
)

func assertRuntimeActivationFailureRecovery(env *framework.ScenarioEnv, session *e2eutils.Session) {
	namespace, err := naming.TemplateNamespaceForBuiltin("default")
	Expect(err).NotTo(HaveOccurred())
	waitForReadyDefaultIdlePod(env, namespace)

	claim := claimSandboxEventually(env, session, "default")
	DeferCleanup(func() {
		_ = session.DeleteSandbox(env.TestCtx.Context, GinkgoT(), claim.SandboxId)
	})

	sandbox := waitForSandboxPodReadyEventually(env, session, claim.SandboxId, namespace)
	before := getE2EPod(env, namespace, sandbox.PodName)
	assertRuntimePodReady(before)
	waitForReadyDefaultIdlePod(env, namespace)

	revision := before.Metadata.Annotations[runtimecontrol.AnnotationAssignmentRevision]
	Expect(revision).NotTo(BeEmpty())
	Expect(before.Metadata.Annotations[runtimecontrol.AnnotationAssignmentReady]).To(Equal(revision))
	generation := before.Metadata.Annotations[runtimecontrol.AnnotationRuntimeGeneration]

	Expect(overwritePodAnnotation(
		env,
		namespace,
		before.Metadata.Name,
		runtimecontrol.AnnotationAssignmentReady,
		"invalid-revision",
	)).To(Succeed())
	restored := false
	DeferCleanup(func() {
		if !restored {
			_ = overwritePodAnnotation(
				env,
				namespace,
				before.Metadata.Name,
				runtimecontrol.AnnotationAssignmentReady,
				revision,
			)
		}
	})

	Eventually(func() error {
		current := getE2EPod(env, namespace, before.Metadata.Name)
		if !podHasCondition(current, string(mgrv1alpha1.SandboxPodReadinessConditionType), "False") {
			return fmt.Errorf("runtime readiness has not failed closed")
		}
		if !podHasCondition(current, string(mgrv1alpha1.SandboxPodLivenessConditionType), "True") {
			return fmt.Errorf("live procd must not become a crash-recovery candidate")
		}
		return nil
	}).WithTimeout(30 * time.Second).WithPolling(runtimeRecoveryPolling).Should(Succeed())

	Consistently(func() error {
		current := getE2EPod(env, namespace, before.Metadata.Name)
		if current.Metadata.UID != before.Metadata.UID || current.Metadata.DeletionTimestamp != nil {
			return fmt.Errorf("runtime pod was reconstructed")
		}
		if current.Metadata.Annotations[runtimecontrol.AnnotationRuntimeGeneration] != generation {
			return fmt.Errorf("runtime generation changed")
		}
		if !podHasCondition(current, string(mgrv1alpha1.SandboxPodReadinessConditionType), "False") {
			return fmt.Errorf("invalid activation became routable")
		}
		if !podHasCondition(current, string(mgrv1alpha1.SandboxPodLivenessConditionType), "True") {
			return fmt.Errorf("activation failure changed liveness")
		}
		if readyDefaultIdlePodCount(env, namespace) < 1 {
			return fmt.Errorf("idle pool was consumed by recovery")
		}
		return nil
	}).WithTimeout(runtimeFailureStabilityWindow).WithPolling(5 * time.Second).Should(Succeed())

	Expect(overwritePodAnnotation(
		env,
		namespace,
		before.Metadata.Name,
		runtimecontrol.AnnotationAssignmentReady,
		revision,
	)).To(Succeed())
	restored = true

	Eventually(func() error {
		current := getE2EPod(env, namespace, before.Metadata.Name)
		if current.Metadata.UID != before.Metadata.UID {
			return fmt.Errorf("runtime pod changed while correcting its manifest")
		}
		return runtimePodReadyError(current)
	}).WithTimeout(2 * time.Minute).WithPolling(runtimeRecoveryPolling).Should(Succeed())

	current, status, err := session.GetSandbox(env.TestCtx.Context, GinkgoT(), claim.SandboxId)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(current.Status).To(Equal(apispec.SandboxLifecycleStatusRunning))
	Expect(current.PodName).To(Equal(before.Metadata.Name))
}

func assertRuntimeControlCtldFailover(env *framework.ScenarioEnv, session *e2eutils.Session) {
	namespace, err := naming.TemplateNamespaceForBuiltin("default")
	Expect(err).NotTo(HaveOccurred())
	waitForReadyDefaultIdlePod(env, namespace)

	claim := claimSandboxEventually(env, session, "default")
	DeferCleanup(func() {
		_ = session.DeleteSandbox(env.TestCtx.Context, GinkgoT(), claim.SandboxId)
	})

	sandbox := waitForSandboxPodReadyEventually(env, session, claim.SandboxId, namespace)
	before := getE2EPod(env, namespace, sandbox.PodName)
	assertRuntimePodReady(before)
	waitForReadyDefaultIdlePod(env, namespace)

	readyTransition := podCondition(before, string(mgrv1alpha1.SandboxPodReadinessConditionType)).LastTransitionTime
	primary := currentCtldPrimaryForNode(env, before.Spec.NodeName)
	Expect(primary).NotTo(BeEmpty())
	Expect(framework.Kubectl(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		"delete", "pod", primary,
		"--namespace", env.Infra.Namespace,
		"--wait=false",
	)).To(Succeed())

	Eventually(func() error {
		output, getErr := framework.KubectlOutput(
			env.TestCtx.Context,
			env.Config.Kubeconfig,
			"get", "pod", primary,
			"--namespace", env.Infra.Namespace,
			"--ignore-not-found=true",
			"-o", "name",
		)
		if getErr != nil {
			return getErr
		}
		if strings.TrimSpace(output) != "" {
			return fmt.Errorf("old ctld primary still exists")
		}
		return nil
	}).WithTimeout(2 * time.Minute).WithPolling(runtimeRecoveryPolling).Should(Succeed())

	Eventually(func() error {
		current := getE2EPod(env, namespace, before.Metadata.Name)
		if current.Metadata.UID != before.Metadata.UID {
			return fmt.Errorf("sandbox runtime was reconstructed during ctld failover")
		}
		if err := runtimePodReadyError(current); err != nil {
			return err
		}
		transition := podCondition(current, string(mgrv1alpha1.SandboxPodReadinessConditionType)).LastTransitionTime
		if !transition.After(readyTransition.Time) {
			return fmt.Errorf("runtime readiness did not pass through reconnect")
		}
		return nil
	}).WithTimeout(2 * time.Minute).WithPolling(runtimeRecoveryPolling).Should(Succeed())

	waitForTwoReadyCtldSlots(env, before.Spec.NodeName)
	Expect(readyDefaultIdlePodCount(env, namespace)).To(BeNumerically(">=", 1))

	current, status, err := session.GetSandbox(env.TestCtx.Context, GinkgoT(), claim.SandboxId)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(current.Status).To(Equal(apispec.SandboxLifecycleStatusRunning))
	Expect(current.PodName).To(Equal(before.Metadata.Name))
}

func assertTerminatedProcdAutonomousRecovery(env *framework.ScenarioEnv, session *e2eutils.Session) {
	namespace, err := naming.TemplateNamespaceForBuiltin("default")
	Expect(err).NotTo(HaveOccurred())
	waitForReadyDefaultIdlePod(env, namespace)

	claim := claimSandboxEventually(env, session, "default")
	DeferCleanup(func() {
		_ = session.DeleteSandbox(env.TestCtx.Context, GinkgoT(), claim.SandboxId)
	})

	sandbox := waitForSandboxPodReadyEventually(env, session, claim.SandboxId, namespace)
	before := getE2EPod(env, namespace, sandbox.PodName)
	assertRuntimePodReady(before)
	waitForReadyDefaultIdlePod(env, namespace)

	oldGeneration, err := strconv.ParseInt(
		before.Metadata.Annotations[runtimecontrol.AnnotationRuntimeGeneration],
		10,
		64,
	)
	Expect(err).NotTo(HaveOccurred())

	markerPath := fmt.Sprintf("/root/runtime-recovery-%d", time.Now().UnixNano())
	_, err = execInSandboxPod(env, namespace, before.Metadata.Name, "printf recovered > "+shellQuote(markerPath)+"; sync")
	Expect(err).NotTo(HaveOccurred())

	_, _ = framework.KubectlExecContainerOutput(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		namespace,
		before.Metadata.Name,
		"procd",
		"/bin/sh", "-lc", "kill -KILL 1",
	)

	var recovered e2ePod
	Eventually(func() error {
		pods, listErr := listE2EPods(env, namespace, "")
		if listErr != nil {
			return listErr
		}
		for _, pod := range pods.Items {
			if pod.Metadata.Name == before.Metadata.Name ||
				pod.Metadata.DeletionTimestamp != nil ||
				pod.Metadata.Annotations[runtimecontrol.AnnotationSandboxID] != claim.SandboxId {
				continue
			}
			generation, parseErr := strconv.ParseInt(
				pod.Metadata.Annotations[runtimecontrol.AnnotationRuntimeGeneration],
				10,
				64,
			)
			if parseErr != nil || generation != oldGeneration+1 {
				continue
			}
			if err := runtimePodReadyError(pod); err != nil {
				return err
			}
			recovered = pod
			return nil
		}
		return fmt.Errorf("replacement runtime is not ready")
	}).WithTimeout(8 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())

	output, err := execInSandboxPod(env, namespace, recovered.Metadata.Name, "cat "+shellQuote(markerPath))
	Expect(err).NotTo(HaveOccurred())
	Expect(strings.TrimSpace(output)).To(Equal("recovered"))

	Eventually(func() error {
		if readyDefaultIdlePodCount(env, namespace) < 1 {
			return fmt.Errorf("idle pool has not replenished")
		}
		current := getE2EPod(env, namespace, recovered.Metadata.Name)
		if current.Metadata.UID != recovered.Metadata.UID {
			return fmt.Errorf("replacement runtime changed again")
		}
		return runtimePodReadyError(current)
	}).WithTimeout(3 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())

	Consistently(func() error {
		current := getE2EPod(env, namespace, recovered.Metadata.Name)
		if current.Metadata.UID != recovered.Metadata.UID || current.Metadata.DeletionTimestamp != nil {
			return fmt.Errorf("replacement runtime was reconstructed again")
		}
		if readyDefaultIdlePodCount(env, namespace) < 1 {
			return fmt.Errorf("idle pool was consumed after recovery")
		}
		return runtimePodReadyError(current)
	}).WithTimeout(40 * time.Second).WithPolling(5 * time.Second).Should(Succeed())

	current, status, err := session.GetSandbox(env.TestCtx.Context, GinkgoT(), claim.SandboxId)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(current.Status).To(Equal(apispec.SandboxLifecycleStatusRunning))
	Expect(current.PodName).To(Equal(recovered.Metadata.Name))
	Expect(current.RuntimeGeneration).To(Equal(oldGeneration + 1))
}

func getE2EPod(env *framework.ScenarioEnv, namespace, name string) e2ePod {
	output, err := framework.KubectlOutput(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		"get", "pod", name,
		"--namespace", namespace,
		"-o", "json",
	)
	Expect(err).NotTo(HaveOccurred())
	var pod e2ePod
	Expect(json.Unmarshal([]byte(output), &pod)).To(Succeed())
	return pod
}

func listE2EPods(env *framework.ScenarioEnv, namespace, selector string) (e2ePodList, error) {
	args := []string{"get", "pods", "--namespace", namespace, "-o", "json"}
	if selector != "" {
		args = append(args, "--selector", selector)
	}
	output, err := framework.KubectlOutput(env.TestCtx.Context, env.Config.Kubeconfig, args...)
	if err != nil {
		return e2ePodList{}, err
	}
	var pods e2ePodList
	if err := json.Unmarshal([]byte(output), &pods); err != nil {
		return e2ePodList{}, err
	}
	return pods, nil
}

func overwritePodAnnotation(env *framework.ScenarioEnv, namespace, podName, key, value string) error {
	return framework.Kubectl(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		"annotate", "pod", podName,
		"--namespace", namespace,
		key+"="+value,
		"--overwrite",
	)
}

func assertRuntimePodReady(pod e2ePod) {
	Expect(runtimePodReadyError(pod)).NotTo(HaveOccurred())
}

func runtimePodReadyError(pod e2ePod) error {
	if pod.Metadata.DeletionTimestamp != nil || pod.Status.Phase != "Running" {
		return fmt.Errorf("runtime pod is not running")
	}
	revision := strings.TrimSpace(pod.Metadata.Annotations[runtimecontrol.AnnotationAssignmentRevision])
	if revision == "" ||
		pod.Metadata.Annotations[runtimecontrol.AnnotationAssignmentReady] != revision ||
		pod.Metadata.Annotations[runtimecontrol.AnnotationObservedRevision] != revision ||
		pod.Metadata.Annotations[runtimecontrol.AnnotationObservedState] != string(runtimecontrol.ObservedReady) {
		return fmt.Errorf("runtime assignment is not fully observed")
	}
	if pod.Metadata.Annotations[runtimecontrol.AnnotationObservedGeneration] !=
		pod.Metadata.Annotations[runtimecontrol.AnnotationRuntimeGeneration] {
		return fmt.Errorf("runtime observation generation does not match")
	}
	if !podHasCondition(pod, string(mgrv1alpha1.SandboxPodReadinessConditionType), "True") {
		return fmt.Errorf("runtime readiness condition is not true")
	}
	return nil
}

func podCondition(pod e2ePod, conditionType string) e2ePodCondition {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == conditionType {
			return condition
		}
	}
	return e2ePodCondition{}
}

func readyDefaultIdlePodCount(env *framework.ScenarioEnv, namespace string) int {
	pods, err := listE2EPods(
		env,
		namespace,
		"sandbox0.ai/template-id=default,sandbox0.ai/pool-type=idle",
	)
	Expect(err).NotTo(HaveOccurred())
	count := 0
	for _, pod := range pods.Items {
		if pod.Metadata.DeletionTimestamp == nil &&
			podHasCondition(pod, string(mgrv1alpha1.SandboxPodReadinessConditionType), "True") {
			count++
		}
	}
	return count
}

func waitForReadyDefaultIdlePod(env *framework.ScenarioEnv, namespace string) {
	Eventually(func() int {
		return readyDefaultIdlePodCount(env, namespace)
	}).WithTimeout(3 * time.Minute).WithPolling(5 * time.Second).Should(BeNumerically(">=", 1))
}

func currentCtldPrimaryForNode(env *framework.ScenarioEnv, nodeName string) string {
	pods, err := listE2EPods(env, env.Infra.Namespace, "app.kubernetes.io/component=ctld")
	Expect(err).NotTo(HaveOccurred())
	for _, pod := range pods.Items {
		if pod.Spec.NodeName != nodeName || pod.Metadata.DeletionTimestamp != nil {
			continue
		}
		logs, logErr := framework.KubectlOutput(
			env.TestCtx.Context,
			env.Config.Kubeconfig,
			"logs", pod.Metadata.Name,
			"--namespace", env.Infra.Namespace,
			"--tail=1000",
		)
		if logErr == nil && strings.Contains(logs, "ctld primary started network runtime") {
			return pod.Metadata.Name
		}
	}
	return ""
}

func waitForTwoReadyCtldSlots(env *framework.ScenarioEnv, nodeName string) {
	Eventually(func() error {
		pods, err := listE2EPods(env, env.Infra.Namespace, "app.kubernetes.io/component=ctld")
		if err != nil {
			return err
		}
		ready := 0
		for _, pod := range pods.Items {
			if pod.Spec.NodeName == nodeName &&
				pod.Metadata.DeletionTimestamp == nil &&
				podHasCondition(pod, "Ready", "True") {
				ready++
			}
		}
		if ready != 2 {
			return fmt.Errorf("ready ctld slots on node = %d, want 2", ready)
		}
		return nil
	}).WithTimeout(3 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())
}
